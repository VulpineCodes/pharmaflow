"""M12 — durable dead-letter history.

A *completed* ``stream-consume`` run appends every poison event it
dead-lettered to the ``dead_letters`` table in the same SQLite
database — errors plus source topic/partition/offset — so poison
events are queryable from the operational store without reading the
broker log. The broker DLQ topic (M4) still receives the record
during the run regardless; the store history answers "which poison
events have we seen, and why?", the broker gauges (M8) answer "what
is waiting in the DLQ right now?", and the consumption history (M11)
answers "what have completed runs done so far?".

The migration test builds a database with the frozen pre-M12 (v3)
schema by hand, so opening it must migrate additively: existing rows
intact, version bumped to 4, dead-letter recording working afterwards.
"""
import json
import sqlite3
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from api.app import create_app
from pharmaflow import SqliteStore
from pharmaflow.__main__ import main as cli_main
from pharmaflow.observability import render_dead_letters_prometheus
from pharmaflow.streaming import (
    DEFAULT_TOPIC,
    DLQ_TOPIC,
    DeadLetter,
    EventProducer,
    FileLogBroker,
    SaleRecorded,
    SimulatedCrashError,
    StockReceived,
    StreamingConsumer,
    consume_once,
)

ROOT = Path(__file__).resolve().parents[1]
GROUP = "reconciliation-v1"
OTHER_GROUP = "alerts-v1"

# Frozen pre-M12 schema (v3): every table up to and including
# stream_consumption_runs, with schema_version recorded as 3. Copied
# from the v3 _SCHEMA so the migration test does not depend on the
# current schema definition.
V3_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_hashes (
    file_hash TEXT PRIMARY KEY,
    imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS row_hashes (
    row_hash TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS imports (
    file_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    kind TEXT NOT NULL,
    rows_in INTEGER NOT NULL DEFAULT 0,
    rows_valid INTEGER NOT NULL DEFAULT 0,
    rows_quarantined INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
    imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    unit_cost REAL NOT NULL DEFAULT 0,
    expiry_date TEXT,
    supplier TEXT NOT NULL DEFAULT '',
    batch TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_date TEXT,
    unit_price REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_number INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    errors_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    action TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_date TEXT NOT NULL,
    succeeded INTEGER NOT NULL,
    failed_task TEXT,
    recorded_at TEXT NOT NULL,
    result_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_consumption_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    records_read INTEGER NOT NULL DEFAULT 0,
    applied INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    dlq INTEGER NOT NULL DEFAULT 0,
    lag_before INTEGER NOT NULL DEFAULT 0,
    lag_after INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL
);
"""


def _make_v3_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(V3_SCHEMA)
    conn.execute(
        "INSERT INTO schema_version (version, applied_at) VALUES (3, '2026-11-02T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO inventory (sku, product_name, quantity_on_hand, reorder_point, unit_cost, expiry_date, supplier, batch)"
        " VALUES ('MED-001', 'Paracetamol 500mg 100 tabs', 120, 20, 4.50, '2027-06-30', '', '')"
    )
    conn.commit()
    conn.close()


def _seed_events_with_poison(producer: EventProducer) -> None:
    """2 valid events + 2 poison events, all keyed to MED-001 so
    publish order is consume order (one partition)."""
    producer.publish(StockReceived(event_id="evt-1", sku="MED-001",
                                   product_name="Paracetamol 500mg 100 tabs",
                                   quantity=100, reorder_point=20, unit_cost=4.50))
    producer.publish_raw(
        {"event_type": "SaleRecorded", "event_id": "evt-poison-1",
         "sale_id": "S-9", "sku": "MED-001", "quantity_sold": -2,
         "unit_price": 6.00},
        key="MED-001")
    producer.publish(SaleRecorded(event_id="evt-3", sale_id="S-1", sku="MED-001",
                                  quantity_sold=10, unit_price=6.00))
    producer.publish_raw(
        {"event_type": "BogusType", "event_id": "evt-poison-2",
         "sku": "MED-001"},
        key="MED-001")


def _dlq_broker_records(broker: FileLogBroker) -> int:
    return sum(broker.end_offset(DLQ_TOPIC, p) for p in range(broker.num_partitions))


def test_consume_once_records_dead_letters(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events_with_poison(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 4 and result.applied == 2
    assert result.dlq == 2 and result.record_id == 1
    assert len(result.dead_letters) == 2
    assert result.dead_letter_ids == [1, 2]
    # The run's in-memory dead letters carry errors + coordinates.
    first, second = result.dead_letters
    assert first.event_id == "evt-poison-1"
    assert any("quantity_sold must be >= 1" in e for e in first.errors)
    assert first.source_topic == DEFAULT_TOPIC
    assert first.source_partition == second.source_partition
    assert second.event_id == "evt-poison-2"
    assert any("unknown or missing event_type" in e for e in second.errors)
    # The broker DLQ topic still receives the records (M4 behaviour).
    assert _dlq_broker_records(FileLogBroker(log_dir)) == 2
    # A fresh store instance on the same file sees the recorded rows.
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        assert store.count_dead_letters() == 2
        assert store.count_stream_consumption() == 1
        letters = store.list_dead_letters()
        assert [r.record_id for r in letters] == [2, 1]  # newest first
        latest = store.latest_dead_letter()
        assert latest is not None and latest.record_id == 2
        assert latest.group == GROUP and latest.event_id == "evt-poison-2"
        assert latest.topic == DEFAULT_TOPIC
        assert latest.partition == second.source_partition
        assert latest.offset == second.source_offset
        assert any("unknown or missing event_type" in e for e in latest.errors)
        assert latest.recorded_at


def test_second_group_records_its_own_dead_letters(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events_with_poison(EventProducer(FileLogBroker(log_dir)))
    consume_once(log_dir, db, group=GROUP)
    result = consume_once(log_dir, db, group=OTHER_GROUP)
    # The second group re-reads the same 4 events: the valid ones are
    # duplicates (dedupe is per-store, M4), the poison ones are
    # dead-lettered again — under the second group's name.
    assert result.applied == 0 and result.duplicates == 2
    assert result.dlq == 2 and result.dead_letter_ids == [3, 4]
    with SqliteStore(db) as store:
        assert store.count_dead_letters() == 4
        assert store.count_dead_letters(group=GROUP) == 2
        assert store.count_dead_letters(group=OTHER_GROUP) == 2
        assert [r.event_id for r in store.list_dead_letters(group=OTHER_GROUP)] == [
            "evt-poison-2", "evt-poison-1"]
        totals = store.dead_letter_totals_by_group()
        assert totals == {GROUP: 2, OTHER_GROUP: 2}


def test_crash_run_records_nothing_then_recovery_records(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    producer.publish_raw(
        {"event_type": "SaleRecorded", "event_id": "evt-poison-1",
         "sale_id": "S-9", "sku": "MED-001", "quantity_sold": -2,
         "unit_price": 6.00},
        key="MED-001")
    producer.publish(StockReceived(event_id="evt-1", sku="MED-001",
                                   product_name="Paracetamol 500mg 100 tabs",
                                   quantity=100, reorder_point=20, unit_cost=4.50))
    producer.publish(SaleRecorded(event_id="evt-3", sale_id="S-1", sku="MED-001",
                                  quantity_sold=10, unit_price=6.00))
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db)
    try:
        consumer = StreamingConsumer(broker, store, group=GROUP)
        with pytest.raises(SimulatedCrashError):
            consumer.consume_until_caught_up(crash_after_applies=1)
        # The interrupted run recorded nothing: the poison event was
        # already dead-lettered to the broker before the crash, but
        # the store history only ever records completed runs.
        assert store.count_dead_letters() == 0
        assert store.count_stream_consumption() == 0
        assert store.inventory["MED-001"].quantity_on_hand == 100
    finally:
        store.close()
    # Recovery: the poison event is re-read and dead-lettered again,
    # and the completed run records it once (plus the redelivery
    # duplicate of the stock receipt).
    result = consume_once(log_dir, db, group=GROUP)
    assert result.record_id == 1
    assert result.records_read == 3 and result.applied == 1
    assert result.duplicates == 1 and result.dlq == 1
    assert result.dead_letter_ids == [1]
    with SqliteStore(db) as store:
        assert store.count_dead_letters() == 1
        assert store.inventory["MED-001"].quantity_on_hand == 90


def test_record_false_skips_dead_letter_history(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events_with_poison(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP, record=False)
    assert result.applied == 2 and result.dlq == 2
    assert result.record_id is None and result.dead_letter_ids == []
    # The run still collected the dead letters in memory (the broker
    # DLQ write happened); only the durable recording was skipped.
    assert len(result.dead_letters) == 2
    with SqliteStore(db) as store:
        assert store.count_dead_letters() == 0
        assert store.count_stream_consumption() == 0
        assert store.latest_dead_letter() is None


def test_v3_database_migrates_additively(tmp_path):
    db = tmp_path / "pharma.db"
    _make_v3_db(db)
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        # Pre-existing data survived the migration untouched.
        assert len(store.inventory) == 1
        assert store.inventory["MED-001"].quantity_on_hand == 120
        assert store.count_dead_letters() == 0
        assert store.count_stream_consumption() == 0
    # Recording works on the migrated database, and reopening stays v4.
    log_dir = tmp_path / "log"
    _seed_events_with_poison(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.record_id == 1
    assert result.dead_letter_ids == [1, 2]
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        assert store.count_dead_letters() == 2
        assert store.count_stream_consumption() == 1


def test_store_api_filtering_and_limits(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as store:
        for i, group in enumerate([GROUP, GROUP, OTHER_GROUP]):
            store.record_dead_letter(DeadLetter(
                group=group, event_id=f"evt-{i}",
                errors=[f"error {i}"], source_topic=DEFAULT_TOPIC,
                source_partition=2, source_offset=i))
        assert store.count_dead_letters() == 3
        assert store.count_dead_letters(group=GROUP) == 2
        assert store.count_dead_letters(group="no-such-group") == 0
        assert [r.record_id for r in store.list_dead_letters(limit=1)] == [3]
        # Limit clamps: 0 behaves as 1, 500 behaves as 100 (returns all 3).
        assert len(store.list_dead_letters(limit=0)) == 1
        assert len(store.list_dead_letters(limit=500)) == 3
        assert [r.event_id for r in store.list_dead_letters(group=GROUP)] == [
            "evt-1", "evt-0"]
        latest = store.latest_dead_letter(group=OTHER_GROUP)
        assert latest is not None and latest.event_id == "evt-2"
        assert latest.errors == ["error 2"]
        assert store.latest_dead_letter(group="no-such-group") is None
        assert store.dead_letter_totals_by_group() == {GROUP: 2, OTHER_GROUP: 1}


def test_malformed_event_without_event_id_is_recorded(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    producer.publish_raw({"event_type": "BogusType", "sku": "MED-001"},
                         key="MED-001")
    result = consume_once(log_dir, db, group=GROUP)
    assert result.dlq == 1 and result.dead_letter_ids == [1]
    assert result.dead_letters[0].event_id == ""
    with SqliteStore(db) as store:
        latest = store.latest_dead_letter()
        assert latest is not None
        assert latest.event_id == ""
        assert any("unknown or missing event_type" in e for e in latest.errors)


def test_render_dead_letters_prometheus():
    totals = {GROUP: 2, OTHER_GROUP: 1}
    text = render_dead_letters_prometheus(totals)
    assert f'pharmaflow_dead_letters_total{{group="{GROUP}"}} 2' in text
    assert f'pharmaflow_dead_letters_total{{group="{OTHER_GROUP}"}} 1' in text
    assert render_dead_letters_prometheus({}) == ""


def test_cli_stream_consume_records_dead_letters_and_lists(tmp_path, capsys):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    events_file = tmp_path / "events.jsonl"
    events_file.write_text("\n".join([
        json.dumps({"event_type": "StockReceived", "event_id": "evt-1",
                    "sku": "MED-001", "product_name": "Paracetamol 500mg 100 tabs",
                    "quantity": 100, "reorder_point": 20, "unit_cost": 4.50}),
        json.dumps({"event_type": "SaleRecorded", "event_id": "evt-poison-1",
                    "sale_id": "S-9", "sku": "MED-001", "quantity_sold": -2,
                    "unit_price": 6.00}),
        json.dumps({"event_type": "SaleRecorded", "event_id": "evt-3",
                    "sale_id": "S-1", "sku": "MED-001", "quantity_sold": 10,
                    "unit_price": 6.00}),
    ]) + "\n", encoding="utf-8")
    assert cli_main(["stream-produce", "--log-dir", str(log_dir),
                     "--events", str(events_file)]) == 0
    capsys.readouterr()
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Recorded as stream consumption #1 (group reconciliation-v1)" in out
    assert "Recorded 1 dead letter(s) (group reconciliation-v1)" in out
    assert "Consumed: read=3 applied=2 skipped_dlq=1" in out
    assert cli_main(["dead-letters", "--db", str(db), "--json"]) == 0
    letters = json.loads(capsys.readouterr().out)
    assert len(letters) == 1
    assert letters[0]["record_id"] == 1 and letters[0]["event_id"] == "evt-poison-1"
    assert any("quantity_sold must be >= 1" in e for e in letters[0]["errors"])
    assert letters[0]["topic"] == DEFAULT_TOPIC
    assert cli_main(["dead-letters", "--db", str(db), "--latest"]) == 0
    out = capsys.readouterr().out
    assert "#1 group=reconciliation-v1 event_id=evt-poison-1" in out
    assert "quantity_sold must be >= 1" in out
    assert cli_main(["dead-letters", "--db", str(db),
                     "--group", "no-such-group"]) == 0
    assert "No dead letters recorded." in capsys.readouterr().out


def test_api_endpoints_and_metrics(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    client = TestClient(create_app(db_path=db))
    # Empty history: list is empty, latest is 404, no dead-letter gauge.
    assert client.get("/dead-letters").json() == []
    assert client.get("/dead-letters/latest").status_code == 404
    assert "pharmaflow_dead_letters_total" not in client.get("/metrics").text
    # After a recorded run: endpoints serve the rows and /metrics
    # carries the per-group gauge.
    _seed_events_with_poison(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.dead_letter_ids == [1, 2]
    letters = client.get("/dead-letters").json()
    assert len(letters) == 2
    assert letters[0]["record_id"] == 2 and letters[0]["event_id"] == "evt-poison-2"
    assert letters[1]["record_id"] == 1 and letters[1]["event_id"] == "evt-poison-1"
    assert any("unknown or missing event_type" in e for e in letters[0]["errors"])
    latest = client.get("/dead-letters/latest").json()
    assert latest["record_id"] == 2 and latest["group"] == GROUP
    assert client.get("/dead-letters",
                      params={"group": "no-such-group"}).json() == []
    assert client.get("/dead-letters/latest",
                      params={"group": "no-such-group"}).status_code == 404
    metrics = client.get("/metrics").text
    assert f'pharmaflow_dead_letters_total{{group="{GROUP}"}} 2' in metrics
    # A second app instance on the same DB serves the same history
    # (the history is durable, not per-process).
    client2 = TestClient(create_app(db_path=db))
    assert len(client2.get("/dead-letters").json()) == 2
    assert f'pharmaflow_dead_letters_total{{group="{GROUP}"}} 2' in client2.get("/metrics").text
