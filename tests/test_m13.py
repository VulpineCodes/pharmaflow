"""M13 — dead-letter replay.

A recorded dead letter can be corrected and re-driven into the
consumer: ``replay_dead_letter`` reads the original record back
from the broker at the dead letter's source coordinates, applies
the caller's field corrections, validates the result through the
same ``parse_event`` rules the consumer applies, and re-publishes
it to the original topic *with the original event id* — so the
consumer's dedupe makes a double replay a no-op. The replay is
appended to the ``dead_letter_replays`` table (schema v5); the
*outcome* (applied / duplicate / dead-lettered again) is decided
by the next ``stream-consume`` run and shows up in its history.

The migration test builds a database with the frozen pre-M13 (v4)
schema by hand, so opening it must migrate additively: existing
rows intact, version bumped to 5, replay recording working
afterwards.
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
from pharmaflow.observability import render_dead_letter_replays_prometheus
from pharmaflow.streaming import (
    DEFAULT_TOPIC,
    DeadLetter,
    DeadLetterReplay,
    EventProducer,
    EventValidationError,
    FileLogBroker,
    SaleRecorded,
    StockReceived,
    consume_once,
    replay_dead_letter,
)

ROOT = Path(__file__).resolve().parents[1]
GROUP = "reconciliation-v1"
OTHER_GROUP = "alerts-v1"

# Frozen pre-M13 schema (v4): every table up to and including
# dead_letters, with schema_version recorded as 4. Copied from the
# v4 _SCHEMA so the migration test does not depend on the current
# schema definition.
V4_SCHEMA = """
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
CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    event_id TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL DEFAULT 0,
    offset INTEGER NOT NULL DEFAULT 0,
    errors_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
"""


def _make_v4_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(V4_SCHEMA)
    conn.execute(
        "INSERT INTO schema_version (version, applied_at) VALUES (4, '2026-11-04T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO inventory (sku, product_name, quantity_on_hand, reorder_point, unit_cost, expiry_date, supplier, batch)"
        " VALUES ('MED-001', 'Paracetamol 500mg 100 tabs', 120, 20, 4.50, '2027-06-30', '', '')"
    )
    conn.commit()
    conn.close()


def _seed_stock_plus_poison_sale(producer: EventProducer) -> None:
    """One valid stock receipt + one poison sale (quantity -2), both
    keyed to MED-001 so publish order is consume order."""
    producer.publish(StockReceived(event_id="evt-1", sku="MED-001",
                                   product_name="Paracetamol 500mg 100 tabs",
                                   quantity=100, reorder_point=20, unit_cost=4.50))
    producer.publish_raw(
        {"event_type": "SaleRecorded", "event_id": "evt-poison-1",
         "sale_id": "S-9", "sku": "MED-001", "quantity_sold": -2,
         "unit_price": 6.00},
        key="MED-001")


def _dead_lettered_db(log_dir: Path, db: Path) -> int:
    """Seed + consume so dead letter #1 (evt-poison-1) is recorded;
    return its record id."""
    _seed_stock_plus_poison_sale(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1 and result.dlq == 1
    assert result.dead_letter_ids == [1]
    return 1


def test_replay_corrected_sale_applies_on_next_consume(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    replay = replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    assert replay.replay_id == 1
    assert replay.dead_letter_id == 1 and replay.event_id == "evt-poison-1"
    assert replay.group == GROUP and replay.topic == DEFAULT_TOPIC
    assert replay.corrected_event["quantity_sold"] == 5
    assert replay.corrected_event["sale_id"] == "S-9"  # untouched fields kept
    # The corrected event is a new broker record, after the poison one.
    assert replay.offset == 2
    # The next consume run is the arbiter: the corrected sale applies.
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 1 and result.applied == 1
    assert result.duplicates == 0 and result.dlq == 0
    with SqliteStore(db) as store:
        assert store.schema_version == 5
        assert store.inventory["MED-001"].quantity_on_hand == 95
        # The dead letter is still in history (append-only); the
        # replay is recorded alongside it.
        assert store.count_dead_letters() == 1
        assert store.count_dead_letter_replays() == 1
        stored = store.latest_dead_letter_replay()
        assert stored is not None and stored.replay_id == 1
        assert stored.corrected_event["quantity_sold"] == 5
        assert stored.partition == replay.partition
        assert stored.offset == replay.offset


def test_replay_without_corrections_of_parse_failure_records_nothing(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    broker = FileLogBroker(log_dir)
    end_before = sum(broker.end_offset(DEFAULT_TOPIC, p)
                     for p in range(broker.num_partitions))
    # Re-publishing the original payload unchanged still fails the
    # same validation — nothing is published, nothing is recorded,
    # and the dead letter stays replayable.
    with pytest.raises(EventValidationError) as excinfo:
        replay_dead_letter(log_dir, db, 1)
    assert any("quantity_sold must be >= 1" in e for e in excinfo.value.errors)
    end_after = sum(broker.end_offset(DEFAULT_TOPIC, p)
                    for p in range(broker.num_partitions))
    assert end_after == end_before
    with SqliteStore(db) as store:
        assert store.count_dead_letter_replays() == 0
        assert store.latest_dead_letter_replay() is None


def test_replay_without_corrections_of_state_failure_after_sku_exists(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    # Parse-valid sale for a SKU that does not exist yet: the poison
    # is a state failure at apply time, not a malformed payload.
    producer.publish_raw(
        {"event_type": "SaleRecorded", "event_id": "evt-unknown-sku",
         "sale_id": "S-7", "sku": "MED-009", "quantity_sold": 7,
         "unit_price": 3.00},
        key="MED-009")
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 0 and result.dlq == 1
    assert result.dead_letter_ids == [1]
    assert any("sale for unknown sku" in e for e in result.dead_letters[0].errors)
    # The SKU arrives later; now the original payload is replayable
    # unchanged — no corrections needed.
    producer.publish(StockReceived(event_id="evt-stock-9", sku="MED-009",
                                   product_name="Ibuprofen 200mg 50 tabs",
                                   quantity=50, reorder_point=10, unit_cost=3.00))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1  # the stock receipt
    replay = replay_dead_letter(log_dir, db, 1)
    assert replay.replay_id == 1 and replay.event_id == "evt-unknown-sku"
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1 and result.duplicates == 0
    with SqliteStore(db) as store:
        assert store.inventory["MED-009"].quantity_on_hand == 43


def test_double_replay_is_deduped_at_consume(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    first = replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    second = replay_dead_letter(log_dir, db, 1, corrections={"quantity_sold": 5})
    assert first.replay_id == 1 and second.replay_id == 2
    assert first.event_id == second.event_id == "evt-poison-1"
    # Two rows in the replay history (append-only), but the consumer
    # applies the event once: the second copy is a duplicate.
    result = consume_once(log_dir, db, group=GROUP)
    assert result.records_read == 2 and result.applied == 1
    assert result.duplicates == 1 and result.dlq == 0
    with SqliteStore(db) as store:
        assert store.inventory["MED-001"].quantity_on_hand == 95
        assert store.count_dead_letter_replays() == 2
        assert store.count_dead_letter_replays(group=GROUP) == 2
        assert store.dead_letter_replay_totals_by_group() == {GROUP: 2}


def test_replay_unknown_id_raises_and_publishes_nothing(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    broker = FileLogBroker(log_dir)
    end_before = sum(broker.end_offset(DEFAULT_TOPIC, p)
                     for p in range(broker.num_partitions))
    with pytest.raises(ValueError, match="no dead letter recorded with id 99"):
        replay_dead_letter(log_dir, db, 99, corrections={"quantity_sold": 5})
    end_after = sum(broker.end_offset(DEFAULT_TOPIC, p)
                    for p in range(broker.num_partitions))
    assert end_after == end_before
    with SqliteStore(db) as store:
        assert store.count_dead_letter_replays() == 0


def test_replay_rejects_event_id_change(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    with pytest.raises(ValueError, match="must not change event_id"):
        replay_dead_letter(log_dir, db, 1,
                           corrections={"event_id": "evt-other",
                                        "quantity_sold": 5})
    with SqliteStore(db) as store:
        assert store.count_dead_letter_replays() == 0


def test_replay_supplies_event_id_when_original_had_none(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    producer.publish_raw({"event_type": "BogusType", "sku": "MED-001"},
                         key="MED-001")
    result = consume_once(log_dir, db, group=GROUP)
    assert result.dlq == 1 and result.dead_letter_ids == [1]
    assert result.dead_letters[0].event_id == ""
    # Without an event id the replay cannot publish at all.
    with pytest.raises(EventValidationError):
        replay_dead_letter(log_dir, db, 1, corrections={"event_type": "StockReceived"})
    # With a full corrected event (including the id) it publishes
    # and the next consume applies it.
    replay = replay_dead_letter(log_dir, db, 1, corrections={
        "event_type": "StockReceived", "event_id": "evt-fixed",
        "sku": "MED-001", "product_name": "Paracetamol 500mg 100 tabs",
        "quantity": 3})
    assert replay.event_id == "evt-fixed"
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 1
    with SqliteStore(db) as store:
        assert store.inventory["MED-001"].quantity_on_hand == 3


def test_replay_missing_broker_record_raises(tmp_path):
    log_dir = tmp_path / "log"
    other_log = tmp_path / "other-log"
    db = tmp_path / "pharma.db"
    _dead_lettered_db(log_dir, db)
    # A different (empty) log dir has no record at the dead letter's
    # coordinates: refuse rather than replaying the wrong record.
    with pytest.raises(ValueError, match="not in the broker log"):
        replay_dead_letter(other_log, db, 1, corrections={"quantity_sold": 5})
    with SqliteStore(db) as store:
        assert store.count_dead_letter_replays() == 0


def test_v4_database_migrates_additively(tmp_path):
    db = tmp_path / "pharma.db"
    _make_v4_db(db)
    with SqliteStore(db) as store:
        assert store.schema_version == 5
        # Pre-existing data survived the migration untouched.
        assert len(store.inventory) == 1
        assert store.inventory["MED-001"].quantity_on_hand == 120
        assert store.count_dead_letters() == 0
        assert store.count_dead_letter_replays() == 0
        letter_id = store.record_dead_letter(DeadLetter(
            group=GROUP, event_id="evt-poison-1",
            errors=["quantity_sold must be >= 1"],
            source_topic=DEFAULT_TOPIC, source_partition=2, source_offset=1))
        assert letter_id == 1
        replay_id = store.record_dead_letter_replay(DeadLetterReplay(
            dead_letter_id=letter_id, group=GROUP,
            event_id="evt-poison-1", topic=DEFAULT_TOPIC,
            partition=2, offset=2,
            corrected_event={"event_type": "SaleRecorded",
                             "event_id": "evt-poison-1",
                             "quantity_sold": 5}))
        assert replay_id == 1
        assert store.get_dead_letter(1) is not None
        assert store.get_dead_letter(99) is None
    # Reopening stays v5 and the rows are still there.
    with SqliteStore(db) as store:
        assert store.schema_version == 5
        assert store.count_dead_letter_replays() == 1


def test_store_api_filtering_and_limits(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as store:
        for i, group in enumerate([GROUP, GROUP, OTHER_GROUP]):
            store.record_dead_letter_replay(DeadLetterReplay(
                dead_letter_id=i + 1, group=group, event_id=f"evt-{i}",
                topic=DEFAULT_TOPIC, partition=2, offset=i,
                corrected_event={"event_id": f"evt-{i}"}))
        assert store.count_dead_letter_replays() == 3
        assert store.count_dead_letter_replays(group=GROUP) == 2
        assert store.count_dead_letter_replays(group="no-such-group") == 0
        assert [r.replay_id for r in store.list_dead_letter_replays(limit=1)] == [3]
        # Limit clamps: 0 behaves as 1, 500 behaves as 100 (returns all 3).
        assert len(store.list_dead_letter_replays(limit=0)) == 1
        assert len(store.list_dead_letter_replays(limit=500)) == 3
        assert [r.event_id for r in store.list_dead_letter_replays(group=GROUP)] == [
            "evt-1", "evt-0"]
        assert [r.replay_id for r in
                store.list_dead_letter_replays(dead_letter_id=2)] == [2]
        latest = store.latest_dead_letter_replay(group=OTHER_GROUP)
        assert latest is not None and latest.event_id == "evt-2"
        assert latest.corrected_event == {"event_id": "evt-2"}
        assert store.latest_dead_letter_replay(group="no-such-group") is None
        assert store.dead_letter_replay_totals_by_group() == {
            GROUP: 2, OTHER_GROUP: 1}


def test_render_dead_letter_replays_prometheus():
    totals = {GROUP: 2, OTHER_GROUP: 1}
    text = render_dead_letter_replays_prometheus(totals)
    assert f'pharmaflow_dead_letter_replays_total{{group="{GROUP}"}} 2' in text
    assert f'pharmaflow_dead_letter_replays_total{{group="{OTHER_GROUP}"}} 1' in text
    assert render_dead_letter_replays_prometheus({}) == ""


def test_cli_replay_flow(tmp_path, capsys):
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
    ]) + "\n", encoding="utf-8")
    assert cli_main(["stream-produce", "--log-dir", str(log_dir),
                     "--events", str(events_file)]) == 0
    capsys.readouterr()
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Recorded 1 dead letter(s) (group reconciliation-v1)" in out
    assert cli_main(["dead-letter-replay", "--db", str(db),
                     "--log-dir", str(log_dir), "--id", "1",
                     "--corrections", '{"quantity_sold": 5}']) == 0
    out = capsys.readouterr().out
    assert "Replayed dead letter #1 (event_id=evt-poison-1) as replay #1" in out
    assert "partition=" in out and "offset=" in out
    assert "next stream-consume run" in out
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Consumed: read=1 applied=1 skipped_dlq=0 duplicates=0" in out
    with SqliteStore(db) as store:
        assert store.inventory["MED-001"].quantity_on_hand == 95
    assert cli_main(["dead-letter-replays", "--db", str(db), "--json"]) == 0
    replays = json.loads(capsys.readouterr().out)
    assert len(replays) == 1
    assert replays[0]["replay_id"] == 1 and replays[0]["dead_letter_id"] == 1
    assert replays[0]["corrected_event"]["quantity_sold"] == 5
    assert cli_main(["dead-letter-replays", "--db", str(db), "--latest"]) == 0
    out = capsys.readouterr().out
    assert "#1 dead_letter=#1 group=reconciliation-v1 event_id=evt-poison-1" in out
    assert cli_main(["dead-letter-replays", "--db", str(db),
                     "--group", "no-such-group"]) == 0
    assert "No dead-letter replays recorded." in capsys.readouterr().out
    # Bad corrections JSON and an unknown id both fail with exit 2
    # and record nothing.
    assert cli_main(["dead-letter-replay", "--db", str(db),
                     "--log-dir", str(log_dir), "--id", "1",
                     "--corrections", '{"quantity_sold": }']) == 2
    assert cli_main(["dead-letter-replay", "--db", str(db),
                     "--log-dir", str(log_dir), "--id", "99",
                     "--corrections", '{"quantity_sold": 5}']) == 2
    with SqliteStore(db) as store:
        assert store.count_dead_letter_replays() == 1


def test_api_replay_endpoints_and_metrics(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    client = TestClient(create_app(db_path=db, stream_log_dir=log_dir))
    # Empty history: list is empty, latest is 404, no replay gauge.
    assert client.get("/dead-letter-replays").json() == []
    assert client.get("/dead-letter-replays/latest").status_code == 404
    assert "pharmaflow_dead_letter_replays_total" not in client.get("/metrics").text
    # Unknown id -> 404; still-invalid corrections -> 422.
    _dead_lettered_db(log_dir, db)
    assert client.post("/dead-letters/99/replay",
                       json={"corrections": {"quantity_sold": 5}}).status_code == 404
    bad = client.post("/dead-letters/1/replay",
                      json={"corrections": {"quantity_sold": -2}})
    assert bad.status_code == 422
    assert "quantity_sold must be >= 1" in str(bad.json())
    # Re-publishing the original unchanged also fails validation.
    assert client.post("/dead-letters/1/replay", json={}).status_code == 422
    # A good replay: 201, the recorded replay, and the gauge.
    resp = client.post("/dead-letters/1/replay",
                       json={"corrections": {"quantity_sold": 5}})
    assert resp.status_code == 201
    body = resp.json()
    assert body["replay_id"] == 1 and body["dead_letter_id"] == 1
    assert body["event_id"] == "evt-poison-1" and body["group"] == GROUP
    assert body["corrected_event"]["quantity_sold"] == 5
    assert body["topic"] == DEFAULT_TOPIC
    replays = client.get("/dead-letter-replays").json()
    assert len(replays) == 1 and replays[0]["replay_id"] == 1
    latest = client.get("/dead-letter-replays/latest").json()
    assert latest["replay_id"] == 1
    assert client.get("/dead-letter-replays",
                      params={"group": "no-such-group"}).json() == []
    metrics = client.get("/metrics").text
    assert f'pharmaflow_dead_letter_replays_total{{group="{GROUP}"}} 1' in metrics
    # A second app instance on the same DB serves the same history
    # (the history is durable, not per-process).
    client2 = TestClient(create_app(db_path=db, stream_log_dir=log_dir))
    assert len(client2.get("/dead-letter-replays").json()) == 1
    assert f'pharmaflow_dead_letter_replays_total{{group="{GROUP}"}} 1' in client2.get("/metrics").text
    # Without a broker log directory configured, replay is 503.
    client3 = TestClient(create_app(db_path=db))
    resp = client3.post("/dead-letters/1/replay",
                        json={"corrections": {"quantity_sold": 5}})
    assert resp.status_code == 503
    assert "PHARMAFLOW_STREAM_LOG_DIR" in resp.json()["detail"]
