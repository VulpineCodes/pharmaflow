"""M11 — durable stream-consumption history.

A *completed* ``stream-consume`` run is appended to the
``stream_consumption_runs`` table in the same SQLite database, so a
consumer group's consumption history (records read, applied,
duplicates from redelivery, poison events dead-lettered, lag
before/after) survives a process restart and can be served by the API
and rendered as Prometheus gauges. The broker's own lag/DLQ gauges
(M8) still describe live state; these totals describe what completed
runs have done so far.

The migration test builds a database with the frozen pre-M11 (v2)
schema by hand, so opening it must migrate additively: existing rows
intact, version bumped to 3, consumption recording working afterwards.
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
from pharmaflow.observability import render_stream_consumption_prometheus
from pharmaflow.streaming import (
    DEFAULT_TOPIC,
    EventProducer,
    FileLogBroker,
    InventoryAdjusted,
    SaleRecorded,
    SimulatedCrashError,
    StockReceived,
    StreamingConsumer,
    consume_once,
)

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "sample_data"
GROUP = "reconciliation-v1"
OTHER_GROUP = "alerts-v1"

# Frozen pre-M11 schema (v2): every table up to and including
# pipeline_runs, with schema_version recorded as 2. Copied from the
# v2 _SCHEMA so the migration test does not depend on the current
# schema definition.
V2_SCHEMA = """
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
"""


def _make_v2_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(V2_SCHEMA)
    conn.execute(
        "INSERT INTO schema_version (version, applied_at) VALUES (2, '2026-10-30T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO inventory (sku, product_name, quantity_on_hand, reorder_point, unit_cost, expiry_date, supplier, batch)"
        " VALUES ('MED-001', 'Paracetamol 500mg 100 tabs', 120, 20, 4.50, '2027-06-30', '', '')"
    )
    conn.commit()
    conn.close()


def _seed_events(producer: EventProducer) -> None:
    producer.publish(StockReceived(event_id="evt-1", sku="MED-001",
                                   product_name="Paracetamol 500mg 100 tabs",
                                   quantity=100, reorder_point=20, unit_cost=4.50))
    producer.publish(StockReceived(event_id="evt-2", sku="MED-002",
                                   product_name="Ibuprofen 200mg 50 tabs",
                                   quantity=50, reorder_point=15, unit_cost=3.20))
    producer.publish(SaleRecorded(event_id="evt-3", sale_id="S-1", sku="MED-001",
                                  quantity_sold=10, unit_price=6.00))
    producer.publish(InventoryAdjusted(event_id="evt-4", sku="MED-002",
                                       delta=-5, reason="damaged stock"))


def test_consume_once_records_completed_run(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.applied == 4 and result.records_read == 4
    assert result.duplicates == 0 and result.dlq == 0
    assert result.lag_before == 4 and result.lag_after == 0
    assert result.record_id == 1
    # A fresh store instance on the same file sees the recorded run.
    with SqliteStore(db) as store:
        assert store.schema_version == 5
        assert store.count_stream_consumption() == 1
        latest = store.latest_stream_consumption()
        assert latest is not None
        assert latest.run_id == 1
        assert latest.group == GROUP and latest.topic == DEFAULT_TOPIC
        assert latest.records_read == 4 and latest.applied == 4
        assert latest.duplicates == 0 and latest.dlq == 0
        assert latest.lag_before == 4 and latest.lag_after == 0
        assert latest.recorded_at


def test_second_consume_appends_and_totals_sum(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    first = consume_once(log_dir, db, group=GROUP)
    second = consume_once(log_dir, db, group=GROUP)
    assert first.record_id == 1 and second.record_id == 2
    # The no-op catch-up read 0 records and is still history.
    assert second.records_read == 0 and second.applied == 0
    assert second.lag_before == 0 and second.lag_after == 0
    with SqliteStore(db) as store:
        assert store.count_stream_consumption() == 2
        runs = store.list_stream_consumption()
        assert [r.run_id for r in runs] == [2, 1]  # newest first
        totals = store.stream_consumption_totals(group=GROUP)
        assert totals == {"records_read": 4, "applied": 4,
                          "duplicates": 0, "dlq": 0, "runs": 2}
        by_group = store.stream_consumption_totals_by_group()
        assert by_group == {GROUP: totals}


def test_crash_run_is_not_recorded_then_recovery_run_is(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db)
    try:
        consumer = StreamingConsumer(broker, store, group=GROUP)
        with pytest.raises(SimulatedCrashError):
            consumer.consume_until_caught_up(crash_after_applies=2)
        # The interrupted run recorded nothing, even though 2 events
        # were applied before the crash (the two StockReceived events:
        # MED-001 is at 100, the sale of 10 has not been applied yet).
        assert store.count_stream_consumption() == 0
        assert store.inventory["MED-001"].quantity_on_hand == 100
    finally:
        store.close()
    # Recovery: redelivery is recognised as duplicates and the
    # completed run IS recorded, duplicates included.
    result = consume_once(log_dir, db, group=GROUP)
    assert result.record_id == 1
    assert result.records_read == 4 and result.applied == 2
    assert result.duplicates == 2 and result.dlq == 0
    assert result.lag_after == 0
    with SqliteStore(db) as store:
        assert store.stream_consumption_totals() == {
            "records_read": 4, "applied": 2,
            "duplicates": 2, "dlq": 0, "runs": 1}


def test_record_false_skips_history(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP, record=False)
    assert result.applied == 4 and result.record_id is None
    with SqliteStore(db) as store:
        assert store.count_stream_consumption() == 0
        assert store.latest_stream_consumption() is None


def test_v2_database_migrates_additively(tmp_path):
    db = tmp_path / "pharma.db"
    _make_v2_db(db)
    with SqliteStore(db) as store:
        assert store.schema_version == 5
        # Pre-existing data survived the migration untouched.
        assert len(store.inventory) == 1
        assert store.inventory["MED-001"].quantity_on_hand == 120
        assert store.count_stream_consumption() == 0
        assert store.count_pipeline_runs() == 0
    # Recording works on the migrated database, and reopening stays v4.
    log_dir = tmp_path / "log"
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.record_id == 1
    with SqliteStore(db) as store:
        assert store.schema_version == 5
        assert len(store.inventory) == 2  # consume added MED-002
        assert store.count_stream_consumption() == 1


def test_group_filtering(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    consume_once(log_dir, db, group=GROUP)
    consume_once(log_dir, db, group=OTHER_GROUP)
    with SqliteStore(db) as store:
        assert store.count_stream_consumption() == 2
        assert store.count_stream_consumption(group=GROUP) == 1
        assert [r.group for r in store.list_stream_consumption(group=OTHER_GROUP)] == [OTHER_GROUP]
        latest = store.latest_stream_consumption(group=GROUP)
        assert latest is not None and latest.group == GROUP
        by_group = store.stream_consumption_totals_by_group()
        assert sorted(by_group) == sorted([GROUP, OTHER_GROUP])
        assert by_group[GROUP]["applied"] == 4
        # The second group reads the same 4 events but the dedupe
        # table is per-store, not per-group (M4 design: exactly-once
        # application regardless of group, so a sale is never applied
        # twice). Its run therefore records 0 applied, 4 duplicates.
        assert by_group[OTHER_GROUP]["applied"] == 0
        assert by_group[OTHER_GROUP]["duplicates"] == 4


def test_render_stream_consumption_prometheus():
    totals = {
        GROUP: {"records_read": 4, "applied": 4, "duplicates": 2, "dlq": 1, "runs": 2},
        OTHER_GROUP: {"records_read": 4, "applied": 4, "duplicates": 0, "dlq": 0, "runs": 1},
    }
    text = render_stream_consumption_prometheus(totals)
    assert f'pharmaflow_stream_consumption_runs_total{{group="{GROUP}"}} 2' in text
    assert f'pharmaflow_stream_consumed_applied_total{{group="{GROUP}"}} 4' in text
    assert f'pharmaflow_stream_consumed_duplicates_total{{group="{GROUP}"}} 2' in text
    assert f'pharmaflow_stream_consumed_dlq_total{{group="{GROUP}"}} 1' in text
    assert f'pharmaflow_stream_consumed_read_total{{group="{OTHER_GROUP}"}} 4' in text
    assert render_stream_consumption_prometheus({}) == ""


def test_cli_stream_consume_records_and_lists(tmp_path, capsys):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    assert cli_main(["stream-produce", "--log-dir", str(log_dir),
                     "--events", str(DATA_DIR / "stream_events.jsonl")]) == 0
    capsys.readouterr()
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Recorded as stream consumption #1 (group reconciliation-v1)" in out
    assert "Consumed: read=4 applied=4" in out
    assert cli_main(["stream-consumption", "--db", str(db), "--json"]) == 0
    runs = json.loads(capsys.readouterr().out)
    assert len(runs) == 1
    assert runs[0]["run_id"] == 1 and runs[0]["group"] == GROUP
    assert runs[0]["applied"] == 4 and runs[0]["lag_after"] == 0
    assert cli_main(["stream-consumption", "--db", str(db), "--latest"]) == 0
    out = capsys.readouterr().out
    assert "#1 group=reconciliation-v1" in out and "applied=4" in out
    # A second consume is a no-op catch-up that still appends history.
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", GROUP]) == 0
    out = capsys.readouterr().out
    assert "Recorded as stream consumption #2" in out
    assert cli_main(["stream-consumption", "--db", str(db),
                     "--group", "no-such-group"]) == 0
    assert "No stream consumption runs recorded." in capsys.readouterr().out


def test_api_endpoints_and_metrics(tmp_path):
    log_dir = tmp_path / "log"
    db = tmp_path / "pharma.db"
    client = TestClient(create_app(db_path=db))
    # Empty history: list is empty, latest is 404, no consumption gauges.
    assert client.get("/stream-consumption").json() == []
    assert client.get("/stream-consumption/latest").status_code == 404
    assert "pharmaflow_stream_consumed_" not in client.get("/metrics").text
    assert "pharmaflow_stream_consumption_runs_total" not in client.get("/metrics").text
    # After a recorded run: endpoints serve it and /metrics carries gauges.
    _seed_events(EventProducer(FileLogBroker(log_dir)))
    result = consume_once(log_dir, db, group=GROUP)
    assert result.record_id == 1
    runs = client.get("/stream-consumption").json()
    assert len(runs) == 1
    assert runs[0]["run_id"] == 1 and runs[0]["applied"] == 4
    assert runs[0]["lag_before"] == 4 and runs[0]["lag_after"] == 0
    latest = client.get("/stream-consumption/latest").json()
    assert latest["run_id"] == 1 and latest["group"] == GROUP
    assert client.get("/stream-consumption",
                      params={"group": "no-such-group"}).json() == []
    assert client.get("/stream-consumption/latest",
                      params={"group": "no-such-group"}).status_code == 404
    metrics = client.get("/metrics").text
    assert f'pharmaflow_stream_consumption_runs_total{{group="{GROUP}"}} 1' in metrics
    assert f'pharmaflow_stream_consumed_applied_total{{group="{GROUP}"}} 4' in metrics
    assert f'pharmaflow_stream_consumed_duplicates_total{{group="{GROUP}"}} 0' in metrics
    assert f'pharmaflow_stream_consumed_dlq_total{{group="{GROUP}"}} 0' in metrics
    # A second app instance on the same DB serves the same history
    # (the history is durable, not per-process).
    client2 = TestClient(create_app(db_path=db))
    assert len(client2.get("/stream-consumption").json()) == 1
    assert f'pharmaflow_stream_consumption_runs_total{{group="{GROUP}"}} 1' in client2.get("/metrics").text
