"""M9 — durable pipeline-run history.

A completed ``run_daily`` (success or failure) is appended to the
``pipeline_runs`` table in the same SQLite database, so the last run
survives a process restart and can be served by the API and rendered
as Prometheus gauges. Re-runs of the same logical date append new
rows; ingest idempotency — not history overwriting — is what keeps
inventory from duplicating.

The migration test builds a database with the frozen pre-M9 (v1)
schema by hand, so opening it must migrate additively: existing rows
intact, version bumped to 2, run recording working afterwards.
"""
import json
import sqlite3
from datetime import date
from pathlib import Path

import pytest

pytest.importorskip("duckdb")
pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from api.app import create_app
from pharmaflow import SqliteStore
from pharmaflow.__main__ import main as cli_main
from pharmaflow.observability import render_pipeline_prometheus
from pharmaflow.orchestration import PipelineResult, TaskOutcome, run_daily

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "sample_data"
REF = date(2026, 10, 22)

# Frozen pre-M9 schema (v1): every table except pipeline_runs, with
# schema_version recorded as 1. Copied from the v1 _SCHEMA so the
# migration test does not depend on the current schema definition.
V1_SCHEMA = """
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
"""


def _make_v1_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(V1_SCHEMA)
    conn.execute(
        "INSERT INTO schema_version (version, applied_at) VALUES (1, '2026-10-22T00:00:00+00:00')"
    )
    conn.execute(
        "INSERT INTO inventory (sku, product_name, quantity_on_hand, reorder_point, unit_cost, expiry_date, supplier, batch)"
        " VALUES ('MED-001', 'Paracetamol 500mg 100 tabs', 120, 20, 4.50, '2027-06-30', '', '')"
    )
    conn.commit()
    conn.close()


def test_run_daily_persists_successful_run(tmp_path):
    db = tmp_path / "pharma.db"
    result = run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF)
    assert result.succeeded
    assert result.run_id == 1
    # A fresh store instance on the same file sees the recorded run.
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        assert store.count_pipeline_runs() == 1
        latest = store.latest_pipeline_run()
        assert latest is not None
        assert latest.run_id == 1
        assert latest.logical_date == REF
        assert latest.succeeded is True
        assert latest.failed_task is None
        assert latest.recorded_at
        rebuilt = PipelineResult.from_dict(latest.result_dict)
        assert rebuilt == PipelineResult(
            logical_date=REF, outcomes=result.outcomes
        )
        assert [o.name for o in rebuilt.outcomes] == ["ingest", "transform", "report"]
        assert all(o.status == "success" for o in rebuilt.outcomes)


def test_failed_run_is_persisted_too(tmp_path):
    db = tmp_path / "pharma.db"
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    result = run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF,
                       transform_data_dir=empty_dir)
    assert not result.succeeded and result.failed_task == "transform"
    assert result.run_id == 1
    with SqliteStore(db) as store:
        latest = store.latest_pipeline_run()
        assert latest is not None
        assert latest.succeeded is False
        assert latest.failed_task == "transform"
        rebuilt = PipelineResult.from_dict(latest.result_dict)
        assert rebuilt.status_of("report") == "skipped"
        assert rebuilt.status_of("transform") == "failed"
        skipped = [o for o in rebuilt.outcomes if o.name == "report"][0]
        assert skipped.attempts == 0 and skipped.result is None


def test_rerun_same_date_appends_history(tmp_path):
    db = tmp_path / "pharma.db"
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    first = run_daily(db, DATA_DIR, logical_date=REF, retries=1, today=REF,
                      transform_data_dir=empty_dir)
    second = run_daily(db, DATA_DIR, logical_date=REF, retries=1, today=REF)
    assert not first.succeeded and second.succeeded
    assert first.run_id == 1 and second.run_id == 2
    with SqliteStore(db) as store:
        assert store.count_pipeline_runs() == 2
        runs = store.list_pipeline_runs()
        assert [r.run_id for r in runs] == [2, 1]  # newest first
        assert runs[0].succeeded is True and runs[1].succeeded is False
        assert store.latest_pipeline_run().run_id == 2
        # Inventory still not duplicated: history appends, ingest dedupes.
        assert len(store.inventory) == 5


def test_persist_false_records_nothing(tmp_path):
    db = tmp_path / "pharma.db"
    result = run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF,
                       persist=False)
    assert result.succeeded and result.run_id is None
    with SqliteStore(db) as store:
        assert store.count_pipeline_runs() == 0
        assert store.latest_pipeline_run() is None


def test_v1_database_migrates_additively(tmp_path):
    db = tmp_path / "pharma.db"
    _make_v1_db(db)
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        # Pre-existing data survived the migration untouched.
        assert len(store.inventory) == 1
        assert store.inventory["MED-001"].quantity_on_hand == 120
        assert store.count_pipeline_runs() == 0
    # Recording works on the migrated database, and reopening stays v2.
    result = run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF)
    assert result.succeeded and result.run_id == 1
    with SqliteStore(db) as store:
        assert store.schema_version == 6
        assert len(store.inventory) == 5  # ingest added the other SKUs
        assert store.count_pipeline_runs() == 1


def test_result_serialisation_round_trip():
    result = PipelineResult(logical_date=REF, outcomes=[
        TaskOutcome(name="ingest", status="success", attempts=1,
                    result={"inventory_skus": 5}),
        TaskOutcome(name="transform", status="failed", attempts=3,
                    error="FileNotFoundError: missing"),
        TaskOutcome(name="report", status="skipped", attempts=0),
    ])
    payload = result.to_dict()
    assert json.loads(json.dumps(payload, default=str)) == payload
    rebuilt = PipelineResult.from_dict(payload)
    assert rebuilt == result
    assert rebuilt.succeeded is False and rebuilt.failed_task == "transform"


def test_render_pipeline_prometheus_with_logical_date_label():
    result = PipelineResult(logical_date=REF, outcomes=[
        TaskOutcome(name="ingest", status="success", attempts=1),
        TaskOutcome(name="transform", status="failed", attempts=3),
        TaskOutcome(name="report", status="skipped", attempts=0),
    ])
    text = render_pipeline_prometheus(result, logical_date=REF, runs_total=2)
    assert 'pharmaflow_pipeline_succeeded{logical_date="2026-10-22"} 0' in text
    assert 'pharmaflow_pipeline_task_success{task="ingest",logical_date="2026-10-22"} 1' in text
    assert 'pharmaflow_pipeline_task_success{task="report",logical_date="2026-10-22"} 0' in text
    assert "pharmaflow_pipeline_runs_total 2" in text
    # The unlabeled M8 form is unchanged when no label is given.
    plain = render_pipeline_prometheus(result)
    assert "pharmaflow_pipeline_succeeded 0" in plain
    assert "logical_date" not in plain and "runs_total" not in plain


def test_api_pipeline_runs_endpoints(tmp_path):
    app = create_app(tmp_path / "api.db")
    with TestClient(app) as client:
        assert client.get("/pipeline-runs").json() == []
        assert client.get("/pipeline-runs/latest").status_code == 404
        assert "pharmaflow_pipeline_succeeded" not in client.get("/metrics").text
    # Run the pipeline against the same DB file, then re-read via API.
    db = tmp_path / "api.db"
    run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF)
    with TestClient(create_app(db)) as client:
        latest = client.get("/pipeline-runs/latest").json()
        assert latest["run_id"] == 1
        assert latest["logical_date"] == "2026-10-22"
        assert latest["succeeded"] is True
        assert latest["failed_task"] is None
        assert [o["name"] for o in latest["outcomes"]] == ["ingest", "transform", "report"]
        runs = client.get("/pipeline-runs?limit=5").json()
        assert len(runs) == 1 and runs[0]["run_id"] == 1
        metrics = client.get("/metrics").text
        assert 'pharmaflow_pipeline_succeeded{logical_date="2026-10-22"} 1' in metrics
        assert "pharmaflow_pipeline_runs_total 1" in metrics


def test_cli_pipeline_runs(tmp_path, capsys):
    db = tmp_path / "pharma.db"
    with SqliteStore(db):
        pass  # empty database, no runs recorded yet
    rc = cli_main(["pipeline-runs", "--db", str(db)])
    assert rc == 0 and "No pipeline runs recorded." in capsys.readouterr().out
    rc = cli_main(["run-daily", "--db", str(db), "--data-dir", str(DATA_DIR),
                   "--date", "2026-10-22"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Recorded as pipeline run #1" in out
    rc = cli_main(["pipeline-runs", "--db", str(db)])
    out = capsys.readouterr().out
    assert rc == 0
    assert "#1 logical_date=2026-10-22" in out and "succeeded=True" in out
    rc = cli_main(["pipeline-runs", "--db", str(db), "--latest", "--json"])
    payload = json.loads(capsys.readouterr().out)
    assert rc == 0
    assert len(payload) == 1 and payload[0]["run_id"] == 1
    assert payload[0]["result"]["outcomes"][0]["name"] == "ingest"
