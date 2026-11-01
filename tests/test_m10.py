"""M10 — durable ingestion counters derived from the persisted record.

The documented M5/M8 limitation was that ingestion counters were
per-process: a fresh API process on the same database started at zero
even though the imports themselves persisted. M10 derives the counters
at read time from the recorded imports + audit trail, so they survive
restarts. Each distinct file is counted once, at its first import; a
duplicate-file re-import adds to ``files_already_imported`` only, never
to ``rows_in`` again. The session counters still count every ingest
call (including re-imports) — the difference is asserted, not hidden.

Sample-data expectations are hand-known from the foundation run
(ingest of inventory.csv + sales.csv + inventory_with_errors.csv):
rows_in=15, valid=11, quarantined=3, duplicates_skipped=1.
"""
import subprocess
import sys
from pathlib import Path

import pytest

from pharmaflow import SqliteStore, build_report, durable_metrics, ingest_file
from pharmaflow.ingestion import PharmaStore
from pharmaflow.metrics import Metrics
from pharmaflow.models import ImportResult

ROOT = Path(__file__).resolve().parents[1]
INV = ROOT / "sample_data" / "inventory.csv"
SALES = ROOT / "sample_data" / "sales.csv"
ERR = ROOT / "sample_data" / "inventory_with_errors.csv"


def test_from_imports_sums_recorded_imports():
    imports = [
        ImportResult(source_file="a.csv", file_hash="h1", kind="inventory",
                     rows_in=5, rows_valid=5),
        ImportResult(source_file="b.csv", file_hash="h2", kind="sales",
                     rows_in=3, rows_valid=2, rows_quarantined=1),
        ImportResult(source_file="c.csv", file_hash="h3", kind="inventory",
                     rows_in=7, rows_valid=3, rows_quarantined=3,
                     duplicates_skipped=1),
    ]
    m = Metrics.from_imports(imports, files_already_imported=2)
    assert m.counters == {
        "rows_in": 15, "rows_valid": 10, "rows_quarantined": 4,
        "duplicates_skipped": 1, "imports_total": 3,
        "files_already_imported": 2,
    }
    assert Metrics.from_imports([]).counters["rows_in"] == 0
    assert Metrics.from_imports([]).counters["imports_total"] == 0


def test_durable_matches_dq_report_totals():
    store = PharmaStore()
    for f in (INV, SALES, ERR):
        ingest_file(f, store)
    m = durable_metrics(store)
    report = build_report(store)
    assert m.counters["rows_in"] == report.total_rows_in == 15
    assert m.counters["rows_valid"] == report.total_rows_valid == 11
    assert m.counters["rows_quarantined"] == report.total_rows_quarantined == 3
    assert m.counters["duplicates_skipped"] == report.total_duplicates_skipped == 1
    assert m.counters["imports_total"] == len(report.imports) == 3
    assert m.counters["files_already_imported"] == 0
    text = m.render_prometheus()
    assert "pharmaflow_rows_in_total 15" in text
    assert "pharmaflow_imports_total_total 3" in text


def test_duplicate_reimport_counted_once_durable_not_session():
    store = PharmaStore()
    ingest_file(INV, store)
    second = ingest_file(INV, store)
    assert second.already_imported is True
    # Durable: the file's rows count once; the re-import is one skip.
    d = durable_metrics(store)
    assert d.counters["rows_in"] == 5
    assert d.counters["rows_valid"] == 5
    assert d.counters["imports_total"] == 1
    assert d.counters["files_already_imported"] == 1
    assert d.counters["duplicates_skipped"] == 0
    # Session: the same re-import added its rows again (documented
    # difference — the session counter counts every ingest call).
    assert store.metrics.counters["rows_in"] == 10
    assert store.metrics.counters["files_already_imported"] == 1


def test_durable_survives_sqlite_reopen(tmp_path):
    db = tmp_path / "m10.db"
    store = SqliteStore(db)
    ingest_file(INV, store)
    ingest_file(ERR, store)
    ingest_file(INV, store)  # duplicate-file re-import
    before = durable_metrics(store)
    store.close()
    # A fresh store object on the same file is a new process in effect:
    # its session counters start at zero, the durable ones must not.
    reopened = SqliteStore(db)
    try:
        assert reopened.metrics.counters["rows_in"] == 0
        after = durable_metrics(reopened)
        assert after.counters == before.counters
        assert after.counters["rows_in"] == 12  # 5 + 7
        assert after.counters["rows_valid"] == 8  # 5 + 3
        assert after.counters["rows_quarantined"] == 3
        assert after.counters["duplicates_skipped"] == 1
        assert after.counters["imports_total"] == 2
        assert after.counters["files_already_imported"] == 1
    finally:
        reopened.close()


def test_api_metrics_survive_new_app_instance(tmp_path):
    pytest.importorskip("fastapi")
    pytest.importorskip("httpx")
    from fastapi.testclient import TestClient

    from api.app import create_app

    db = tmp_path / "api.db"
    with TestClient(create_app(db)) as c:
        assert c.post("/imports", files={"file": ("inventory.csv", INV.read_bytes(), "text/csv")}).status_code == 200
        first = c.get("/metrics").text
    # Second app instance on the same DB file: a fresh process.
    with TestClient(create_app(db)) as c2:
        second = c2.get("/metrics").text
        assert "pharmaflow_rows_in_total 5" in second
        assert "pharmaflow_rows_valid_total 5" in second
        assert "pharmaflow_imports_total_total 1" in second
        assert "pharmaflow_files_already_imported_total 0" in second
        # The numbers are identical to the first process's, not reset.
        assert first == second
        # Duplicate POST on the new process: rows stay counted once.
        dup = c2.post("/imports", files={"file": ("inventory.csv", INV.read_bytes(), "text/csv")}).json()
        assert dup["already_imported"] is True
        third = c2.get("/metrics").text
        assert "pharmaflow_rows_in_total 5" in third
        assert "pharmaflow_files_already_imported_total 1" in third


def test_cli_ingest_prints_durable_metrics_across_processes(tmp_path):
    db = tmp_path / "cli.db"
    cmd = [sys.executable, "-m", "pharmaflow", "ingest", str(INV),
           "--db", str(db), "--today", "2026-10-22"]
    first = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    assert "pharmaflow_rows_in_total 5" in first.stdout
    assert "pharmaflow_files_already_imported_total 0" in first.stdout
    second = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)
    assert second.returncode == 0, second.stderr
    assert "already_imported=True" in second.stdout
    # Second process, same DB: rows still counted once, skip counted.
    assert "pharmaflow_rows_in_total 5" in second.stdout
    assert "pharmaflow_files_already_imported_total 1" in second.stdout
