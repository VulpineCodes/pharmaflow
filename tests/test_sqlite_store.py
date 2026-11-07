"""M2 — durable SQLite persistence tests.

Headline behaviour: idempotency ACROSS process restarts. A "restart" here
is a brand-new SqliteStore instance on the same DB file after the previous
instance was closed — exactly what separate CLI processes do.
"""
from datetime import date
from pathlib import Path

import pytest

from pharmaflow import PharmaStore, SqliteStore, build_report, ingest_file
from pharmaflow.__main__ import main as cli_main
from pharmaflow.data_quality import build_report_from_imports, render_report

ROOT = Path(__file__).resolve().parents[1]
INV = ROOT / "sample_data" / "inventory.csv"
ERR = ROOT / "sample_data" / "inventory_with_errors.csv"
SALES = ROOT / "sample_data" / "sales.csv"


def test_persistence_across_store_close_reopen(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        r = ingest_file(INV, s)
        assert r.rows_valid == 5
    # New instance, same file: data must still be there.
    with SqliteStore(db) as s2:
        assert len(s2.inventory) == 5
        assert s2.inventory["MED-001"].quantity_on_hand == 120
        assert s2.inventory["MED-001"].product_name == "Paracetamol 500mg 100 tabs"
        assert s2.inventory["MED-003"].expiry_date == date(2026, 11, 10)
        assert len(s2.file_hashes) == 1


def test_sales_persist_across_reopen(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        ingest_file(SALES, s)
    with SqliteStore(db) as s2:
        assert len(s2.sales) == 3
        assert s2.sales[0].sale_id == "S-1001"


def test_duplicate_file_idempotency_across_restarts(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        r1 = ingest_file(INV, s)
        assert not r1.already_imported and r1.rows_valid == 5
    # "Restart": new store instance, same DB file, same input file.
    with SqliteStore(db) as s2:
        snap = dict(s2.inventory)
        r2 = ingest_file(INV, s2)
        assert r2.already_imported is True
        assert r2.duplicates_skipped == r1.rows_in
        assert r2.rows_valid == 0
        assert s2.inventory == snap
        assert len(s2.inventory) == 5
    # And again after another restart — still a no-op.
    with SqliteStore(db) as s3:
        r3 = ingest_file(INV, s3)
        assert r3.already_imported is True
        assert len(s3.inventory) == 5


def test_duplicate_rows_skipped_across_restarts(tmp_path):
    # Row-level dedupe must also survive: a different file (different bytes,
    # so different file hash) containing an already-loaded row is skipped.
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        ingest_file(ERR, s)
    csv = (
        b"sku,product_name,quantity_on_hand,unit_cost,expiry_date,reorder_point,supplier,batch\n"
        b"MED-101,ORS Sachets 20 pack,60,1.80,2027-08-31,10,Synthetic Supplier A,C001\n"
        b"MED-200,New Product,10,1.00,2027-01-01,5,Synthetic Supplier A,C100\n"
    )
    from pharmaflow import ingest_bytes
    with SqliteStore(db) as s2:
        r = ingest_bytes(csv, "reexport.csv", s2)
        assert r.duplicates_skipped == 1
        assert r.rows_valid == 1
        assert "MED-200" in s2.inventory


def test_quarantine_and_audit_persist_and_are_queryable(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        ingest_file(ERR, s)
    with SqliteStore(db) as s2:
        assert len(s2.quarantine) == 3
        reasons = "; ".join(
            err.message for e in s2.quarantine.entries for err in e.errors
        )
        assert "sku is required" in reasons
        assert "must be an integer" in reasons
        # Queryable by source / row, not just a blob count.
        rows = sorted(e.row_number for e in s2.quarantine.entries)
        assert rows == [5, 6, 7]
        assert all(e.source_file.endswith("inventory_with_errors.csv") for e in s2.quarantine.entries)
        actions = [e.action for e in s2.audit.entries]
        assert actions == ["import_completed"]
        assert s2.audit.entries[0].file_hash
    # A duplicate re-import after restart appends a queryable audit entry.
    with SqliteStore(db) as s3:
        ingest_file(ERR, s3)
        actions = [e.action for e in s3.audit.entries]
        assert actions == ["import_completed", "import_skipped_duplicate_file"]
        assert len(s3.quarantine) == 3  # no new quarantine rows from the no-op


def test_schema_creation_is_idempotent(tmp_path):
    db = tmp_path / "pharma.db"
    s = SqliteStore(db)
    assert s.schema_version == 5
    s._init_schema()  # re-running schema creation must not fail or duplicate
    s._init_schema()
    assert s.schema_version == 5
    s.close()
    # Opening the same file again runs schema creation again — still fine.
    with SqliteStore(db) as s2:
        assert s2.schema_version == 5
        ingest_file(INV, s2)
        assert len(s2.inventory) == 5


def test_sqlite_and_memory_stores_share_ingest_contract(tmp_path):
    # Same inputs, same observable results from both store implementations.
    mem = PharmaStore()
    r_mem = ingest_file(ERR, mem)
    with SqliteStore(tmp_path / "pharma.db") as s:
        r_db = ingest_file(ERR, s)
        assert (r_db.rows_in, r_db.rows_valid, r_db.rows_quarantined, r_db.duplicates_skipped) == (
            r_mem.rows_in, r_mem.rows_valid, r_mem.rows_quarantined, r_mem.duplicates_skipped
        )
        assert set(s.inventory) == set(mem.inventory)
        assert len(s.quarantine) == len(mem.quarantine)


# --- data quality report ---------------------------------------------------

def test_data_quality_report_from_sqlite_store(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        ingest_file(INV, s)
        ingest_file(ERR, s)
    with SqliteStore(db) as s2:
        report = build_report(s2)
    assert report.total_rows_in == 12
    assert report.total_rows_valid == 8
    assert report.total_rows_quarantined == 3
    assert report.total_duplicates_skipped == 1
    by_file = {i.source_file.rsplit("/", 1)[-1]: i for i in report.imports}
    err = by_file["inventory_with_errors.csv"]
    assert err.rows_in == 7 and err.rows_valid == 3 and err.rows_quarantined == 3
    assert err.duplicates_skipped == 1
    assert err.duplicate_rate == pytest.approx(1 / 7)
    inv = by_file["inventory.csv"]
    assert inv.rows_in == 5 and inv.duplicate_rate == 0.0
    # Quarantine reason breakdown by validation code.
    assert report.quarantine_reason_breakdown == {
        "required": 1, "type": 1, "business_rule": 1,
    }
    text = render_report(report)
    assert "data-quality report" in text
    assert "duplicate_rate=14.29%" in text
    d = report.as_dict()
    assert d["totals"]["rows_in"] == 12


def test_data_quality_report_from_memory_store():
    s = PharmaStore()
    ingest_file(ERR, s)
    report = build_report(s)
    assert report.total_rows_in == 7
    assert report.quarantine_reason_breakdown["required"] == 1


def test_report_cli_against_db(tmp_path, capsys):
    db = tmp_path / "pharma.db"
    assert cli_main(["ingest", str(ERR), "--db", str(db)]) == 0
    capsys.readouterr()  # discard ingest output
    assert cli_main(["report", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "data-quality report" in out
    assert "rows_in=7" in out
    assert "business_rule: 1" in out
