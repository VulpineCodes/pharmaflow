from datetime import date
from pathlib import Path
import pytest
from pharmaflow import PharmaStore, ingest_file, ingest_bytes, generate_alerts, reconcile
from pharmaflow.reconciliation import reconcile_store, detect_overselling
from pharmaflow.models import SalesRecord

ROOT = Path(__file__).resolve().parents[1]
INV = ROOT/"sample_data"/"inventory.csv"
ERR = ROOT/"sample_data"/"inventory_with_errors.csv"
SALES = ROOT/"sample_data"/"sales.csv"
TODAY = date(2026,10,22)

def test_valid_import():
    s=PharmaStore(); r=ingest_file(INV,s)
    assert r.rows_in==5 and r.rows_valid==5 and r.rows_quarantined==0 and not r.already_imported
    assert len(s.inventory)==5 and s.inventory["MED-001"].quantity_on_hand==120

def test_sales_import():
    s=PharmaStore(); r=ingest_file(SALES,s)
    assert r.kind=="sales" and r.rows_valid==3 and len(s.sales)==3

def test_invalid_rows_quarantined_with_reasons():
    s=PharmaStore(); r=ingest_file(ERR,s)
    assert r.rows_quarantined==3
    assert len(s.quarantine)==3
    reasons=" ".join(e.reasons[0] if False else "; ".join(x.message for x in e.errors) for e in s.quarantine.entries)
    assert "sku is required" in reasons
    assert "must be an integer" in reasons
    assert ">= 0" in reasons  # negative stock business rule

def test_duplicate_file_reimport_idempotent():
    s=PharmaStore(); r1=ingest_file(INV,s); snap=dict(s.inventory)
    r2=ingest_file(INV,s)
    assert r2.already_imported and r2.duplicates_skipped==r1.rows_in and r2.rows_valid==0
    assert s.inventory==snap and s.metrics.counters["rows_valid"]==5

def test_duplicate_rows_skipped():
    s=PharmaStore(); r=ingest_file(ERR,s)
    assert r.duplicates_skipped==1  # exact duplicate MED-101 row

def test_reconciliation_discrepancies():
    discs=reconcile({"MED-001":110,"MED-002":10,"MED-999":5},{"MED-001":100,"MED-002":12})
    kinds={d.sku:d.kind for d in discs}
    assert kinds["MED-001"]=="shrinkage" and kinds["MED-002"]=="count_drift" and kinds["MED-999"]=="missing_sku"
    assert reconcile({"A":5},{"A":5})==[]

def test_overselling_detection():
    discs=detect_overselling({"MED-001":5},[SalesRecord("S1","MED-001",9)])
    assert len(discs)==1 and discs[0].kind=="overselling"

def test_low_stock_and_expiry_alerts():
    s=PharmaStore(); ingest_file(INV,s)
    alerts=generate_alerts(s.inventory, today=TODAY)
    kinds={(a.sku,a.kind) for a in alerts}
    assert ("MED-002","low_stock") in kinds  # 8 <= 15
    assert ("MED-005","low_stock") in kinds
    assert ("MED-003","expiring_soon") in kinds  # 2026-11-10 within 30d of 2026-10-22
    assert ("MED-005","expired") in kinds  # 2026-09-01 before today
    assert ("MED-001","expiring_soon") not in kinds

def test_partial_failure_recovery():
    # Bad rows are in the middle; rows before and after must survive.
    s=PharmaStore(); r=ingest_file(ERR,s)
    assert "MED-101" in s.inventory and "MED-102" in s.inventory and "MED-105" in s.inventory
    assert r.rows_valid==3

def test_metrics_prometheus_render():
    s=PharmaStore(); ingest_file(ERR,s)
    text=s.metrics.render_prometheus()
    assert "pharmaflow_rows_in_total 7" in text
    assert "pharmaflow_rows_quarantined_total 3" in text
    assert "# TYPE pharmaflow_rows_valid counter" in text

def test_audit_log_appended():
    s=PharmaStore(); ingest_file(INV,s); ingest_file(INV,s)
    actions=[e.action for e in s.audit.entries]
    assert actions==["import_completed","import_skipped_duplicate_file"]

def test_ingest_bytes_sales_and_invalid_type():
    csv=b"sale_id,sku,quantity_sold,sale_date,unit_price\nS1,MED-001,abc,2026-10-22,1.00\nS2,MED-001,2,2026-10-22,1.00\n"
    s=PharmaStore(); r=ingest_bytes(csv,"mem.csv",s)
    assert r.rows_quarantined==1 and r.rows_valid==1 and s.sales[0].sale_id=="S2"
