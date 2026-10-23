"""M3 — DuckDB analytics, executed transforms, orchestrated daily pipeline.

Analytics expectations are HAND-COMPUTED from sample_data/inventory.csv +
sales.csv (not snapshotted from the code):

inventory.csv (reference date 2026-10-22):
  MED-001 Paracetamol  120 x 4.50 = 540.00  expiry 2027-06-30 (+251d)
  MED-002 Ibuprofen      8 x 3.20 =  25.60  expiry 2027-03-15 (+144d)
  MED-003 Vitamin C     45 x 6.00 = 270.00  expiry 2026-11-10 ( +19d)
  MED-004 Antiseptic    30 x 2.75 =  82.50  expiry 2028-01-31 (+466d)
  MED-005 Cough Syrup    5 x 5.10 =  25.50  expiry 2026-09-01 ( -51d)
  total value = 943.60
sales.csv: window = 2026-10-20..2026-10-21 = 2 days
  MED-001 sold 10 -> 5.0/day -> 120/5   = 24.0 days of stock
  MED-002 sold  3 -> 1.5/day ->   8/1.5 =  5.3333 days
  MED-004 sold  2 -> 1.0/day ->  30/1   = 30.0 days
  MED-003 / MED-005: no sales -> velocity 0, days_of_stock = None
expiry buckets: expired={MED-005: 25.50}, 0-30={MED-003: 270.00},
  31-90={}, 91-180={MED-002: 25.60}, >180={MED-001+MED-004: 622.50}
"""
from datetime import date
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from pharmaflow import SqliteStore, ingest_file
from pharmaflow.__main__ import main as cli_main
from pharmaflow.analytics import analytics_for_db, run_analytics
from pharmaflow.orchestration import run_daily
from pharmaflow.transform import resolve_model_sql, run_transform

ROOT = Path(__file__).resolve().parents[1]
INV = ROOT / "sample_data" / "inventory.csv"
SALES = ROOT / "sample_data" / "sales.csv"
DATA_DIR = ROOT / "sample_data"
REF = date(2026, 10, 22)


@pytest.fixture()
def loaded_db(tmp_path):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as s:
        ingest_file(INV, s)
        ingest_file(SALES, s)
    return db


# --- 1. DuckDB analytics ------------------------------------------------------

def test_analytics_total_valuation_hand_computed(loaded_db):
    r = analytics_for_db(loaded_db, today=REF)
    assert r.total_skus == 5
    assert r.total_inventory_value == pytest.approx(943.60)
    assert r.sales_window_days == 2


def test_analytics_valuation_by_category_hand_computed(loaded_db):
    r = analytics_for_db(loaded_db, today=REF)
    by_cat = {v.category: v for v in r.valuation_by_category}
    assert by_cat["Pain Relief"].total_value == pytest.approx(565.60)  # 540.00 + 25.60
    assert by_cat["Pain Relief"].sku_count == 2
    assert by_cat["Pain Relief"].total_quantity == 128
    assert by_cat["Vitamins & Rehydration"].total_value == pytest.approx(270.00)
    assert by_cat["First Aid & Antiseptic"].total_value == pytest.approx(82.50)
    assert by_cat["Respiratory"].total_value == pytest.approx(25.50)
    assert sum(v.total_value for v in by_cat.values()) == pytest.approx(943.60)


def test_analytics_velocity_and_days_of_stock_hand_computed(loaded_db):
    r = analytics_for_db(loaded_db, today=REF)
    by_sku = {v.sku: v for v in r.velocity}
    assert by_sku["MED-001"].total_sold == 10
    assert by_sku["MED-001"].velocity_per_day == pytest.approx(5.0)
    assert by_sku["MED-001"].days_of_stock_remaining == pytest.approx(24.0)
    assert by_sku["MED-002"].velocity_per_day == pytest.approx(1.5)
    assert by_sku["MED-002"].days_of_stock_remaining == pytest.approx(5.3333)
    assert by_sku["MED-004"].velocity_per_day == pytest.approx(1.0)
    assert by_sku["MED-004"].days_of_stock_remaining == pytest.approx(30.0)
    assert by_sku["MED-003"].velocity_per_day == 0.0
    assert by_sku["MED-003"].days_of_stock_remaining is None
    assert by_sku["MED-005"].days_of_stock_remaining is None


def test_analytics_expiry_risk_hand_computed(loaded_db):
    r = analytics_for_db(loaded_db, today=REF)
    by_bucket = {b.bucket: b for b in r.expiry_risk}
    assert (by_bucket["expired"].sku_count, by_bucket["expired"].value_at_risk) == (1, pytest.approx(25.50))
    assert (by_bucket["0-30"].sku_count, by_bucket["0-30"].value_at_risk) == (1, pytest.approx(270.00))
    assert (by_bucket["31-90"].sku_count, by_bucket["31-90"].value_at_risk) == (0, 0.0)
    assert (by_bucket["91-180"].sku_count, by_bucket["91-180"].value_at_risk) == (1, pytest.approx(25.60))
    assert (by_bucket[">180"].sku_count, by_bucket[">180"].value_at_risk) == (2, pytest.approx(622.50))
    assert by_bucket[">180"].total_quantity == 150
    assert r.value_at_risk_30d == pytest.approx(295.50)  # 25.50 + 270.00


def test_analytics_cli(loaded_db, capsys):
    assert cli_main(["analytics", "--db", str(loaded_db), "--today", "2026-10-22"]) == 0
    out = capsys.readouterr().out
    assert "Total inventory value: 943.60" in out
    assert "Value at risk (expired + <=30d): 295.50" in out


# --- 2. Executed transformation (dbt model SQL via DuckDB) --------------------

def test_transform_executes_staging_to_mart_from_store(loaded_db, tmp_path):
    result = run_transform(db_path=loaded_db, duckdb_path=tmp_path / "wh.duckdb")
    assert result.model_row_counts == {"stg_inventory": 5, "stg_sales": 3, "inventory_current": 5}
    assert len(result.tests) == 8
    assert result.all_tests_passed, [t.name for t in result.tests if not t.passed]


def test_transform_from_csv_seeds_matches_store_path(tmp_path):
    result = run_transform(data_dir=DATA_DIR, duckdb_path=tmp_path / "wh.duckdb")
    assert result.model_row_counts["stg_inventory"] == 5
    assert result.model_row_counts["inventory_current"] == 5
    assert result.all_tests_passed


def test_transform_schema_tests_catch_duplicate_sku(tmp_path):
    # Seed with a duplicated SKU: stg_inventory.sku unique test must FAIL
    # (proves the tests are really checking, not rubber-stamping).
    bad = tmp_path / "bad_seed"
    bad.mkdir()
    inv_text = INV.read_text()
    first_row = inv_text.splitlines()[1]
    (bad / "inventory.csv").write_text(inv_text + first_row + "\n")
    (bad / "sales.csv").write_text(SALES.read_text())
    result = run_transform(data_dir=bad, duckdb_path=tmp_path / "wh.duckdb")
    failed = {t.name for t in result.tests if not t.passed}
    assert "unique_stg_inventory_sku" in failed
    assert not result.all_tests_passed


def test_resolve_model_sql_strips_jinja():
    sql = resolve_model_sql("select * from {{ source('raw','raw_inventory') }} join {{ ref('stg_sales') }} on 1=1")
    assert sql == "select * from raw_inventory join stg_sales on 1=1"


# --- 3. Orchestration -----------------------------------------------------------

def test_run_daily_happy_path_end_to_end(tmp_path):
    db = tmp_path / "pharma.db"
    result = run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF)
    assert result.succeeded, [(o.name, o.error) for o in result.outcomes]
    assert [o.name for o in result.outcomes] == ["ingest", "transform", "report"]
    assert all(o.attempts == 1 for o in result.outcomes)
    ingest = result.outcomes[0].result
    assert ingest["inventory_skus"] == 5 and ingest["sales_records"] == 3
    transform = result.outcomes[1].result
    assert transform["models"] == {"stg_inventory": 5, "stg_sales": 3, "inventory_current": 5}
    report = result.outcomes[2].result
    assert report["total_inventory_value"] == pytest.approx(943.60)
    with SqliteStore(db) as s:
        assert len(s.inventory) == 5


def test_run_daily_failing_transform_retries_then_aborts_downstream(tmp_path):
    # Bad seed dir for the transform: inventory.csv missing -> transform
    # raises FileNotFoundError on every attempt. Ingest (from the good
    # data dir) succeeds; report must be SKIPPED, never executed.
    db = tmp_path / "pharma.db"
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    result = run_daily(db, DATA_DIR, logical_date=REF, retries=2, today=REF,
                       transform_data_dir=empty_dir)
    assert not result.succeeded
    assert result.failed_task == "transform"
    assert result.status_of("ingest") == "success"
    assert result.status_of("transform") == "failed"
    transform_outcome = [o for o in result.outcomes if o.name == "transform"][0]
    assert transform_outcome.attempts == 3  # 1 initial + 2 retries
    assert "FileNotFoundError" in (transform_outcome.error or "")
    assert result.status_of("report") == "skipped"
    report_outcome = [o for o in result.outcomes if o.name == "report"][0]
    assert report_outcome.attempts == 0 and report_outcome.result is None


def test_run_daily_recovery_rerun_same_date_no_duplicates(tmp_path):
    # Run for date D fails midway (transform), then re-running the SAME
    # date completes — and M2 idempotency means inventory is not doubled.
    db = tmp_path / "pharma.db"
    empty_dir = tmp_path / "empty"
    empty_dir.mkdir()
    first = run_daily(db, DATA_DIR, logical_date=REF, retries=1, today=REF,
                      transform_data_dir=empty_dir)
    assert not first.succeeded and first.failed_task == "transform"
    with SqliteStore(db) as s:
        counts_after_failure = (len(s.inventory), len(s.sales), len(s.quarantine))
    assert counts_after_failure == (5, 3, 0)

    second = run_daily(db, DATA_DIR, logical_date=REF, retries=1, today=REF)
    assert second.succeeded, [(o.name, o.error) for o in second.outcomes]
    # Ingest on the re-run is a recognised duplicate, not new data.
    imports = second.outcomes[0].result["imports"]
    assert all(i["already_imported"] for i in imports)
    with SqliteStore(db) as s:
        counts_after_recovery = (len(s.inventory), len(s.sales), len(s.quarantine))
    assert counts_after_recovery == counts_after_failure == (5, 3, 0)


def test_run_daily_cli(tmp_path, capsys):
    db = tmp_path / "pharma.db"
    rc = cli_main(["run-daily", "--db", str(db), "--data-dir", str(DATA_DIR), "--date", "2026-10-22"])
    out = capsys.readouterr().out
    assert rc == 0
    assert "Pipeline succeeded: True" in out
    assert "[success] ingest" in out and "[success] transform" in out and "[success] report" in out
