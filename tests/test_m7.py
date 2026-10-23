"""M7 — bulk-load correctness gate.

The M7 optimisation replaced the row-by-row DuckDB load in
``pharmaflow.analytics`` with an appender-based bulk load. The gate:
for the SAME dataset, analytics produced via the old loader and via
the new loader must be IDENTICAL — full report dicts compared, covering
valuation totals + per-category breakdown, per-SKU velocity /
days-of-stock, and every expiry bucket.

Datasets: the hand-checked sample data (via both the SqliteStore raw
path and the generic in-memory-store path) and a generated 5,000-row
dataset (benchmark generator, seed 42, plus deterministic synthetic
sales). Timing is NEVER asserted here — measured numbers live in
benchmarks/RESULTS.md with their conditions.
"""
import sys
from datetime import date
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "benchmarks"))

from run_benchmarks import generate_csv_bytes  # noqa: E402

import pharmaflow.analytics as analytics_module  # noqa: E402
from pharmaflow import PharmaStore, SqliteStore, ingest_bytes, ingest_file  # noqa: E402
from pharmaflow.analytics import run_analytics  # noqa: E402

INV = ROOT / "sample_data" / "inventory.csv"
SALES = ROOT / "sample_data" / "sales.csv"
REF = date(2026, 10, 22)


def _report_via_old_loader(store, monkeypatch):
    monkeypatch.setattr(
        analytics_module, "_load_into_duckdb",
        analytics_module._load_into_duckdb_row_by_row,
    )
    return run_analytics(store, today=REF)


def _assert_identical(old, new):
    # Full-dict equality: reference date, sales window, total value,
    # per-category valuation, EVERY SKU's velocity/days-of-stock, and
    # EVERY expiry bucket must match exactly — not just headline totals.
    assert new.as_dict() == old.as_dict()
    assert new.total_inventory_value == old.total_inventory_value
    assert new.value_at_risk_30d == old.value_at_risk_30d
    assert len(new.velocity) == len(old.velocity) == new.total_skus


def test_bulk_matches_row_by_row_sample_sqlite(tmp_path, monkeypatch):
    db = tmp_path / "pharma.db"
    with SqliteStore(db) as store:
        ingest_file(INV, store)
        ingest_file(SALES, store)
        new = run_analytics(store, today=REF)
        old = _report_via_old_loader(store, monkeypatch)
    _assert_identical(old, new)
    # And the shared expectations still hold (hand-computed in test_m3).
    assert new.total_skus == 5
    assert new.total_inventory_value == pytest.approx(943.60)


def test_bulk_matches_row_by_row_sample_in_memory(monkeypatch):
    # Exercises the generic (non-SqliteStore) bulk path.
    store = PharmaStore()
    ingest_file(INV, store)
    ingest_file(SALES, store)
    new = run_analytics(store, today=REF)
    old = _report_via_old_loader(store, monkeypatch)
    _assert_identical(old, new)
    assert new.total_inventory_value == pytest.approx(943.60)


def _generated_sales_csv(n_skus: int) -> bytes:
    # Deterministic sales over a 3-day window for every 10th bench SKU.
    lines = ["sale_id,sku,quantity_sold,sale_date,unit_price"]
    for j, i in enumerate(range(0, n_skus, 10)):
        day = 20 + (j % 3)  # 2026-10-20..22 -> 3-day window
        lines.append(
            f"BENCH-S{j:05d},BENCH-{i:06d},{(j % 7) + 1},2026-10-{day:02d},9.99"
        )
    return ("\n".join(lines) + "\n").encode()


def test_bulk_matches_row_by_row_generated_5k(tmp_path, monkeypatch):
    db = tmp_path / "bench5k.db"
    with SqliteStore(db) as store:
        inv = ingest_bytes(generate_csv_bytes(5000), "bench_5000.csv", store)
        assert inv.rows_valid == 5000
        sales = ingest_bytes(_generated_sales_csv(5000), "bench_sales.csv", store)
        assert sales.rows_valid == 500
        new = run_analytics(store, today=REF)
        old = _report_via_old_loader(store, monkeypatch)
    _assert_identical(old, new)
    assert new.total_skus == 5000
    assert new.sales_window_days == 3
    # Sales actually flowed into velocity (not an all-zero comparison).
    assert sum(v.total_sold for v in new.velocity) > 0
    assert any(v.days_of_stock_remaining is not None for v in new.velocity)
