"""PharmaFlow benchmarks — M6 methodology, M7 re-run.

Generates synthetic inventory CSVs deterministically (seed 42) at
1k / 10k / 50k rows and times, against the durable SQLite store:

* ingestion (rows/sec),
* dedupe re-import of the identical bytes (file-hash no-op path),
* reconciliation (expected-vs-actual over all SKUs, with ~1% drift
  injected into the expected counts so discrepancies are produced),
* a DuckDB analytics run (``pharmaflow.analytics.run_analytics`` —
  loads the store into DuckDB and aggregates in SQL).

Writes ``benchmarks/RESULTS.md`` next to this file with the machine /
Python / library context and the measured numbers. Since M7 the file
contains two labelled runs: the M6 "before" table (row-by-row DuckDB
loader, preserved verbatim below as ``M6_BEFORE``) and the fresh
"after" run this script just measured (M7 bulk/appender loader).

    .venv/bin/python benchmarks/run_benchmarks.py [--sizes 1000,10000,50000]

HONESTY: these are local, single-machine, single-run figures on the
machine described in RESULTS.md. They are NOT production-scale numbers
and no production throughput is claimed. SQLite commits per row by
design (crash-safety over speed — see docs/FAILURE_MODES.md), so
ingestion rows/sec here reflects that trade-off on this hardware.
"""
from __future__ import annotations

import argparse
import csv
import io
import platform
import random
import sys
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pharmaflow import SqliteStore, ingest_bytes  # noqa: E402
from pharmaflow.reconciliation import reconcile  # noqa: E402

try:
    import duckdb

    DUCKDB_VERSION = duckdb.__version__
except ImportError:  # pragma: no cover
    DUCKDB_VERSION = "not installed"

SEED = 42
PRODUCTS = [
    ("Paracetamol 500mg 100 tabs", 4.50),
    ("Ibuprofen 200mg 50 tabs", 3.20),
    ("Vitamin C 1000mg 30 tabs", 6.00),
    ("Antiseptic Solution 250ml", 2.75),
    ("Cough Syrup 120ml", 5.10),
    ("ORS Sachets 10 pack", 1.80),
    ("Bandage Roll 5cm", 0.95),
    ("Amoxicillin 250mg 21 caps", 7.40),
]
HEADER = ["sku", "product_name", "quantity_on_hand", "unit_cost",
          "expiry_date", "reorder_point", "supplier", "batch"]

# The M6 run, preserved verbatim from the RESULTS.md it produced
# (2026-10-23 00:16:57Z, committed in 369a432). The loader then was
# analytics._load_into_duckdb row-by-row; M7 replaced it with the
# appender bulk load. Columns: rows, ingest_s, ingest_rows_per_sec,
# dedupe_s, reconciliation_s, analytics_s.
M6_BEFORE = [
    (1_000, 0.231, 4_336, 0.003, 0.0005, 1.375),
    (10_000, 1.451, 6_890, 0.028, 0.0065, 12.400),
    (50_000, 7.130, 7_013, 0.144, 0.0376, 61.610),
]
M6_BEFORE_DATE = "2026-10-23 00:16:57Z"
M6_BEFORE_COMMIT = "369a432"


def generate_csv_bytes(n: int, seed: int = SEED) -> bytes:
    """Deterministic synthetic inventory CSV with ``n`` unique SKUs."""
    rng = random.Random(seed)
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(HEADER)
    base = date(2026, 1, 1)
    for i in range(n):
        name, cost = PRODUCTS[i % len(PRODUCTS)]
        expiry = base + timedelta(days=rng.randint(30, 900))
        w.writerow([
            f"BENCH-{i:06d}", name, rng.randint(0, 500),
            f"{cost:.2f}", expiry.isoformat(), rng.choice([5, 10, 15, 20]),
            f"Synthetic Supplier {chr(65 + i % 3)}", f"B{i % 97:03d}",
        ])
    return out.getvalue().encode()


def _timed(fn):
    t0 = time.perf_counter()
    result = fn()
    return result, time.perf_counter() - t0


def bench_size(n: int, workdir: Path) -> dict:
    data = generate_csv_bytes(n)
    db = workdir / f"bench_{n}.db"
    store = SqliteStore(db)
    try:
        result, ingest_s = _timed(lambda: ingest_bytes(data, f"bench_{n}.csv", store))
        assert result.rows_valid == n, (result.rows_valid, n)

        dup, dedupe_s = _timed(lambda: ingest_bytes(data, f"bench_{n}.csv", store))
        assert dup.already_imported and dup.duplicates_skipped == n

        inventory = store.inventory
        assert len(inventory) == n
        expected = {sku: rec.quantity_on_hand for sku, rec in inventory.items()}
        # Inject drift into ~1% of SKUs so reconciliation does real work.
        for i, sku in enumerate(sorted(expected)):
            if i % 100 == 0:
                expected[sku] += 7
        actual = {sku: rec.quantity_on_hand for sku, rec in inventory.items()}
        discrepancies, recon_s = _timed(lambda: reconcile(expected, actual))
        assert len(discrepancies) == max(1, n // 100)

        from pharmaflow.analytics import run_analytics

        report, analytics_s = _timed(lambda: run_analytics(store, today=date(2026, 10, 22)))
        assert report.total_skus == n
    finally:
        store.close()
    return {
        "rows": n,
        "ingest_s": ingest_s,
        "ingest_rows_per_sec": n / ingest_s if ingest_s else 0.0,
        "dedupe_s": dedupe_s,
        "reconciliation_s": recon_s,
        "discrepancies": len(discrepancies),
        "analytics_s": analytics_s,
        "total_value": report.total_inventory_value,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", default="1000,10000,50000")
    args = ap.parse_args(argv)
    sizes = [int(s) for s in args.sizes.split(",") if s.strip()]

    results = []
    with tempfile.TemporaryDirectory(prefix="pharmaflow-bench-") as td:
        for n in sizes:
            print(f"benchmarking {n} rows ...", flush=True)
            r = bench_size(n, Path(td))
            print(f"  ingest {r['ingest_s']:.3f}s ({r['ingest_rows_per_sec']:,.0f} rows/s), "
                  f"dedupe {r['dedupe_s']:.3f}s, reconciliation {r['reconciliation_s']:.4f}s, "
                  f"analytics {r['analytics_s']:.3f}s")
            results.append(r)

    try:
        import fastapi

        fastapi_version = fastapi.__version__
    except ImportError:
        fastapi_version = "not installed"

    run_date = time.strftime('%Y-%m-%d %H:%M:%SZ', time.gmtime())
    table_header = [
        "| Rows | Ingest (s) | Ingest (rows/s) | Dedupe re-import (s) | Reconciliation (s) | DuckDB analytics (s) |",
        "|---:|---:|---:|---:|---:|---:|",
    ]

    def fmt_row(rows, ingest_s, rps, dedupe_s, recon_s, analytics_s):
        return (f"| {rows:,} | {ingest_s:.3f} | {rps:,.0f} | "
                f"{dedupe_s:.3f} | {recon_s:.4f} | {analytics_s:.3f} |")

    before_50k = next((b for b in M6_BEFORE if b[0] == 50_000), None)
    after_50k = next((r for r in results if r["rows"] == 50_000), None)
    speedup = (
        before_50k[5] / after_50k["analytics_s"]
        if before_50k and after_50k and after_50k["analytics_s"] else None
    )

    lines = [
        "# PharmaFlow — Benchmark Results",
        "",
        "> **What these numbers are:** local, single-machine, single-run",
        "> measurements from `benchmarks/run_benchmarks.py` on the machine",
        "> described below, against the durable SQLite store. They are",
        "> **NOT production-scale figures** and no production throughput",
        "> is claimed. Re-running on different hardware will give",
        "> different numbers; that is expected and is why the conditions",
        "> are recorded.",
        "",
        "Two labelled runs are kept side by side: **M6 (before)** — the",
        "original row-by-row DuckDB loader — and **M7 (after)** — the",
        "same methodology and seed, with the M7 appender bulk load.",
        "Analytical results are identical between the two loaders; that",
        "is asserted by the correctness gate in `tests/test_m7.py`",
        "(old vs new loader, sample data + generated 5k dataset), not",
        "just by the totals asserted inside this benchmark.",
        "",
        f"## M6 (before) — {M6_BEFORE_DATE}, commit {M6_BEFORE_COMMIT}",
        "",
        "Conditions: same format as the M7 run below (same machine class,",
        "Python 3.12.3, duckdb 2.0.0, seed 42 datasets, SQLite store in a",
        "temp dir, `time.perf_counter()`, one run per size). The only",
        "difference that matters: `analytics._load_into_duckdb` inserted",
        "every row with its own `con.execute(INSERT …)`.",
        "",
        *table_header,
    ]
    for b in M6_BEFORE:
        lines.append(fmt_row(*b))
    lines += [
        "",
        f"## M7 (after) — {run_date}, bulk-load loader (the M7 commit containing this file; parent {M6_BEFORE_COMMIT})",
        "",
        "### Conditions",
        "",
        f"- Date (UTC): {run_date}",
        f"- OS / platform: {platform.platform()}",
        f"- Machine: {platform.machine()} (processor field: {platform.processor() or 'n/a'})",
        f"- Python: {platform.python_version()} ({sys.implementation.name})",
        f"- duckdb: {DUCKDB_VERSION}; fastapi: {fastapi_version}",
        f"- Datasets: synthetic inventory CSVs generated deterministically "
        f"(seed {SEED}) by `generate_csv_bytes` in `run_benchmarks.py`; "
        "unique SKUs `BENCH-000000…`, 8 rotating synthetic products.",
        "- Store: `SqliteStore` (SQLite file in a temp dir). The SQLite "
        "store commits per row by design (crash-safety over speed — see "
        "`docs/FAILURE_MODES.md`); ingestion rows/sec reflects that.",
        "- Timing: `time.perf_counter()` around each operation, one run "
        "per size (no warm-up, no averaging) — treat small timings as "
        "noisy.",
        "- Reconciliation: expected counts = actual counts with +7 drift "
        "injected into every 100th SKU (~1% discrepancies).",
        "- DuckDB analytics: `run_analytics` loads the SQLite store into "
        "in-memory DuckDB and aggregates in SQL (valuation, velocity, "
        "expiry buckets). M7 load: raw rows streamed from the SQLite "
        "connection in `fetchmany` chunks into DuckDB's appender "
        "(`Appender.append_rows`); the analytics SQL is unchanged.",
        "",
        "### Results",
        "",
        *table_header,
    ]
    for r in results:
        lines.append(fmt_row(
            r["rows"], r["ingest_s"], r["ingest_rows_per_sec"],
            r["dedupe_s"], r["reconciliation_s"], r["analytics_s"],
        ))
    if speedup is not None:
        lines += [
            "",
            f"**Analytics at 50,000 rows: {before_50k[5]:.3f}s (M6) → "
            f"{after_50k['analytics_s']:.3f}s (M7) — {speedup:.1f}× faster**, "
            "with identical analytical results (see the correctness gate "
            "above).",
        ]
    lines += [
        "",
        "Correctness at these sizes (row counts, dedupe no-op, discrepancy",
        "counts, analytics SKU totals) is asserted inside the benchmark run",
        "itself and in `tests/test_benchmarks.py` — timing is never asserted.",
        "",
        "## Reading the numbers",
        "",
        "- Ingestion scales roughly linearly (thousands of rows/s at every",
        "  size on this machine) — the per-row-commit SQLite trade-off above.",
        "- Dedupe re-import is a file-hash check: near-instant at every",
        "  size, by design.",
        "- M6's analytics figure was dominated by *loading*: profiling at",
        "  50k rows (2026-10-23) split the old `run_analytics` into",
        "  materialising the store's Python objects ~0.27s, row-by-row",
        "  DuckDB INSERTs ~58.9s of the ~59.2s load, and the SQL",
        "  aggregations themselves ~0.07s. M7's bulk load measured",
        "  ~0.40s for the same 50k rows in profiling (raw SQLite fetch",
        "  ~0.13s + appender ~0.12s + date conversion / overhead).",
        "  Two plausible bulk paths were measured and rejected:",
        "  `executemany` is per-row in DuckDB's Python client (~50.6s at",
        "  50k — no better), and the sqlite-scanner extension is not",
        "  available offline (DuckDB tries to download it).",
        "- What remains in the M7 analytics time is mostly the load",
        "  itself (~0.4s at 50k) plus DuckDB connection/setup; the SQL",
        "  was never the bottleneck and is unchanged.",
        "",
    ]
    out_path = Path(__file__).resolve().parent / "RESULTS.md"
    out_path.write_text("\n".join(lines))
    print(f"wrote {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
