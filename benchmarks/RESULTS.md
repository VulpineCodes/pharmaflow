# PharmaFlow — Benchmark Results

> **What these numbers are:** local, single-machine, single-run
> measurements from `benchmarks/run_benchmarks.py` on the machine
> described below, against the durable SQLite store. They are
> **NOT production-scale figures** and no production throughput
> is claimed. Re-running on different hardware will give
> different numbers; that is expected and is why the conditions
> are recorded.

Two labelled runs are kept side by side: **M6 (before)** — the
original row-by-row DuckDB loader — and **M7 (after)** — the
same methodology and seed, with the M7 appender bulk load.
Analytical results are identical between the two loaders; that
is asserted by the correctness gate in `tests/test_m7.py`
(old vs new loader, sample data + generated 5k dataset), not
just by the totals asserted inside this benchmark.

## M6 (before) — 2026-10-23 00:16:57Z, commit 369a432

Conditions: same format as the M7 run below (same machine class,
Python 3.12.3, duckdb 2.0.0, seed 42 datasets, SQLite store in a
temp dir, `time.perf_counter()`, one run per size). The only
difference that matters: `analytics._load_into_duckdb` inserted
every row with its own `con.execute(INSERT …)`.

| Rows | Ingest (s) | Ingest (rows/s) | Dedupe re-import (s) | Reconciliation (s) | DuckDB analytics (s) |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.231 | 4,336 | 0.003 | 0.0005 | 1.375 |
| 10,000 | 1.451 | 6,890 | 0.028 | 0.0065 | 12.400 |
| 50,000 | 7.130 | 7,013 | 0.144 | 0.0376 | 61.610 |

## M7 (after) — 2026-10-23 00:34:30Z, bulk-load loader (the M7 commit containing this file; parent 369a432)

### Conditions

- Date (UTC): 2026-10-23 00:34:30Z
- OS / platform: Linux-7.0.0-39-generic-x86_64-with-glibc2.39
- Machine: x86_64 (processor field: x86_64)
- Python: 3.12.3 (cpython)
- duckdb: 2.0.0; fastapi: 0.161.1
- Datasets: synthetic inventory CSVs generated deterministically (seed 42) by `generate_csv_bytes` in `run_benchmarks.py`; unique SKUs `BENCH-000000…`, 8 rotating synthetic products.
- Store: `SqliteStore` (SQLite file in a temp dir). The SQLite store commits per row by design (crash-safety over speed — see `docs/FAILURE_MODES.md`); ingestion rows/sec reflects that.
- Timing: `time.perf_counter()` around each operation, one run per size (no warm-up, no averaging) — treat small timings as noisy.
- Reconciliation: expected counts = actual counts with +7 drift injected into every 100th SKU (~1% discrepancies).
- DuckDB analytics: `run_analytics` loads the SQLite store into in-memory DuckDB and aggregates in SQL (valuation, velocity, expiry buckets). M7 load: raw rows streamed from the SQLite connection in `fetchmany` chunks into DuckDB's appender (`Appender.append_rows`); the analytics SQL is unchanged.

### Results

| Rows | Ingest (s) | Ingest (rows/s) | Dedupe re-import (s) | Reconciliation (s) | DuckDB analytics (s) |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.135 | 7,384 | 0.003 | 0.0006 | 0.178 |
| 10,000 | 1.433 | 6,976 | 0.029 | 0.0067 | 0.136 |
| 50,000 | 7.150 | 6,993 | 0.145 | 0.0384 | 0.490 |

**Analytics at 50,000 rows: 61.610s (M6) → 0.490s (M7) — 125.7× faster**, with identical analytical results (see the correctness gate above).

Correctness at these sizes (row counts, dedupe no-op, discrepancy
counts, analytics SKU totals) is asserted inside the benchmark run
itself and in `tests/test_benchmarks.py` — timing is never asserted.

## Reading the numbers

- Ingestion scales roughly linearly (thousands of rows/s at every
  size on this machine) — the per-row-commit SQLite trade-off above.
- Dedupe re-import is a file-hash check: near-instant at every
  size, by design.
- M6's analytics figure was dominated by *loading*: profiling at
  50k rows (2026-10-23) split the old `run_analytics` into
  materialising the store's Python objects ~0.27s, row-by-row
  DuckDB INSERTs ~58.9s of the ~59.2s load, and the SQL
  aggregations themselves ~0.07s. M7's bulk load measured
  ~0.40s for the same 50k rows in profiling (raw SQLite fetch
  ~0.13s + appender ~0.12s + date conversion / overhead).
  Two plausible bulk paths were measured and rejected:
  `executemany` is per-row in DuckDB's Python client (~50.6s at
  50k — no better), and the sqlite-scanner extension is not
  available offline (DuckDB tries to download it).
- What remains in the M7 analytics time is mostly the load
  itself (~0.4s at 50k) plus DuckDB connection/setup; the SQL
  was never the bottleneck and is unchanged.
