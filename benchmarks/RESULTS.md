# PharmaFlow Benchmark Results

> **What these numbers are:** local, single-machine, single-run
> measurements from `benchmarks/run_benchmarks.py` on the machine
> described below, against the durable SQLite store. They are
> **NOT production-scale figures** and no production throughput
> is claimed. Re-running on different hardware will give
> different numbers; that is expected and is why the conditions
> are recorded.

Three labelled runs are kept side by side. **M6 (before)** used the
original row-by-row DuckDB loader. **M7 (after)** used the same
methodology and seed, with the M7 appender bulk load. The
**reproduction run** below re-ran the same script and seed on
2026-10-28 to check the M7 numbers. Analytical results are
identical between the loaders; that is asserted by the correctness
gate in `tests/test_m7.py` (old vs new loader, sample data plus a
generated 5k dataset), not just by the totals asserted inside this
benchmark.

## M6 (before): 2026-10-23 00:16:57Z, commit 369a432

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

## M7 (after): 2026-10-23 00:34:30Z, bulk-load loader (the M7 commit containing this file; parent 369a432)

### Conditions

- Date (UTC): 2026-10-23 00:34:30Z
- OS / platform: Linux-7.0.0-39-generic-x86_64-with-glibc2.39
- Machine: x86_64 (processor field: x86_64)
- Python: 3.12.3 (cpython)
- duckdb: 2.0.0; fastapi: 0.161.1
- Datasets: synthetic inventory CSVs generated deterministically (seed 42) by `generate_csv_bytes` in `run_benchmarks.py`; unique SKUs `BENCH-000000…`, 8 rotating synthetic products.
- Store: `SqliteStore` (SQLite file in a temp dir). The SQLite store commits per row by design. That trades speed for crash safety (see `docs/FAILURE_MODES.md`); ingestion rows/sec reflects that.
- Timing: `time.perf_counter()` around each operation, one run per size (no warm-up, no averaging). Treat small timings as noisy.
- Reconciliation: expected counts = actual counts with +7 drift injected into every 100th SKU (~1% discrepancies).
- DuckDB analytics: `run_analytics` loads the SQLite store into in-memory DuckDB and aggregates in SQL (valuation, velocity, expiry buckets). M7 load: raw rows streamed from the SQLite connection in `fetchmany` chunks into DuckDB's appender (`Appender.append_rows`); the analytics SQL is unchanged.

### Results

| Rows | Ingest (s) | Ingest (rows/s) | Dedupe re-import (s) | Reconciliation (s) | DuckDB analytics (s) |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.135 | 7,384 | 0.003 | 0.0006 | 0.178 |
| 10,000 | 1.433 | 6,976 | 0.029 | 0.0067 | 0.136 |
| 50,000 | 7.150 | 6,993 | 0.145 | 0.0384 | 0.490 |

**Analytics at 50,000 rows: 61.610s (M6) to 0.490s (M7), 125.7x faster**, with identical analytical results (see the correctness gate above).

## Reproduction run: 2026-10-29 01:18:22Z (2026-10-28 evening, America/Chicago)

The same script and seed were re-run on 2026-10-28 during the
portfolio audit, after the M7 code was already in place. The
conditions below come from the output of that run. The direction of
the M7 result reproduced: 50k analytics stayed under one second,
far below the 61.610s M6 figure. The exact M7 numbers did not
reproduce. Analytics at 50k measured 0.868s instead of 0.490s,
and ingestion at 50k measured 4,481 rows/s instead of 6,993
rows/s. Single-run timings on a shared machine vary, so quote the
M7 figures as one measured run and the reproduction as a second
one. A safe summary is that 50k analytics fell from about 62
seconds to under one second, and 50k ingestion measured between
about 4,500 and 7,000 rows/s across the two runs.

### Conditions

- Date (UTC): 2026-10-29 01:18:22Z
- OS / platform: Linux-7.0.0-39-generic-x86_64-with-glibc2.39
- Machine: x86_64 (processor field: x86_64)
- Python: 3.12.3 (cpython)
- duckdb: 2.0.0; fastapi: 0.161.1
- Datasets: synthetic inventory CSVs generated deterministically (seed 42) by `generate_csv_bytes` in `run_benchmarks.py`; unique SKUs `BENCH-000000…`, 8 rotating synthetic products.
- Store: `SqliteStore` (SQLite file in a temp dir), same per-row-commit trade-off as the M7 run.
- Timing: `time.perf_counter()` around each operation, one run per size (no warm-up, no averaging). Treat small timings as noisy.
- Reconciliation: expected counts = actual counts with +7 drift injected into every 100th SKU (~1% discrepancies).
- DuckDB analytics: `run_analytics` with the M7 appender bulk load, analytics SQL unchanged.

### Results

| Rows | Ingest (s) | Ingest (rows/s) | Dedupe re-import (s) | Reconciliation (s) | DuckDB analytics (s) |
|---:|---:|---:|---:|---:|---:|
| 1,000 | 0.200 | 4,991 | 0.006 | 0.0005 | 0.419 |
| 10,000 | 1.427 | 7,006 | 0.028 | 0.0064 | 0.117 |
| 50,000 | 11.157 | 4,481 | 0.655 | 0.0898 | 0.868 |

**Analytics at 50,000 rows: 61.610s (M6) to 0.868s (reproduction), 71.0x faster.** The M7 run measured 0.490s (125.7x). Both runs agree that the bulk load removed the bottleneck. They disagree on the exact time, which is normal for single runs.

Correctness at these sizes (row counts, dedupe no-op, discrepancy
counts, analytics SKU totals) is asserted inside the benchmark run
itself and in `tests/test_benchmarks.py`. Timing is never asserted.

## Reading the numbers

- Ingestion scales roughly linearly (thousands of rows/s at every
  size on this machine). That reflects the per-row-commit SQLite trade-off above.
- Dedupe re-import is a file-hash check: near-instant at every
  size, by design. The reproduction run measured 0.655s at 50k,
  higher than the 0.144s and 0.145s of the M6 and M7 runs. That
  gap is part of the single-run variance noted above, not a
  change in behaviour. The re-import still returned the file-hash
  no-op result in every run.
- M6's analytics figure was dominated by *loading*: profiling at
  50k rows (2026-10-23) split the old `run_analytics` into
  materialising the store's Python objects ~0.27s, row-by-row
  DuckDB INSERTs ~58.9s of the ~59.2s load, and the SQL
  aggregations themselves ~0.07s. M7's bulk load measured
  ~0.40s for the same 50k rows in profiling (raw SQLite fetch
  ~0.13s + appender ~0.12s + date conversion / overhead).
  Two plausible bulk paths were measured and rejected:
  `executemany` is per-row in DuckDB's Python client (~50.6s at
  50k, no better), and the sqlite-scanner extension is not
  available offline (DuckDB tries to download it).
- What remains in the M7 analytics time is mostly the load
  itself (~0.4s at 50k) plus DuckDB connection/setup; the SQL
  was never the bottleneck and is unchanged.
