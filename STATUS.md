# PharmaFlow: Status

_Last verified: 2026-10-30 (America/Chicago). All claims below were run locally._

## Audit re-verification (2026-10-28)

The checks behind this file were re-run on 2026-10-28 during the
final portfolio audit. Nothing below rests on the original run
alone.

- Tests: `.venv/bin/python -m pytest -q` returned **76 passed**
  in 33.20s (Python 3.12.3, pytest 9.1.1). Count by file: 12
  foundation, 10 SQLite store, 13 M3, 10 streaming, 9 API, 3
  benchmark correctness, 3 M7 gate, 7 M8, 9 M9.
- dbt: `dbt seed`, `dbt run`, and `dbt test` in `dbt/` (with
  `--profiles-dir .`) returned **PASS=2, PASS=3, PASS=8** again,
  matching the M3 result.
- Benchmark reproduction: `.venv/bin/python
  benchmarks/run_benchmarks.py` re-ran the same script and seed.
  At 50k rows it measured ingestion 4,481 rows/s and DuckDB
  analytics 0.868s, against the committed M7 figures of 6,993
  rows/s and 0.490s. The direction reproduced (50k analytics under
  one second, down from 61.610s), but the exact M7 numbers did not.
  Both runs are now recorded in `benchmarks/RESULTS.md`, and the
  headline claim is quoted as a range, not as a single exact
  figure. See the M6/M7 benchmark section below.
- Remote CI check: a read-only GitHub API check on 2026-10-28 for
  `.github/workflows/ci.yml` in `VulpineCodes/pharmaflow` returned
  that the path does not exist, and the same check for `.github`
  returned the same result. The workflow file is not on the public
  branch. CI has never run.
- Live CLI checks: `run-daily` on a fresh database printed
  `Recorded as pipeline run #1`, `pipeline-runs` listed it, and
  `stream-metrics` reported lag 4 for `reconciliation-v1` after
  producing the 4 sample events (per-partition lag all in
  partition 2, DLQ 0). These repeat the M8/M9 live checks on the
  current code.

## Verified

- Core ingestion / validation / quarantine / dedup / reconciliation / alerts /
  audit / metrics: **implemented, stdlib-only**.
- **M2 durable persistence (SQLite): implemented and tested.** `SqliteStore`
  (stdlib `sqlite3`) persists imported-file hashes, row hashes, inventory,
  sales, quarantine, and audit; schema creation is idempotent
  (`CREATE TABLE IF NOT EXISTS` plus a `schema_version` table, version 1
  before M9, version 2 from M9).
  Ingestion runs against `StoreProtocol`; the in-memory `PharmaStore`
  remains for unit tests and now implements the same interface.
- Tests: **82 passed** as of M10 (`python -m pytest -q` in the local
  venv, Python 3.12, pytest 9.1.1). That is 76 through M9 plus 6 M10
  durable-counter tests. See the M10 section below.
  Environment note: system pip is externally managed (PEP 668), so all
  M3 work ran in the gitignored `.venv/` (`python3 -m venv .venv`).
- CLI (in-memory): `python -m pharmaflow ingest sample_data/inventory.csv sample_data/sales.csv sample_data/inventory_with_errors.csv --today 2026-10-22`
  gives rows_in=15, valid=11, quarantined=3, duplicates_skipped=1, 4 alerts, audit entries=3.
- CLI (durable, two separate processes, same DB file):
  - Run 1: `python -m pharmaflow ingest sample_data/inventory.csv sample_data/inventory_with_errors.csv --db /tmp/pharmaflow-m2-demo.db --today 2026-10-22`
    gives inventory.csv valid=5; inventory_with_errors.csv valid=3, quarantined=3,
    duplicates_skipped=1; **Inventory SKUs loaded: 8**.
  - Run 2 (same command, new process): both files
    `already_imported=True`, `duplicates_skipped=5` and `7`, valid=0;
    **Inventory SKUs loaded: 8 (unchanged)**, quarantined rows still 3,
    audit entries grew 2 to 4 (`import_skipped_duplicate_file` recorded).
- Data-quality report: `python -m pharmaflow report --db /tmp/pharmaflow-m2-demo.db`
  gives totals rows_in=12, valid=8, quarantined=3, duplicates_skipped=1;
  inventory_with_errors.csv duplicate_rate=14.29%; quarantine reason
  breakdown: business_rule=1, required=1, type=1.

## M3: transformation + orchestration EXECUTED (2026-10-22)

### DuckDB analytics: EXECUTED

- `pip install duckdb` in `.venv`: **succeeded** (duckdb 2.0.0 wheel,
  `duckdb.__version__` reports 2.0.0).
- `pharmaflow/analytics.py`: loads the SQLite store into in-memory DuckDB,
  aggregates in SQL. CLI: `python -m pharmaflow analytics --db <file>`.
- **Hand-verified figures** (computed by hand from the CSVs *before*
  asserting them in tests; reference date 2026-10-22):
  - Total inventory value **943.60** = 120x4.50 + 8x3.20 + 45x6.00 +
    30x2.75 + 5x5.10. By derived category: Pain Relief **565.60**
    (540.00+25.60, 2 SKUs, qty 128), Vitamins & Rehydration 270.00,
    First Aid & Antiseptic 82.50, Respiratory 25.50.
    (Source data has **no category column**. Category is a documented,
    deterministic derivation from product name, not source data.)
  - Sales window = 2026-10-20 to 2026-10-21 = **2 days**. MED-001: sold 10,
    5.0/day, **24.0 days of stock**; MED-002: 3, 1.5/day, 5.3333 days;
    MED-004: 2, 1.0/day, 30.0 days; MED-003/MED-005: no sales,
    velocity 0, days-of-stock `None` (unknown, not infinite).
  - Expiry buckets: expired {MED-005, -51d, **25.50**}, 0-30 {MED-003,
    +19d, **270.00**}, 31-90 {}, 91-180 {MED-002, +144d, 25.60},
    over 180 {MED-001 +251d, MED-004 +466d, **622.50**}.
    Value at risk (expired plus 30 days or less) = **295.50**.

### dbt execution: RAN GREEN (real dbt, not just the fallback)

- Install: **one attempt**, `pip install dbt-duckdb` in `.venv`,
  **succeeded** (about 162s, within the attempt): dbt-core **1.12.6**,
  dbt-duckdb **1.11.0**.
- Configured `dbt/profiles.yml` (local DuckDB, no credentials) and seeds
  in `dbt/seeds/` (sample CSVs plus `_file_hash` and `_imported_at` columns).
- One fix was needed: `sources.yml` now sets `schema: main`. The first
  `dbt run` failed with `schema "raw" does not exist` because seeds land
  in DuckDB's `main` schema. After the fix:
  - `dbt seed`: **PASS=2** (raw_inventory 5 rows, raw_sales 3 rows)
  - `dbt run`: **PASS=3** (stg_inventory, stg_sales views;
    inventory_current table)
  - `dbt test`: **PASS=8** (all declared not_null/unique tests)
- Re-run on 2026-10-28 returned the same PASS=2/3/8 counts.
- **Fallback also executed:** `pharmaflow/transform.py` runs the same
  model SQL from `dbt/models/` via DuckDB (Jinja `source()` and `ref()`
  resolved) plus the same 8 schema checks as SQL. Model counts:
  stg_inventory=5, stg_sales=3, inventory_current=5, all checks pass.
  A duplicate-SKU seed correctly *fails* `unique_stg_inventory_sku`
  (proves the checks bite). This is the transform path the daily runner
  uses, so the pipeline does not require dbt to be installed.
  **Label: both paths ran. dbt is the real one. transform.py is the
  no-dbt execution path for the identical SQL.**

### Orchestration without a server: EXECUTED (DAG itself: not run)

- Task logic refactored into importable callables in
  `pharmaflow/orchestration.py` (`ingest_task`, `transform_task`,
  `report_task`); `airflow/dags/pharmaflow_daily.py` now only wires them.
  **No Airflow server exists here, so the DAG is not claimed executed.
  Its callables are, via the local runner below.**
- Local runner: `python -m pharmaflow run-daily --db <file> --data-dir sample_data`
  gives ingest, transform, report all `[success]` (attempts=1),
  `Pipeline succeeded: True`; report task totals value 943.60, at-risk 295.50.
- Retry/abort evidence (test): transform pointed at an empty seed dir
  fails with `FileNotFoundError`, is attempted **3 times** (1 plus 2
  retries), then the pipeline aborts: `failed_task=transform`,
  `report` status **`skipped` with attempts=0** (downstream not run).
- **Recovery/backfill evidence (test):** run for 2026-10-22 fails at
  transform; counts after failure = **(inventory 5, sales 3,
  quarantine 0)**. Re-running the *same date* with the transform fixed
  succeeds; ingest reports `already_imported=True` for both files and
  counts after recovery are **identical: (5, 3, 0)**. M2 idempotency
  makes the re-run safe, with no duplicated inventory.

## M4: event streaming EXECUTED on the file-log broker (2026-10-22)

### File-log broker + idempotent consumer: EXECUTED (real Kafka/Redpanda: NOT run)

- `pharmaflow/streaming.py` (stdlib only): broker abstraction with
  `FileLogBroker` (durable append-only JSONL partitions plus JSON committed
  offsets, **the tested path**) and `KafkaBroker` (kafka-python-style
  adapter, guarded import, **never connected to a broker: no Docker and
  no broker in this environment**). Redpanda service added to
  `docker-compose.yml` (untested, like the rest of that file) and
  `docs/STREAMING_RUNBOOK.md` documents the real-broker bring-up
  (`rpk topic create`, produce/consume/group-describe) as **unverified**,
  with a checklist that must pass before the label changes.
- Events: `StockReceived` / `SaleRecorded` / `InventoryAdjusted`
  dataclasses; validation reuses the batch rules
  (`validate_inventory_row` / `validate_sales_row`, SKU pattern).
  Key = SKU, partition = `sha256(sku) % 4`, per-SKU ordering preserved.
- CLI executed: `stream-produce` on `sample_data/stream_events.jsonl`
  gives 4 events (all landed partition 2, offsets 0 to 3); `stream-consume
  --group reconciliation-v1` gives `Lag before: 4`, `read=4 applied=4
  skipped_dlq=0 duplicates=0`, `Lag after: 0`; resulting inventory
  MED-001 = 90 (100 minus 10 sold), MED-002 = 48 (50 minus 2 adjusted).
  A follow-up CLI pair with one poison sale (quantity_sold = -2) and one
  valid sale gives `read=2 applied=1 skipped_dlq=1`, lag back to 0.
- **Crash / redelivery evidence (test):** 4 events seeded; a consumer
  with `crash_after_applies=2` applies 2 events and raises
  `SimulatedCrashError` **before any offset commit**. Lag stays 4, all
  committed offsets stay 0, but 2 dedupe markers persist. A fresh
  consumer (new broker/store/consumer objects, same log dir and DB file)
  re-reads all 4: `duplicates=2, applied=2, dlq=0`, lag 4 to 0. Final
  inventory = exactly-once outcome: MED-001 **90** (the sale applied
  once, not twice), MED-002 **45**, exactly **1** sales row.
  A focused variant crashes after a single sale of 7 from 20 on hand.
  After restart `duplicates=2, applied=0`, stock **13**, 1 sales row.
- **Poison / DLQ evidence (test):** 5 events (valid receipt, invalid
  sale quantity, unknown event type, adjustment for unknown SKU, valid
  sale) give `applied=2, dlq=3, lag_after=0`; DLQ topic holds 3 records
  carrying the validation errors ("quantity_sold must be >= 1",
  "unknown or missing event_type", "unknown sku") plus source
  topic/partition/offset; a re-run reads 0 and does not re-DLQ.
- **Independent groups (test):** `reconciliation-v1` consuming to lag 0
  leaves `alerts-v1` at lag 4 with all-zero offsets; each group then
  sees a newly published event as lag 1 independently.
- **Bookkeeping (test):** 10 events over 3 SKUs. Same key always maps
  to the same partition; per-partition offsets are gapless from 0 and
  monotonic; end offsets sum to 10; hand-committing offset 2 on one
  partition drops group lag 10 to 8; committing a lower offset does not
  rewind; a new broker instance on the same directory sees identical
  records and offsets.
- Design note: the M4 design said poison events go to the DLQ after
  failing validation **3x**; as implemented they are dead-lettered on
  first failure (a deterministic validation failure cannot succeed on
  retry). Recorded in `docs/STREAMING_DESIGN.md`.

## M5 + M6: serving verified, benchmarks, CI/dbt, Terraform review (2026-10-22)

### FastAPI serving: EXECUTED (was: skeleton, not run)

- Installs into `.venv`: fastapi **0.161.1**, uvicorn **0.54.0**,
  httpx **0.28.1**, python-multipart **0.0.32** (multipart is required
  by FastAPI file uploads, added to `requirements.txt` and
  `requirements-dev.txt`).
- `api/app.py` rewritten as `create_app(db_path)` factory wired to
  `SqliteStore` at env `PHARMAFLOW_DB` (default `pharmaflow.db`):
  `GET /health`, `POST /imports` (multipart CSV bytes to ingest result),
  `GET /imports`, `GET /inventory`, `GET /alerts` (`?today=` pins the
  date), `GET /reconciliation`, `GET /metrics` (Prometheus text),
  `GET /data-quality`.
- **Live uvicorn run executed:** server on port 8765 with
  `PHARMAFLOW_DB=/tmp/m5.db`; curl verified `/health` (0 SKUs),
  `POST /imports` with `sample_data/inventory.csv` (rows_valid=5),
  `/inventory` (MED-001 120 at 4.50, and the rest), `/alerts?today=2026-10-22`
  (MED-002 low_stock, and the rest), `/metrics`, `/data-quality`.
- **API tests (TestClient, `tests/test_api.py`, 9 tests):** ingest a
  CSV through the API and read it back via `/inventory`; duplicate
  POST is idempotent (`already_imported=True`, inventory unchanged);
  `/metrics` contains the counters (`pharmaflow_rows_in_total 5`, and others);
  alerts reflect the sample data (MED-002/MED-005 low_stock, MED-005
  expired, MED-003 expiring_soon on 2026-10-22); reconciliation and
  data-quality endpoints verified; **a second app instance on the same
  DB file serves the same inventory** (persistence, not in-memory);
  invalid CSV gives 422.
- One real fix: FastAPI's threadpool hit
  `sqlite3.ProgrammingError: SQLite objects created in a thread`.
  `SqliteStore` now connects with `check_same_thread=False`
  (documented in code; sqlite3 threadsafety=3 on this runtime
  serialises the shared connection).

### Benchmarks: EXECUTED (`benchmarks/run_benchmarks.py` to `benchmarks/RESULTS.md`)

- Deterministic (seed 42) synthetic inventory CSVs at 1k / 10k / 50k
  rows, timed against `SqliteStore` on this machine. **Conditions are
  recorded in RESULTS.md** (Linux-7.0.0-39-generic x86_64,
  Python 3.12.3, duckdb 2.0.0, single run per size, `perf_counter`).
  Final committed run (2026-10-23 00:16 UTC):
  - Ingestion: 1k 0.231s (**4,336 rows/s**), 10k 1.451s (**6,890 rows/s**),
    50k 7.130s (**7,013 rows/s**).
  - Dedupe re-import: 0.003s / 0.028s / 0.144s (file-hash no-op).
  - Reconciliation: 0.0005s / 0.0065s / 0.0376s (about 1% discrepancies).
  - DuckDB analytics: 1.375s / 12.400s / **61.610s**. That figure was
    dominated by the row-by-row Python load into DuckDB (`_load_into_duckdb`),
    not the SQL; called out in RESULTS.md as future bulk-load work.
    **Fixed in M7 (below): 61.610s to 0.490s at 50k in the M7 run.**
  - **These are local single-machine figures, explicitly NOT
    production scale.** An earlier run the same evening gave ingestion
    6,044/5,647/6,100 rows/s. Run-to-run variance is real, and that is why
    conditions plus single-run labelling are recorded.
- Correctness at scale: `tests/test_benchmarks.py` (3 tests) asserts
  counts only (generator determinism; 1k gives 10 discrepancies; 10k gives 100).
  **No timing assertions anywhere.**

### CI: workflow updated, YAML validated / CI run itself: NOT run

- `.github/workflows/ci.yml`: `test` job now installs
  `requirements.txt` plus `requirements-dev.txt` (fastapi/duckdb included)
  before pytest plus CLI smoke; new `dbt` job: `pip install dbt-duckdb`,
  then `dbt seed/run/test` in `dbt/`.
- Validated locally with `python -c yaml.safe_load` (pyyaml in venv):
  **valid YAML**, jobs `test` (5 steps) and `dbt` (4 steps).
- The dbt job's exact commands were also re-run locally in `dbt/`:
  seed **PASS=2**, run **PASS=3**, test **PASS=8** (and again on
  2026-10-28, same counts).
- **CI itself has never run.** The workflow file exists only in the
  local checkout. A read-only GitHub check on 2026-10-28 found no
  `.github/workflows/ci.yml` and no `.github` directory on the public
  branch, consistent with the workflow having been excluded from
  pushes (the GitHub App integration lacks workflows permission).
  Do not read the above as a green CI badge.

### Terraform: reviewed, drift fixed, still UNAPPLIED/UNVALIDATED

- `infra/terraform/main.tf` reviewed against the running architecture:
  added variables `db_name` and `db_username` (default `pharmaflow`,
  matching compose), `db_engine_version` (`16`, matching
  postgres:16-alpine), `api_image`, `api_port` (8000, matching
  Dockerfile/uvicorn); `db_password` is now actually referenced by the
  (commented) DB resource sketch; added an `architecture_summary`
  output. Header now states the status plainly.
- **No terraform binary exists here, so no `fmt`, `validate`, or `plan` was
  run and nothing was applied.** No cloud account is configured.

### Tests

- **57 passed** (`python -m pytest -q` in `.venv`, Python 3.12,
  pytest 9.1.1): 45 previous (all still green) plus 9 API (M5) plus
  3 benchmark-correctness (M6).

## M7: bulk-load analytics fix (2026-10-23)

### Profile first: where the 61.610s actually went (50k rows)

- Materialising the store's Python objects from SQLite: **about 0.27s**.
- Row-by-row DuckDB INSERTs (`con.execute` per row): **about 58.9s** of the
  about 59.2s `_load_into_duckdb` load. That was the bottleneck, as suspected.
- The analytics SQL aggregations themselves: **about 0.07s**. Never the SQL.
- Rejected bulk paths, measured: `executemany` about 50.6s (per-row in
  DuckDB's Python client, no better); sqlite-scanner extension
  **unavailable offline** (DuckDB tries to download it).

### Fix: EXECUTED

- `_load_into_duckdb` now bulk-loads via DuckDB's appender
  (`Appender.append_rows`): for `SqliteStore`, raw rows stream from
  the SQLite connection in `fetchmany(10_000)` chunks (no dataclass
  materialisation); other stores chunk their read views. Analytics
  SQL and output structures are unchanged. The old loader is kept
  verbatim as `_load_into_duckdb_row_by_row` for the gate below.
- Profiling of the new load at 50k: **about 0.40s total** (raw fetch about 0.13s
  plus appender about 0.12s plus date conversion/overhead).

### Correctness gate: PASSED (`tests/test_m7.py`, 3 tests)

- Old loader vs new loader on the same dataset, full report dicts
  asserted identical (valuation totals plus per-category, every SKU's
  velocity/days-of-stock, every expiry bucket): sample data via
  SQLite **and** via the in-memory store, plus a generated 5,000-row
  dataset (seed 42 plus 500 deterministic sales over a 3-day window).

### Re-benchmark: EXECUTED (`benchmarks/RESULTS.md` keeps both runs)

- Same script/seed/conditions format; M6 table preserved as "before"
  (2026-10-23 00:16:57Z, commit 369a432), M7 run labelled "after"
  (2026-10-23). DuckDB analytics: 1k 1.375s to 0.178s, 10k 12.400s to
  0.136s, **50k 61.610s to 0.490s (125.7x faster)**. Target under 10s met.
  (Small-size timings are single-run noisy, as documented.)
- **Reproduction (2026-10-28 audit):** the same script and seed
  measured 50k analytics at **0.868s (71.0x faster than M6)** and 50k
  ingestion at **4,481 rows/s**. The M7 exact figures did not
  reproduce. Quote the improvement as 50k analytics falling from
  about 62s to under 1s, and 50k ingestion at about 4,500 to 7,000
  rows/s across the two runs. Full tables and conditions for all
  three runs are in `benchmarks/RESULTS.md`.
- **Tests: 60 passed** (57 previous, all still green, plus 3 M7 gate).

## M8: streaming + pipeline observability in Prometheus text (2026-10-26)

### What was built: EXECUTED

- `pharmaflow/observability.py` (new, stdlib only): `snapshot_streaming`
  reads lag straight from the broker's own bookkeeping (per-partition
  end offset minus committed offset, per group) and the DLQ count from
  the DLQ topic's own size. There is no second counter that can drift from the
  broker. `render_streaming_prometheus` emits `pharmaflow_stream_lag_records`,
  `pharmaflow_stream_partition_lag_records`, and
  `pharmaflow_stream_dlq_records` gauges with Prometheus-escaped labels.
  `render_pipeline_prometheus` renders a `PipelineResult` (M3) as
  `pharmaflow_pipeline_succeeded`, per-task success, and per-task
  attempts gauges; a skipped task renders success 0, never green.
- CLI: `python -m pharmaflow stream-metrics --log-dir <dir> --group <g>`
  (repeatable) prints the streaming gauges.
- API: `GET /metrics` gains the streaming gauges when a broker is
  configured (`create_app(..., stream_log_dir, stream_groups)` args, or
  `PHARMAFLOW_STREAM_LOG_DIR` plus `PHARMAFLOW_STREAM_GROUPS` env vars).
  Gauges appear only when the log directory already exists, so an
  unconfigured app returns exactly its previous output and a GET never
  creates broker files.
- Docs: `docs/OBSERVABILITY.md` documents every metric, its source of
  truth, and the honest limits (ingestion counters are per-process;
  pipeline runs are not persisted, so no "last run" gauge is served;
  Prometheus/Grafana remain compose declarations, no scraper run).
  Note: that parenthetical about pipeline runs was true in M8 and
  was superseded by M9 below, which persists runs.

### Evidence: EXECUTED (`tests/test_m8.py`, 7 tests)

- Scenario, hand-checkable: 4 sample events plus 1 poison sale
  (quantity -2) produced; `reconciliation-v1` consumes all 5
  (applied 4, DLQ 1, lag 0); `alerts-v1` never consumes (lag 5);
  DLQ count 1; per-partition gauges sum to the group total (5).
- Pipeline render on a failed run (ingest success / transform failed
  after 3 attempts / report skipped): succeeded 0, transform success 0
  with 3 attempts, skipped report success 0.
- CLI `stream-metrics` output and both API cases (configured:
  gauges present; unconfigured: no stream gauges) asserted.
- Live check beyond the tests: `run_daily` on the sample data
  succeeded and its real `PipelineResult` rendered all three tasks at
  success 1, attempts 1.
- **Tests: 67 passed** (60 previous, all still green, plus 7 M8).

### Still NOT done (unchanged, stated plainly)

- No Prometheus server, scraper, dashboard, or alert rule has been run
  (no Docker here); the exposition format is what a scraper would
  read, not a verified scrape. Pipeline runs *are* now persisted
  (M9 below); runs executed under Airflow are not recorded, and the
  Postgres store does not persist runs.

## M9: durable pipeline-run history (2026-10-28)

### What was built: EXECUTED

- Schema version **1 to 2**: `pipeline_runs` table added
  (`id`, `logical_date`, `succeeded`, `failed_task`, `recorded_at`,
  `result_json`). Migration is additive: `CREATE TABLE IF NOT EXISTS`
  plus a version bump in `_init_schema`; no existing table or row is
  altered. A hand-built v1 database (frozen v1 schema plus one inventory
  row) opens as v2 with its row intact, then records runs normally.
- `run_daily(..., persist=True)` (the default) appends the completed
  result, success **or** failure, to `pipeline_runs` in the same
  database and sets `result.run_id`; `persist=False` keeps the old
  in-memory-only behaviour. Every run appends, including re-runs of
  the same logical date; ingest idempotency, not history overwriting,
  is what prevents duplicate inventory. `PipelineResult.to_dict()` and
  `from_dict()` round-trip the full outcome list (results, errors,
  attempts, skipped tasks).
- Store API on `SqliteStore`: `record_pipeline_run` (returns the row
  id), `latest_pipeline_run`, `list_pipeline_runs(limit)` newest-first
  (clamped 1 to 100), `count_pipeline_runs`. A recording failure raises
  rather than silently losing the run record.
- API (version 0.4.0): `GET /pipeline-runs` (newest first, `?limit=`,
  clamped 1 to 100), `GET /pipeline-runs/latest` (404 when no run
  exists), and `/metrics` renders the last stored run's pipeline
  gauges labelled with its `logical_date`, plus
  `pharmaflow_pipeline_runs_total`. No stored run means no pipeline
  gauges, as before. The unlabeled M8 render is unchanged.
- CLI: `pipeline-runs --db <file> [--latest] [--limit N] [--json]`;
  `run-daily` prints `Recorded as pipeline run #N (logical date ...)`.
- Docs: `docs/OBSERVABILITY.md` updated (new metrics/endpoints,
  migration note, revised limits).

### Evidence: EXECUTED (`tests/test_m9.py`, 9 tests)

- Successful run persists and rebuilds identically after reopen
  (run id 1, outcomes ingest/transform/report, totals 943.60/295.50
  via the run itself in the live CLI check below).
- Failed run persists: succeeded false, `failed_task=transform`,
  report skipped with 0 attempts.
- Failed-then-successful re-run of the same date appends rows 1 and
  2; latest is row 2; inventory stays 5 SKUs.
- `persist=False` records nothing; v1 migration test passes;
  serialisation round-trip and labelled Prometheus render asserted.
- API: empty history gives `[]` plus 404 plus no pipeline gauges; after a run,
  `/pipeline-runs/latest` returns the run and `/metrics` carries the
  labelled gauges and `runs_total 1`. CLI `pipeline-runs` (human and
  `--json`) asserted.
- Live check beyond the tests: `run-daily` CLI on a fresh DB printed
  `Recorded as pipeline run #1`; `pipeline-runs` listed it; a real
  uvicorn server served `/pipeline-runs/latest` (200, full outcomes)
  and `/metrics` (labelled pipeline gauges, `runs_total 1`).
- **Tests: 76 passed** (67 previous, all still green, plus 9 M9).

### Still NOT done (stated plainly)

- Runs executed under Airflow are not recorded: the DAG wires the
  individual task callables, not `run_daily`, and the DAG itself has
  never run (no Airflow server). Only local `run-daily` executions
  are persisted.
- `PostgresStore` does not persist runs (its schema stays at version
  1; the path is code-only and unverified in any case).

## M10: durable ingestion counters (2026-10-30)

### What was built: EXECUTED

- `pharmaflow/metrics.py`: `Metrics.from_imports(imports,
  files_already_imported)` builds the six counters from recorded
  import summaries, and `durable_metrics(store)` derives them at read
  time from any store's `list_imports()` plus the count of
  `import_skipped_duplicate_file` audit entries. No new table or
  migration: the M2 imports table and audit table already held
  everything. Each distinct file is counted once, at its first
  import; a duplicate-file re-import adds to
  `files_already_imported` only, never to `rows_in` again. The four
  row counters plus `imports_total` are exactly the data-quality
  report totals, by construction.
- `GET /metrics` (API version 0.5.0) and the CLI `ingest` metrics
  print now serve the durable counters instead of the per-process
  `store.metrics`. The session counters are kept — ingestion still
  increments them, and the foundation tests still assert them — but
  nothing user-facing reads them anymore. The documented M5/M8
  limitation ("a fresh API process starts counters at zero") is
  closed by this milestone.
- Docs: `docs/OBSERVABILITY.md` updated (table row, new M10 section,
  revised limits).

### Evidence: EXECUTED (`tests/test_m10.py`, 6 tests)

- `from_imports` sums verified against hand-built import records
  (15/10/4/1, imports_total 3, skips 2).
- Durable counters equal the DQ report totals on the three sample
  files (rows_in 15, valid 11, quarantined 3, duplicates_skipped 1,
  imports_total 3).
- Duplicate re-import: durable rows_in stays 5,
  files_already_imported 1; session rows_in 10 (difference asserted
  explicitly, not hidden).
- Cross-restart: ingest into a `SqliteStore`, close, reopen a new
  instance on the same file — session counters 0, durable counters
  identical (12/8/3/1, imports 2, skips 1).
- API: second app instance on the same DB serves byte-identical
  `/metrics`; duplicate POST on the new process leaves rows_in 5,
  raises files_already_imported to 1.
- CLI: two separate processes ingesting the same file into the same
  DB print rows_in_total 5 both times, files_already_imported 0 then 1.
- Live check beyond the tests: real uvicorn server on port 8767,
  `POST /imports` (rows_valid 5), server killed and restarted on the
  same DB — `/metrics` still rows_in 5 / imports_total 1, inventory
  intact (MED-001 120); duplicate POST on the restarted server
  returns already_imported=true and files_already_imported 1.
- **Tests: 82 passed** (76 previous, all still green, plus 6 M10).

### Still NOT done (stated plainly)

- The durable counters describe recorded imports only. Streaming
  events consumed into inventory (M4) do not change them, and neither
  do pipeline runs; those have their own gauges.
- The Postgres store does not persist imports (code-only path), so
  durable counters are a SQLite/in-memory-store feature until the
  Postgres path is executed against a real server.

## Acceptance check (foundation slice)

- [x] Valid import persists inventory by SKU
- [x] Invalid rows quarantined with reasons (required / type / business_rule)
- [x] Duplicate file re-import is a no-op; duplicate rows skipped
- [x] Reconciliation detects shrinkage, count drift, missing SKU, overselling
- [x] Low-stock and expiry (30 days or less / expired) alerts
- [x] Partial failure: good rows after bad rows are still imported
- [x] Prometheus text metrics; append-only audit log
- [x] FastAPI skeleton code-complete (M1). **Superseded in M5: installed, wired to SQLite, run live under uvicorn, and covered by TestClient tests.**

## Acceptance check (M2: persistence)

- [x] SQLite store persists file hashes / inventory / sales / quarantine / audit across close+reopen (new store instance, same DB file)
- [x] Duplicate-file idempotency across "restarts": second ingest reports `already_imported`, inventory counts unchanged
- [x] Row-level dedupe also persists across restarts (re-exported duplicate row skipped)
- [x] Quarantine + audit rows persist and are queryable (by row number, source file, action)
- [x] Schema creation idempotent: re-running `_init_schema` and reopening the DB do not fail or duplicate the version row
- [ ] Postgres store executed against a real server. **Not done, no server available** (code + migration prepared, see below)

## Acceptance check (M3: transforms + orchestration)

- [x] DuckDB analytics executed; figures hand-verified from the CSVs and asserted exactly in tests
- [x] dbt installed (one attempt), seeded from sample CSVs, `dbt run` PASS=3 and `dbt test` PASS=8. That followed fixing the source schema (`raw` to `main`). Re-run green 2026-10-28.
- [x] Same transformation SQL executed without dbt via `pharmaflow/transform.py`, with schema checks that demonstrably fail on bad data
- [x] Pipeline task logic in importable callables; DAG wires them (DAG not executed, no Airflow server)
- [x] Local runner executes tasks in DAG order with retries; exhausted task aborts pipeline, downstream skipped and not executed
- [x] Recovery: failed run for a date, re-run of the same date completes with inventory counts unchanged (5/3/0 to 5/3/0)
- [x] All 22 previous tests still pass (35 total)

## Acceptance check (M4: streaming, file-log broker)

- [x] Produce to consume applies StockReceived / SaleRecorded / InventoryAdjusted and updates inventory correctly (90 / 45 in tests; 90 / 48 in the CLI demo)
- [x] Crash after applying events but before offset commit: redelivery deduped by event id; final inventory equals the exactly-once outcome, no duplicate sales rows
- [x] Poison events (schema-invalid, unknown type, unknown SKU) go to the DLQ topic with errors plus source coordinates; offset still commits; later valid events processed
- [x] Two consumer groups progress independently (separate offsets and lag)
- [x] Partition/offset bookkeeping: deterministic key partitioning, gapless monotonic per-partition offsets, offsets never rewind, lag = sum of (end minus committed), state survives broker restart
- [x] All 35 previous tests still pass (45 total)
- [ ] Real Kafka/Redpanda path executed. **Not done, no Docker/broker available** (`KafkaBroker` adapter + compose service + runbook prepared, explicitly unverified)

## Acceptance check (M5: serving)

- [x] FastAPI + uvicorn + httpx + python-multipart installed in `.venv` (versions recorded above)
- [x] API wired to `SqliteStore` via `PHARMAFLOW_DB`; endpoints serve real persisted state (second app instance on same DB reads inventory back)
- [x] `POST /imports` uploads CSV bytes; duplicate POST is idempotent through the API
- [x] `/metrics` exposes the ingestion counters; `/alerts`, `/reconciliation`, `/data-quality` verified against sample data
- [x] Live uvicorn run with curl evidence; 9 TestClient tests green
- [ ] Deployed API. **Not done** (no deployment target/credentials; local serving only)

## Acceptance check (M6: benchmarks / CI / infra review)

- [x] Benchmark script generates seeded 1k/10k/50k datasets and times ingestion, dedupe re-import, reconciliation, DuckDB analytics
- [x] `benchmarks/RESULTS.md` committed with machine/Python/library conditions and actual measured numbers, labelled local single-machine, NOT production scale. A reproduction run (2026-10-28) is now recorded alongside the M6 and M7 runs.
- [x] Benchmark correctness asserted in pytest (counts only, no timing asserts)
- [x] CI workflow installs full dev deps and runs pytest; dbt job added (seed/run/test); YAML validated locally with pyyaml
- [ ] CI workflow executed on GitHub. **Not done.** The workflow file is not on the public branch (checked 2026-10-28). Commands re-run locally instead: dbt PASS=2/3/8.
- [x] Terraform reviewed for consistency with the running architecture; obvious drift fixed
- [ ] Terraform fmt/validate/plan/apply. **Not done, no terraform binary / no cloud account** (explicitly unvalidated)
- [x] All 45 previous tests still pass (57 total)

## Acceptance check (M7: bulk-load analytics)

- [x] Bottleneck profiled and split recorded (load about 59.2s of which about 58.9s row-by-row INSERTs; SQL about 0.07s; materialisation about 0.27s at 50k)
- [x] Bulk load implemented (DuckDB appender; raw SQLite streaming for `SqliteStore`); analytics SQL + outputs unchanged
- [x] Correctness gate: old vs new loader produce identical full reports (sample via SQLite + in-memory; generated 5k dataset)
- [x] Full benchmark re-run; RESULTS.md keeps M6 before (369a432) and M7 after side by side: 50k analytics 61.610s to 0.490s in the M7 run (0.868s on the 2026-10-28 reproduction run)
- [x] All 57 previous tests still pass (60 total)

## Known limitations

- **Postgres path is code only, NOT executed:** `pharmaflow/postgres_store.py`
  (guarded psycopg import) plus `db/migrations/001_initial_schema.sql` plus
  docker-compose mount are prepared and aligned with the SQLite schema, but
  no Postgres server / Docker exists in this environment, so none of it has
  been run. See `docs/POSTGRES.md` for the bring-up/verification checklist.
- No Docker in the current build environment, so docker-compose is untested locally.
- **Airflow DAG not executed** (no Airflow installed/server): only the
  callables it wires were run, via the local runner. Airflow retry/backfill
  *declarations* in the DAG mirror the runner's tested semantics.
  Airflow-executed runs are not recorded in pipeline-run history.
- Metrics counters are durable since M10: `/metrics` and the CLI
  ingest print derive them from the recorded imports + audit trail,
  so a fresh process on the same database serves the same numbers.
  The per-process session counters still exist in-memory (ingestion
  increments them; foundation tests assert them) but nothing
  user-facing reads them. Imports, quarantine, and audit are the
  persisted record, and the DQ report totals equal the durable
  counters by construction.
- Analytics category is *derived* from product names (no source category
  column); sales velocity uses the observed sales window (2 days in the
  sample data). Both definitions are documented in `analytics.py` and
  would need revisiting with real, longer sales history.
- dbt/DuckDB work ran in a local `.venv` (system pip is PEP 668
  externally managed); CI now *defines* a dbt job but the workflow has
  not executed on GitHub (the file is not on the public branch; see the
  audit note at the top of this file).
- **Benchmarks are local single-machine, single-run figures** (see
  `benchmarks/RESULTS.md` for conditions). Since M7 the DuckDB
  analytics number uses the appender bulk load (50k: 0.490s in the M7
  run, 0.868s in the 2026-10-28 reproduction run, vs the
  M6 row-by-row loader's 61.610s, kept in RESULTS.md as "before").
  No figure here says anything about production scale.
- API metrics counters are durable since M10 (derived from the
  store's recorded imports + audit); a fresh API process serves the
  same counters. The DQ endpoint (derived from persisted imports)
  agrees with them by construction.
- Grafana/Prometheus dashboards are still compose declarations only
  (no Docker here); observability verification is the `/metrics`
  endpoint plus DQ report, not a dashboard.
- **Streaming real-broker path is code only, NOT executed:** `KafkaBroker`
  (guarded kafka-python import; its group/offset methods are stubs.
  Real Kafka consumption is group-based polling and the adapter will
  need the most work there) plus the docker-compose `redpanda` service plus
  `docs/STREAMING_RUNBOOK.md` are prepared, but no broker exists here,
  so nothing about Redpanda/Kafka behaviour is claimed. Only the
  `FileLogBroker` semantics are tested. The file log is single-process
  in practice (offsets read-modify-write per consume run; no concurrent
  consumer support is claimed) and fsync-per-record is a durability
  choice, not a performance one. No throughput benchmarks are claimed
  for streaming.
- No cloud deployment, no real pharmacy users, no real data of any kind.

## Roadmap

- **M1: Foundation + vertical slice (done, 2026-10-22).**
- **M2: Persistence + data quality reporting (done for the SQLite path, 2026-10-22):**
  durable SQLite store behind the store protocol, cross-restart idempotency,
  DQ report per import. **Postgres = next:** bring up `PostgresStore` against
  a real server (docker-compose), run the persistence test scenarios against
  it, and only then mark it verified.
- **M3: Orchestration + transforms (done, 2026-10-22):** DuckDB analytics
  executed with hand-verified figures; dbt seed/run/test green on DuckDB
  (re-run green 2026-10-28); same SQL executed via `transform.py`; local daily-pipeline runner with
  retry/abort and same-date recovery proven by tests.
- **M4: Streaming (done for the file-log broker, 2026-10-22):** event
  schemas reusing batch validation, at-least-once delivery with an
  idempotent consumer (event-id dedupe), DLQ for poison events,
  independent consumer groups. All executed and tested against
  `FileLogBroker`. **Real Redpanda/Kafka = unverified:** adapter,
  compose service, and runbook prepared; execution waits on a Docker
  environment (see `docs/STREAMING_RUNBOOK.md` checklist).
- **M5: Infrastructure (partially done, 2026-10-22):** FastAPI serving
  verified against SQLite; Terraform skeleton reviewed for consistency
  (Postgres 16 + container service variables/outputs) but **unapplied
  and unvalidated**. No terraform binary, no cloud account, no
  `terraform plan` in CI until a push exists.
- **M6: Observability + hardening (partially done, 2026-10-22):**
  benchmarks with documented conditions executed and committed
  (`benchmarks/RESULTS.md`, plus a reproduction run recorded
  2026-10-28); `/metrics` verified through the API; CI
  extended with full dev deps + a dbt job (YAML validated, commands
  re-run locally, workflow itself not run on GitHub and not present
  on the public branch). Prometheus/Grafana
  dashboards remain compose-only (no Docker).
- **M7: Analytics bulk load (done, 2026-10-23):** profiled the M6
  bottleneck (row-by-row DuckDB INSERTs, about 58.9s of the about 59.2s load at
  50k; SQL about 0.07s), replaced the loader with DuckDB's appender bulk
  load (raw SQLite streaming for `SqliteStore`), gated on old-vs-new
  identical results (`tests/test_m7.py`), and re-benchmarked:
  50k analytics **61.610s to 0.490s (125.7x)** in the M7 run
  (0.868s, 71.0x, on the 2026-10-28 reproduction run); 60 tests green.
- **M8: Observability (done, 2026-10-26):** streaming lag + DLQ
  gauges in Prometheus text derived from the broker's own offsets,
  per-task pipeline gauges from `PipelineResult`, `stream-metrics`
  CLI, optional streaming gauges on `GET /metrics`, `docs/OBSERVABILITY.md`;
  67 tests green. No Prometheus/Grafana run yet; pipeline runs not persisted
  (superseded by M9, which persists them).
- **M9: Durable run history (done, 2026-10-28):** `run_daily`
  appends every completed run (success or failure, including
  same-date re-runs) to a `pipeline_runs` table (schema v2, additive
  migration from v1 tested); `GET /pipeline-runs[/latest]`,
  `pipeline-runs` CLI, and logical-date-labelled pipeline gauges on
  `/metrics` serve the stored history; 76 tests green. Airflow-executed
  runs are not recorded; the Postgres store does not persist runs.
- **M10: Durable counters (done, 2026-10-30):** ingestion counters
  derived at read time from recorded imports + audit trail
  (`durable_metrics`); `GET /metrics` (API 0.5.0) and the CLI ingest
  print serve them, so a fresh process on the same database serves
  the same numbers (each file counted once; duplicate re-imports
  raise `files_already_imported` only). Verified by 6 new tests plus
  live CLI (two processes) and uvicorn restart checks; 82 tests green.
  Streaming-consumed events do not change these counters; Postgres
  path still code-only.

## Next actionable task

Still waiting on a Docker/server environment, unchanged: (1) Postgres:
apply `db/migrations/001_initial_schema.sql` and run the M2 persistence
scenarios against `PostgresStore`; (2) real Redpanda: the
`docs/STREAMING_RUNBOOK.md` checklist. Needs no new infrastructure:
(a) push to GitHub so the CI workflow (incl. the dbt job) actually
runs — the workflow file is still absent from the public branch
(verified 2026-10-28; the GitHub App lacks workflows permission, so
it needs a permission grant or a web-UI upload by Franklin). M8, M9,
and the audit are already on GitHub (remote head 76e839b0, verified
2026-10-29); M10 is the next push.
(b) recording runs executed under Airflow (DAG wires task callables,
not `run_daily`) is future work, not claimed.
(Done in M9: durable pipeline-run history. Done in M8:
pipeline/streaming metrics: consumer lag and DLQ count in Prometheus
text from the broker's own offsets, plus per-task pipeline gauges.
Done in M7: the bulk-load fix for `analytics._load_into_duckdb`.
61.610s to 0.490s at 50k in the M7 run, 0.868s on reproduction.
Benchmarks re-run, RESULTS.md updated.)
