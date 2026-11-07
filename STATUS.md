# PharmaFlow: Status

_Last verified: 2026-11-02 (America/Chicago). All claims below were run locally._

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
- Tests: **114 passed** as of M13 (`python -m pytest -q` in the local
  venv, Python 3.12, pytest 9.1.1). That is 101 through M12 plus 13 M13
  dead-letter-replay tests. See the M13 section below.
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
  do pipeline runs; those have their own gauges. **Superseded in
  part by M11 below:** streaming consumption now has its own durable
  history and gauges; the ingestion counters themselves still only
  describe batch imports.
- The Postgres store does not persist imports (code-only path), so
  durable counters are a SQLite/in-memory-store feature until the
  Postgres path is executed against a real server.

## M11: durable stream-consumption history (2026-11-02)

### What was built: EXECUTED

- Schema version **2 to 3**: `stream_consumption_runs` table added
  (`id`, `group_name`, `topic`, `records_read`, `applied`,
  `duplicates`, `dlq`, `lag_before`, `lag_after`, `recorded_at`).
  Migration is additive: `CREATE TABLE IF NOT EXISTS` plus a version
  bump in `_init_schema`; no existing table or row is altered. A
  hand-built v2 database (frozen v2 schema plus one inventory row)
  opens as v3 with its row intact, then records consumption normally.
- `consume_once` (the CLI helper) records the finished
  `ConsumeResult` on the store when the run *completes* and sets
  `result.record_id`; `record=False` keeps the old consume-only
  behaviour, and stores without `record_stream_consumption` (the
  in-memory store) are left alone. A run interrupted mid-consume
  (including the crash-simulation hook) records nothing.
- Store API on `SqliteStore`: `record_stream_consumption` (returns
  the run id), `latest_stream_consumption(group)`,
  `list_stream_consumption(group, limit)` newest-first (clamped 1
  to 100), `count_stream_consumption(group)`,
  `stream_consumption_totals(group)`, and
  `stream_consumption_totals_by_group()` (one GROUP BY query, so
  groups that only appear in runs older than any listing limit are
  still included in `/metrics`). A recording failure raises rather
  than silently losing the run record.
- `pharmaflow/observability.py`: `render_stream_consumption_prometheus`
  renders per-group `pharmaflow_stream_consumption_runs_total`,
  `pharmaflow_stream_consumed_read_total`,
  `pharmaflow_stream_consumed_applied_total`,
  `pharmaflow_stream_consumed_duplicates_total`, and
  `pharmaflow_stream_consumed_dlq_total` gauges; an empty mapping
  renders nothing.
- API (version 0.6.0): `GET /stream-consumption` (newest first,
  `?group=`, `?limit=`, clamped 1 to 100), `GET
  /stream-consumption/latest` (404 when no run exists, `?group=`
  supported), and `/metrics` renders the per-group consumption
  gauges when any run is recorded. No recorded run means no
  consumption gauges, as before.
- CLI: `stream-consume` now records via `consume_once` and prints
  `Recorded as stream consumption #N (group ...)`; new
  `stream-consumption --db <file> [--group G] [--latest] [--limit N] [--json]`.
- Docs: `docs/OBSERVABILITY.md` updated (new metrics table rows, new
  M11 section, revised limits); README updated (endpoints, M11
  section, layout test count).

### Evidence: EXECUTED (`tests/test_m11.py`, 9 tests)

- `consume_once` records run #1 (4/4/0/0, lag 4 to 0) and a fresh
  store instance on the same file sees it; schema version 3.
- Second consume (no-op catch-up, read 0) appends run #2; totals sum
  (applied 4, runs 2).
- Crash run is not recorded: consumer with `crash_after_applies=2`
  raises after applying 2 events (MED-001 at 100, sale not yet
  applied), history stays empty; recovery `consume_once` records
  run #1 with duplicates=2, applied=2, lag 0.
- `record=False` records nothing; v2 migration test passes.
- Two groups: the second group reads the same 4 events but the
  dedupe table is per-store, not per-group (M4 design: exactly-once
  application regardless of group), so its run records applied 0,
  duplicates 4. Group filtering and per-group totals asserted.
- Prometheus render asserted (both groups, all five gauges; empty
  mapping renders nothing).
- CLI: `stream-produce` + `stream-consume` prints the record line;
  `stream-consumption --json` and `--latest` asserted; unknown group
  prints "No stream consumption runs recorded."
- API: empty history gives `[]` plus 404 plus no consumption gauges;
  after a recorded run, both endpoints serve it and `/metrics`
  carries all five gauges; a second app instance on the same DB
  serves the same history and gauges.
- Live check beyond the tests: real CLI produce (4 events) →
  consume printed `Recorded as stream consumption #1`; a fresh CLI
  process listed it as JSON; a real uvicorn server served
  `/stream-consumption`, `/stream-consumption/latest`, and `/metrics`
  with all five consumption gauges for `reconciliation-v1`.
- **Tests: 91 passed** (82 previous, all still green, plus 9 M11).
  The M9 tests and the SQLite-store schema test that asserted
  `schema_version == 2` were updated to 3 (the current version);
  no behaviour changed, only the version constant.

### Still NOT done (stated plainly)

- Consumption history records completed runs only. A run
  interrupted mid-consume contributes nothing, and consumption under
  the real Kafka/Redpanda adapter (unverified, no broker here) is not
  recorded by this mechanism.
- The Postgres store does not persist consumption runs (its schema
  stays at version 1; the path is code-only and unverified in any
  case).
- Live broker lag/DLQ gauges (M8) remain the source of truth for
  what is waiting right now; the M11 gauges answer what completed
  runs have consumed so far.
- Dead letters themselves were recorded only in the broker DLQ topic
  until M12 below; a poison event's errors and source coordinates
  were not queryable from the operational store. **Superseded by M12.**

## M12: durable dead-letter history (2026-11-04)

### What was built: EXECUTED

- Schema version **3 to 4**: `dead_letters` table added
  (`id`, `group_name`, `event_id`, `topic`, `partition`, `offset`,
  `errors_json`, `recorded_at`). Migration is additive:
  `CREATE TABLE IF NOT EXISTS` plus a version bump in
  `_init_schema`; no existing table or row is altered. A hand-built
  v3 database (frozen v3 schema plus one inventory row) opens as v4
  with its row intact, then records dead letters normally.
- `streaming.DeadLetter` dataclass: the same payload the broker's
  DLQ topic carries (errors plus source topic/partition/offset),
  plus the consumer group that dead-lettered it. `ConsumeResult`
  now collects every poison event the run dead-lettered in
  `dead_letters` (in consume order) and, once `consume_once` has
  persisted them, their store row ids in `dead_letter_ids`. The
  broker DLQ write still happens during the run, unchanged (M4
  behaviour); only the store write waits for completion.
- `consume_once` records each dead letter on the store when the run
  *completes* and sets `result.dead_letter_ids`; `record=False`
  keeps the old consume-only behaviour (the in-memory list is still
  populated; only the durable recording is skipped), and stores
  without `record_dead_letter` are left alone. A run interrupted
  mid-consume (including the crash-simulation hook) records nothing.
- Store API on `SqliteStore`: `record_dead_letter` (returns the
  record id), `latest_dead_letter(group)`,
  `list_dead_letters(group, limit)` newest-first (clamped 1 to 100),
  `count_dead_letters(group)`, and `dead_letter_totals_by_group()`
  (one GROUP BY query, so groups that only appear in rows older
  than any listing limit are still included in `/metrics`). A
  recording failure raises rather than silently losing the record.
- `pharmaflow/observability.py`: `render_dead_letters_prometheus`
  renders per-group `pharmaflow_dead_letters_total` gauges; an
  empty mapping renders nothing.
- API (version 0.7.0): `GET /dead-letters` (newest first,
  `?group=`, `?limit=`, clamped 1 to 100), `GET
  /dead-letters/latest` (404 when no dead letter exists, `?group=`
  supported), and `/metrics` renders the per-group dead-letter
  totals when any dead letter is recorded. No recorded dead letter
  means no dead-letter gauge, as before.
- CLI: `stream-consume` now prints
  `Recorded N dead letter(s) (group ...)` when the completed run
  dead-lettered any; new `dead-letters --db <file> [--group G]
  [--latest] [--limit N] [--json]`.
- Docs: `docs/OBSERVABILITY.md` updated (new metrics table row,
  new M12 section, revised limits); README updated (endpoints, M12
  section, test count).

### Evidence: EXECUTED (`tests/test_m12.py`, 10 tests)

- `consume_once` records dead letters #1 and #2 for a 4-event run
  (2 valid applied, 1 poison sale with quantity -2, 1 unknown
  event type), with errors and source coordinates asserted on both
  the in-memory `DeadLetter` objects and the stored rows; schema
  version 4; the broker DLQ topic holds 2 records (M4 behaviour
  preserved).
- Second group re-reads the same 4 events: valid ones are
  duplicates, poison ones are re-dead-lettered under the second
  group's name (rows #3 and #4); per-group totals {2, 2}.
- Crash run records nothing: poison dead-lettered to the broker
  before the crash, history stays empty; recovery `consume_once`
  records run #1 with duplicates=1, applied=1, dlq=1 and dead
  letter #1; MED-001 ends at 90.
- `record=False` records nothing (consumption or dead letters)
  while the in-memory `dead_letters` list is still populated.
- v3 migration test passes; malformed event with no event id is
  recorded with `event_id` empty and the unknown-type error.
- Store API: group filtering, latest per group, limit clamping
  (0 behaves as 1, 500 as 100), per-group totals.
- Prometheus render asserted (both groups; empty mapping renders
  nothing).
- CLI: `stream-produce` + `stream-consume` prints the dead-letter
  record line; `dead-letters --json` and `--latest` asserted;
  unknown group prints "No dead letters recorded."
- API: empty history gives `[]` plus 404 plus no dead-letter
  gauge; after a recorded run, both endpoints serve the rows and
  `/metrics` carries `pharmaflow_dead_letters_total{group=...} 2`;
  a second app instance on the same DB serves the same history and
  gauge.
- Live check beyond the tests: real CLI produce (4 events, 2
  poison) → consume printed `Recorded as stream consumption #1`
  and `Recorded 2 dead letter(s)`; a fresh CLI process listed both
  as JSON and `--latest` showed #2; a real uvicorn server served
  `/dead-letters`, `/dead-letters/latest`, and `/metrics` with
  `pharmaflow_dead_letters_total{group="reconciliation-v1"} 2`.
- **Tests: 101 passed** (91 previous, all still green, plus 10
  M12). The M9/M11 tests and the SQLite-store schema test that
  asserted `schema_version == 3` were updated to 4 (the current
  version); no behaviour changed, only the version constant.

### Still NOT done (stated plainly)

- Dead-letter history records completed runs only. A poison event
  dead-lettered by a crash-interrupted run lives in the broker DLQ
  topic only, and a poison event re-read after a crash is recorded
  once per completed run that dead-lettered it (the same per-run
  semantics as the consumption history's duplicates column).
- **Superseded by M13 below:** there is now a replay/reprocess
  path — a recorded dead letter can be corrected and re-published
  with its original event id.
- The Postgres store does not persist dead letters (its schema
  stays at version 1; the path is code-only and unverified in any
  case).

## M13: dead-letter replay (2026-11-06)

### What was built: EXECUTED

- Schema version **4 to 5**: `dead_letter_replays` table added
  (`id`, `dead_letter_id`, `group_name`, `event_id`, `topic`,
  `partition`, `offset`, `corrected_json`, `replayed_at`).
  Migration is additive: `CREATE TABLE IF NOT EXISTS` plus a
  version bump in `_init_schema`; no existing table or row is
  altered. A hand-built v4 database (frozen v4 schema plus one
  inventory row) opens as v5 with its row intact, then records
  dead letters and replays normally.
- `streaming.DeadLetterReplay` dataclass plus
  `replay_dead_letter(log_dir, db_path, record_id, corrections=None,
  group=None)`: reads the original record back from the broker at
  the dead letter's source coordinates (and refuses when the record
  at those coordinates carries a different event id, or is absent
  entirely), overlays the caller's field corrections, validates the
  result through the same `parse_event` rules the consumer applies,
  and re-publishes it to the original topic via
  `EventProducer.publish` (validated publish path). The event id is
  immutable — a correction changing it is rejected, because dedupe
  is keyed on it — except when the original record carried none, in
  which case the corrections must supply one. Omitting corrections
  re-publishes the original payload unchanged, which is the right
  fix for a state failure at apply time (a sale for a SKU that did
  not exist yet) rather than a malformed payload. A payload that
  still fails validation raises `EventValidationError` and records
  nothing; the dead letter stays replayable. The replay is appended
  to `dead_letter_replays` and its row id is set on
  `replay.replay_id`. A recording failure raises rather than
  silently losing the record.
- The replay never applies an event directly: the corrected event
  flows through the normal consumer path, so the outcome (applied /
  duplicate / dead-lettered again) is decided by the next
  `stream-consume` run and shows up in its consumption history.
  Replaying the same dead letter twice appends two replay rows;
  the consumer's event-id dedupe makes the second copy a no-op.
- Store API on `SqliteStore`: `get_dead_letter(record_id)`,
  `record_dead_letter_replay` (returns the replay id),
  `latest_dead_letter_replay(dead_letter_id, group)`,
  `list_dead_letter_replays(dead_letter_id, group, limit)`
  newest-first (clamped 1 to 100),
  `count_dead_letter_replays(group)`, and
  `dead_letter_replay_totals_by_group()` (one GROUP BY query, so
  groups that only appear in rows older than any listing limit are
  still included in `/metrics`).
- `pharmaflow/observability.py`: `render_dead_letter_replays_prometheus`
  renders per-group `pharmaflow_dead_letter_replays_total` gauges;
  an empty mapping renders nothing. The gauge counts re-publishes,
  not applications — that distinction is stated in the docstring.
- API (version 0.8.0): `POST /dead-letters/{record_id}/replay`
  (201; body `{"corrections": {...}}`, empty means re-publish
  unchanged; 404 unknown id, 422 still-invalid corrections, 400
  unproceedable replay, 503 when no broker log directory is
  configured via `PHARMAFLOW_STREAM_LOG_DIR`), `GET
  /dead-letter-replays` (newest first, `?dead_letter_id=`,
  `?group=`, `?limit=`, clamped 1 to 100), `GET
  /dead-letter-replays/latest` (404 when no replay exists), and
  `/metrics` renders the per-group replay totals when any replay
  is recorded. No recorded replay means no replay gauge, as before.
- CLI: `dead-letter-replay --db <file> --log-dir <dir> --id N
  [--corrections '{"field": value}'] [--group G]` (exit 2 on bad
  JSON, unknown id, or still-invalid corrections) and
  `dead-letter-replays --db <file> [--dead-letter-id N] [--group G]
  [--latest] [--limit N] [--json]`.
- Docs: `docs/OBSERVABILITY.md` updated (new metrics table row,
  new M13 section, revised limits); README updated (endpoints, M13
  section, test count).

### Evidence: EXECUTED (`tests/test_m13.py`, 13 tests)

- Corrected sale (quantity -2 to 5) replays as replay #1 at the
  next broker offset; the next consume applies it (MED-001
  100 to 95); the dead letter stays in history and the replay row
  carries the corrected payload plus broker coordinates.
- Replay without corrections of a parse-failing payload raises
  `EventValidationError` and publishes/records nothing.
- Replay without corrections of a state-failure event (sale for
  unknown SKU) succeeds once the SKU exists (MED-009: 50 stocked,
  then 43 after the replayed sale of 7).
- Double replay: two replay rows, next consume applies 1 and
  counts 1 duplicate (MED-001 ends at 95, not 90).
- Unknown id raises `ValueError` and publishes nothing; a
  correction changing `event_id` is rejected; a missing broker
  record (wrong log dir) is refused.
- A malformed event with no event id cannot replay without one,
  then replays with a full corrected event (MED-001 ends at 3).
- v4 migration test passes; store API filtering/limit-clamping
  and per-group totals asserted; Prometheus render asserted (both
  groups; empty mapping renders nothing).
- CLI: `stream-produce` + `stream-consume` + `dead-letter-replay
  --corrections` + second consume asserted end to end;
  `dead-letter-replays --json` and `--latest` asserted; bad JSON
  and unknown id exit 2 and record nothing.
- API: empty history gives `[]` plus 404 plus no replay gauge;
  unknown id 404, still-invalid corrections 422, re-publish
  unchanged 422; a good replay returns 201 and `/metrics` carries
  `pharmaflow_dead_letter_replays_total{group="reconciliation-v1"} 1`;
  a second app instance on the same DB serves the same history and
  gauge; an app without a broker log dir returns 503 naming
  `PHARMAFLOW_STREAM_LOG_DIR`.
- Live check beyond the tests: real CLI produce (2 events) →
  consume (applied 1, dlq 1) → `dead-letter-replay
  --corrections '{"quantity_sold": 5}'` printed replay #1 at
  partition 2 offset 2 → consume (applied 1, MED-001 95) →
  `dead-letter-replays --json` and `--latest` listed it; a real
  uvicorn server served `/dead-letter-replays`,
  `/dead-letter-replays/latest`, and `/metrics` with
  `pharmaflow_dead_letter_replays_total{group="reconciliation-v1"} 1`,
  then a curl POST replay returned 201 (replay #2, offset 3) and
  the gauge moved to 2, and a POST for id 99 returned 404.
- **Tests: 114 passed** (101 previous, all still green, plus 13
  M13). The M9/M11/M12 tests and the SQLite-store schema test that
  asserted `schema_version == 4` were updated to 5 (the current
  version); no behaviour changed, only the version constant.

### Still NOT done (stated plainly)

- A replayed event that still fails at apply time is
  dead-lettered again by the next consume run as a *new* dead
  letter row; there is no automatic link from the new dead letter
  back to the replay that produced it (the event id is the same,
  so the two rows are correlatable by id, not by a foreign key).
- Replays are recorded only when `replay_dead_letter` is used
  (CLI/API); a corrected event published by hand through
  `stream-produce` is a normal event, not a replay, and is not
  linked to any dead letter.
- The Postgres store does not persist replays (its schema stays
  at version 1; the path is code-only and unverified in any case).

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
- **M11: Durable stream-consumption history (done, 2026-11-02):**
  completed `stream-consume` runs appended to a
  `stream_consumption_runs` table (schema v3, additive v2 to v3
  migration tested); `GET /stream-consumption[/latest]`,
  `stream-consumption` CLI, and per-group summed consumption gauges
  on `/metrics` (API 0.6.0) serve the stored history; 91 tests green.
  Crash-interrupted runs are never recorded; the Postgres store does
  not persist consumption runs.
- **M12: Durable dead-letter history (done, 2026-11-04):**
  each poison event a completed `stream-consume` run dead-letters
  appended to a `dead_letters` table (schema v4, additive v3 to v4
  migration tested) with errors plus source topic/partition/offset;
  `GET /dead-letters[/latest]`, `dead-letters` CLI, and per-group
  `pharmaflow_dead_letters_total` gauges on `/metrics` (API 0.7.0)
  serve the stored history; 101 tests green. Crash-interrupted runs
  record nothing; replay is now M13 (below).
- **M13: Dead-letter replay (done, 2026-11-06):** a recorded dead
  letter can be corrected and re-published with its original event
  id (`dead-letter-replay` CLI, `POST
  /dead-letters/{id}/replay`), validated through the same
  `parse_event` rules, recorded in a `dead_letter_replays` table
  (schema v5, additive v4 to v5 migration tested); `GET
  /dead-letter-replays[/latest]`, `dead-letter-replays` CLI, and
  per-group `pharmaflow_dead_letter_replays_total` gauges on
  `/metrics` (API 0.8.0) serve the stored history; 114 tests green.
  Double replay is deduped by event id at consume time; a replayed
  event that still fails at apply time is dead-lettered again as a
  new row. The Postgres store does not persist replays.

## Next actionable task

Still waiting on a Docker/server environment, unchanged: (1) Postgres:
apply `db/migrations/001_initial_schema.sql` and run the M2 persistence
scenarios against `PostgresStore`; (2) real Redpanda: the
`docs/STREAMING_RUNBOOK.md` checklist. Needs no new infrastructure:
(a) push to GitHub so the CI workflow (incl. the dbt job) actually
runs — the workflow file is still absent from the public branch
(verified 2026-10-28; the GitHub App lacks workflows permission, so
it needs a permission grant or a web-UI upload by Franklin). M8, M9,
M10, and the audit are already on GitHub (remote head 3802646d,
verified 2026-11-01); M11 and M12 are the next push.
(b) recording runs executed under Airflow (DAG wires task callables,
not `run_daily`) is future work, not claimed.
(c) **M14 candidate — replay outcome linkage:** a replayed event
  that still fails at apply time is dead-lettered again as a new
  row, but nothing links that new dead letter back to the replay
  that produced it (the event id is the same, so the rows are
  correlatable by id only). Record, per replay, what the next
  consume run did with the re-published event — applied, duplicate,
  or dead-lettered again (with the new dead-letter id when
  applicable) — so an operator can close the loop without
  cross-referencing two histories by event id.
(Done in M13: dead-letter replay. Done in M12: durable dead-letter history. Done in M11: durable
stream-consumption history. Done in M10:
durable ingestion counters. Done in M9: durable pipeline-run
history. Done in M8: pipeline/streaming metrics: consumer lag and
DLQ count in Prometheus text from the broker's own offsets, plus
per-task pipeline gauges. Done in M7: the bulk-load fix for
`analytics._load_into_duckdb`. 61.610s to 0.490s at 50k in the M7
run, 0.868s on reproduction. Benchmarks re-run, RESULTS.md updated.)
