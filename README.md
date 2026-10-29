# PharmaFlow

PharmaFlow is an inventory and operations data platform for a small
pharmacy. It answers four questions from the records a shop already
keeps: what is on the shelf, what was sold, what is about to expire,
and which rows in an import cannot be trusted. Ingestion is
idempotent, invalid rows are quarantined with reasons instead of
dropped, and every row keeps its lineage back to the file it came
from.

My father ran a small pharmacy in Lagos. During COVID, foot traffic
fell and customers stayed away, while the operation still ran
largely on paper ledgers, spreadsheets, and whoever was available to
make a delivery. The problem was records. When the count in the book
and the count on the shelf disagree, every decision after that is a
guess. PharmaFlow is the system a shop like his could have used to
keep those counts straight.

> **Scope, stated plainly:** inventory and operations only. No
> patient, prescription, or customer data, ever. All sample data is
> synthetic. No regulatory or compliance claims are made. This is a
> portfolio project under active development. It is not a deployed
> product and no real pharmacy uses it.

## Quick start

The ingestion core uses only the Python standard library. Install
the extra packages only for the API, analytics, and tests.

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
```

Ingest the sample data into a durable SQLite store, then run the
analytics and the daily pipeline:

```bash
.venv/bin/python -m pharmaflow ingest sample_data/inventory.csv sample_data/sales.csv --db pharmaflow.db --today 2026-10-22
.venv/bin/python -m pharmaflow analytics --db pharmaflow.db --today 2026-10-22
#   Total inventory value: 943.60. Value at risk (expired plus expiring within 30 days): 295.50.
.venv/bin/python -m pharmaflow run-daily --db pharmaflow.db --data-dir sample_data --date 2026-10-22
#   Ingest, transform, and report all succeed. The run is recorded; list it with:
.venv/bin/python -m pharmaflow pipeline-runs --db pharmaflow.db
.venv/bin/python -m pytest -q
#   76 passed
```

Serve the same store over HTTP:

```bash
PHARMAFLOW_DB=pharmaflow.db .venv/bin/python -m uvicorn api.app:app --port 8000
curl -F file=@sample_data/inventory.csv localhost:8000/imports   # rows_valid=5
curl localhost:8000/inventory                                    # the same 5 SKUs, read back from SQLite
curl -F file=@sample_data/inventory.csv localhost:8000/imports   # already_imported=true
curl "localhost:8000/alerts?today=2026-10-22"                    # MED-002 low_stock, MED-005 expired
curl localhost:8000/metrics
curl localhost:8000/pipeline-runs/latest
```

## Status at a glance

Full evidence, with commands and counts, is in [STATUS.md](STATUS.md).
The design is in [ARCHITECTURE.md](ARCHITECTURE.md).

| Component | Status | Evidence |
|---|---|---|
| CSV ingestion, validation, quarantine, reconciliation, alerts | Verified locally | 76 pytest tests pass; CLI runs above |
| SQLite persistence and idempotency | Verified locally | Tests plus two-process CLI runs in STATUS.md |
| DuckDB analytics and dbt transforms | Verified locally | Hand-checked figures; `dbt seed/run/test` green (PASS=2/3/8), re-run 2026-10-28 |
| Daily pipeline runner and run history | Verified locally | `run-daily` records every run in SQLite (schema v2); API and CLI serve the history |
| Event streaming on the file-log broker | Verified locally | Crash and redelivery tests pass; lag and DLQ gauges served from broker offsets |
| FastAPI serving | Verified locally | Live uvicorn run plus 9 TestClient tests |
| Real Kafka/Redpanda broker | Not run | Adapter, compose service, and runbook are preparation only |
| Postgres store | Not run | Code and migration written; no live server has been used |
| Airflow DAG | Not run | DAG wires the task callables; no Airflow server has run it, and Airflow runs are not recorded |
| Terraform | Reviewed, unapplied, unvalidated | No terraform binary or cloud account; nothing planned or applied |
| CI workflow | Exists locally only | YAML validated; the workflow file is not on the public GitHub branch and has never run |
| Prometheus/Grafana | Compose declarations only | No scraper or dashboard has run; `/metrics` serves the exposition format |
| Deployment and real users | None | Local runs only; no real pharmacy data |

## Benchmarks

`benchmarks/run_benchmarks.py` generates deterministic synthetic
CSVs (seed 42) at 1k, 10k, and 50k rows and times ingestion, dedupe
re-import, reconciliation, and DuckDB analytics against the SQLite
store. Full tables and conditions are in
[benchmarks/RESULTS.md](benchmarks/RESULTS.md).

The headline result is the DuckDB analytics fix in M7. The old
row-by-row loader took 61.610s at 50k rows. The bulk load with
DuckDB's appender took 0.490s in the M7 run. A reproduction run on
2026-10-28 measured 0.868s at 50k rows, so the exact figure varies
between single runs. Both runs put 50k analytics under one second,
down from about 62 seconds. Ingestion at 50k measured 6,993 rows/s
in the M7 run and 4,481 rows/s in the reproduction run. These are
local, single-machine, single-run figures on synthetic data. They
are not production performance, and timing is never asserted in
tests.

## What is built

### Ingestion core (M1 and M2)

CSV ingestion for inventory and sales with a defined schema.
Validation covers required fields, types, and business rules, and
every failure becomes a structured error with row, field, code, and
message. Invalid rows go to quarantine with their reasons. File
SHA-256 makes re-importing the same file a no-op, and per-row
hashes skip duplicate rows across files. A bad row in the middle of
a file does not lose the good rows around it. Reconciliation
compares expected and actual counts and reports typed
discrepancies: shrinkage, count drift, missing SKU, unexpected SKU,
and overselling. Alerts cover low stock and expiry within 30 days
or past expiry. An append-only audit log records imports and
skipped duplicates, and counters render in Prometheus text format.

The durable store is `SqliteStore` (stdlib `sqlite3`). It persists
file hashes, inventory, sales, quarantine, audit entries, and
pipeline runs across process restarts, with idempotent schema
creation. Ingestion runs against a store protocol, so the in-memory
store remains for fast unit tests. A data-quality report is derived
from the persisted imports: rows in, valid, quarantined, duplicate
rate, and a quarantine reason breakdown.

The Postgres path is code only and has not run. `pharmaflow/postgres_store.py`
implements the same store interface, aligned with the migration in
`db/migrations/001_initial_schema.sql` and the compose `postgres`
service. No Postgres server is available in this environment. See
[docs/POSTGRES.md](docs/POSTGRES.md) for the bring-up checklist.

### Analytics, transforms, and orchestration (M3)

`pharmaflow/analytics.py` loads the SQLite store into DuckDB and
aggregates in SQL: inventory valuation by derived category (the
source data has no category column, so category is derived from the
product name by documented rules), sales velocity and days of stock
per SKU, and expiry risk by days-to-expiry bucket. Test expectations
are hand-computed from the sample CSVs: total inventory value
943.60, value at risk 295.50.

Transforms run two ways over the same SQL files in `dbt/models/`.
Real dbt runs against DuckDB (`dbt seed/run/test`, PASS=2/3/8, run
again green on 2026-10-28). `pharmaflow/transform.py` executes the
same model SQL through DuckDB with the same schema checks, and the
local pipeline runner uses that path so it does not depend on dbt
being installed.

Orchestration puts the task logic in importable callables in
`pharmaflow/orchestration.py` (`ingest_task`, `transform_task`,
`report_task`). The Airflow DAG only wires those callables. The
local `run-daily` runner executes them in order with retries: a
task that exhausts its retries aborts the pipeline and downstream
tasks are skipped, not run. Re-running a failed date completes
without duplicating inventory, because file and row hash
idempotency makes the re-run safe. The Airflow DAG itself has not
run. No Airflow server exists here.

### Event streaming (M4)

No Docker exists in this environment, so no real Kafka or Redpanda
broker can run here. Streaming is built against a broker abstraction
with two implementations. `FileLogBroker` is the tested path: a
durable append-only event log on disk, with topics, partitions
(`sha256(sku) % 4`), per-group committed offsets, and lag computed
from those offsets. `KafkaBroker` is an adapter over the same
interface. It has never connected to a broker, and its group and
offset methods are stubs. A Redpanda service in `docker-compose.yml`
and a runbook in
[docs/STREAMING_RUNBOOK.md](docs/STREAMING_RUNBOOK.md) prepare the
real-broker path. Nothing about that path is claimed as executed.

Events are `StockReceived`, `SaleRecorded`, and
`InventoryAdjusted`, validated with the same rules as batch
ingestion. The consumer is idempotent: an event-id dedupe table in
the SQLite store is written during application, before offsets
commit. The crash test proves the scope of that claim. A consumer
applies 2 of 4 events and crashes before any offset commit. After
restart, all 4 events are redelivered, the 2 already applied are
deduped, and a redelivered sale does not decrement stock twice.
Final state equals the exactly-once outcome. Poison events go to
the DLQ topic with their errors and source coordinates, their
offsets commit, and later valid events still process.

```bash
.venv/bin/python -m pharmaflow stream-produce --log-dir /tmp/m4/log --events sample_data/stream_events.jsonl
.venv/bin/python -m pharmaflow stream-consume --log-dir /tmp/m4/log --db /tmp/m4/pharma.db --group reconciliation-v1
#   Lag before: 4. Consumed: read=4 applied=4 skipped_dlq=0 duplicates=0. Lag after: 0.
```

### Serving, benchmarks, CI, and Terraform (M5 and M6)

`api/app.py` is a FastAPI app factory wired to `SqliteStore` at
`PHARMAFLOW_DB`. Endpoints are `/health`, `POST /imports`
(multipart CSV upload), `GET /imports`, `/inventory`, `/alerts`,
`/reconciliation`, `/data-quality`, `/metrics`, and (from M9)
`/pipeline-runs` and `/pipeline-runs/latest`. The API was verified
live under uvicorn and by 9 TestClient tests, including a second
app instance on the same database file reading inventory back.
Serving from a threadpool surfaced one real fix: `SqliteStore`
opens with `check_same_thread=False`.

Benchmarks, CI, and Terraform are covered in the status table
above and in STATUS.md. The CI workflow installs the full dev
dependencies, runs pytest, and adds a dbt job. The YAML is valid
and the dbt commands were re-run locally, but the workflow file
exists only in this local checkout. A read-only check of the public
GitHub branch on 2026-10-28 found no `.github/workflows/ci.yml`
and no `.github` directory there, so CI has never run. Terraform
was reviewed for consistency with the running architecture
(Postgres 16, API port 8000, database name and user matching
compose). It remains unapplied and unvalidated.

### Analytics bulk load (M7)

Profiling showed the 61.610s at 50k rows was load time, not SQL:
about 58.9s of row-by-row DuckDB INSERTs, against about 0.07s for
the aggregations. The loader now streams raw rows from SQLite in
`fetchmany` chunks into DuckDB's appender. The analytics SQL and
output structures are unchanged. A correctness gate in
`tests/test_m7.py` asserts the old and new loaders produce
identical full reports on the sample data and on a generated 5,000
row dataset. See the Benchmarks section for the re-measured
figures and their variance.

### Observability (M8)

`pharmaflow/observability.py` renders streaming lag and DLQ counts
as Prometheus gauges, computed from the broker's own offsets and
DLQ topic size at read time, so the gauges cannot drift from the
broker's bookkeeping. It also renders per-task pipeline gauges
from a `PipelineResult`. A skipped task renders success 0, never
green. The `stream-metrics` CLI prints the streaming gauges, and
`GET /metrics` includes them when a broker log directory is
configured through arguments or environment variables. Gauges
appear only when the log directory already exists, so an
unconfigured app returns exactly its previous output and a GET
never creates broker files. Prometheus and Grafana remain compose
declarations. No scraper, dashboard, or alert rule has run. See
[docs/OBSERVABILITY.md](docs/OBSERVABILITY.md).

### Durable pipeline-run history (M9)

Schema version 1 to 2 adds a `pipeline_runs` table. The migration
is additive: a new table plus a version bump, with no existing
table or row altered. A hand-built version 1 database opens as
version 2 with its rows intact. `run_daily` appends every completed
run, success or failure, including re-runs of the same logical
date, and sets `result.run_id`. History is served by `GET
/pipeline-runs` (newest first, limit clamped to 1 to 100), `GET
/pipeline-runs/latest` (404 when no run exists), the
`pipeline-runs` CLI, and logical-date-labelled pipeline gauges on
`/metrics` plus `pharmaflow_pipeline_runs_total`. Runs executed
under Airflow are not recorded, because the DAG wires the
individual task callables rather than `run_daily`. The Postgres
store does not persist runs.

## Layout

```text
pharmaflow/     core: ingestion, validation, quarantine, reconciliation, alerts, audit, metrics (stdlib only)
                plus the store protocol, SQLite store (M2), Postgres store (M2, code only, not run),
                data-quality report (M2), analytics.py (DuckDB, M3), transform.py (dbt model SQL, M3),
                orchestration.py (task callables plus local runner, M3), streaming.py (M4: FileLogBroker
                tested, Kafka adapter not run, idempotent consumer, DLQ), observability.py (M8: lag, DLQ,
                and pipeline gauges in Prometheus text)
api/            FastAPI serving layer (M5: SQLite-backed via PHARMAFLOW_DB, verified live and by tests;
                pipeline-runs endpoints added in M9)
benchmarks/     run_benchmarks.py and RESULTS.md with documented conditions (M6, M7, reproduction run)
sample_data/    synthetic CSVs, including deliberately bad and duplicate rows; stream_events.jsonl (M4)
tests/          pytest suite, 76 tests through M9
db/             SQL migrations: 001 initial schema, Postgres dialect (M2, not run locally)
dbt/            transformation project, executed in M3 (seed, run, and test green on DuckDB)
airflow/        daily DAG: wiring only; task logic in pharmaflow/orchestration.py (M3, DAG not run)
infra/terraform infra skeleton, reviewed but unapplied and unvalidated (M5/M6)
docs/           failure modes, data quality, observability, streaming design and runbook, Postgres path
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for how the layers fit
together and [STATUS.md](STATUS.md) for what is verified, what is
not run, and what comes next.

## Why the core is stdlib-only

The ingestion core uses only `csv`, `hashlib`, and dataclasses.
That is a deliberate trade-off. The logic that decides whether a
record is kept, quarantined, or skipped as a duplicate should be
testable anywhere, with no database, no Docker, and no cloud
account. FastAPI, DuckDB, dbt, and Airflow sit around that core.
They do not hold it hostage.
