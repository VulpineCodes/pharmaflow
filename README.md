# PharmaFlow

A data platform for the unglamorous work that keeps a small pharmacy running:
knowing what is actually on the shelf, what was sold, what is about to expire,
and what nobody wrote down properly.

My father ran a small pharmacy in Lagos. During COVID, foot traffic fell,
customers stayed away, and the operation still ran largely on paper ledgers,
spreadsheets, and whoever was available to make a delivery. Nothing about that
was a technology problem in the abstract — it was a *records* problem. If the
count in the book and the count on the shelf disagree, every decision after
that is a guess. PharmaFlow is the system I wish a shop like his had: boring,
reliable ingestion that never silently loses a record.

> **Scope, honestly:** inventory and operations only. No patient, prescription,
> or customer data — ever. All sample data is synthetic. No regulatory or
> compliance claims are made. This is a portfolio project under active
> development, not a deployed product used by real pharmacies.

## What works today (Milestones 1–6)

### M5/M6 — FastAPI serving verified, benchmarks, dbt in CI (2026-10-22)

```bash
.venv/bin/pip install -r requirements.txt python-multipart httpx
PHARMAFLOW_DB=pharmaflow.db .venv/bin/python -m uvicorn api.app:app --port 8000
curl -F file=@sample_data/inventory.csv localhost:8000/imports   # rows_valid=5
curl localhost:8000/inventory                                    # the same 5 SKUs, from SQLite
curl -F file=@sample_data/inventory.csv localhost:8000/imports   # already_imported=true
curl "localhost:8000/alerts?today=2026-10-22"                    # MED-002 low_stock, MED-005 expired, …
curl localhost:8000/metrics localhost:8000/data-quality
.venv/bin/python benchmarks/run_benchmarks.py                    # regenerates benchmarks/RESULTS.md
```

- **FastAPI serving — actually run, not a skeleton.** `api/app.py` is wired
  to the durable `SqliteStore` (env `PHARMAFLOW_DB`): `/health`,
  `POST /imports` (multipart CSV upload → ingest result),
  `GET /inventory`, `/alerts` (`?today=` pins the reference date),
  `/reconciliation`, `/metrics` (Prometheus text), `/data-quality`.
  Verified live under uvicorn *and* by 9 TestClient tests
  (`tests/test_api.py`), including a second app instance on the same DB
  file reading inventory back — persisted state, not in-memory demo data.
  One real fix fell out of this: `SqliteStore` now opens with
  `check_same_thread=False` (sqlite3 threadsafety=3 serialises the
  shared connection) because FastAPI serves from a threadpool.
- **Benchmarks with documented conditions** — `benchmarks/RESULTS.md`:
  synthetic seeded CSVs at 1k/10k/50k rows; on this local machine
  (Linux x86_64, Python 3.12.3, conditions recorded in the file)
  ingestion ran 4,336 / 6,890 / 7,013 rows/s, dedupe re-import
  0.003–0.144 s, DuckDB analytics 1.4 s / 12.4 s / 61.6 s (dominated by
  the row-by-row load into DuckDB — called out in RESULTS.md, not
  hidden). **M7 fixed that loader** (DuckDB appender bulk load,
  profile-gated on identical results in `tests/test_m7.py`):
  re-benchmarked analytics are 0.178 s / 0.136 s / **0.490 s**, i.e.
  61.6 s → 0.490 s at 50k (125.7×); RESULTS.md keeps both runs
  labelled by date/commit. **Local single-machine figures only; no
  production scale is claimed.** Correctness at scale is asserted in
  `tests/test_benchmarks.py`; timing never is.
- **CI** — `.github/workflows/ci.yml` now installs the full dev deps
  (fastapi/duckdb included), runs pytest, and has a separate `dbt` job
  (`dbt seed/run/test`). The YAML is validated and the dbt commands were
  re-run locally (PASS=2/3/8), but **CI itself has not run — there is no
  GitHub push from this environment.**
- **Terraform** — `infra/terraform/main.tf` reviewed for consistency
  with the running architecture (Postgres 16, API port 8000, db
  name/user defaults matching compose; new variables/outputs). Still
  **unapplied and unvalidated** (no terraform binary, no cloud account).

### M4 — event streaming: at-least-once delivery, idempotent consumer, DLQ (2026-10-22)

No Docker exists in this environment, so no real Kafka/Redpanda broker can
run here. The streaming layer (`pharmaflow/streaming.py`) is therefore built
against a broker abstraction with two implementations — one tested, one
honestly not:

```bash
.venv/bin/python -m pharmaflow stream-produce --log-dir /tmp/m4/log --events sample_data/stream_events.jsonl
.venv/bin/python -m pharmaflow stream-consume --log-dir /tmp/m4/log --db /tmp/m4/pharma.db --group reconciliation-v1
#   Lag before: 4
#   Consumed: read=4 applied=4 skipped_dlq=0 duplicates=0
#   Lag after: 0     (MED-001: 100 received − 10 sold = 90; MED-002: 50 − 2 adjusted = 48)
```

- **`FileLogBroker` — the tested path.** A durable append-only event log on
  disk: topics, partitions (`sha256(sku) % 4`), per-group committed offsets,
  lag. It implements the Kafka semantics the design depends on, so the
  producer/consumer logic above it is broker-independent.
- **Events** — `StockReceived` / `SaleRecorded` / `InventoryAdjusted`
  dataclasses, validated with the *same* rules as batch ingestion.
- **Idempotent consumer** — an event-id dedupe table in the SQLite store is
  written during application, *before* offsets commit. Crash test evidence:
  apply 2 of 4 events, crash with no offsets committed → restart redelivers
  all 4, the 2 applied ones are deduped (`duplicates=2, applied=2`), and a
  redelivered sale does **not** decrement stock twice (final MED-001 = 90,
  exactly the exactly-once outcome).
- **Poison events → DLQ.** Invalid or unappliable events are published to
  `pharmaflow.inventory.dlq` with their errors and source coordinates, their
  offsets commit, and later valid events still process — the streaming
  analogue of batch quarantine.
- **`KafkaBroker` adapter — code only, NOT run.** kafka-python-style adapter
  over the same interface (guarded import), plus a Redpanda service in
  `docker-compose.yml` and a step-by-step runbook with a verification
  checklist in [docs/STREAMING_RUNBOOK.md](docs/STREAMING_RUNBOOK.md).
  Nothing claims this path was executed.

### M3 — executed analytics, transforms, orchestration (2026-10-22)

```bash
python -m venv .venv && .venv/bin/pip install duckdb pytest   # system pip is externally managed here
.venv/bin/python -m pharmaflow ingest sample_data/inventory.csv sample_data/sales.csv --db pharmaflow.db --today 2026-10-22
.venv/bin/python -m pharmaflow analytics --db pharmaflow.db --today 2026-10-22
#   Total inventory value: 943.60 | Pain Relief 565.60, Vitamins & Rehydration 270.00, ...
#   MED-001 velocity 5.0/day -> 24.0 days of stock; expiry value at risk (expired + <=30d): 295.50
.venv/bin/python -m pharmaflow run-daily --db pharmaflow.db --data-dir sample_data --date 2026-10-22
#   [success] ingest -> [success] transform -> [success] report; Pipeline succeeded: True

# The dbt path itself (heavy install, one attempt — it succeeded):
.venv/bin/pip install dbt-duckdb   # dbt-core 1.12.6 / dbt-duckdb 1.11.0
cd dbt && ../.venv/bin/dbt seed --profiles-dir . && ../.venv/bin/dbt run --profiles-dir . && ../.venv/bin/dbt test --profiles-dir .
#   seed PASS=2, run PASS=3, test PASS=8
```

- **DuckDB analytics** — `pharmaflow/analytics.py` loads the SQLite store
  into DuckDB and aggregates *in SQL*: inventory valuation by (derived)
  category, sales velocity / days-of-stock per SKU, and an expiry-risk
  report by days-to-expiry bucket. Definitions are in the module docstring;
  test expectations are hand-computed from the sample CSVs.
- **Executed transformations, two ways** — (a) real `dbt seed/run/test`
  against DuckDB (green, above), and (b) `pharmaflow/transform.py`, which
  executes the *same* SQL files from `dbt/models/` via DuckDB and runs the
  same `not_null`/`unique` schema checks — that is the path the local
  pipeline runner uses, so the pipeline does not depend on dbt being
  installed.
- **Orchestration without a server** — task logic lives in
  `pharmaflow/orchestration.py` (`ingest_task` / `transform_task` /
  `report_task`); the Airflow DAG now only wires those callables.
  `run-daily` executes them in DAG order with retries: a task that
  exhausts retries aborts the pipeline and downstream tasks are skipped
  (not run). Re-running a failed date completes without duplicating
  inventory — recovery evidence (counts 5 SKUs / 3 sales before and after)
  is in `STATUS.md` and covered by tests. The Airflow DAG itself has
  **not** been run (no Airflow server here) — only its callables have.

## What worked in Milestones 1–2

```bash
pip install pytest   # the core needs nothing else
python -m pytest -q
python -m pharmaflow ingest sample_data/inventory.csv sample_data/sales.csv sample_data/inventory_with_errors.csv --today 2026-10-22

# M2 — durable persistence (SQLite, stdlib only):
python -m pharmaflow ingest sample_data/inventory.csv sample_data/inventory_with_errors.csv --db pharmaflow.db --today 2026-10-22
python -m pharmaflow ingest sample_data/inventory.csv sample_data/inventory_with_errors.csv --db pharmaflow.db --today 2026-10-22
#   ^ second run, new process: already_imported=True for both files,
#     duplicates_skipped=5 and 7, inventory count unchanged (8 SKUs)
python -m pharmaflow report --db pharmaflow.db   # data-quality report
```

- **Durable store (M2)** — `pharmaflow/sqlite_store.py` (stdlib `sqlite3`)
  persists imported-file hashes, inventory, sales, quarantine, and audit
  across process restarts. Schema creation is idempotent
  (`CREATE TABLE IF NOT EXISTS` + `schema_version` table). Ingestion runs
  against a store protocol (`pharmaflow/store.py`); the in-memory store
  remains for fast unit tests.
- **Data-quality report (M2)** — `pharmaflow/data_quality.py` + the CLI
  `report` subcommand: per-import rows in / valid / quarantined /
  duplicate rate, plus a quarantine reason breakdown (by validation code).
- **Postgres path (M2, code only — NOT run locally)** — `pharmaflow/postgres_store.py`
  implements the same store interface with psycopg-style SQL, aligned with
  the migration in `db/migrations/001_initial_schema.sql` and the
  docker-compose `postgres` service. No Postgres server is available in the
  current environment, so nothing here claims it was executed; see
  [docs/POSTGRES.md](docs/POSTGRES.md) for the bring-up checklist.

- **CSV ingestion** for inventory and sales with a defined schema.
- **Validation** — required fields, types, and business rules. Every failure is a
  structured error (row, field, code, message), never a silent drop.
- **Quarantine** — invalid rows are stored with their reasons and can be inspected.
- **Idempotency** — file SHA-256 makes re-importing the same file a no-op; per-row
  hashes skip duplicate rows. Re-running an import is safe, by design.
- **Partial-failure recovery** — a bad row in the middle of a file does not lose
  the good rows before or after it.
- **Reconciliation** — expected vs. actual counts produce typed discrepancies:
  `shrinkage`, `count_drift`, `missing_sku`, `unexpected_sku`, `overselling`.
- **Alerts** — low stock (on hand ≤ reorder point) and expiry within 30 days / expired.
- **Audit log** — append-only record of every import and skipped duplicate.
- **Metrics** — counters (`rows_in`, `rows_valid`, `rows_quarantined`,
  `duplicates_skipped`) rendered in Prometheus text format, no server required.
- **API skeleton (M1, superseded in M5)** — the original FastAPI surface;
  see the M5/M6 section above for the verified SQLite-backed serving layer.

## Layout

```
pharmaflow/     core: ingestion, validation, quarantine, reconciliation, alerts, audit, metrics (stdlib only)
                + store protocol, SQLite store (M2), Postgres store skeleton (M2, unverified), data-quality report (M2)
                + analytics.py (DuckDB, M3), transform.py (executed dbt model SQL, M3), orchestration.py (task callables + local runner, M3)
                + streaming.py (M4: FileLogBroker tested, Kafka adapter unverified, idempotent consumer, DLQ)
api/            FastAPI serving layer (M5: SQLite-backed via PHARMAFLOW_DB, verified live + TestClient)
benchmarks/     run_benchmarks.py + RESULTS.md with documented conditions (M6)
sample_data/    synthetic CSVs — including deliberately bad/duplicate rows; stream_events.jsonl (M4)
tests/          pytest suite (45 through M4, +9 API (M5), +3 benchmark-correctness (M6) = 57)
db/             SQL migrations — 001 initial schema, Postgres dialect (M2, not run locally)
dbt/            transformation project — executed in M3 (seed/run/test green on DuckDB)
airflow/        daily DAG — wiring only; task logic in pharmaflow/orchestration.py (M3)
infra/terraform infra skeleton (M5)
docs/           failure modes, data quality, streaming design + runbook (M4), Postgres path
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for how the layers fit together and
[STATUS.md](STATUS.md) for what is verified, what is skeleton, and what is next.

## Why the core is stdlib-only

The ingestion core uses only `csv`, `hashlib`, and dataclasses. That is a
deliberate trade-off: the most important logic — *never lose a record, never
double-count one* — should be testable anywhere, with no database, no Docker,
and no cloud account. FastAPI, DuckDB, dbt, and Airflow sit around that core;
they do not hold it hostage.
