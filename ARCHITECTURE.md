# PharmaFlow: Architecture

## Layers (raw to staging to mart)

1. **Raw / landing.** CSV files as received, identified by SHA-256.
   Nothing is mutated in place. The hash is the identity of an
   import.
2. **Validation and quarantine** (`pharmaflow/validation.py`,
   `quarantine.py`). Each row is checked for required fields, types,
   and business rules. Valid rows proceed. Invalid rows go to the
   quarantine store with structured reasons. There is no third
   outcome. A row is never dropped without a record.
3. **Operational store.** Ingestion runs against `StoreProtocol`
   (`pharmaflow/store.py`). Implementations: `PharmaStore`
   (in-memory, for tests), `SqliteStore` (durable, stdlib `sqlite3`,
   the M2 verified path), and `PostgresStore` (same interface, code
   only, not run locally; see `docs/POSTGRES.md`). Current inventory
   by SKU, sales records, quarantine, audit log, per-import
   summaries, and (from M9) pipeline-run history all persist in the
   durable stores.
4. **Staging (dbt).** Typed and cleaned models with a 1:1 shape to
   the sources (`stg_inventory`, `stg_sales`).
5. **Marts (dbt).** Analytics-ready models (`inventory_current`
   with low-stock and expiring flags), with `not_null` and `unique`
   schema tests. **M3 execution (both paths ran green,
   2026-10-22, and dbt ran green again on 2026-10-28):** (a) real
   dbt, `dbt seed/run/test` with dbt-duckdb against a local DuckDB
   file (seed PASS=2, run PASS=3, test PASS=8; sources point at the
   `main` schema where seeds land); (b) `pharmaflow/transform.py`,
   which resolves the `source()` and `ref()` Jinja in the same model
   files and executes them through DuckDB, running the same schema
   checks as SQL. The local pipeline runner uses (b) so it never
   depends on a dbt installation.
6. **Analytics (DuckDB, M3).** `pharmaflow/analytics.py` loads the
   operational store into in-memory DuckDB and aggregates in SQL:
   valuation by derived category (no category column exists in
   source data; derivation rules are in the module), sales velocity
   over the observed sales window, and expiry-risk buckets. See
   STATUS.md for the hand-verified figures. M7 replaced the loader
   with a bulk load through DuckDB's appender; the SQL is unchanged.
7. **Orchestration (M3).** Task logic is importable callables in
   `pharmaflow/orchestration.py`. The Airflow DAG is wiring only,
   and the local `run-daily` runner executes the same callables
   with retry-then-abort semantics (downstream tasks skipped, never
   run, after an exhausted task). Re-running a logical date is the
   backfill and recovery mechanism, made safe by file and row-hash
   idempotency. The DAG itself has never run. No Airflow server
   exists here.
8. **Serving (M5, verified).** `api/app.py` is a FastAPI app
   factory (`create_app(db_path)`) bound to the durable
   `SqliteStore` at `PHARMAFLOW_DB`. Uvicorn serves the module-level
   instance. Endpoints: `/health`, `POST /imports` (multipart CSV
   upload), `GET /imports`, `/inventory`, `/alerts` (`?today=` pins
   the reference date), `/reconciliation`, `/data-quality`,
   `/metrics` (Prometheus text), and (M9) `/pipeline-runs` and
   `/pipeline-runs/latest`. Because FastAPI serves from a
   threadpool, `SqliteStore` opens its connection with
   `check_same_thread=False` (sqlite3 threadsafety=3 serialises
   it). DuckDB remains the local analytical engine for reproducible
   batch analysis over the marts.
9. **Observability (M8 and M9).** `pharmaflow/observability.py`
   renders streaming lag and DLQ gauges from the broker's own
   offsets, and pipeline gauges from a `PipelineResult` or from
   the last stored run. `GET /metrics` serves the streaming gauges
   only when a broker log directory is configured and exists, and
   serves stored-run pipeline gauges only when a run has been
   recorded. No Prometheus server or Grafana dashboard has run.
   See `docs/OBSERVABILITY.md`.

## Lineage

Every loaded row traces to: source file, file hash, row hash, import
audit entry. Quarantined rows keep the same lineage plus their
validation errors. dbt models declare their sources in
`sources.yml`, extending lineage into the warehouse layer.

## Idempotency design

- **File level:** SHA-256 of file bytes. A known hash gives a no-op
  result (`already_imported=True`), inventory unchanged, and an
  audit entry recorded.
- **Row level:** SHA-256 of the normalised row. Duplicate rows (in
  the same file or in a re-export with different file bytes) are
  skipped and counted.
- **Crash safety:** the file hash is recorded only after all rows
  are processed. A process that dies mid-file can be re-run. Row
  hashes dedupe whatever was already loaded. See
  `docs/FAILURE_MODES.md`.
- **Streaming (M4):** an event-id dedupe table in the operational
  store, written during event application and before the broker
  offset commit. That is the same "marker before completion record"
  ordering as the batch path, applied to at-least-once delivery.
  The tested scope is the file-log broker. The crash test covers a
  consumer that dies before committing offsets, a restart that
  re-reads the log, and dedupe that keeps the final state equal to
  the exactly-once outcome.

## Single responsibility per component

| Component | Owns | Does not own |
|---|---|---|
| ingestion | reading files, hashing, orchestration of a single import | validation rules, alert logic |
| validation | deciding if a row is well-formed and business-valid | storage, side effects |
| quarantine | holding rejected rows plus reasons | fixing or replaying them (future) |
| reconciliation | comparing expected vs actual, typing discrepancies | importing data |
| alerts | deriving low-stock and expiry signals from inventory | sending notifications (future) |
| audit | append-only import history | mutation of past entries (no API for it) |
| metrics | counters plus Prometheus text rendering | running a metrics server |
| api | HTTP surface over the core | business logic |
| dbt | SQL transformation staging to marts | ingestion |
| transform.py | executing the dbt model SQL plus schema checks without dbt installed | defining models (that stays in dbt/models/) |
| analytics.py | DuckDB aggregations over the operational store | ingestion, validation |
| orchestration.py | task callables plus local runner: order, retries, abort and skip, recovery, run persistence (M9) | transformation and analytics logic itself |
| streaming.py | event schemas, broker abstraction (file-log tested, Kafka not run), idempotent consumer, DLQ | batch ingestion, alert logic |
| observability.py | lag, DLQ, and pipeline gauges in Prometheus text, read from broker and store state | running Prometheus or Grafana |
| airflow | scheduling, retries, backfills (wiring only, never run) | transformation logic itself |
| terraform | provisioning the infra the app actually uses (reviewed skeleton, unapplied) | app deployment logic |

## Streaming (Milestone 4, implemented on the file-log broker)

Kafka/Redpanda is deliberately not in the batch MVP: batch CSV
import is the real workflow for the target pharmacies. The
streaming extension (`pharmaflow/streaming.py`) is built against a
broker abstraction:

- **`FileLogBroker` (tested).** Durable append-only JSONL partition
  files under a log directory. Partition = `sha256(key) % num_partitions`
  (key = SKU, so per-SKU order is preserved). Per-group committed
  offsets are atomically-replaced JSON files. Offsets never move
  backwards. Lag = sum of (end minus committed) over partitions.
  This implements the Kafka semantics the system depends on, with
  no broker process at all.
- **`KafkaBroker` (not run).** kafka-python-style adapter over the
  same interface, guarded import. No broker or Docker exists in
  this environment. The Redpanda compose service and
  `docs/STREAMING_RUNBOOK.md` are prepared but unverified. The
  adapter's group and offset methods are stubs.

**Delivery semantics:** at-least-once. Offsets commit only after
processing. **Exactly-once application** comes from the consumer,
not the broker: an event-id dedupe table (`processed_events`) lives
in the same SQLite store the consumer mutates and is written during
application, before the offset commit, so a crash in that window
causes redelivery that is recognised and skipped. That behaviour is
proven by the crash tests in `tests/test_streaming.py` against the
file-log broker only. **Poison events** (validation failure, or
unappliable against current state, such as an unknown SKU) are
published to the DLQ topic `pharmaflow.inventory.dlq` with errors
and source coordinates, and their offsets commit. That is the
streaming analogue of the batch quarantine store. Consumer groups
hold fully independent offsets. Full design and as-implemented
notes: `docs/STREAMING_DESIGN.md`.

## Observability

Structured counters today (Prometheus text from the core,
`/metrics` on the API, verified through the API in M5), plus the
M8 streaming gauges and M9 stored-run pipeline gauges described
above. Prometheus and Grafana containers are declared in
`docker-compose.yml` (untested, no Docker here). Dashboards for
rows quarantined, import duration, and data freshness are still
future work. No scraper has read `/metrics`, so no dashboard claim
is made.

## Benchmarks (M6, M7, reproduction run)

`benchmarks/run_benchmarks.py` generates seeded synthetic CSVs
(1k, 10k, 50k rows) and times ingestion, dedupe re-import,
reconciliation, and DuckDB analytics against `SqliteStore`,
writing `benchmarks/RESULTS.md` with full machine, Python, and
version conditions. Headline finding recorded there: analytics
time was dominated by the row-by-row Python load into DuckDB, not
the SQL. **M7 fixed it:** the loader now streams raw rows from
SQLite into DuckDB's appender (`append_rows`, chunked). The
analytics SQL is unchanged and an old-vs-new correctness gate
(`tests/test_m7.py`) asserts identical results. Re-benchmarked
50k analytics: 61.610s to 0.490s in the M7 run. A reproduction run
on 2026-10-28 measured 0.868s at 50k. All figures are local,
single-machine, single-run figures on synthetic data only.

## Infrastructure (M5 and M6)

`infra/terraform/main.tf` is a reviewed-but-unapplied skeleton:
variables and defaults aligned with docker-compose (Postgres 16,
database name and user `pharmaflow`, API image and port 8000). No
provider block, no plan, no apply. No cloud account or terraform
binary exists here. CI (`.github/workflows/ci.yml`) exists only in
the local checkout. It defines a pytest job with full dev
dependencies plus a separate dbt seed, run, and test job. The YAML
is validated locally, but the workflow itself has not run. A
read-only check of the public GitHub branch on 2026-10-28 found no
`.github` directory there at all.
