# PharmaFlow — Architecture

## Layers (raw → staging → mart)

1. **Raw / landing** — CSV files as received, identified by SHA-256. Nothing is
   mutated in place; the hash is the identity of an import.
2. **Validation + quarantine** (`pharmaflow/validation.py`, `quarantine.py`) —
   each row is checked (required, type, business rules). Valid rows proceed;
   invalid rows go to the quarantine store with structured reasons. There is no
   third outcome — a row is never silently dropped.
3. **Operational store** — ingestion runs against `StoreProtocol`
   (`pharmaflow/store.py`). Implementations: `PharmaStore` (in-memory, for
   tests), `SqliteStore` (durable, stdlib `sqlite3` — the M2 verified path),
   and `PostgresStore` (same interface, code only, not run locally — see
   `docs/POSTGRES.md`). Current inventory by SKU, sales records, quarantine,
   audit log, and per-import summaries all persist in the durable stores.
4. **Staging (dbt)** — 1:1 typed/cleaned models (`stg_inventory`, `stg_sales`).
5. **Marts (dbt)** — analytics-ready models (`inventory_current` with low-stock
   and expiring flags), with `not_null`/`unique` schema tests.
   **M3 execution (both paths run green, 2026-10-22):** (a) real dbt —
   `dbt seed/run/test` with dbt-duckdb against a local DuckDB file
   (seed PASS=2, run PASS=3, test PASS=8; sources point at the `main`
   schema where seeds land); (b) `pharmaflow/transform.py` — resolves the
   `source()`/`ref()` Jinja in the *same* model files and executes them
   via DuckDB, running the same schema checks as SQL. The local pipeline
   runner uses (b) so it never depends on a dbt installation.
6. **Analytics (DuckDB, M3)** — `pharmaflow/analytics.py` loads the
   operational store into in-memory DuckDB and aggregates in SQL:
   valuation by derived category (no category column exists in source
   data — derivation rules are in the module), sales velocity over the
   observed sales window, expiry-risk buckets. See STATUS.md for the
   hand-verified figures.
7. **Orchestration (M3)** — task logic is importable callables in
   `pharmaflow/orchestration.py`; the Airflow DAG is wiring only, and the
   local `run-daily` runner executes the same callables with
   retry-then-abort semantics (downstream tasks skipped, never run, after
   an exhausted task). Re-running a logical date is the backfill/recovery
   mechanism, made safe by file/row-hash idempotency.
8. **Serving (M5, verified)** — `api/app.py` is a FastAPI app factory
   (`create_app(db_path)`) bound to the durable `SqliteStore` at
   `PHARMAFLOW_DB`; uvicorn serves the module-level instance. Endpoints:
   `/health`, `POST /imports` (multipart CSV upload), `GET /imports`,
   `/inventory`, `/alerts` (`?today=` pins the reference date),
   `/reconciliation`, `/data-quality`, `/metrics` (Prometheus text).
   Because FastAPI serves from a threadpool, `SqliteStore` opens its
   connection with `check_same_thread=False` (sqlite3 threadsafety=3
   serialises it). DuckDB remains the local analytical engine for
   reproducible batch analysis over the marts.

## Lineage
Every loaded row traces to: source file → file hash → row hash → import audit
entry. Quarantined rows keep the same lineage plus their validation errors.
dbt models declare their sources in `sources.yml`, extending lineage into the
warehouse layer.

## Idempotency design
- **File level:** SHA-256 of file bytes. Known hash → no-op result
  (`already_imported=True`), inventory unchanged, audit entry recorded.
- **Row level:** SHA-256 of the normalised row. Duplicate rows (same file or a
  re-export with different file bytes) are skipped and counted.
- **Crash safety:** the file hash is recorded only *after* all rows are
  processed. A process that dies mid-file can be re-run; row hashes dedupe
  whatever was already loaded. See `docs/FAILURE_MODES.md`.
- **Streaming (M4):** event-id dedupe table in the operational store,
  written during event application and before the broker offset commit —
  the same "marker before completion record" ordering as the batch path,
  applied to at-least-once delivery.

## Single responsibility per component
| Component | Owns | Does not own |
|---|---|---|
| ingestion | reading files, hashing, orchestration of a single import | validation rules, alert logic |
| validation | deciding if a row is well-formed and business-valid | storage, side effects |
| quarantine | holding rejected rows + reasons | fixing/replaying them (future) |
| reconciliation | comparing expected vs actual, typing discrepancies | importing data |
| alerts | deriving low-stock / expiry signals from inventory | sending notifications (future) |
| audit | append-only import history | mutation of past entries (no API for it) |
| metrics | counters + Prometheus text rendering | running a metrics server |
| api | HTTP surface over the core | business logic |
| dbt | SQL transformation staging → marts | ingestion |
| transform.py | executing the dbt model SQL + schema checks without dbt installed | defining models (that stays in dbt/models/) |
| analytics.py | DuckDB aggregations over the operational store | ingestion, validation |
| orchestration.py | task callables + local runner: order, retries, abort/skip, recovery | transformation/analytics logic itself |
| streaming.py | event schemas, broker abstraction (file-log tested / Kafka unverified), idempotent consumer, DLQ | batch ingestion, alert logic |
| airflow | scheduling, retries, backfills (wiring only) | transformation logic itself |
| terraform | provisioning the infra the app actually uses | app deployment logic |

## Streaming (Milestone 4 — implemented on the file-log broker)
Kafka/Redpanda is deliberately *not* in the batch MVP: batch CSV import is the
real workflow for the target pharmacies. The streaming extension
(`pharmaflow/streaming.py`) is built against a broker abstraction:

* **`FileLogBroker` (tested)** — durable append-only JSONL partition files
  under a log directory; partition = `sha256(key) % num_partitions`
  (key = SKU, so per-SKU order is preserved); per-group committed
  offsets as atomically-replaced JSON files; offsets never move
  backwards; lag = Σ(end − committed). This implements the Kafka
  semantics the system depends on with no broker process at all.
* **`KafkaBroker` (NOT run)** — kafka-python-style adapter over the same
  interface, guarded import. No broker/Docker exists in this
  environment; the Redpanda compose service and
  `docs/STREAMING_RUNBOOK.md` are prepared but unverified.

**Delivery semantics:** at-least-once — offsets commit only *after*
processing. **Exactly-once application** comes from the consumer, not
the broker: an event-id dedupe table (`processed_events`) lives in the
same SQLite store the consumer mutates and is written during
application, before the offset commit, so a crash in that window causes
redelivery that is recognised and skipped (proven by the crash tests in
`tests/test_streaming.py`). **Poison events** (validation failure, or
unappliable against current state, e.g. unknown SKU) are published to
the DLQ topic `pharmaflow.inventory.dlq` with errors + source
coordinates and their offsets commit — the streaming analogue of the
batch quarantine store. Consumer groups hold fully independent
offsets. Full design + as-implemented notes: `docs/STREAMING_DESIGN.md`.

## Observability
Structured counters today (Prometheus text from the core, `/metrics` on the API —
verified through the API in M5);
Prometheus + Grafana containers are declared in `docker-compose.yml`
(untested — no Docker here), with dashboards for rows quarantined, import
duration, and data freshness still future work.

## Benchmarks (M6)
`benchmarks/run_benchmarks.py` generates seeded synthetic CSVs
(1k/10k/50k rows) and times ingestion, dedupe re-import, reconciliation,
and DuckDB analytics against `SqliteStore`, writing
`benchmarks/RESULTS.md` with full machine/Python/version conditions.
Headline finding recorded there: analytics time was dominated by the
row-by-row Python load into DuckDB, not the SQL. **M7 fixed it:** the
loader now streams raw rows from SQLite into DuckDB's appender
(`append_rows`, chunked); the analytics SQL is unchanged and an
old-vs-new correctness gate (`tests/test_m7.py`) asserts identical
results. Re-benchmarked 50k analytics: 61.610s → 0.490s. All figures
are local single-machine only.

## Infrastructure (M5/M6)
`infra/terraform/main.tf` is a reviewed-but-unapplied skeleton:
variables/defaults aligned with docker-compose (Postgres 16, db
name/user `pharmaflow`, API image/port 8000). No provider block, no
plan, no apply — no cloud account or terraform binary exists here.
CI (`.github/workflows/ci.yml`) runs pytest with full dev deps plus a
separate dbt seed/run/test job; the YAML is validated locally but the
workflow itself has not run (no GitHub push from this environment).
