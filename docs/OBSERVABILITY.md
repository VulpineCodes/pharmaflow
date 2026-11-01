# PharmaFlow observability (M8, M9, M10)

## What is exposed, and where it comes from

| Metric | Meaning | Source of truth |
|---|---|---|
| `pharmaflow_rows_in_total` etc. | Ingestion counters (M1/M2, durable since M10) | Derived at read time from recorded imports + audit trail (`durable_metrics`); survives restarts |
| `pharmaflow_stream_lag_records{group,topic}` | Unconsumed records for a consumer group | Broker: partition end offsets minus committed offsets |
| `pharmaflow_stream_partition_lag_records{group,topic,partition}` | Same, per partition (find a stuck partition) | Broker, same calculation |
| `pharmaflow_stream_dlq_records{topic}` | Records dead-lettered to the DLQ topic | DLQ topic size in the broker |
| `pharmaflow_pipeline_succeeded{logical_date}` | 1/0 for the last *stored* daily run | `pipeline_runs` table (M9); label identifies the run |
| `pharmaflow_pipeline_task_success{task,logical_date}` / `pharmaflow_pipeline_task_attempts{task,logical_date}` | Per-task outcome / attempts of the last stored run | Same `pipeline_runs` row |
| `pharmaflow_pipeline_runs_total` | Runs recorded in the store | `COUNT(*)` on `pipeline_runs` |
| `pharmaflow_pipeline_succeeded` (unlabeled) | 1/0 for an in-memory `PipelineResult` | `render_pipeline_prometheus` without a label (M8 form, unchanged) |

The streaming numbers are deliberately **not** a second counter: they
are computed from the broker's own offsets at scrape time, so they
cannot drift from what the consumer will actually do next. A lag that
grows while the DLQ count stays flat means the consumer is behind; a
DLQ count that grows means upstream data is bad (see
`docs/STREAMING_DESIGN.md` for the poison-event policy).

## Pipeline runs are stored (M9)

Every completed `run-daily` execution is appended to the
`pipeline_runs` table in the operational SQLite database (schema
version 2. Opening a version-1 database migrates additively: a new
table only, existing rows untouched, version bumped to 2). Successes,
failures, and re-runs of the same logical date are all recorded; the
latest run is the highest row id. History is queryable without
re-running anything:

```bash
python -m pharmaflow pipeline-runs --db pharmaflow.db            # newest first
python -m pharmaflow pipeline-runs --db pharmaflow.db --latest --json
curl localhost:8000/pipeline-runs/latest
curl localhost:8000/metrics   # last stored run's gauges, logical_date-labelled
```

`GET /pipeline-runs/latest` returns 404 when no run has been recorded
yet, and `/metrics` serves no pipeline gauges in that case. That
matches the behaviour before M9. The Postgres store does not persist runs
(code-only path, unverified); the Airflow DAG wires the individual
task callables, not `run_daily`, so runs executed under Airflow are
not recorded by this mechanism. Only local `run-daily` executions
are. That is a stated limit, not an oversight to paper over.

## Using it

```bash
# CLI: lag for two groups + DLQ size, as Prometheus text
python -m pharmaflow stream-metrics --log-dir /var/pharmaflow/broker \
  --group reconciliation-v1 --group alerts-v1

# API: /metrics gains the streaming gauges when a broker is configured
PHARMAFLOW_STREAM_LOG_DIR=/var/pharmaflow/broker \
PHARMAFLOW_STREAM_GROUPS=reconciliation-v1,alerts-v1 \
  uvicorn api.app:app --port 8000
curl localhost:8000/metrics
```

Without that configuration, `/metrics` returns exactly the M5
ingestion counters. No empty streaming gauges appear, and a GET never
creates broker files (gauges appear only when the log directory
already exists).

## Ingestion counters are durable (M10)

The four row counters plus `imports_total` are the data-quality
report totals, computed from the recorded imports at read time, so
`/metrics` and `python -m pharmaflow ingest` print the same numbers
a fresh process would. Each distinct file is counted once, at its
first import; a duplicate-file re-import adds to
`files_already_imported` only (counted from the audit trail), never to
`rows_in` again. The in-memory session counter still counts every
ingest call, including re-imports — the two views differ by design,
and the difference is the honest statement of what a re-import did.
No new table or migration was needed: the imports table (M2) and the
audit table already held everything. Verified by
`tests/test_m10.py`: cross-restart SQLite reopen, a second FastAPI
app instance on the same DB file, and two separate CLI processes.

## Honest limits

- Ingestion counters are durable from M10 onward (see above); the
  per-process session counters still exist in-memory and are what the
  foundation tests assert. The durable view is the one `/metrics`
  and the CLI print serve.
- Pipeline gauges on `/metrics` describe the last *stored* run; runs
  executed under Airflow (task callables, not `run_daily`) are not
  recorded, and the Postgres store does not persist runs.
- Prometheus/Grafana remain compose declarations only (no Docker in
  this environment): the exposition format above is what a scraper
  would read, but no scraper or dashboard has been run against it.
