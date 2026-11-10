# PharmaFlow observability (M8, M9, M10, M11, M12, M13, M14)

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
| `pharmaflow_stream_consumption_runs_total{group}` | Completed consume runs recorded for a group | `stream_consumption_runs` table (M11) |
| `pharmaflow_stream_consumed_read_total{group}` / `pharmaflow_stream_consumed_applied_total{group}` / `pharmaflow_stream_consumed_duplicates_total{group}` / `pharmaflow_stream_consumed_dlq_total{group}` | Sums over completed, recorded consume runs for a group | Same table (M11); sums, not live broker state |
| `pharmaflow_dead_letters_total{group}` | Poison events dead-lettered by completed consume runs for a group | `dead_letters` table (M12); count per group, not live broker state |
| `pharmaflow_dead_letter_replays_total{group}` | Corrected re-publishes of recorded dead letters for a group | `dead_letter_replays` table (M13); count per group — a re-publish, not a claim the event was applied |
| `pharmaflow_dead_letter_replay_outcomes_total{group,outcome}` | What the next completed consume run did with each replayed event, per group and outcome (`applied` / `duplicate` / `dead_lettered`) | `dead_letter_replay_outcomes` table (M14); a replay with no outcome row (not consumed yet) contributes to no count |
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

## Stream consumption history is stored (M11)

Every *completed* `stream-consume` execution is appended to the
`stream_consumption_runs` table in the operational SQLite database
(schema version 3. Opening a version-2 database migrates additively:
a new table only, existing rows untouched, version bumped to 3).
The recorded row is the finished `ConsumeResult`: group, topic,
records read, applied, duplicates (redelivered events recognised by
event id), poison events dead-lettered, and lag before/after. A
no-op catch-up (read 0) is still recorded — it proves the group was
checked and was current — and a re-run after a crash records the
duplicates column showing the redelivery. History is queryable
without re-running anything:

```bash
python -m pharmaflow stream-consumption --db pharmaflow.db            # newest first
python -m pharmaflow stream-consumption --db pharmaflow.db --group reconciliation-v1 --latest --json
curl localhost:8000/stream-consumption/latest
curl localhost:8000/metrics   # per-group summed consumption gauges
```

`GET /stream-consumption/latest` returns 404 when no consume run has
been recorded yet, and `/metrics` serves no stream-consumption gauges
in that case. The gauges are sums over recorded, completed runs —
they answer "what has this group consumed so far?", while the M8
broker gauges answer "what is waiting right now?". A run interrupted
mid-consume (including the crash-simulation hook) is never recorded:
its partial application shows up in inventory and in the dedupe
table, not in this history. The Postgres store does not persist
consumption runs (code-only path, unverified).

## Dead letters are stored (M12)

Every poison event a *completed* `stream-consume` run dead-letters
is appended to the `dead_letters` table in the operational SQLite
database (schema version 4. Opening a version-3 database migrates
additively: a new table only, existing rows untouched, version
bumped to 4). The recorded row is the finished `DeadLetter`: the
consumer group that dead-lettered it, the event id (empty when the
raw record carried none), the validation errors, and the source
topic/partition/offset where the poison record sits in the broker
log. History is queryable without reading the broker log:

```bash
python -m pharmaflow dead-letters --db pharmaflow.db            # newest first
python -m pharmaflow dead-letters --db pharmaflow.db --group reconciliation-v1 --latest --json
curl localhost:8000/dead-letters/latest
curl localhost:8000/metrics   # per-group dead-letter totals
```

`GET /dead-letters/latest` returns 404 when no dead letter has been
recorded yet, and `/metrics` serves no dead-letter gauge in that
case. The gauge is a count per group over completed, recorded runs —
it answers "which poison events have we seen, and how many per
group?", while the M8 `pharmaflow_stream_dlq_records` gauge answers
"how many records are waiting in the DLQ topic right now?". A run
interrupted mid-consume is never recorded: its dead letters live in
the broker DLQ topic only. A poison event re-read after a crash is
recorded once per completed run that dead-lettered it (the same
per-run semantics as the consumption history's duplicates column),
so one event id can legitimately appear more than once, under
different groups or different runs. The Postgres store does not
persist dead letters (code-only path, unverified).

## Dead letters can be replayed (M13)

A recorded dead letter can be corrected and re-driven into the
consumer instead of sitting in the DLQ forever:

```bash
python -m pharmaflow dead-letter-replay --db pharmaflow.db --log-dir /var/pharmaflow/broker \
  --id 1 --corrections '{"quantity_sold": 5}'
python -m pharmaflow dead-letter-replays --db pharmaflow.db --json
curl -X POST localhost:8000/dead-letters/1/replay \
  -H 'Content-Type: application/json' -d '{"corrections": {"quantity_sold": 5}}'
curl localhost:8000/dead-letter-replays/latest
curl localhost:8000/metrics   # per-group replay totals
```

The replay reads the original record back from the broker at the
dead letter's source coordinates, applies the caller's field
corrections on top of it, validates the result through the same
`parse_event` rules the consumer applies, and re-publishes it to
the original topic **with the original event id**. Omitting
`--corrections` re-publishes the original payload unchanged, which
is the right fix when the poison was a state failure at apply time
(a sale for a SKU that did not exist yet) rather than a malformed
payload. A payload that still fails validation is rejected and
records nothing; the dead letter stays replayable. The event id
cannot be changed by a correction (it is the dedupe key), except
when the original record carried none — then the corrections must
supply one.

The replay is appended to the `dead_letter_replays` table (schema
version 5. Opening a version-4 database migrates additively: a new
table only, existing rows untouched, version bumped to 5): which
dead letter it fixed, the group, the corrected payload as
published, and the broker coordinates of the re-published record.
Replaying the same dead letter twice appends two rows; the
consumer's event-id dedupe is what makes the second copy a no-op
when it is consumed, not any uniqueness rule in the replay table.

The replay gauge counts re-publishes, not applications: whether
the corrected event was *applied* is decided by the next
`stream-consume` run and shows up in its consumption history
(M11). `POST /dead-letters/{id}/replay` returns 404 for an unknown
dead-letter id, 422 when the corrected payload still fails
validation, 400 when the replay cannot proceed (a correction
changing the event id, or the original record missing from the
broker log), and 503 when no broker log directory is configured
(`PHARMAFLOW_STREAM_LOG_DIR`). `GET /dead-letter-replays/latest`
returns 404 when no replay has been recorded yet, and `/metrics`
serves no replay gauge in that case. The Postgres store does not
persist replays (code-only path, unverified).

## Replay outcomes close the loop (M14)

The M13 gauge says a replay was re-published; it does not say what
happened next. M14 records exactly that, per replay, when the
replay's group's next *completed* `stream-consume` run processes
the re-published record:

```bash
python -m pharmaflow replay-outcomes --db pharmaflow.db --json
python -m pharmaflow replay-outcomes --db pharmaflow.db --replay-id 1 --latest
curl localhost:8000/replay-outcomes/latest
curl localhost:8000/dead-letter-replays/1   # the replay, with its outcomes embedded
curl localhost:8000/metrics   # per-group, per-outcome totals
```

One outcome row is appended to the `dead_letter_replay_outcomes`
table (schema version 6. Opening a version-5 database migrates
additively: a new table only, existing rows untouched, version
bumped to 6) per (replay, group): the outcome — `applied`,
`duplicate`, or `dead_lettered` — the new dead-letter row id when
the replayed event was dead-lettered again, and the consumption
run id that decided it. A replay with no outcome row has not been
consumed yet: it is pending, not failed, and contributes to no
outcome count. `GET /dead-letter-replays/{id}` returns 404 for an
unknown replay id, and `GET /replay-outcomes/latest` returns 404
when no outcome has been recorded yet; `/metrics` serves no outcome
gauge in that case.

The link key is the broker coordinates of the re-published record
(topic/partition/offset), not the event id: coordinates identify
the record even when the run could not recover its event id. Only
the replay's own group gets an outcome — another group consuming
the same topic sees the replayed event as its own business
(usually a duplicate, since the dedupe table is per-store, see
M11), not as the replay's fate. A run interrupted mid-consume
(including the crash-simulation hook) records nothing, so an event
applied pre-crash and redelivered afterwards is seen as a
duplicate by the recovery run — and that is what is recorded,
because the outcome describes what the completed run did. One
outcome per (replay, group) is enforced by a UNIQUE constraint: a
replay's outcome is decided once and never overwritten. The
Postgres store does not persist outcomes (code-only path,
unverified).

## Honest limits

- Stream-consumption gauges sum over recorded, completed runs only;
  a crash-interrupted run contributes nothing, and consumption under
  the real Kafka/Redpanda adapter (unverified, no broker here) is not
  recorded by this mechanism.
- Dead-letter history records completed runs only; a poison event
  dead-lettered by a crash-interrupted run is in the broker DLQ topic
  only, and a re-read poison event is recorded once per completed run
  that dead-lettered it. The Postgres store does not persist dead
  letters.
- Dead-letter replays re-publish through the broker; they never
  apply an event directly, so a replayed event that still fails at
  apply time is dead-lettered again by the next consume run as a
  *new* dead-letter row. The replay gauge counts re-publishes, not
  applications. Since M14 that new dead letter is linked back to
  the replay that produced it through the outcome row
  (`new_dead_letter_id`), so the chain dead letter -> replay ->
  outcome -> new dead letter is traversable from one record.
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
