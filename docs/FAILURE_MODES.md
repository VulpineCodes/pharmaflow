# Failure modes: what actually happens (batch core, verified by tests)

## Database unavailable

The running core is file-based (SQLite), so ingestion does not
depend on Postgres. The API serves the SQLite store, and
`/health` reports the store path and SKU count. It does not report
a degraded state for a database it does not use. The Postgres
store has never run against a server (see `docs/POSTGRES.md`), so
no Postgres failure behaviour is claimed. When a Postgres serving
layer is verified, `/health` behaviour for a down database will
need its own test.

## Job fails halfway through a file

Implemented: rows are processed one by one. Valid rows before the
bad or crashed point stay in the store. An invalid row goes to
quarantine and processing continues with the next row (see
`test_partial_failure_recovery`: MED-105, after three bad rows, is
still imported). If the process dies mid-file, the file hash was
not yet recorded, so re-running the file is safe: row hashes
deduplicate rows already loaded.

## Duplicate file imported

Implemented and tested: SHA-256 of the file bytes. A second import
of identical bytes is a no-op (`already_imported=True`,
`duplicates_skipped == rows_in`, inventory unchanged, audit entry
`import_skipped_duplicate_file`). Duplicate rows inside a file (or
across files with different bytes) are skipped by per-row hash.

## Consumer resumes after interruption: file-log broker (Milestone 4, tested)

The consumer exists and is tested against `FileLogBroker`. Offsets
commit only after processing, so a consumer that dies before
committing re-reads the uncommitted events on restart. The
event-id dedupe table makes that redelivery a no-op for events
already applied (see the crash tests in
`tests/test_streaming.py`). The tested scope is the file-log
broker only. No real Kafka or Redpanda broker has run, so no
resume behaviour against a real broker is claimed. See
`docs/STREAMING_DESIGN.md` and `docs/STREAMING_RUNBOOK.md`.

## Pipeline task fails (local runner)

Implemented and tested: the local runner retries a failing task
(up to 3 attempts in the runner's tested semantics; the DAG
declares 2 retries with a 5-minute delay for Airflow, which has
not run). When retries are exhausted, the pipeline aborts and
downstream tasks are skipped, not run. The failed run is recorded
in `pipeline_runs` (M9). Re-running the same logical date
completes without duplicating inventory, because file and row-hash
idempotency makes the re-run safe.
