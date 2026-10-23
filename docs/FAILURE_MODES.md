# Failure modes — what actually happens (batch core, verified by tests)

## Database unavailable
Current core is in-memory/file-based, so ingestion does not depend on Postgres yet.
When the Postgres serving layer lands (M3/M5): API `/health` must report degraded,
ingestion jobs must fail fast, Airflow retry policy (2 retries, 5-min delay) applies,
and no partial import is marked complete — the file hash is only recorded after all
rows are processed (valid or quarantined), so a crashed import can be safely re-run.

## Job fails halfway through a file
Implemented: rows are processed one by one. Valid rows before the bad/crashed point
stay in the store; an invalid row goes to quarantine and processing continues with
the next row (see `test_partial_failure_recovery` — MED-105, after three bad rows,
is still imported). If the *process* dies mid-file, the file hash was not yet
recorded, so re-running the file is safe: row hashes deduplicate rows already loaded.

## Duplicate file imported
Implemented + tested: SHA-256 of the file bytes. A second import of identical bytes
is a no-op (`already_imported=True`, `duplicates_skipped == rows_in`, inventory
unchanged, audit entry `import_skipped_duplicate_file`). Duplicate *rows* inside a
file (or across files with different bytes) are skipped by per-row hash.

## Consumer resumes after interruption — Kafka/Redpanda (Milestone 4, NOT implemented)
Designed, not built: see docs/STREAMING_DESIGN.md. No consumer exists yet, so there
is no resume behaviour to claim.
