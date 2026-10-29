# Streaming design: Milestone 4

> **Status (2026-10-22): IMPLEMENTED on the file-log broker (tested).
> Real-broker path NOT run.** The semantics below are implemented in
> `pharmaflow/streaming.py` and proven by `tests/test_streaming.py`
> (10 tests) against `FileLogBroker`. The `KafkaBroker` adapter against
> a real Redpanda/Kafka broker is code-only and unverified. No
> Docker or broker exists in this environment. Bring-up steps and the
> verification checklist live in [STREAMING_RUNBOOK.md](STREAMING_RUNBOOK.md).

Topic: `pharmaflow.inventory.events.v1` (Redpanda locally, Kafka-compatible).
Producer: inventory changes emitted after a committed import.
Delivery semantics: at-least-once. Consumers must be idempotent.
They reuse the core's row and event hash deduplication, and offsets
are committed only after successful processing.
Consumer groups: `reconciliation-v1`, `alerts-v1` (independent lag and monitoring).
Recovery: on restart a consumer resumes from its last committed offset. Uncommitted
events are redelivered and deduplicated, never double-applied. That recovery
behaviour is tested against the file-log broker only.
Dead-letter: events failing validation go to `pharmaflow.inventory.dlq` with the
validation errors attached. The DLQ is the streaming analogue of the batch quarantine store.

## As implemented (M4)

- **Events.** `StockReceived`, `SaleRecorded`, and `InventoryAdjusted`
  dataclasses. Validation reuses the batch rules
  (`validate_inventory_row` / `validate_sales_row`, SKU pattern, quantity
  bounds). Event key = SKU, so per-SKU ordering holds within a partition.
- **Broker abstraction.** `FileLogBroker` (tested): append-only JSONL
  partition files, partition = `sha256(key) % 4`, per-group committed
  offsets as atomically-replaced JSON files, offsets never move
  backwards, lag = sum of (end minus committed) over partitions.
  `KafkaBroker` (unverified): kafka-python-style adapter over the same
  interface, guarded import. Its group and offset methods are stubs.
- **Idempotent consumer.** The `processed_events` event-id dedupe table in
  the SQLite store is written during application and before the
  offset commit. Crash evidence, file-log broker only: apply 2 of 4 events,
  crash before any offset commit, restart, and all 4 are redelivered.
  Two are deduped, and final inventory equals the exactly-once outcome
  (MED-001 = 90, not 80).
- **Poison handling diverges from the original design in one detail.**
  The design said "failing validation 3x" before the DLQ. As
  implemented, a deterministically-invalid event is dead-lettered on
  first failure. Retrying a schema or business-rule failure cannot
  succeed, so the 3x retry would only delay the same outcome. Transient
  failures (store or broker errors) still abort the run without
  committing offsets, which is the retry mechanism.
