# Streaming runbook — the real-broker (Redpanda/Kafka) path

> **Status: UNVERIFIED.** Everything on this page describes how to run
> PharmaFlow streaming against a real Redpanda broker **once Docker is
> available**. None of it has been executed in the current environment
> (no Docker, no broker). The path that *has* been executed and tested
> is the file-log broker — see "Verified path" below.

## Verified path (file-log broker) — this is what the tests run

```bash
.venv/bin/python -m pytest -q        # 45 passed (incl. 10 streaming tests)

.venv/bin/python -m pharmaflow stream-produce \
  --log-dir /tmp/m4-demo/log --events sample_data/stream_events.jsonl
#   published key=MED-001 partition=2 offset=0 event_id=evt-0001 ...
#   Produced 4 event(s) to topic pharmaflow.inventory.events.v1

.venv/bin/python -m pharmaflow stream-consume \
  --log-dir /tmp/m4-demo/log --db /tmp/m4-demo/pharma.db --group reconciliation-v1
#   Lag before: 4
#   Consumed: read=4 applied=4 skipped_dlq=0 duplicates=0
#   Lag after: 0
```

Semantics (implemented in `pharmaflow/streaming.py`, proven by
`tests/test_streaming.py`): partitions = `sha256(sku) % 4`; per-group
committed offsets stored as JSON files; offsets committed only after
processing; event-id dedupe table (`processed_events`) in the SQLite
store makes redelivery after a crash a no-op; poison events are
dead-lettered to `pharmaflow.inventory.dlq` and their offsets commit.

## Unverified path (real broker) — steps to run when Docker exists

1. Start Redpanda (service defined in `docker-compose.yml`):

   ```bash
   docker compose up -d redpanda
   docker compose exec redpanda rpk cluster info
   ```

2. Create the topics (4 partitions to match `FileLogBroker`'s default):

   ```bash
   docker compose exec redpanda rpk topic create pharmaflow.inventory.events.v1 -p 4
   docker compose exec redpanda rpk topic create pharmaflow.inventory.dlq -p 4
   docker compose exec redpanda rpk topic list
   ```

3. Install the client library and point the (unverified) adapter at the
   broker:

   ```bash
   pip install kafka-python
   # KafkaBroker(bootstrap_servers="localhost:19092")  # host port in compose
   ```

4. Produce / consume / inspect from the host:

   ```bash
   # Produce one event with rpk (bypasses the Python producer entirely):
   echo '{"event_type":"SaleRecorded","event_id":"rpk-1","sale_id":"S-1","sku":"MED-001","quantity_sold":1,"unit_price":6.0}' \
     | docker compose exec -T redpanda rpk topic produce pharmaflow.inventory.events.v1 -k MED-001

   docker compose exec redpanda rpk topic consume pharmaflow.inventory.events.v1 --num 5
   docker compose exec redpanda rpk group describe reconciliation-v1   # lag per partition
   docker compose exec redpanda rpk topic consume pharmaflow.inventory.dlq --num 5
   ```

5. **Verification checklist before removing the "unverified" label:**
   - [ ] `KafkaBroker` produce/consume round-trip against Redpanda
         (the adapter's group/offset methods are currently stubs —
         real Kafka consumption is group-based polling, so expect the
         adapter, not the consumer, to need the most work);
   - [ ] crash/restart redelivery test re-run against the real broker
         with the same expected counts as `tests/test_streaming.py`;
   - [ ] DLQ record appears in `pharmaflow.inventory.dlq` for a poison
         event produced via `rpk`;
   - [ ] `rpk group describe` lag matches the consumer's reported lag.

## Delivery semantics, both paths

- **At-least-once delivery; exactly-once application** via the
  `processed_events` event-id dedupe table in the operational store.
- Offsets are committed **after** processing, never before.
- A poison event is dead-lettered once and its offset commits — the
  consumer never retries a deterministically-invalid event forever.
- Consumer groups (`reconciliation-v1`, `alerts-v1`) hold independent
  offsets; either can lag, crash, and catch up without affecting the
  other.
