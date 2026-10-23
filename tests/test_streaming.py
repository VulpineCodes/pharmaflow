"""M4 — event streaming tests (FileLogBroker, the tested path).

Semantics under test (see pharmaflow/streaming.py module docstring):
at-least-once delivery, offsets committed only after processing,
idempotent application via the event-id dedupe table, poison events to
the DLQ with progress past them, independent consumer groups, and
partition/offset bookkeeping.

The KafkaBroker adapter is deliberately NOT tested here — no broker
exists in this environment and it is labelled unverified.
"""
import json
from pathlib import Path

import pytest

from pharmaflow.__main__ import main as cli_main
from pharmaflow.sqlite_store import SqliteStore
from pharmaflow.streaming import (
    DEFAULT_NUM_PARTITIONS,
    DLQ_TOPIC,
    DEFAULT_TOPIC,
    EventProducer,
    FileLogBroker,
    InventoryAdjusted,
    SaleRecorded,
    SimulatedCrashError,
    StockReceived,
    StreamingConsumer,
    partition_for_key,
    processed_event_ids,
)

GROUP = "reconciliation-v1"


def _seed_events(producer: EventProducer) -> None:
    producer.publish(StockReceived(event_id="evt-1", sku="MED-001",
                                   product_name="Paracetamol 500mg 100 tabs",
                                   quantity=100, reorder_point=20, unit_cost=4.50))
    producer.publish(StockReceived(event_id="evt-2", sku="MED-002",
                                   product_name="Ibuprofen 200mg 50 tabs",
                                   quantity=50, reorder_point=15, unit_cost=3.20))
    producer.publish(SaleRecorded(event_id="evt-3", sale_id="S-1", sku="MED-001",
                                  quantity_sold=10, unit_price=6.00))
    producer.publish(InventoryAdjusted(event_id="evt-4", sku="MED-002",
                                       delta=-5, reason="damaged stock"))


def _consume(log_dir, db, group=GROUP):
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db)
    try:
        result = StreamingConsumer(broker, store, group=group).consume_until_caught_up()
        return result, broker
    finally:
        store.close()


# --- (a) produce -> consume applies events --------------------------------

def test_produce_consume_applies_events_and_updates_inventory(tmp_path):
    log_dir, db = tmp_path / "log", tmp_path / "pharma.db"
    broker = FileLogBroker(log_dir)
    _seed_events(EventProducer(broker))
    assert broker.lag(GROUP, DEFAULT_TOPIC) == 4

    result, broker = _consume(log_dir, db)
    assert result.records_read == 4
    assert result.applied == 4 and result.duplicates == 0 and result.dlq == 0
    assert result.lag_before == 4 and result.lag_after == 0
    assert broker.lag(GROUP, DEFAULT_TOPIC) == 0

    with SqliteStore(db) as s:
        # 100 received - 10 sold = 90; 50 received - 5 adjusted = 45.
        assert s.inventory["MED-001"].quantity_on_hand == 90
        assert s.inventory["MED-002"].quantity_on_hand == 45
        assert len(s.sales) == 1 and s.sales[0].sale_id == "S-1"
        assert processed_event_ids(s) == {"evt-1", "evt-2", "evt-3", "evt-4"}

    # A second consume run is caught up immediately: nothing re-read.
    result2, _ = _consume(log_dir, db)
    assert result2.records_read == 0 and result2.applied == 0


def test_stock_received_restock_adds_to_existing_sku(tmp_path):
    log_dir, db = tmp_path / "log", tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    producer.publish(StockReceived(event_id="r1", sku="MED-009", product_name="ORS",
                                   quantity=30, reorder_point=5))
    producer.publish(StockReceived(event_id="r2", sku="MED-009", product_name="ORS",
                                   quantity=12, reorder_point=5))
    _consume(log_dir, db)
    with SqliteStore(db) as s:
        assert s.inventory["MED-009"].quantity_on_hand == 42


# --- (b) crash simulation: redelivery is deduped, exactly-once outcome -----

def test_crash_before_offset_commit_redelivers_and_dedupes(tmp_path):
    log_dir, db = tmp_path / "log", tmp_path / "pharma.db"
    broker = FileLogBroker(log_dir)
    _seed_events(EventProducer(broker))

    # "Process" #1: applies 2 events, then crashes BEFORE committing
    # any offsets — the classic at-least-once crash window.
    store = SqliteStore(db)
    consumer = StreamingConsumer(broker, store, group=GROUP)
    with pytest.raises(SimulatedCrashError):
        consumer.consume_until_caught_up(crash_after_applies=2)
    store.close()

    # Evidence of the crash state: no offsets committed (full lag),
    # but the 2 applied events left dedupe markers + store mutations.
    assert broker.lag(GROUP, DEFAULT_TOPIC) == 4
    assert broker.committed_offsets(GROUP, DEFAULT_TOPIC) == {
        p: 0 for p in range(DEFAULT_NUM_PARTITIONS)}
    with SqliteStore(db) as s:
        assert len(processed_event_ids(s)) == 2
        partial_qty = sum(r.quantity_on_hand for r in s.inventory.values())
    assert partial_qty in (100, 50, 150)  # exactly the first 2 applied events

    # "Restart": a brand-new broker + store + consumer, same log/db/group.
    # The 2 applied events are REDELIVERED (offsets never committed) and
    # must be deduped; the remaining 2 apply normally.
    result, broker2 = _consume(log_dir, db)
    assert result.records_read == 4          # everything redelivered
    assert result.duplicates == 2            # the crash-applied pair
    assert result.applied == 2               # only the un-applied pair
    assert result.dlq == 0 and result.lag_after == 0

    # Final state equals the exactly-once outcome from test (a).
    with SqliteStore(db) as s:
        assert s.inventory["MED-001"].quantity_on_hand == 90  # sale NOT applied twice
        assert s.inventory["MED-002"].quantity_on_hand == 45
        assert len(s.sales) == 1             # no duplicate sales row either
        assert processed_event_ids(s) == {"evt-1", "evt-2", "evt-3", "evt-4"}


def test_crash_after_sale_does_not_double_decrement(tmp_path):
    # Focused version of (b) on the dangerous event: one SKU, one sale.
    log_dir, db = tmp_path / "log", tmp_path / "pharma.db"
    producer = EventProducer(FileLogBroker(log_dir))
    producer.publish(StockReceived(event_id="a1", sku="MED-001", product_name="P",
                                   quantity=20, reorder_point=5))
    producer.publish(SaleRecorded(event_id="a2", sale_id="S-9", sku="MED-001",
                                  quantity_sold=7, unit_price=6.00))
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db)
    with pytest.raises(SimulatedCrashError):
        StreamingConsumer(broker, store, group=GROUP).consume_until_caught_up(
            crash_after_applies=2)
    store.close()
    result, _ = _consume(log_dir, db)
    assert result.duplicates == 2 and result.applied == 0
    with SqliteStore(db) as s:
        assert s.inventory["MED-001"].quantity_on_hand == 13  # 20 - 7, once
        assert len(s.sales) == 1


# --- (c) poison events -> DLQ, later valid events still processed ----------

def test_poison_events_go_to_dlq_and_consumer_progresses(tmp_path):
    log_dir, db = tmp_path / "log", tmp_path / "pharma.db"
    broker = FileLogBroker(log_dir)
    producer = EventProducer(broker)
    producer.publish(StockReceived(event_id="ok-1", sku="MED-001", product_name="P",
                                   quantity=10, reorder_point=2))
    # Poison 1: schema-invalid (negative sale quantity, missing event id fields ok)
    producer.publish_raw({"event_type": "SaleRecorded", "event_id": "bad-1",
                          "sale_id": "S-X", "sku": "MED-001",
                          "quantity_sold": -3, "unit_price": 1.0}, key="MED-001")
    # Poison 2: unknown event type.
    producer.publish_raw({"event_type": "TeleportStock", "event_id": "bad-2",
                          "sku": "MED-001", "quantity": 999}, key="MED-001")
    # Poison 3: state-dependent — adjustment for a SKU that does not exist.
    producer.publish_raw({"event_type": "InventoryAdjusted", "event_id": "bad-3",
                          "sku": "MED-404", "delta": 5, "reason": "ghost"}, key="MED-404")
    # A later VALID event for the same SKU as the poisons must still apply.
    producer.publish(SaleRecorded(event_id="ok-2", sale_id="S-2", sku="MED-001",
                                  quantity_sold=4, unit_price=6.00))

    result, broker = _consume(log_dir, db)
    assert result.records_read == 5
    assert result.applied == 2               # ok-1, ok-2
    assert result.dlq == 3 and result.skipped == 3
    assert result.duplicates == 0
    assert result.lag_after == 0             # not stuck on the poison records

    with SqliteStore(db) as s:
        assert s.inventory["MED-001"].quantity_on_hand == 6  # 10 - 4
        assert processed_event_ids(s) == {"ok-1", "ok-2"}

    # DLQ contents are inspectable: 3 records, errors + source coordinates.
    assert broker.lag("dlq-inspector", DLQ_TOPIC) == 3
    dlq_records = []
    for p in range(broker.num_partitions):
        dlq_records.extend(broker.read(DLQ_TOPIC, p, 0))
    assert len(dlq_records) == 3
    reasons = " ".join(" ".join(r.value["errors"]) for r in dlq_records)
    assert "quantity_sold must be >= 1" in reasons
    assert "unknown or missing event_type" in reasons
    assert "unknown sku" in reasons
    for r in dlq_records:
        assert r.value["source_topic"] == DEFAULT_TOPIC
        assert "original_value" in r.value

    # Re-running the consumer does not reprocess or re-DLQ anything.
    result2, broker = _consume(log_dir, db)
    assert result2.records_read == 0
    assert broker.lag("dlq-inspector", DLQ_TOPIC) == 3


# --- (d) two consumer groups progress independently ------------------------

def test_two_consumer_groups_progress_independently(tmp_path):
    log_dir, db_a, db_b = tmp_path / "log", tmp_path / "a.db", tmp_path / "b.db"
    broker = FileLogBroker(log_dir)
    _seed_events(EventProducer(broker))

    # Group A consumes everything.
    result_a, broker = _consume(log_dir, db_a, group="reconciliation-v1")
    assert result_a.applied == 4
    assert broker.lag("reconciliation-v1", DEFAULT_TOPIC) == 0
    # Group B has not moved: its offsets are entirely separate.
    assert broker.lag("alerts-v1", DEFAULT_TOPIC) == 4
    assert broker.committed_offsets("alerts-v1", DEFAULT_TOPIC) == {
        p: 0 for p in range(DEFAULT_NUM_PARTITIONS)}

    # Group B now consumes the same log into its own store and catches up.
    result_b, broker = _consume(log_dir, db_b, group="alerts-v1")
    assert result_b.applied == 4 and result_b.duplicates == 0
    assert broker.lag("alerts-v1", DEFAULT_TOPIC) == 0
    with SqliteStore(db_b) as s:
        assert s.inventory["MED-001"].quantity_on_hand == 90

    # New events: both groups see them; A consuming first does not affect B.
    EventProducer(broker).publish(
        SaleRecorded(event_id="evt-5", sale_id="S-5", sku="MED-001",
                     quantity_sold=5, unit_price=6.00))
    assert broker.lag("reconciliation-v1", DEFAULT_TOPIC) == 1
    assert broker.lag("alerts-v1", DEFAULT_TOPIC) == 1
    _consume(log_dir, db_a, group="reconciliation-v1")
    assert broker.lag("reconciliation-v1", DEFAULT_TOPIC) == 0
    assert broker.lag("alerts-v1", DEFAULT_TOPIC) == 1  # still pending for B


# --- (e) partition / offset bookkeeping -------------------------------------

def test_partition_and_offset_bookkeeping(tmp_path):
    log_dir = tmp_path / "log"
    broker = FileLogBroker(log_dir, num_partitions=4)
    producer = EventProducer(broker)

    published = []
    for i in range(10):
        sku = f"MED-{i % 3:03d}"
        published.append(producer.publish(StockReceived(
            event_id=f"p-{i}", sku=sku, product_name="P", quantity=1, reorder_point=0)))

    # Same key always lands in the same partition (ordering per SKU).
    by_sku: dict[str, int] = {}
    for (partition, _offset), i in zip(published, range(10)):
        sku = f"MED-{i % 3:03d}"
        assert partition == partition_for_key(sku, 4)
        by_sku.setdefault(sku, partition)
        assert by_sku[sku] == partition

    # Offsets are per-partition, start at 0, and increase monotonically
    # with no gaps.
    per_partition: dict[int, list[int]] = {}
    for partition, offset in published:
        per_partition.setdefault(partition, []).append(offset)
    for partition, offsets in per_partition.items():
        assert offsets == list(range(len(offsets)))
        assert broker.end_offset(DEFAULT_TOPIC, partition) == len(offsets)
    total = sum(broker.end_offset(DEFAULT_TOPIC, p) for p in range(4))
    assert total == 10

    # Lag math: fresh group lags by all 10; after committing a partial
    # offset by hand, lag drops by exactly that many.
    assert broker.lag("g1", DEFAULT_TOPIC) == 10
    some_partition = next(iter(per_partition))
    broker.commit_offsets("g1", DEFAULT_TOPIC, {some_partition: 2})
    assert broker.committed_offsets("g1", DEFAULT_TOPIC)[some_partition] == 2
    assert broker.lag("g1", DEFAULT_TOPIC) == 8
    # Committed offsets never move backwards.
    broker.commit_offsets("g1", DEFAULT_TOPIC, {some_partition: 1})
    assert broker.committed_offsets("g1", DEFAULT_TOPIC)[some_partition] == 2

    # The log survives a "restart": a new broker instance on the same
    # directory sees the same records and the same committed offsets.
    broker2 = FileLogBroker(log_dir, num_partitions=4)
    assert sum(broker2.end_offset(DEFAULT_TOPIC, p) for p in range(4)) == 10
    assert broker2.committed_offsets("g1", DEFAULT_TOPIC)[some_partition] == 2
    recs = broker2.read(DEFAULT_TOPIC, some_partition, 0)
    assert [r.offset for r in recs] == sorted(r.offset for r in recs)
    assert recs[0].value["event_type"] == "StockReceived"


# --- event schema validation (reused batch rules) ---------------------------

def test_event_validation_reuses_batch_rules(tmp_path):
    producer = EventProducer(FileLogBroker(tmp_path / "log"))
    # quantity_sold must be >= 1 (batch sales rule) — rejected at publish.
    with pytest.raises(ValueError):
        producer.publish(SaleRecorded(event_id="v1", sale_id="S", sku="MED-001",
                                      quantity_sold=0))
    # Negative stock receipt quantity (batch inventory rule, >= 0) — rejected.
    with pytest.raises(ValueError):
        producer.publish(StockReceived(event_id="v2", sku="MED-001",
                                       product_name="P", quantity=-1))
    # Bad SKU characters (batch business rule) — rejected.
    with pytest.raises(ValueError):
        producer.publish(InventoryAdjusted(event_id="v3", sku="bad sku!",
                                           delta=1))
    # Zero adjustment is not an event.
    with pytest.raises(ValueError):
        producer.publish(InventoryAdjusted(event_id="v4", sku="MED-001", delta=0))


# --- CLI ---------------------------------------------------------------------

def test_stream_cli_produce_and_consume(tmp_path, capsys):
    root = Path(__file__).resolve().parents[1]
    log_dir, db = tmp_path / "log", tmp_path / "cli.db"
    events = root / "sample_data" / "stream_events.jsonl"
    assert cli_main(["stream-produce", "--log-dir", str(log_dir),
                     "--events", str(events)]) == 0
    out = capsys.readouterr().out
    assert "Produced 4 event(s)" in out

    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", "reconciliation-v1"]) == 0
    out = capsys.readouterr().out
    assert "Lag before: 4" in out
    assert "applied=4" in out and "duplicates=0" in out and "skipped_dlq=0" in out
    assert "Lag after: 0" in out

    with SqliteStore(db) as s:
        assert s.inventory["MED-001"].quantity_on_hand == 90  # 100 - 10
        assert s.inventory["MED-002"].quantity_on_hand == 48  # 50 - 2

    # Second CLI consume: caught up, nothing applied, lag stays 0.
    assert cli_main(["stream-consume", "--log-dir", str(log_dir),
                     "--db", str(db), "--group", "reconciliation-v1"]) == 0
    out = capsys.readouterr().out
    assert "applied=0" in out and "Lag after: 0" in out


def test_kafka_broker_is_guarded_and_labelled_unverified(tmp_path):
    # Importing the module and constructing a FileLogBroker must never
    # require kafka; constructing KafkaBroker must fail loudly if the
    # package/broker is unavailable rather than silently pretending.
    from pharmaflow.streaming import KafkaBroker
    try:
        import kafka  # noqa: F401
        pytest.skip("kafka-python is installed here; guard path not exercised")
    except ImportError:
        with pytest.raises(RuntimeError, match="NOT|requires|no broker|KafkaBroker"):
            KafkaBroker()
    assert "NOT RUN" in KafkaBroker.__doc__
