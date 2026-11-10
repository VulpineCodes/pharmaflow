"""Event-driven ingestion — M4 (streaming).

Implements the design in ``docs/STREAMING_DESIGN.md`` against a *broker
abstraction* with two implementations:

1. ``FileLogBroker`` — **the tested path.** A durable, append-only,
   partitioned-by-key event log on local disk (JSON lines per partition +
   JSON committed-offset files per consumer group). It faithfully
   implements the Kafka semantics this project depends on:

   * **topics** and **partitions** — a record's partition is
     ``sha256(key) % num_partitions`` (deterministic, stable across
     processes — Python's built-in ``hash()`` is salted and is *not* used);
   * **consumer groups** with independent committed offsets;
   * **at-least-once delivery** — an offset is committed only *after* the
     record at that offset has been processed (applied, deduplicated, or
     dead-lettered). A consumer that crashes before committing gets the
     same records redelivered on restart;
   * **offsets are per-partition and monotonic**; lag is
     ``end_offset - committed_offset`` summed over partitions.

2. ``KafkaBroker`` — an adapter against the same interface written in
   kafka-python style. **NOT run / NOT verified:** there is no Docker and
   no Kafka/Redpanda broker in this environment, so this code has never
   connected to a real broker. It exists so the consumer/producer logic
   above the abstraction does not change when a real broker exists.
   See ``docs/STREAMING_RUNBOOK.md`` for the (unverified) real-broker
   bring-up steps.

Idempotent consumer
-------------------
At-least-once delivery means duplicates are *expected*. The consumer
(:class:`StreamingConsumer`) therefore records every applied ``event_id``
in a ``processed_events`` table in the same SQLite store it mutates
(created idempotently on first use). The dedupe marker is written as part
of applying an event — *before* the broker offset is committed — so the
failure window is safe in the right direction:

* crash before apply finishes → no dedupe marker, no offset commit →
  redelivery re-applies from scratch;
* crash after apply but before offset commit → dedupe marker exists,
  offset not committed → redelivery is recognised as a duplicate and
  skipped. A redelivered ``SaleRecorded`` therefore does **not**
  decrement stock twice. The net effect is exactly-once *application*
  over at-least-once *delivery*.

Poison events
-------------
An event that fails schema/business validation (or cannot be applied,
e.g. an adjustment that would drive stock negative, or an event for an
unknown SKU) is published to the DLQ topic
(``pharmaflow.inventory.dlq``) with its validation errors and source
coordinates attached, and its offset *is* committed — mirroring the batch
pipeline's quarantine philosophy (never silently drop, never get stuck).
Later valid events in the same partition are still processed.

Event key: the SKU (all three event types carry one), so every event for
a SKU lands in the same partition and is consumed in publish order for
that SKU.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Protocol, runtime_checkable

from .models import InventoryRecord, SalesRecord
from .validation import SKU_RE, validate_inventory_row, validate_sales_row

DEFAULT_TOPIC = "pharmaflow.inventory.events.v1"
DLQ_TOPIC = "pharmaflow.inventory.dlq"
DEFAULT_NUM_PARTITIONS = 4


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def partition_for_key(key: str, num_partitions: int) -> int:
    """Deterministic partition for a key: sha256(key) % num_partitions."""
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
    return int(digest, 16) % num_partitions


# ---------------------------------------------------------------------------
# Event schemas
# ---------------------------------------------------------------------------

class EventValidationError(ValueError):
    """An event failed validation. Carries the list of error messages."""

    def __init__(self, errors: list[str]):
        self.errors = list(errors)
        super().__init__("; ".join(self.errors))


def _check_event_id(raw: dict, errors: list[str]) -> str:
    event_id = str(raw.get("event_id") or "").strip()
    if not event_id:
        errors.append("event_id is required")
    return event_id


def _check_sku(raw: dict, errors: list[str]) -> str:
    sku = str(raw.get("sku") or "").strip()
    if not sku:
        errors.append("sku is required")
    elif not SKU_RE.match(sku):
        errors.append(f"sku has invalid characters: {sku!r}")
    return sku


@dataclass
class StockReceived:
    """New stock arrived for a SKU (creates the SKU if unknown)."""

    event_id: str
    sku: str
    product_name: str
    quantity: int
    reorder_point: int = 0
    unit_cost: float = 0.0
    expiry_date: date | None = None
    supplier: str = ""
    batch: str = ""
    occurred_at: str = ""

    event_type = "StockReceived"

    @property
    def key(self) -> str:
        return self.sku

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type, "event_id": self.event_id,
            "sku": self.sku, "product_name": self.product_name,
            "quantity": self.quantity, "reorder_point": self.reorder_point,
            "unit_cost": self.unit_cost,
            "expiry_date": self.expiry_date.isoformat() if self.expiry_date else None,
            "supplier": self.supplier, "batch": self.batch,
            "occurred_at": self.occurred_at,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "StockReceived":
        # Reuse the batch inventory validation rules verbatim: a stock
        # receipt IS an inventory row whose quantity_on_hand is the
        # received quantity.
        rec, errors = validate_inventory_row({
            "sku": str(raw.get("sku") or ""),
            "product_name": str(raw.get("product_name") or ""),
            "quantity_on_hand": _as_str(raw.get("quantity")),
            "reorder_point": _as_str(raw.get("reorder_point", 0)),
            "unit_cost": _as_str(raw.get("unit_cost", 0)),
            "expiry_date": _as_str(raw.get("expiry_date")),
            "supplier": str(raw.get("supplier") or ""),
            "batch": str(raw.get("batch") or ""),
        }, row_number=0)
        msgs = [e.message for e in errors]
        event_id = _check_event_id(raw, msgs)
        if msgs:
            raise EventValidationError(msgs)
        assert rec is not None
        return cls(
            event_id=event_id, sku=rec.sku, product_name=rec.product_name,
            quantity=rec.quantity_on_hand, reorder_point=rec.reorder_point,
            unit_cost=rec.unit_cost, expiry_date=rec.expiry_date,
            supplier=rec.supplier, batch=rec.batch,
            occurred_at=str(raw.get("occurred_at") or ""),
        )


@dataclass
class SaleRecorded:
    """A sale: decrements stock for the SKU and appends a sales record."""

    event_id: str
    sale_id: str
    sku: str
    quantity_sold: int
    unit_price: float = 0.0
    sale_date: date | None = None
    occurred_at: str = ""

    event_type = "SaleRecorded"

    @property
    def key(self) -> str:
        return self.sku

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type, "event_id": self.event_id,
            "sale_id": self.sale_id, "sku": self.sku,
            "quantity_sold": self.quantity_sold, "unit_price": self.unit_price,
            "sale_date": self.sale_date.isoformat() if self.sale_date else None,
            "occurred_at": self.occurred_at,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "SaleRecorded":
        # Reuse the batch sales validation rules verbatim.
        rec, errors = validate_sales_row({
            "sale_id": str(raw.get("sale_id") or ""),
            "sku": str(raw.get("sku") or ""),
            "quantity_sold": _as_str(raw.get("quantity_sold")),
            "unit_price": _as_str(raw.get("unit_price", 0)),
            "sale_date": _as_str(raw.get("sale_date")),
        }, row_number=0)
        msgs = [e.message for e in errors]
        event_id = _check_event_id(raw, msgs)
        # validate_sales_row does not check the SKU pattern; batch rules
        # for inventory do — apply the same pattern here for consistency.
        sku = str(raw.get("sku") or "").strip()
        if sku and not SKU_RE.match(sku):
            msgs.append(f"sku has invalid characters: {sku!r}")
        if msgs:
            raise EventValidationError(msgs)
        assert rec is not None
        return cls(
            event_id=event_id, sale_id=rec.sale_id, sku=rec.sku,
            quantity_sold=rec.quantity_sold, unit_price=rec.unit_price,
            sale_date=rec.sale_date,
            occurred_at=str(raw.get("occurred_at") or ""),
        )


@dataclass
class InventoryAdjusted:
    """A manual stock correction (count drift, damage, shrinkage): +/- delta."""

    event_id: str
    sku: str
    delta: int
    reason: str = ""
    occurred_at: str = ""

    event_type = "InventoryAdjusted"

    @property
    def key(self) -> str:
        return self.sku

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_type": self.event_type, "event_id": self.event_id,
            "sku": self.sku, "delta": self.delta, "reason": self.reason,
            "occurred_at": self.occurred_at,
        }

    @classmethod
    def from_dict(cls, raw: dict) -> "InventoryAdjusted":
        errors: list[str] = []
        event_id = _check_event_id(raw, errors)
        sku = _check_sku(raw, errors)
        delta_raw = raw.get("delta")
        delta: int | None
        if delta_raw is None or (isinstance(delta_raw, str) and not delta_raw.strip()):
            errors.append("delta is required")
            delta = None
        else:
            try:
                delta = int(delta_raw)  # type: ignore[arg-type]
                if isinstance(delta_raw, float) and delta_raw != delta:
                    raise ValueError
            except (TypeError, ValueError):
                errors.append(f"delta must be an integer, got {delta_raw!r}")
                delta = None
        if delta is not None:
            if delta == 0:
                errors.append("delta must be non-zero (a zero adjustment is not an event)")
            if abs(delta) > 100000:
                errors.append("delta implausibly large (|delta| > 100000)")
        if errors:
            raise EventValidationError(errors)
        assert delta is not None
        return cls(event_id=event_id, sku=sku, delta=delta,
                   reason=str(raw.get("reason") or ""),
                   occurred_at=str(raw.get("occurred_at") or ""))


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v == int(v):
        return str(int(v))
    return str(v)


_EVENT_TYPES = {
    "StockReceived": StockReceived,
    "SaleRecorded": SaleRecorded,
    "InventoryAdjusted": InventoryAdjusted,
}

StreamEvent = StockReceived | SaleRecorded | InventoryAdjusted


def parse_event(value: Any) -> StreamEvent:
    """Parse + validate a raw broker value into a typed event.

    Raises :class:`EventValidationError` for anything that is not a
    well-formed event — the consumer turns that into a DLQ record.
    """
    if not isinstance(value, dict):
        raise EventValidationError([f"event value must be a JSON object, got {type(value).__name__}"])
    event_type = str(value.get("event_type") or "").strip()
    cls = _EVENT_TYPES.get(event_type)
    if cls is None:
        raise EventValidationError([f"unknown or missing event_type: {event_type!r}"])
    return cls.from_dict(value)


# ---------------------------------------------------------------------------
# Broker abstraction
# ---------------------------------------------------------------------------

@dataclass
class BrokerRecord:
    topic: str
    partition: int
    offset: int
    key: str
    value: dict[str, Any]


@runtime_checkable
class BrokerProtocol(Protocol):
    """The minimum broker surface the producer/consumer depend on."""

    num_partitions: int

    def produce(self, topic: str, key: str, value: dict) -> tuple[int, int]:
        """Append a record; return (partition, offset)."""
        ...

    def end_offset(self, topic: str, partition: int) -> int:
        """Offset the next produced record in this partition will get."""
        ...

    def read(self, topic: str, partition: int, offset: int,
             max_records: int | None = None) -> list[BrokerRecord]:
        """Records from ``offset`` (inclusive) onwards, in offset order."""
        ...

    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:
        """Per-partition next-offset-to-consume for a group (default 0)."""
        ...

    def commit_offsets(self, group: str, topic: str, offsets: dict[int, int]) -> None:
        """Persist a group's per-partition next-offsets."""
        ...

    def lag(self, group: str, topic: str) -> int:
        """Total records between committed offsets and partition ends."""
        ...


_SAFE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_name(name: str) -> str:
    return _SAFE.sub("_", name)


class FileLogBroker:
    """Durable append-only event log on local disk — the TESTED broker.

    Layout under ``log_dir``::

        topics/<topic>/partition-<n>.jsonl   one JSON record per line
        offsets/<group>/<topic>.json         {"0": 3, "1": 0, ...}

    A record's offset is its line number within its partition file, so
    offsets are monotonic per partition by construction and survive
    process restarts (a new ``FileLogBroker`` on the same directory sees
    exactly the same log and the same committed offsets).
    """

    def __init__(self, log_dir: str | Path, num_partitions: int = DEFAULT_NUM_PARTITIONS):
        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")
        self.log_dir = Path(log_dir)
        self.num_partitions = num_partitions
        (self.log_dir / "topics").mkdir(parents=True, exist_ok=True)
        (self.log_dir / "offsets").mkdir(parents=True, exist_ok=True)

    # --- paths ---------------------------------------------------------
    def _partition_path(self, topic: str, partition: int) -> Path:
        d = self.log_dir / "topics" / _safe_name(topic)
        d.mkdir(parents=True, exist_ok=True)
        return d / f"partition-{partition}.jsonl"

    def _offsets_path(self, group: str, topic: str) -> Path:
        d = self.log_dir / "offsets" / _safe_name(group)
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{_safe_name(topic)}.json"

    # --- producer side ---------------------------------------------------
    def produce(self, topic: str, key: str, value: dict) -> tuple[int, int]:
        partition = partition_for_key(key, self.num_partitions)
        path = self._partition_path(topic, partition)
        offset = self.end_offset(topic, partition)
        record = {"offset": offset, "key": key, "value": value,
                  "produced_at": _now()}
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, sort_keys=True) + "\n")
            fh.flush()
            os.fsync(fh.fileno())  # durable before we report the offset
        return partition, offset

    # --- consumer side ---------------------------------------------------
    def end_offset(self, topic: str, partition: int) -> int:
        path = self._partition_path(topic, partition)
        if not path.exists():
            return 0
        with open(path, encoding="utf-8") as fh:
            return sum(1 for line in fh if line.strip())

    def read(self, topic: str, partition: int, offset: int,
             max_records: int | None = None) -> list[BrokerRecord]:
        path = self._partition_path(topic, partition)
        out: list[BrokerRecord] = []
        if not path.exists():
            return out
        with open(path, encoding="utf-8") as fh:
            for i, line in enumerate(fh):
                if i < offset or not line.strip():
                    continue
                raw = json.loads(line)
                out.append(BrokerRecord(topic=topic, partition=partition,
                                        offset=i, key=raw.get("key", ""),
                                        value=raw.get("value") or {}))
                if max_records is not None and len(out) >= max_records:
                    break
        return out

    def topics(self) -> list[str]:
        base = self.log_dir / "topics"
        return sorted(p.name for p in base.iterdir() if p.is_dir()) if base.exists() else []

    # --- consumer-group offsets -------------------------------------------
    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:
        path = self._offsets_path(group, topic)
        stored: dict[str, int] = {}
        if path.exists():
            stored = json.loads(path.read_text(encoding="utf-8"))
        return {p: int(stored.get(str(p), 0)) for p in range(self.num_partitions)}

    def commit_offsets(self, group: str, topic: str, offsets: dict[int, int]) -> None:
        # Never move an offset backwards: a stale consumer must not be
        # able to rewind a group's progress.
        current = self.committed_offsets(group, topic)
        merged = {p: max(current[p], int(offsets.get(p, current[p])))
                  for p in range(self.num_partitions)}
        path = self._offsets_path(group, topic)
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps({str(p): merged[p] for p in sorted(merged)},
                                  sort_keys=True), encoding="utf-8")
        os.replace(tmp, path)  # atomic rename: no torn offset files

    def lag(self, group: str, topic: str) -> int:
        committed = self.committed_offsets(group, topic)
        return sum(self.end_offset(topic, p) - committed[p]
                   for p in range(self.num_partitions))


class KafkaBroker:
    """Kafka/Redpanda adapter over the same interface — **NOT RUN.**

    Written in kafka-python style against the same ``BrokerProtocol``
    surface. There is no broker (and no Docker to run Redpanda) in this
    environment, so this class has never connected to anything; it is
    guarded so importing this module and using ``FileLogBroker`` never
    requires the ``kafka`` package. See ``docs/STREAMING_RUNBOOK.md``
    for the unverified bring-up procedure.
    """

    def __init__(self, bootstrap_servers: str = "localhost:19092",
                 num_partitions: int = DEFAULT_NUM_PARTITIONS):
        try:
            from kafka import KafkaConsumer, KafkaProducer  # type: ignore
        except ImportError as exc:  # pragma: no cover - no broker env
            raise RuntimeError(
                "KafkaBroker requires the 'kafka-python' package AND a "
                "running Kafka/Redpanda broker — neither is available in "
                "this environment. Use FileLogBroker (the tested path) or "
                "follow docs/STREAMING_RUNBOOK.md."
            ) from exc
        self._producer_cls, self._consumer_cls = KafkaProducer, KafkaConsumer
        self.bootstrap_servers = bootstrap_servers
        self.num_partitions = num_partitions
        self._producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all", enable_idempotence=True,
        )

    # The methods below mirror BrokerProtocol but are UNVERIFIED — they
    # have never been executed against a real broker in this project.
    def produce(self, topic: str, key: str, value: dict) -> tuple[int, int]:  # pragma: no cover
        meta = self._producer.send(topic, key=key, value=value).get(timeout=30)
        return meta.partition, meta.offset

    def end_offset(self, topic: str, partition: int) -> int:  # pragma: no cover
        raise NotImplementedError("unverified adapter: use consumer.end_offsets via a KafkaConsumer")

    def read(self, topic: str, partition: int, offset: int,
             max_records: int | None = None) -> list[BrokerRecord]:  # pragma: no cover
        raise NotImplementedError("unverified adapter: real consumption is group-based in Kafka")

    def committed_offsets(self, group: str, topic: str) -> dict[int, int]:  # pragma: no cover
        raise NotImplementedError("unverified adapter")

    def commit_offsets(self, group: str, topic: str, offsets: dict[int, int]) -> None:  # pragma: no cover
        raise NotImplementedError("unverified adapter")

    def lag(self, group: str, topic: str) -> int:  # pragma: no cover
        raise NotImplementedError("unverified adapter")


# ---------------------------------------------------------------------------
# Producer API
# ---------------------------------------------------------------------------

class EventProducer:
    """Publishes typed events. Typed publishes are validated *before*
    they reach the broker; ``publish_raw`` exists so tests (and genuinely
    broken upstream producers) can put unvalidated records on the topic —
    which is exactly what the consumer's DLQ path exists for."""

    def __init__(self, broker: BrokerProtocol, topic: str = DEFAULT_TOPIC):
        self.broker = broker
        self.topic = topic

    def publish(self, event: StreamEvent) -> tuple[int, int]:
        # Round-trip through from_dict so the exact validation the
        # consumer will apply is enforced at publish time too.
        parse_event(event.to_dict())
        return self.broker.produce(self.topic, event.key, event.to_dict())

    def publish_raw(self, value: dict, key: str = "") -> tuple[int, int]:
        if not key:
            key = str(value.get("sku") or value.get("event_id") or "unknown")
        return self.broker.produce(self.topic, key, value)


def load_events_file(path: str | Path) -> list[dict]:
    """Load an events file for ``stream-produce``: JSON Lines (one event
    object per line) or a single JSON array of event objects."""
    text = Path(path).read_text(encoding="utf-8")
    stripped = text.strip()
    if not stripped:
        return []
    if stripped.startswith("["):
        data = json.loads(stripped)
        if not isinstance(data, list):
            raise ValueError("events file JSON array expected")
        return data
    return [json.loads(line) for line in stripped.splitlines() if line.strip()]


# ---------------------------------------------------------------------------
# Idempotent consumer
# ---------------------------------------------------------------------------

class SimulatedCrashError(RuntimeError):
    """Raised by the consumer's crash-simulation hook: the process
    'dies' after applying events but *before* committing offsets."""


@dataclass
class DeadLetter:
    """One poison event dead-lettered by a completed consume run (M12).

    The same payload the broker's DLQ topic carries (errors plus
    source coordinates), plus the consumer group that dead-lettered
    it — so a poison event is queryable from the operational store
    without reading the broker log. ``event_id`` may be empty when
    the raw record carried none (e.g. a malformed event with no id
    at all); the source topic/partition/offset still identifies it.
    """

    group: str
    event_id: str
    errors: list[str]
    source_topic: str
    source_partition: int
    source_offset: int
    dead_lettered_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "group": self.group, "event_id": self.event_id,
            "errors": list(self.errors),
            "source_topic": self.source_topic,
            "source_partition": self.source_partition,
            "source_offset": self.source_offset,
            "dead_lettered_at": self.dead_lettered_at,
        }


@dataclass
class EventOutcome:
    """One record's disposition in a consume run (M14).

    The consumer appends one of these per record it processes, in
    consume order, so a completed run can say — per record, not just
    per run — what happened: ``applied``, ``duplicate`` (redelivered,
    recognised by event id), or ``dead_lettered`` (poison, written to
    the DLQ topic). ``event_id`` may be empty when the record failed
    to parse badly enough that no id could be recovered; the source
    coordinates (topic/partition/offset) always identify the record.
    These are the raw material for replay-outcome linkage: a
    re-published replay sits at known broker coordinates, so the
    outcome of the record at those coordinates *is* the replay's
    outcome.
    """

    event_id: str
    outcome: str  # "applied" | "duplicate" | "dead_lettered"
    topic: str
    partition: int
    offset: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "outcome": self.outcome,
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
        }


@dataclass
class ConsumeResult:
    group: str
    topic: str
    records_read: int = 0
    applied: int = 0
    duplicates: int = 0
    dlq: int = 0  # poison events dead-lettered (== "skipped" in CLI output)
    lag_before: int = 0
    lag_after: int = 0
    # M11: set by consume_once when the completed run was recorded in
    # the store's durable history; None when not recorded (in-memory
    # store, record=False, or a run that never completed).
    record_id: int | None = None
    # M12: the poison events this run dead-lettered, in the order they
    # were seen. Populated during the run whether or not the run is
    # recorded; ``dead_letter_ids`` holds the store row ids once
    # consume_once has persisted them (empty when not recorded).
    dead_letters: list[DeadLetter] = field(default_factory=list)
    dead_letter_ids: list[int] = field(default_factory=list)
    # M14: one outcome per record processed, in consume order (see
    # EventOutcome). Populated during the run whether or not the run
    # is recorded; ``replay_outcome_ids`` holds the store row ids
    # once consume_once has linked them to replays (empty when not
    # recorded, or when the run processed no replayed record).
    event_outcomes: list[EventOutcome] = field(default_factory=list)
    replay_outcome_ids: list[int] = field(default_factory=list)

    @property
    def skipped(self) -> int:
        return self.dlq

    def summary(self) -> str:
        return (f"group={self.group} topic={self.topic} read={self.records_read} "
                f"applied={self.applied} duplicates={self.duplicates} "
                f"skipped_dlq={self.dlq} lag_before={self.lag_before} lag_after={self.lag_after}")


_DEDUPE_SCHEMA = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
"""


def _ensure_dedupe_table(store) -> None:
    conn = getattr(store, "_conn", None)
    if conn is not None:
        conn.executescript(_DEDUPE_SCHEMA)
        conn.commit()
    elif not hasattr(store, "_processed_event_ids"):
        store._processed_event_ids = set()  # in-memory fallback (tests)


def _is_processed(store, event_id: str) -> bool:
    conn = getattr(store, "_conn", None)
    if conn is not None:
        return conn.execute(
            "SELECT 1 FROM processed_events WHERE event_id = ?", (event_id,)
        ).fetchone() is not None
    return event_id in store._processed_event_ids


def _mark_processed(store, event: StreamEvent) -> None:
    conn = getattr(store, "_conn", None)
    if conn is not None:
        conn.execute(
            "INSERT OR IGNORE INTO processed_events (event_id, event_type, applied_at) VALUES (?, ?, ?)",
            (event.event_id, event.event_type, _now()),
        )
        conn.commit()
    else:
        store._processed_event_ids.add(event.event_id)


def processed_event_ids(store) -> set[str]:
    """All event ids recorded as applied in this store (test/inspection)."""
    conn = getattr(store, "_conn", None)
    if conn is not None:
        _ensure_dedupe_table(store)
        return {r[0] for r in conn.execute("SELECT event_id FROM processed_events")}
    return set(getattr(store, "_processed_event_ids", set()))


def _apply_event(store, event: StreamEvent) -> None:
    """Apply one validated event to the store.

    Raises :class:`EventValidationError` for state-dependent business
    failures (unknown SKU, adjustment below zero) — the consumer treats
    those as poison too, because retrying them will never succeed.
    """
    inventory = store.inventory  # dict[str, InventoryRecord] snapshot
    if isinstance(event, StockReceived):
        existing = inventory.get(event.sku)
        if existing is None:
            store.upsert_inventory(InventoryRecord(
                sku=event.sku, product_name=event.product_name,
                quantity_on_hand=event.quantity, reorder_point=event.reorder_point,
                unit_cost=event.unit_cost, expiry_date=event.expiry_date,
                supplier=event.supplier, batch=event.batch))
        else:
            store.upsert_inventory(InventoryRecord(
                sku=existing.sku,
                product_name=event.product_name or existing.product_name,
                quantity_on_hand=existing.quantity_on_hand + event.quantity,
                reorder_point=event.reorder_point or existing.reorder_point,
                unit_cost=event.unit_cost or existing.unit_cost,
                expiry_date=event.expiry_date or existing.expiry_date,
                supplier=event.supplier or existing.supplier,
                batch=event.batch or existing.batch))
    elif isinstance(event, SaleRecorded):
        existing = inventory.get(event.sku)
        if existing is None:
            raise EventValidationError([f"sale for unknown sku: {event.sku!r}"])
        store.upsert_inventory(InventoryRecord(
            sku=existing.sku, product_name=existing.product_name,
            quantity_on_hand=existing.quantity_on_hand - event.quantity_sold,
            reorder_point=existing.reorder_point, unit_cost=existing.unit_cost,
            expiry_date=existing.expiry_date, supplier=existing.supplier,
            batch=existing.batch))
        store.append_sales(SalesRecord(
            sale_id=event.sale_id, sku=event.sku,
            quantity_sold=event.quantity_sold, sale_date=event.sale_date,
            unit_price=event.unit_price))
    elif isinstance(event, InventoryAdjusted):
        existing = inventory.get(event.sku)
        if existing is None:
            raise EventValidationError([f"adjustment for unknown sku: {event.sku!r}"])
        new_qty = existing.quantity_on_hand + event.delta
        if new_qty < 0:
            raise EventValidationError(
                [f"adjustment of {event.delta} would drive {event.sku} stock negative ({existing.quantity_on_hand} on hand)"])
        store.upsert_inventory(InventoryRecord(
            sku=existing.sku, product_name=existing.product_name,
            quantity_on_hand=new_qty,
            reorder_point=existing.reorder_point, unit_cost=existing.unit_cost,
            expiry_date=existing.expiry_date, supplier=existing.supplier,
            batch=existing.batch))
    else:  # pragma: no cover - parse_event guarantees the three types
        raise EventValidationError([f"unhandled event type: {type(event).__name__}"])


class StreamingConsumer:
    """Consumes a topic into a store, idempotently, until caught up.

    Processing order: partitions in ascending order, offsets ascending
    within a partition. Per-SKU ordering is preserved because a SKU's
    events all share one partition (key = SKU).

    Offsets are committed to the broker only after *every* record read
    in this run has been applied, deduplicated, or dead-lettered. The
    dedupe marker for an applied event is persisted during application,
    before that commit — see the module docstring.
    """

    def __init__(self, broker: BrokerProtocol, store, group: str,
                 topic: str = DEFAULT_TOPIC, dlq_topic: str = DLQ_TOPIC):
        self.broker = broker
        self.store = store
        self.group = group
        self.topic = topic
        self.dlq_topic = dlq_topic
        _ensure_dedupe_table(store)

    def _dead_letter(self, record: BrokerRecord, errors: list[str]) -> DeadLetter:
        """Publish the poison record to the DLQ topic and describe it.

        Returns the :class:`DeadLetter` so the caller can collect it on
        the run result (M12: durable history). The broker write happens
        here, during the run; the store write happens only later, in
        ``consume_once``, and only for runs that complete — a run
        interrupted mid-consume leaves its dead letters in the broker
        DLQ topic only, never in the store history.
        """
        dlq_value = {
            "original_value": record.value,
            "errors": errors,
            "source_topic": record.topic,
            "source_partition": record.partition,
            "source_offset": record.offset,
            "dead_lettered_at": _now(),
        }
        self.broker.produce(self.dlq_topic, record.key, dlq_value)
        return DeadLetter(
            group=self.group,
            event_id=str(record.value.get("event_id") or ""),
            errors=list(errors),
            source_topic=record.topic,
            source_partition=record.partition,
            source_offset=record.offset,
            dead_lettered_at=dlq_value["dead_lettered_at"],
        )

    def consume_until_caught_up(self, *, crash_after_applies: int | None = None,
                                commit: bool = True) -> ConsumeResult:
        """Process every record past this group's committed offsets.

        ``crash_after_applies`` is a test hook: after that many events
        have been *applied* (dedupe marker persisted, store mutated),
        raise :class:`SimulatedCrashError` **without committing any
        offsets** — the exact crash window at-least-once delivery must
        survive. Restarting a consumer for the same group then redelivers
        those events, and the dedupe table makes them no-ops.
        """
        result = ConsumeResult(group=self.group, topic=self.topic)
        result.lag_before = self.broker.lag(self.group, self.topic)
        committed = self.broker.committed_offsets(self.group, self.topic)
        new_offsets = dict(committed)
        applied_this_run = 0
        for partition in range(self.broker.num_partitions):
            start = committed[partition]
            for record in self.broker.read(self.topic, partition, start):
                result.records_read += 1
                try:
                    event = parse_event(record.value)
                except EventValidationError as exc:
                    result.dead_letters.append(self._dead_letter(record, exc.errors))
                    result.dlq += 1
                    result.event_outcomes.append(EventOutcome(
                        event_id=str(record.value.get("event_id") or ""),
                        outcome="dead_lettered",
                        topic=record.topic, partition=record.partition,
                        offset=record.offset))
                    new_offsets[partition] = record.offset + 1
                    continue
                if _is_processed(self.store, event.event_id):
                    result.duplicates += 1
                    result.event_outcomes.append(EventOutcome(
                        event_id=event.event_id, outcome="duplicate",
                        topic=record.topic, partition=record.partition,
                        offset=record.offset))
                    new_offsets[partition] = record.offset + 1
                    continue
                try:
                    _apply_event(self.store, event)
                except EventValidationError as exc:
                    result.dead_letters.append(self._dead_letter(record, exc.errors))
                    result.dlq += 1
                    result.event_outcomes.append(EventOutcome(
                        event_id=event.event_id, outcome="dead_lettered",
                        topic=record.topic, partition=record.partition,
                        offset=record.offset))
                    new_offsets[partition] = record.offset + 1
                    continue
                # Dedupe marker is persisted NOW, before any offset
                # commit — that ordering is the whole safety argument.
                _mark_processed(self.store, event)
                result.applied += 1
                applied_this_run += 1
                result.event_outcomes.append(EventOutcome(
                    event_id=event.event_id, outcome="applied",
                    topic=record.topic, partition=record.partition,
                    offset=record.offset))
                new_offsets[partition] = record.offset + 1
                if crash_after_applies is not None and applied_this_run >= crash_after_applies:
                    raise SimulatedCrashError(
                        f"simulated crash after applying {applied_this_run} event(s); "
                        f"offsets NOT committed (group={self.group})")
        if commit:
            self.broker.commit_offsets(self.group, self.topic, new_offsets)
        result.lag_after = self.broker.lag(self.group, self.topic)
        return result


def consume_once(log_dir: str | Path, db_path: str | Path, group: str,
                 topic: str = DEFAULT_TOPIC, record: bool = True) -> ConsumeResult:
    """CLI helper: one consume-until-caught-up run against a SQLite DB.

    M11: when the run *completes* and the store supports durable
    consumption history (``SqliteStore``), the outcome is appended to
    ``stream_consumption_runs`` and the run id is set on
    ``result.record_id``. A run that raises (e.g. the crash-simulation
    hook) records nothing — its partial application is visible in the
    inventory/dedupe state, not in the history. ``record=False`` keeps
    the old consume-only behaviour; stores without
    ``record_stream_consumption`` (the in-memory store) are left alone.

    M12: the same completion rule applies to the run's dead letters.
    Each poison event the completed run dead-lettered is appended to
    the ``dead_letters`` table (errors plus source coordinates) and
    its row id is collected on ``result.dead_letter_ids``. A poison
    event re-read after a crash is recorded once per completed run
    that dead-lettered it — the same per-run semantics as the
    duplicates column. The broker DLQ topic still receives the record
    during the run regardless of recording (M4 behaviour, unchanged).

    M14: the same completion rule applies to replay outcomes. For
    every record the completed run processed that sits at the broker
    coordinates of a recorded, still-unresolved replay *for this
    group*, one outcome row is appended to
    ``dead_letter_replay_outcomes`` (applied / duplicate /
    dead-lettered again, with the new dead-letter row id when the
    replayed event was dead-lettered again, and the consumption run
    id), and its row id is collected on
    ``result.replay_outcome_ids``. A run that raises records nothing.
    """
    from .sqlite_store import SqliteStore
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db_path)
    try:
        consumer = StreamingConsumer(broker, store, group=group, topic=topic)
        result = consumer.consume_until_caught_up()
        if record and hasattr(store, "record_stream_consumption"):
            result.record_id = store.record_stream_consumption(result)
        if record and hasattr(store, "record_dead_letter"):
            result.dead_letter_ids = [
                store.record_dead_letter(dl) for dl in result.dead_letters
            ]
        if record and hasattr(store, "record_replay_outcome"):
            result.replay_outcome_ids = _record_replay_outcomes(store, result)
        return result
    finally:
        store.close()


# ---------------------------------------------------------------------------
# Replay outcome linkage (M14)
# ---------------------------------------------------------------------------

@dataclass
class ReplayOutcome:
    """What the next consume run did with a re-published replay (M14).

    One row per (replay, group): the outcome the replay's group's
    next *completed* consume run recorded for the re-published
    record — ``applied``, ``duplicate`` (a second copy of a double
    replay, or an event another group already applied first: the
    dedupe table is per-store, not per-group, see M11), or
    ``dead_lettered`` (the replayed event still failed at apply
    time). ``new_dead_letter_id`` is the row id of the fresh dead
    letter when the outcome is ``dead_lettered`` — the link back
    from the new poison event to the replay that produced it — and
    ``None`` otherwise. ``consumption_run_id`` is the recorded
    consume run that decided the outcome. ``outcome_id`` is the
    store row id once the outcome has been recorded (``None``
    before recording). A replay with no outcome row has simply not
    been consumed yet: it is pending, not failed.
    """

    replay_id: int
    dead_letter_id: int
    group: str
    event_id: str
    outcome: str  # "applied" | "duplicate" | "dead_lettered"
    new_dead_letter_id: int | None = None
    consumption_run_id: int | None = None
    outcome_id: int | None = None
    recorded_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "outcome_id": self.outcome_id,
            "replay_id": self.replay_id,
            "dead_letter_id": self.dead_letter_id,
            "group": self.group,
            "event_id": self.event_id,
            "outcome": self.outcome,
            "new_dead_letter_id": self.new_dead_letter_id,
            "consumption_run_id": self.consumption_run_id,
            "recorded_at": self.recorded_at,
        }


def _record_replay_outcomes(store, result: ConsumeResult) -> list[int]:
    """Link a completed consume run's per-record outcomes to replays.

    For every record the run processed, look up a recorded replay
    sitting at that record's broker coordinates (topic/partition/
    offset) for this group that has no outcome yet, and record what
    the run did with it. Coordinates — not the event id — are the
    link key, because they identify the re-published record even
    when the run could not recover its event id (a record that
    fails to parse badly enough carries no id at all). The
    outcome's event id is the replay's own id: the record at those
    coordinates *is* the re-published record.

    Only the replay's own group gets an outcome: another group
    consuming the same topic sees the replayed event as its own
    business (usually a duplicate, per-store dedupe), not as the
    replay's fate. A crash-interrupted run never reaches here, so
    an event applied pre-crash and redelivered afterwards is seen
    as a duplicate by the recovery run — and that is what is
    recorded, because the outcome describes what the completed run
    did, not what the interrupted one attempted.
    """
    # Map (topic, partition, offset) -> the new dead-letter row id,
    # for poison events this run dead-lettered (consume order).
    new_dead_letter_ids: dict[tuple[str, int, int], int] = {}
    for dead_letter, row_id in zip(result.dead_letters, result.dead_letter_ids):
        new_dead_letter_ids[(dead_letter.source_topic,
                             dead_letter.source_partition,
                             dead_letter.source_offset)] = row_id
    outcome_ids: list[int] = []
    for event_outcome in result.event_outcomes:
        replay = store.find_unresolved_replay_at_coordinates(
            event_outcome.topic, event_outcome.partition,
            event_outcome.offset, result.group)
        if replay is None:
            continue
        outcome_ids.append(store.record_replay_outcome(ReplayOutcome(
            replay_id=replay.replay_id,
            dead_letter_id=replay.dead_letter_id,
            group=result.group,
            event_id=replay.event_id,
            outcome=event_outcome.outcome,
            new_dead_letter_id=new_dead_letter_ids.get(
                (event_outcome.topic, event_outcome.partition,
                 event_outcome.offset)),
            consumption_run_id=result.record_id,
        )))
    return outcome_ids


# ---------------------------------------------------------------------------
# Dead-letter replay (M13)
# ---------------------------------------------------------------------------

@dataclass
class DeadLetterReplay:
    """One corrected re-publish of a recorded dead letter (M13).

    A replay does NOT apply the event directly: it publishes the
    corrected payload back to the original topic, with the original
    event id, so the normal consumer path — validation, dedupe,
    offsets, and the next consume run's history — is the arbiter of
    what happens next. ``replay_id`` is the store row id once the
    replay has been recorded (``None`` before recording);
    ``partition``/``offset`` are the coordinates of the re-published
    record in the original topic.
    """

    dead_letter_id: int
    group: str
    event_id: str
    topic: str
    partition: int
    offset: int
    corrected_event: dict[str, Any]
    replay_id: int | None = None
    replayed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "replay_id": self.replay_id,
            "dead_letter_id": self.dead_letter_id,
            "group": self.group,
            "event_id": self.event_id,
            "topic": self.topic,
            "partition": self.partition,
            "offset": self.offset,
            "corrected_event": dict(self.corrected_event),
            "replayed_at": self.replayed_at,
        }


def replay_dead_letter(log_dir: str | Path, db_path: str | Path,
                       record_id: int,
                       corrections: dict[str, Any] | None = None,
                       group: str | None = None) -> DeadLetterReplay:
    """Re-publish a recorded dead letter with corrections (M13).

    ``record_id`` is the row id in the ``dead_letters`` table (see
    ``dead-letters`` CLI / ``GET /dead-letters``). The original
    record is read back from the broker at the dead letter's source
    coordinates; ``corrections`` (field overrides, top level only)
    are applied on top of it, and the result is validated through
    the same :func:`parse_event` rules the consumer applies.

    The event id is immutable: a correction that changes it is
    rejected, because dedupe is keyed on it — a replayed event must
    be recognised as the *same* event if it is replayed twice. The
    one exception is a dead letter whose original record carried no
    event id at all (``event_id`` empty): then the corrections must
    supply one, since a publishable event needs an id.

    ``corrections`` may be empty or omitted to re-publish the
    original payload unchanged — legitimate when the poison was a
    state failure at apply time (e.g. a sale for a SKU that did not
    exist yet) rather than a malformed payload. A payload that still
    fails :func:`parse_event` raises :class:`EventValidationError`
    and records nothing; the dead letter stays replayable.

    The corrected event is published with
    :class:`EventProducer.publish` (validated publish path) and the
    replay is appended to the store's ``dead_letter_replays`` table;
    the replay id is set on the returned object. A recording
    failure raises rather than silently losing the record. The
    *outcome* of the replay (applied / duplicate / dead-lettered
    again) is decided by the next ``stream-consume`` run for the
    group and shows up in its consumption history — replaying the
    same dead letter twice is safe: the second copy is deduplicated
    by event id at consume time.

    M14: that outcome is now also linked to the replay itself — see
    :class:`ReplayOutcome` and :func:`consume_once`. A replay with
    no outcome row simply has not been consumed yet (pending).
    """
    from .sqlite_store import SqliteStore
    if corrections is not None and not isinstance(corrections, dict):
        raise ValueError("corrections must be a JSON object (dict) or omitted")
    broker = FileLogBroker(log_dir)
    store = SqliteStore(db_path)
    try:
        letter = store.get_dead_letter(record_id)
        if letter is None:
            raise ValueError(f"no dead letter recorded with id {record_id}")
        records = broker.read(letter.topic, letter.partition,
                              letter.offset, max_records=1)
        if not records:
            raise ValueError(
                f"original record for dead letter #{record_id} is not in "
                f"the broker log ({letter.topic} partition {letter.partition} "
                f"offset {letter.offset}); check --log-dir")
        original = records[0].value
        if letter.event_id and str(original.get("event_id") or "") != letter.event_id:
            raise ValueError(
                f"broker record at dead letter #{record_id}'s coordinates "
                f"carries event_id {str(original.get('event_id') or '')!r}, "
                f"not the recorded {letter.event_id!r}; refusing to replay")
        base = dict(original) if isinstance(original, dict) else {}
        corrected = dict(base)
        corrected.update(corrections or {})
        new_event_id = str(corrected.get("event_id") or "").strip()
        if letter.event_id:
            if new_event_id != letter.event_id:
                raise ValueError(
                    "corrections must not change event_id "
                    f"({letter.event_id!r} -> {new_event_id!r}); the event id "
                    "is the dedupe key")
        elif not new_event_id:
            raise EventValidationError(
                ["event_id is required (the original record carried none; "
                 "supply it in the corrections)"])
        event = parse_event(corrected)
        replay_group = group or letter.group
        partition, offset = EventProducer(broker, topic=letter.topic).publish(event)
        replay = DeadLetterReplay(
            dead_letter_id=letter.record_id,
            group=replay_group,
            event_id=event.event_id,
            topic=letter.topic,
            partition=partition,
            offset=offset,
            corrected_event=event.to_dict(),
            replayed_at=_now(),
        )
        replay.replay_id = store.record_dead_letter_replay(replay)
        return replay
    finally:
        store.close()
