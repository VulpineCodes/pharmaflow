"""Durable SQLite store — the M2 verified persistence path.

Uses only the Python standard library (``sqlite3``), so it is verifiable
anywhere the core runs: no server, no Docker, no third-party driver.

Behaviour matches the in-memory ``PharmaStore``:

* imported-file hashes survive process restarts — re-ingesting the same file
  against the same DB file is a no-op (``already_imported=True``);
* row hashes survive restarts, so duplicate rows in later files are skipped;
* inventory, sales, quarantine and audit rows are persisted and queryable
  after the store is closed and reopened;
* schema creation is idempotent: ``CREATE TABLE IF NOT EXISTS`` plus a
  ``schema_version`` table, safe to run on every open. Version 2 (M9)
  adds the ``pipeline_runs`` table; version 3 (M11) adds the
  ``stream_consumption_runs`` table; version 4 (M12) adds the
  ``dead_letters`` table; version 5 (M13) adds the
  ``dead_letter_replays`` table; version 6 (M14) adds the
  ``dead_letter_replay_outcomes`` table. Opening an older database
  migrates it additively (new table only, existing rows untouched)
  and bumps the recorded version to 6.
* pipeline runs (M9): ``record_pipeline_run`` / ``latest_pipeline_run``
  / ``list_pipeline_runs`` persist ``PipelineResult`` dicts (see
  ``orchestration.PipelineResult.to_dict``) so the last run survives a
  process restart and can be served by the API.
* stream consumption runs (M11): ``record_stream_consumption`` /
  ``latest_stream_consumption`` / ``list_stream_consumption`` persist
  completed ``ConsumeResult`` outcomes (see ``streaming.ConsumeResult``)
  so a group's consumption history survives a process restart and can
  be served by the API and rendered as Prometheus gauges.
* dead letters (M12): ``record_dead_letter`` / ``latest_dead_letter``
  / ``list_dead_letters`` persist each poison event a completed
  consume run dead-lettered (see ``streaming.DeadLetter``), so
  poison events are queryable without reading the broker log. Only
  completed runs record; a crash-interrupted run leaves its dead
  letters in the broker DLQ topic only.
* dead-letter replays (M13): ``record_dead_letter_replay`` /
  ``latest_dead_letter_replay`` / ``list_dead_letter_replays``
  persist each corrected re-publish of a recorded dead letter (see
  ``streaming.DeadLetterReplay``), so a replay is traceable from the
  dead letter it fixed to the broker coordinates of the corrected
  event — and a second replay of the same dead letter is visible as
  a second row, not a silent overwrite.
* replay outcomes (M14): ``record_replay_outcome`` /
  ``latest_replay_outcome`` / ``list_replay_outcomes`` persist what
  the replay's group's next completed consume run did with the
  re-published record (see ``streaming.ReplayOutcome``) — applied,
  duplicate, or dead-lettered again, with the new dead-letter row
  id when applicable — so an operator can close the loop on a
  replay without cross-referencing two histories by event id. A
  replay with no outcome row has not been consumed yet.

Each mutation is committed immediately. That is deliberate: a process that
dies mid-file has already durably recorded the rows it processed, and the
file hash is only recorded after all rows are processed (see
``docs/FAILURE_MODES.md``), so a re-run dedupes by row hash.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from .audit import AuditEntry, AuditLog
from .metrics import Metrics
from .models import (
    ImportResult,
    InventoryRecord,
    QuarantineEntry,
    SalesRecord,
    ValidationError,
)
from .quarantine import QuarantineStore

SCHEMA_VERSION = 6

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS file_hashes (
    file_hash TEXT PRIMARY KEY,
    imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS row_hashes (
    row_hash TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS imports (
    file_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    kind TEXT NOT NULL,
    rows_in INTEGER NOT NULL DEFAULT 0,
    rows_valid INTEGER NOT NULL DEFAULT 0,
    rows_quarantined INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
    imported_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    unit_cost REAL NOT NULL DEFAULT 0,
    expiry_date TEXT,
    supplier TEXT NOT NULL DEFAULT '',
    batch TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS sales (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_date TEXT,
    unit_price REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS quarantine (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    row_number INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    raw_json TEXT NOT NULL,
    errors_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    action TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    details TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    logical_date TEXT NOT NULL,
    succeeded INTEGER NOT NULL,
    failed_task TEXT,
    recorded_at TEXT NOT NULL,
    result_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS stream_consumption_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    topic TEXT NOT NULL,
    records_read INTEGER NOT NULL DEFAULT 0,
    applied INTEGER NOT NULL DEFAULT 0,
    duplicates INTEGER NOT NULL DEFAULT 0,
    dlq INTEGER NOT NULL DEFAULT 0,
    lag_before INTEGER NOT NULL DEFAULT 0,
    lag_after INTEGER NOT NULL DEFAULT 0,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    group_name TEXT NOT NULL,
    event_id TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL DEFAULT 0,
    offset INTEGER NOT NULL DEFAULT 0,
    errors_json TEXT NOT NULL,
    recorded_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letter_replays (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    dead_letter_id INTEGER NOT NULL,
    group_name TEXT NOT NULL,
    event_id TEXT NOT NULL DEFAULT '',
    topic TEXT NOT NULL,
    partition INTEGER NOT NULL DEFAULT 0,
    offset INTEGER NOT NULL DEFAULT 0,
    corrected_json TEXT NOT NULL,
    replayed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dead_letter_replay_outcomes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    replay_id INTEGER NOT NULL,
    dead_letter_id INTEGER NOT NULL,
    group_name TEXT NOT NULL,
    event_id TEXT NOT NULL DEFAULT '',
    outcome TEXT NOT NULL,
    new_dead_letter_id INTEGER,
    consumption_run_id INTEGER,
    recorded_at TEXT NOT NULL,
    UNIQUE (replay_id, group_name)
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class StoredPipelineRun:
    """One persisted daily-pipeline run (M9).

    ``result_dict`` is the ``PipelineResult.to_dict()`` payload exactly
    as recorded: ``{"logical_date": ..., "outcomes": [...]}``. It is
    kept as plain JSON data (not a reconstructed ``PipelineResult``)
    so storage never depends on the orchestration module — callers
    rebuild with ``PipelineResult.from_dict`` when they need behaviour.
    ``run_id`` is the append-only row id: ordering by it is insertion
    order, which is the definition of "latest" even if clocks skew.
    """

    run_id: int
    logical_date: date
    recorded_at: str
    result_dict: dict

    @property
    def succeeded(self) -> bool:
        return all(o.get("status") == "success" for o in self.result_dict.get("outcomes", []))

    @property
    def failed_task(self) -> str | None:
        for o in self.result_dict.get("outcomes", []):
            if o.get("status") == "failed":
                return o.get("name")
        return None


@dataclass
class StoredStreamConsumption:
    """One persisted, completed stream-consume run (M11).

    A snapshot of a :class:`streaming.ConsumeResult` exactly as the
    consume run finished it: records read, applied, duplicates
    (redelivered events recognised by event id), poison events
    dead-lettered, and lag before/after. ``run_id`` is the append-only
    row id: ordering by it is insertion order, which is the definition
    of "latest" even if clocks skew. Only runs that *completed* are
    recorded — a run interrupted by a crash (or by the crash-simulation
    hook) never reaches the record step, so its partial application
    shows up in inventory/dedupe state, not in this history.
    """

    run_id: int
    group: str
    topic: str
    records_read: int
    applied: int
    duplicates: int
    dlq: int
    lag_before: int
    lag_after: int
    recorded_at: str


@dataclass
class StoredDeadLetter:
    """One poison event dead-lettered by a completed consume run (M12).

    A snapshot of a :class:`streaming.DeadLetter` exactly as the run
    finished it: the consumer group that dead-lettered it, the event
    id (possibly empty for a malformed record), the validation errors,
    and the source topic/partition/offset where the poison record
    sits. ``record_id`` is the append-only row id: ordering by it is
    insertion order, which is the definition of "latest" even if
    clocks skew. Only completed runs record — a run interrupted by a
    crash (or by the crash-simulation hook) never reaches the record
    step, so its dead letters live in the broker DLQ topic only.
    """

    record_id: int
    group: str
    event_id: str
    errors: list[str]
    topic: str
    partition: int
    offset: int
    recorded_at: str


@dataclass
class StoredDeadLetterReplay:
    """One recorded dead-letter replay (M13).

    A snapshot of a :class:`streaming.DeadLetterReplay`: which dead
    letter it fixed (``dead_letter_id``), the consumer group it was
    replayed for, the (unchanged) event id, the corrected event
    payload as published, and the topic/partition/offset of the
    re-published record. ``replay_id`` is the append-only row id:
    ordering by it is insertion order, which is the definition of
    "latest" even if clocks skew. A dead letter replayed twice has
    two rows — the history is append-only, never overwritten — and
    the consumer's event-id dedupe makes the second copy a no-op
    when it is consumed.
    """

    replay_id: int
    dead_letter_id: int
    group: str
    event_id: str
    topic: str
    partition: int
    offset: int
    corrected_event: dict
    replayed_at: str


@dataclass
class StoredReplayOutcome:
    """One recorded replay outcome (M14).

    A snapshot of a :class:`streaming.ReplayOutcome`: which replay
    it closes the loop on (``replay_id``, and the dead letter that
    replay fixed), the consumer group whose completed consume run
    decided it, the (unchanged) event id, and the outcome —
    ``applied``, ``duplicate``, or ``dead_lettered``. When the
    replayed event was dead-lettered again, ``new_dead_letter_id``
    is the row id of the fresh dead letter, so the chain
    dead letter -> replay -> outcome -> new dead letter is
    traversable without cross-referencing histories by event id.
    ``consumption_run_id`` is the recorded consume run that decided
    the outcome. ``outcome_id`` is the append-only row id: ordering
    by it is insertion order, which is the definition of "latest"
    even if clocks skew. One outcome per (replay, group) — enforced
    by a UNIQUE constraint — so a replay's outcome is never
    overwritten by a later run; a replay with no row here has not
    been consumed yet.
    """

    outcome_id: int
    replay_id: int
    dead_letter_id: int
    group: str
    event_id: str
    outcome: str
    new_dead_letter_id: int | None
    consumption_run_id: int | None
    recorded_at: str


class SqliteStore:
    """StoreProtocol implementation backed by a single SQLite file."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        if self.path.parent and str(self.path.parent) not in ("", "."):
            self.path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False: the FastAPI serving layer (M5) handles
        # requests on a threadpool sharing this one store/connection.
        # sqlite3.threadsafety == 3 on CPython 3.12/SQLite 3.45 (the
        # supported runtime), i.e. connections are serialised internally,
        # so sharing is safe; the CLI/tests use it single-threaded anyway.
        self._conn = sqlite3.connect(str(self.path), check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self.metrics = Metrics()
        self._init_schema()

    # --- lifecycle ----------------------------------------------------
    def _init_schema(self) -> None:
        """Idempotent: safe to call on every open / repeatedly.

        Migration is additive: ``CREATE TABLE IF NOT EXISTS`` adds any
        table a database predates (e.g. ``pipeline_runs`` on a v1 file),
        and the recorded version is then bumped to ``SCHEMA_VERSION``.
        No existing table or row is altered or deleted, so a v1 database
        keeps its inventory, imports, quarantine, and audit rows.
        """
        self._conn.executescript(_SCHEMA)
        row = self._conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        if row is None:
            self._conn.execute(
                "INSERT INTO schema_version (version, applied_at) VALUES (?, ?)",
                (SCHEMA_VERSION, _now()),
            )
        elif int(row["version"]) < SCHEMA_VERSION:
            self._conn.execute(
                "UPDATE schema_version SET version = ?, applied_at = ?",
                (SCHEMA_VERSION, _now()),
            )
        self._conn.commit()

    @property
    def schema_version(self) -> int:
        row = self._conn.execute("SELECT version FROM schema_version LIMIT 1").fetchone()
        return int(row["version"]) if row else 0

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None  # type: ignore[assignment]

    def __enter__(self) -> "SqliteStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- file / row idempotency --------------------------------------
    def has_file_hash(self, file_hash: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM file_hashes WHERE file_hash = ?", (file_hash,)
        ).fetchone() is not None

    def add_file_hash(self, file_hash: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO file_hashes (file_hash, imported_at) VALUES (?, ?)",
            (file_hash, _now()),
        )
        self._conn.commit()

    def has_row_hash(self, row_hash: str) -> bool:
        return self._conn.execute(
            "SELECT 1 FROM row_hashes WHERE row_hash = ?", (row_hash,)
        ).fetchone() is not None

    def add_row_hash(self, row_hash: str, file_hash: str = "") -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO row_hashes (row_hash, file_hash) VALUES (?, ?)",
            (row_hash, file_hash),
        )
        self._conn.commit()

    # --- records --------------------------------------------------------
    def upsert_inventory(self, record: InventoryRecord) -> None:
        self._conn.execute(
            """
            INSERT INTO inventory (sku, product_name, quantity_on_hand, reorder_point,
                                   unit_cost, expiry_date, supplier, batch)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(sku) DO UPDATE SET
                product_name=excluded.product_name,
                quantity_on_hand=excluded.quantity_on_hand,
                reorder_point=excluded.reorder_point,
                unit_cost=excluded.unit_cost,
                expiry_date=excluded.expiry_date,
                supplier=excluded.supplier,
                batch=excluded.batch
            """,
            (
                record.sku, record.product_name, record.quantity_on_hand,
                record.reorder_point, record.unit_cost,
                record.expiry_date.isoformat() if record.expiry_date else None,
                record.supplier, record.batch,
            ),
        )
        self._conn.commit()

    def append_sales(self, record: SalesRecord) -> None:
        self._conn.execute(
            "INSERT INTO sales (sale_id, sku, quantity_sold, sale_date, unit_price) VALUES (?, ?, ?, ?, ?)",
            (
                record.sale_id, record.sku, record.quantity_sold,
                record.sale_date.isoformat() if record.sale_date else None,
                record.unit_price,
            ),
        )
        self._conn.commit()

    def add_quarantine(self, entry: QuarantineEntry) -> None:
        self._conn.execute(
            "INSERT INTO quarantine (row_number, source_file, file_hash, raw_json, errors_json) VALUES (?, ?, ?, ?, ?)",
            (
                entry.row_number, entry.source_file, entry.file_hash,
                json.dumps(entry.raw, sort_keys=True),
                json.dumps([e.to_dict() for e in entry.errors]),
            ),
        )
        self._conn.commit()

    def append_audit(self, action, source_file, file_hash, details):
        entry = AuditEntry(action, source_file, file_hash, details)
        self._conn.execute(
            "INSERT INTO audit (timestamp, action, source_file, file_hash, details) VALUES (?, ?, ?, ?, ?)",
            (entry.timestamp, entry.action, entry.source_file, entry.file_hash, entry.details),
        )
        self._conn.commit()
        return entry

    # --- reporting ------------------------------------------------------
    def record_import(self, result: ImportResult) -> None:
        # INSERT OR IGNORE: the first completed import is the record of
        # record; a later duplicate attempt must not overwrite its counts.
        self._conn.execute(
            """
            INSERT OR IGNORE INTO imports
                (file_hash, source_file, kind, rows_in, rows_valid,
                 rows_quarantined, duplicates_skipped, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result.file_hash, result.source_file, result.kind, result.rows_in,
                result.rows_valid, result.rows_quarantined,
                result.duplicates_skipped, _now(),
            ),
        )
        self._conn.commit()

    def list_imports(self) -> list[ImportResult]:
        rows = self._conn.execute(
            "SELECT * FROM imports ORDER BY imported_at, file_hash"
        ).fetchall()
        return [
            ImportResult(
                source_file=r["source_file"], file_hash=r["file_hash"], kind=r["kind"],
                rows_in=r["rows_in"], rows_valid=r["rows_valid"],
                rows_quarantined=r["rows_quarantined"],
                duplicates_skipped=r["duplicates_skipped"],
            )
            for r in rows
        ]

    # --- pipeline runs (M9) -------------------------------------------------
    def record_pipeline_run(self, result_dict: dict) -> int:
        """Append one completed pipeline run; return its run id.

        Every run is appended, including failed ones and re-runs of the
        same logical date — run history is the audit trail, and ingest
        idempotency (not run-history overwriting) is what keeps a
        re-run from duplicating inventory. ``result_dict`` must be a
        ``PipelineResult.to_dict()`` payload; outcome ``result`` values
        are JSON-encoded with ``default=str`` so an exotic value can
        never break recording of an otherwise finished run.
        """
        outcomes = result_dict.get("outcomes", [])
        succeeded = 1 if all(o.get("status") == "success" for o in outcomes) else 0
        failed_task = next(
            (o.get("name") for o in outcomes if o.get("status") == "failed"), None
        )
        cur = self._conn.execute(
            """
            INSERT INTO pipeline_runs
                (logical_date, succeeded, failed_task, recorded_at, result_json)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                str(result_dict["logical_date"]),
                succeeded,
                failed_task,
                _now(),
                json.dumps(result_dict, default=str, sort_keys=True),
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def count_pipeline_runs(self) -> int:
        row = self._conn.execute("SELECT COUNT(*) AS n FROM pipeline_runs").fetchone()
        return int(row["n"])

    def latest_pipeline_run(self) -> StoredPipelineRun | None:
        """The most recently recorded run, or ``None`` if no run exists."""
        rows = self.list_pipeline_runs(limit=1)
        return rows[0] if rows else None

    def list_pipeline_runs(self, limit: int = 20) -> list[StoredPipelineRun]:
        """Runs newest-first. ``limit`` is clamped to [1, 100]."""
        limit = max(1, min(int(limit), 100))
        rows = self._conn.execute(
            "SELECT * FROM pipeline_runs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [self._stored_run_from_row(r) for r in rows]

    @staticmethod
    def _stored_run_from_row(r) -> StoredPipelineRun:
        return StoredPipelineRun(
            run_id=int(r["id"]),
            logical_date=date.fromisoformat(r["logical_date"]),
            recorded_at=r["recorded_at"],
            result_dict=json.loads(r["result_json"]),
        )

    # --- stream consumption runs (M11) ------------------------------------
    def record_stream_consumption(self, result) -> int:
        """Append one completed consume run; return its run id.

        ``result`` is duck-typed (``group``/``topic``/``records_read``/
        ``applied``/``duplicates``/``dlq``/``lag_before``/``lag_after``)
        so tests can build it without running a consumer; a
        :class:`streaming.ConsumeResult` satisfies it directly. Every
        completed run is appended, including a run that read 0 records
        (a no-op catch-up is still history: it proves the group was
        checked and was current) and re-runs after a crash (the
        duplicates column is what shows the redelivery). A recording
        failure raises rather than silently losing the run record.
        """
        cur = self._conn.execute(
            """
            INSERT INTO stream_consumption_runs
                (group_name, topic, records_read, applied, duplicates, dlq,
                 lag_before, lag_after, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(result.group),
                str(result.topic),
                int(result.records_read),
                int(result.applied),
                int(result.duplicates),
                int(result.dlq),
                int(result.lag_before),
                int(result.lag_after),
                _now(),
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def count_stream_consumption(self, group: str | None = None) -> int:
        """Recorded consume runs, optionally for one group."""
        if group is None:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM stream_consumption_runs"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM stream_consumption_runs WHERE group_name = ?",
                (group,),
            ).fetchone()
        return int(row["n"])

    def latest_stream_consumption(self, group: str | None = None) -> StoredStreamConsumption | None:
        """The most recently recorded consume run, or ``None``."""
        rows = self.list_stream_consumption(group=group, limit=1)
        return rows[0] if rows else None

    def list_stream_consumption(self, group: str | None = None,
                                limit: int = 20) -> list[StoredStreamConsumption]:
        """Consume runs newest-first. ``limit`` is clamped to [1, 100]."""
        limit = max(1, min(int(limit), 100))
        if group is None:
            rows = self._conn.execute(
                "SELECT * FROM stream_consumption_runs ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM stream_consumption_runs WHERE group_name = ? "
                "ORDER BY id DESC LIMIT ?",
                (group, limit),
            ).fetchall()
        return [self._stored_consumption_from_row(r) for r in rows]

    def stream_consumption_totals(self, group: str | None = None) -> dict[str, int]:
        """Summed counters over recorded consume runs, optionally one group.

        Sums are over *completed, recorded* runs only — the same scope
        as the history itself. A crash-interrupted run contributes
        nothing (it was never recorded); the broker's own lag/DLQ
        gauges (M8) remain the source of truth for live stream state.
        """
        if group is None:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(records_read), 0) AS records_read,"
                " COALESCE(SUM(applied), 0) AS applied,"
                " COALESCE(SUM(duplicates), 0) AS duplicates,"
                " COALESCE(SUM(dlq), 0) AS dlq,"
                " COUNT(*) AS runs FROM stream_consumption_runs"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COALESCE(SUM(records_read), 0) AS records_read,"
                " COALESCE(SUM(applied), 0) AS applied,"
                " COALESCE(SUM(duplicates), 0) AS duplicates,"
                " COALESCE(SUM(dlq), 0) AS dlq,"
                " COUNT(*) AS runs FROM stream_consumption_runs WHERE group_name = ?",
                (group,),
            ).fetchone()
        return {
            "records_read": int(row["records_read"]),
            "applied": int(row["applied"]),
            "duplicates": int(row["duplicates"]),
            "dlq": int(row["dlq"]),
            "runs": int(row["runs"]),
        }

    def stream_consumption_totals_by_group(self) -> dict[str, dict[str, int]]:
        """Summed counters per consumer group, one GROUP BY query.

        Groups that only appear in runs older than any listing limit
        are still included — unlike deriving the group set from a
        newest-first listing. Empty store -> empty dict.
        """
        rows = self._conn.execute(
            "SELECT group_name,"
            " COALESCE(SUM(records_read), 0) AS records_read,"
            " COALESCE(SUM(applied), 0) AS applied,"
            " COALESCE(SUM(duplicates), 0) AS duplicates,"
            " COALESCE(SUM(dlq), 0) AS dlq,"
            " COUNT(*) AS runs FROM stream_consumption_runs GROUP BY group_name"
        ).fetchall()
        return {
            r["group_name"]: {
                "records_read": int(r["records_read"]),
                "applied": int(r["applied"]),
                "duplicates": int(r["duplicates"]),
                "dlq": int(r["dlq"]),
                "runs": int(r["runs"]),
            }
            for r in rows
        }

    @staticmethod
    def _stored_consumption_from_row(r) -> StoredStreamConsumption:
        return StoredStreamConsumption(
            run_id=int(r["id"]),
            group=r["group_name"],
            topic=r["topic"],
            records_read=int(r["records_read"]),
            applied=int(r["applied"]),
            duplicates=int(r["duplicates"]),
            dlq=int(r["dlq"]),
            lag_before=int(r["lag_before"]),
            lag_after=int(r["lag_after"]),
            recorded_at=r["recorded_at"],
        )

    # --- dead letters (M12) -------------------------------------------------
    def record_dead_letter(self, dead_letter) -> int:
        """Append one dead-lettered poison event; return its record id.

        ``dead_letter`` is duck-typed (``group``/``event_id``/
        ``errors``/``source_topic``/``source_partition``/
        ``source_offset``) so tests can build it without running a
        consumer; a :class:`streaming.DeadLetter` satisfies it
        directly. A recording failure raises rather than silently
        losing the record. A poison event re-read after a crash is
        recorded once per completed run that dead-lettered it — the
        same per-run semantics as the consumption history's duplicates
        column — so the same event id can legitimately appear more
        than once, under different groups or different runs.
        """
        cur = self._conn.execute(
            """
            INSERT INTO dead_letters
                (group_name, event_id, topic, partition, offset,
                 errors_json, recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(dead_letter.group),
                str(dead_letter.event_id or ""),
                str(dead_letter.source_topic),
                int(dead_letter.source_partition),
                int(dead_letter.source_offset),
                json.dumps(list(dead_letter.errors), sort_keys=True),
                _now(),
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def count_dead_letters(self, group: str | None = None) -> int:
        """Recorded dead letters, optionally for one group."""
        if group is None:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letters"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letters WHERE group_name = ?",
                (group,),
            ).fetchone()
        return int(row["n"])

    def latest_dead_letter(self, group: str | None = None) -> StoredDeadLetter | None:
        """The most recently recorded dead letter, or ``None``."""
        rows = self.list_dead_letters(group=group, limit=1)
        return rows[0] if rows else None

    def list_dead_letters(self, group: str | None = None,
                          limit: int = 20) -> list[StoredDeadLetter]:
        """Dead letters newest-first. ``limit`` is clamped to [1, 100]."""
        limit = max(1, min(int(limit), 100))
        if group is None:
            rows = self._conn.execute(
                "SELECT * FROM dead_letters ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT * FROM dead_letters WHERE group_name = ? "
                "ORDER BY id DESC LIMIT ?",
                (group, limit),
            ).fetchall()
        return [self._stored_dead_letter_from_row(r) for r in rows]

    def dead_letter_totals_by_group(self) -> dict[str, int]:
        """Recorded dead letters per consumer group, one GROUP BY query.

        Groups that only appear in rows older than any listing limit
        are still included — unlike deriving the group set from a
        newest-first listing. Empty store -> empty dict.
        """
        rows = self._conn.execute(
            "SELECT group_name, COUNT(*) AS n FROM dead_letters "
            "GROUP BY group_name"
        ).fetchall()
        return {r["group_name"]: int(r["n"]) for r in rows}

    @staticmethod
    def _stored_dead_letter_from_row(r) -> StoredDeadLetter:
        return StoredDeadLetter(
            record_id=int(r["id"]),
            group=r["group_name"],
            event_id=r["event_id"],
            errors=json.loads(r["errors_json"]),
            topic=r["topic"],
            partition=int(r["partition"]),
            offset=int(r["offset"]),
            recorded_at=r["recorded_at"],
        )

    # --- dead-letter replays (M13) ----------------------------------------
    def get_dead_letter(self, record_id: int) -> StoredDeadLetter | None:
        """One recorded dead letter by its row id, or ``None``."""
        row = self._conn.execute(
            "SELECT * FROM dead_letters WHERE id = ?", (int(record_id),)
        ).fetchone()
        return self._stored_dead_letter_from_row(row) if row else None

    def get_dead_letter_replay(self, replay_id: int) -> StoredDeadLetterReplay | None:
        """One recorded dead-letter replay by its row id, or ``None``."""
        row = self._conn.execute(
            "SELECT * FROM dead_letter_replays WHERE id = ?", (int(replay_id),)
        ).fetchone()
        return self._stored_dead_letter_replay_from_row(row) if row else None

    def record_dead_letter_replay(self, replay) -> int:
        """Append one dead-letter replay; return its replay id.

        ``replay`` is duck-typed (``dead_letter_id``/``group``/
        ``event_id``/``topic``/``partition``/``offset``/
        ``corrected_event``) so tests can build it without publishing
        to a broker; a :class:`streaming.DeadLetterReplay` satisfies
        it directly. A recording failure raises rather than silently
        losing the record. Replaying the same dead letter twice
        appends two rows — the history is append-only, and the
        consumer's event-id dedupe is what makes the second copy a
        no-op, not any uniqueness rule here.
        """
        cur = self._conn.execute(
            """
            INSERT INTO dead_letter_replays
                (dead_letter_id, group_name, event_id, topic, partition,
                 offset, corrected_json, replayed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(replay.dead_letter_id),
                str(replay.group),
                str(replay.event_id or ""),
                str(replay.topic),
                int(replay.partition),
                int(replay.offset),
                json.dumps(dict(replay.corrected_event), sort_keys=True),
                replay.replayed_at or _now(),
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def count_dead_letter_replays(self, group: str | None = None) -> int:
        """Recorded dead-letter replays, optionally for one group."""
        if group is None:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letter_replays"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letter_replays "
                "WHERE group_name = ?",
                (group,),
            ).fetchone()
        return int(row["n"])

    def latest_dead_letter_replay(
        self, dead_letter_id: int | None = None,
        group: str | None = None,
    ) -> StoredDeadLetterReplay | None:
        """The most recently recorded replay, or ``None``."""
        rows = self.list_dead_letter_replays(
            dead_letter_id=dead_letter_id, group=group, limit=1)
        return rows[0] if rows else None

    def list_dead_letter_replays(
        self, dead_letter_id: int | None = None,
        group: str | None = None, limit: int = 20,
    ) -> list[StoredDeadLetterReplay]:
        """Replays newest-first. ``limit`` is clamped to [1, 100]."""
        limit = max(1, min(int(limit), 100))
        clauses: list[str] = []
        params: list = []
        if dead_letter_id is not None:
            clauses.append("dead_letter_id = ?")
            params.append(int(dead_letter_id))
        if group is not None:
            clauses.append("group_name = ?")
            params.append(group)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT * FROM dead_letter_replays {where} "
            "ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        return [self._stored_dead_letter_replay_from_row(r) for r in rows]

    def dead_letter_replay_totals_by_group(self) -> dict[str, int]:
        """Recorded replays per consumer group, one GROUP BY query.

        Groups that only appear in rows older than any listing limit
        are still included — unlike deriving the group set from a
        newest-first listing. Empty store -> empty dict.
        """
        rows = self._conn.execute(
            "SELECT group_name, COUNT(*) AS n FROM dead_letter_replays "
            "GROUP BY group_name"
        ).fetchall()
        return {r["group_name"]: int(r["n"]) for r in rows}

    @staticmethod
    def _stored_dead_letter_replay_from_row(r) -> StoredDeadLetterReplay:
        return StoredDeadLetterReplay(
            replay_id=int(r["id"]),
            dead_letter_id=int(r["dead_letter_id"]),
            group=r["group_name"],
            event_id=r["event_id"],
            topic=r["topic"],
            partition=int(r["partition"]),
            offset=int(r["offset"]),
            corrected_event=json.loads(r["corrected_json"]),
            replayed_at=r["replayed_at"],
        )

    # --- replay outcomes (M14) ------------------------------------------
    def record_replay_outcome(self, outcome) -> int:
        """Append one replay outcome; return its outcome id.

        ``outcome`` is duck-typed (``replay_id``/``dead_letter_id``/
        ``group``/``event_id``/``outcome``/``new_dead_letter_id``/
        ``consumption_run_id``) so tests can build it without running
        a consumer; a :class:`streaming.ReplayOutcome` satisfies it
        directly. One outcome per (replay, group): a second recording
        for the same pair raises (UNIQUE constraint) rather than
        silently overwriting the first — a replay's outcome is
        decided once, by the first completed consume run that
        processes the re-published record. A recording failure raises
        rather than silently losing the record.
        """
        cur = self._conn.execute(
            """
            INSERT INTO dead_letter_replay_outcomes
                (replay_id, dead_letter_id, group_name, event_id,
                 outcome, new_dead_letter_id, consumption_run_id,
                 recorded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(outcome.replay_id),
                int(outcome.dead_letter_id),
                str(outcome.group),
                str(outcome.event_id or ""),
                str(outcome.outcome),
                (int(outcome.new_dead_letter_id)
                 if outcome.new_dead_letter_id is not None else None),
                (int(outcome.consumption_run_id)
                 if outcome.consumption_run_id is not None else None),
                outcome.recorded_at or _now(),
            ),
        )
        self._conn.commit()
        return int(cur.lastrowid)

    def get_replay_outcome(self, replay_id: int,
                           group: str | None = None) -> StoredReplayOutcome | None:
        """The recorded outcome for one replay (and group), or ``None``.

        ``None`` means the replay has no outcome yet for that group —
        it has not been consumed (pending), not that it failed.
        """
        if group is None:
            row = self._conn.execute(
                "SELECT * FROM dead_letter_replay_outcomes "
                "WHERE replay_id = ? ORDER BY id DESC LIMIT 1",
                (int(replay_id),),
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT * FROM dead_letter_replay_outcomes "
                "WHERE replay_id = ? AND group_name = ?",
                (int(replay_id), group),
            ).fetchone()
        return self._stored_replay_outcome_from_row(row) if row else None

    def find_unresolved_replay_at_coordinates(
        self, topic: str, partition: int, offset: int, group: str,
    ) -> StoredDeadLetterReplay | None:
        """A recorded replay at these broker coordinates, for this
        group, that has no outcome yet — or ``None``.

        Coordinates identify the re-published record even when the
        run could not recover its event id, so this is the link
        ``consume_once`` uses to decide which replay a processed
        record closes the loop on. At most one replay sits at any
        (topic, partition, offset): each replay publishes exactly
        one record.
        """
        row = self._conn.execute(
            """
            SELECT * FROM dead_letter_replays r
            WHERE r.topic = ? AND r.partition = ? AND r.offset = ?
              AND r.group_name = ?
              AND NOT EXISTS (
                  SELECT 1 FROM dead_letter_replay_outcomes o
                  WHERE o.replay_id = r.id AND o.group_name = r.group_name)
            ORDER BY r.id ASC LIMIT 1
            """,
            (str(topic), int(partition), int(offset), group),
        ).fetchone()
        return self._stored_dead_letter_replay_from_row(row) if row else None

    def count_replay_outcomes(self, group: str | None = None) -> int:
        """Recorded replay outcomes, optionally for one group."""
        if group is None:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letter_replay_outcomes"
            ).fetchone()
        else:
            row = self._conn.execute(
                "SELECT COUNT(*) AS n FROM dead_letter_replay_outcomes "
                "WHERE group_name = ?",
                (group,),
            ).fetchone()
        return int(row["n"])

    def latest_replay_outcome(
        self, replay_id: int | None = None,
        group: str | None = None,
    ) -> StoredReplayOutcome | None:
        """The most recently recorded replay outcome, or ``None``."""
        rows = self.list_replay_outcomes(
            replay_id=replay_id, group=group, limit=1)
        return rows[0] if rows else None

    def list_replay_outcomes(
        self, replay_id: int | None = None,
        group: str | None = None, outcome: str | None = None,
        limit: int = 20,
    ) -> list[StoredReplayOutcome]:
        """Replay outcomes newest-first. ``limit`` is clamped to [1, 100]."""
        limit = max(1, min(int(limit), 100))
        clauses: list[str] = []
        params: list = []
        if replay_id is not None:
            clauses.append("replay_id = ?")
            params.append(int(replay_id))
        if group is not None:
            clauses.append("group_name = ?")
            params.append(group)
        if outcome is not None:
            clauses.append("outcome = ?")
            params.append(outcome)
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"SELECT * FROM dead_letter_replay_outcomes {where} "
            "ORDER BY id DESC LIMIT ?",
            params,
        ).fetchall()
        return [self._stored_replay_outcome_from_row(r) for r in rows]

    def replay_outcome_totals_by_group(self) -> dict[str, dict[str, int]]:
        """Recorded replay outcomes per consumer group, per outcome.

        One GROUP BY query, so groups that only appear in rows older
        than any listing limit are still included. Empty store ->
        empty dict. The per-outcome split is what lets ``/metrics``
        show applied vs duplicate vs dead-lettered-again counts
        instead of a single total.
        """
        rows = self._conn.execute(
            "SELECT group_name, outcome, COUNT(*) AS n "
            "FROM dead_letter_replay_outcomes "
            "GROUP BY group_name, outcome"
        ).fetchall()
        totals: dict[str, dict[str, int]] = {}
        for r in rows:
            totals.setdefault(r["group_name"], {})[r["outcome"]] = int(r["n"])
        return totals

    @staticmethod
    def _stored_replay_outcome_from_row(r) -> StoredReplayOutcome:
        return StoredReplayOutcome(
            outcome_id=int(r["id"]),
            replay_id=int(r["replay_id"]),
            dead_letter_id=int(r["dead_letter_id"]),
            group=r["group_name"],
            event_id=r["event_id"],
            outcome=r["outcome"],
            new_dead_letter_id=(int(r["new_dead_letter_id"])
                                if r["new_dead_letter_id"] is not None else None),
            consumption_run_id=(int(r["consumption_run_id"])
                                if r["consumption_run_id"] is not None else None),
            recorded_at=r["recorded_at"],
        )

    # --- read views (fresh snapshots from the DB) -----------------------
    @property
    def inventory(self) -> dict[str, InventoryRecord]:
        rows = self._conn.execute("SELECT * FROM inventory ORDER BY sku").fetchall()
        out: dict[str, InventoryRecord] = {}
        for r in rows:
            rec = InventoryRecord(
                sku=r["sku"], product_name=r["product_name"],
                quantity_on_hand=r["quantity_on_hand"],
                reorder_point=r["reorder_point"], unit_cost=r["unit_cost"],
                expiry_date=date.fromisoformat(r["expiry_date"]) if r["expiry_date"] else None,
                supplier=r["supplier"], batch=r["batch"],
            )
            out[rec.sku] = rec
        return out

    @property
    def sales(self) -> list[SalesRecord]:
        rows = self._conn.execute("SELECT * FROM sales ORDER BY id").fetchall()
        return [
            SalesRecord(
                sale_id=r["sale_id"], sku=r["sku"],
                quantity_sold=r["quantity_sold"],
                sale_date=date.fromisoformat(r["sale_date"]) if r["sale_date"] else None,
                unit_price=r["unit_price"],
            )
            for r in rows
        ]

    @property
    def file_hashes(self) -> set[str]:
        return {r["file_hash"] for r in self._conn.execute("SELECT file_hash FROM file_hashes")}

    @property
    def row_hashes(self) -> set[str]:
        return {r["row_hash"] for r in self._conn.execute("SELECT row_hash FROM row_hashes")}

    @property
    def quarantine(self) -> QuarantineStore:
        rows = self._conn.execute("SELECT * FROM quarantine ORDER BY id").fetchall()
        store = QuarantineStore()
        for r in rows:
            errors = [
                ValidationError(
                    row_number=e["row_number"], field=e["field"], code=e["code"],
                    message=e["message"], raw_value=e.get("raw_value", ""),
                )
                for e in json.loads(r["errors_json"])
            ]
            store.add(QuarantineEntry(
                row_number=r["row_number"], source_file=r["source_file"],
                file_hash=r["file_hash"], raw=json.loads(r["raw_json"]), errors=errors,
            ))
        return store

    @property
    def audit(self) -> AuditLog:
        rows = self._conn.execute("SELECT * FROM audit ORDER BY id").fetchall()
        log = AuditLog()
        for r in rows:
            log.entries.append(AuditEntry(
                action=r["action"], source_file=r["source_file"],
                file_hash=r["file_hash"], details=r["details"],
                timestamp=r["timestamp"],
            ))
        return log
