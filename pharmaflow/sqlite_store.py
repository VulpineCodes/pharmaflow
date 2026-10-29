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
  adds the ``pipeline_runs`` table; opening a version-1 database
  migrates it additively (new table only, existing rows untouched)
  and bumps the recorded version to 2.
* pipeline runs (M9): ``record_pipeline_run`` / ``latest_pipeline_run``
  / ``list_pipeline_runs`` persist ``PipelineResult`` dicts (see
  ``orchestration.PipelineResult.to_dict``) so the last run survives a
  process restart and can be served by the API.

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

SCHEMA_VERSION = 2

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
