"""Postgres store — CODE ONLY, NOT RUN LOCALLY.

Honesty note: there is no Postgres server (and no Docker) in the current
build environment, so this module has never been executed against a real
database. It is a skeleton implementing the same StoreProtocol as
``SqliteStore`` (the verified M2 path), written so the migration in
``db/migrations/001_initial_schema.sql`` and this code can be brought up
together when a server is available. See ``docs/POSTGRES.md`` for the
bring-up and verification checklist. Nothing in README/STATUS claims
Postgres was executed.

Differences from the SQLite implementation are only dialect:
``%s`` placeholders instead of ``?``, ``BIGSERIAL`` instead of
``AUTOINCREMENT``, and ``JSONB`` for quarantine payloads.
"""
from __future__ import annotations

import json
from datetime import date, datetime, timezone

try:  # guarded import — psycopg is optional until a server is in use
    import psycopg
except ImportError:  # pragma: no cover - depends on deployment env
    psycopg = None  # type: ignore[assignment]

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

SCHEMA_VERSION = 1

# Mirrors db/migrations/001_initial_schema.sql — keep the two in sync.
SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS file_hashes (
    file_hash TEXT PRIMARY KEY,
    imported_at TIMESTAMPTZ NOT NULL
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
    imported_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    expiry_date DATE,
    supplier TEXT NOT NULL DEFAULT '',
    batch TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS sales (
    id BIGSERIAL PRIMARY KEY,
    sale_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_date DATE,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS quarantine (
    id BIGSERIAL PRIMARY KEY,
    row_number INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    errors_json JSONB NOT NULL
);
CREATE TABLE IF NOT EXISTS audit (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    details TEXT NOT NULL
);
"""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class PostgresStore:
    """StoreProtocol implementation backed by Postgres (UNVERIFIED).

    Construct with a psycopg DSN / connection string, e.g.
    ``postgresql://pharmaflow:...@localhost:5432/pharmaflow``.
    """

    def __init__(self, dsn: str):
        if psycopg is None:
            raise ImportError(
                "psycopg is not installed — install requirements.txt and "
                "provide a reachable Postgres server to use PostgresStore. "
                "Use SqliteStore for the verified local persistence path."
            )
        self.dsn = dsn
        self._conn = psycopg.connect(dsn)
        self.metrics = Metrics()
        self._init_schema()

    # --- lifecycle ------------------------------------------------------
    def _init_schema(self) -> None:
        with self._conn.cursor() as cur:
            cur.execute(SCHEMA_DDL)
            cur.execute("SELECT version FROM schema_version LIMIT 1")
            if cur.fetchone() is None:
                cur.execute(
                    "INSERT INTO schema_version (version, applied_at) VALUES (%s, %s)",
                    (SCHEMA_VERSION, _now()),
                )
        self._conn.commit()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "PostgresStore":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    # --- file / row idempotency ----------------------------------------
    def has_file_hash(self, file_hash: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM file_hashes WHERE file_hash = %s", (file_hash,))
            return cur.fetchone() is not None

    def add_file_hash(self, file_hash: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO file_hashes (file_hash, imported_at) VALUES (%s, %s) "
                "ON CONFLICT (file_hash) DO NOTHING",
                (file_hash, _now()),
            )
        self._conn.commit()

    def has_row_hash(self, row_hash: str) -> bool:
        with self._conn.cursor() as cur:
            cur.execute("SELECT 1 FROM row_hashes WHERE row_hash = %s", (row_hash,))
            return cur.fetchone() is not None

    def add_row_hash(self, row_hash: str, file_hash: str = "") -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO row_hashes (row_hash, file_hash) VALUES (%s, %s) "
                "ON CONFLICT (row_hash) DO NOTHING",
                (row_hash, file_hash),
            )
        self._conn.commit()

    # --- records ----------------------------------------------------------
    def upsert_inventory(self, record: InventoryRecord) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO inventory (sku, product_name, quantity_on_hand,
                                       reorder_point, unit_cost, expiry_date,
                                       supplier, batch)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sku) DO UPDATE SET
                    product_name=EXCLUDED.product_name,
                    quantity_on_hand=EXCLUDED.quantity_on_hand,
                    reorder_point=EXCLUDED.reorder_point,
                    unit_cost=EXCLUDED.unit_cost,
                    expiry_date=EXCLUDED.expiry_date,
                    supplier=EXCLUDED.supplier,
                    batch=EXCLUDED.batch
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
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO sales (sale_id, sku, quantity_sold, sale_date, unit_price) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    record.sale_id, record.sku, record.quantity_sold,
                    record.sale_date.isoformat() if record.sale_date else None,
                    record.unit_price,
                ),
            )
        self._conn.commit()

    def add_quarantine(self, entry: QuarantineEntry) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO quarantine (row_number, source_file, file_hash, raw_json, errors_json) "
                "VALUES (%s, %s, %s, %s, %s)",
                (
                    entry.row_number, entry.source_file, entry.file_hash,
                    json.dumps(entry.raw, sort_keys=True),
                    json.dumps([e.to_dict() for e in entry.errors]),
                ),
            )
        self._conn.commit()

    def append_audit(self, action, source_file, file_hash, details):
        entry = AuditEntry(action, source_file, file_hash, details)
        with self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO audit (timestamp, action, source_file, file_hash, details) "
                "VALUES (%s, %s, %s, %s, %s)",
                (entry.timestamp, entry.action, entry.source_file, entry.file_hash, entry.details),
            )
        self._conn.commit()
        return entry

    # --- reporting --------------------------------------------------------
    def record_import(self, result: ImportResult) -> None:
        with self._conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO imports (file_hash, source_file, kind, rows_in,
                                     rows_valid, rows_quarantined,
                                     duplicates_skipped, imported_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (file_hash) DO NOTHING
                """,
                (
                    result.file_hash, result.source_file, result.kind,
                    result.rows_in, result.rows_valid, result.rows_quarantined,
                    result.duplicates_skipped, _now(),
                ),
            )
        self._conn.commit()

    def list_imports(self) -> list[ImportResult]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT source_file, file_hash, kind, rows_in, rows_valid, "
                "rows_quarantined, duplicates_skipped FROM imports "
                "ORDER BY imported_at, file_hash"
            )
            rows = cur.fetchall()
        return [
            ImportResult(
                source_file=r[0], file_hash=r[1], kind=r[2], rows_in=r[3],
                rows_valid=r[4], rows_quarantined=r[5], duplicates_skipped=r[6],
            )
            for r in rows
        ]

    # --- read views -------------------------------------------------------
    @property
    def inventory(self) -> dict[str, InventoryRecord]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT sku, product_name, quantity_on_hand, reorder_point, "
                "unit_cost, expiry_date, supplier, batch FROM inventory ORDER BY sku"
            )
            rows = cur.fetchall()
        out: dict[str, InventoryRecord] = {}
        for sku, name, qty, reorder, cost, expiry, supplier, batch in rows:
            expiry_date = expiry if isinstance(expiry, date) else (
                date.fromisoformat(expiry) if expiry else None
            )
            rec = InventoryRecord(sku, name, qty, reorder, cost or 0.0, expiry_date, supplier or "", batch or "")
            out[rec.sku] = rec
        return out

    @property
    def sales(self) -> list[SalesRecord]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT sale_id, sku, quantity_sold, sale_date, unit_price "
                "FROM sales ORDER BY id"
            )
            rows = cur.fetchall()
        out = []
        for sale_id, sku, qty, sdate, price in rows:
            sale_date = sdate if isinstance(sdate, date) else (
                date.fromisoformat(sdate) if sdate else None
            )
            out.append(SalesRecord(sale_id, sku, qty, sale_date, price or 0.0))
        return out

    @property
    def file_hashes(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT file_hash FROM file_hashes")
            return {r[0] for r in cur.fetchall()}

    @property
    def row_hashes(self) -> set[str]:
        with self._conn.cursor() as cur:
            cur.execute("SELECT row_hash FROM row_hashes")
            return {r[0] for r in cur.fetchall()}

    @property
    def quarantine(self) -> QuarantineStore:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT row_number, source_file, file_hash, raw_json, errors_json "
                "FROM quarantine ORDER BY id"
            )
            rows = cur.fetchall()
        store = QuarantineStore()
        for row_number, source_file, file_hash, raw_json, errors_json in rows:
            raw = raw_json if isinstance(raw_json, dict) else json.loads(raw_json)
            errs = errors_json if isinstance(errors_json, list) else json.loads(errors_json)
            errors = [
                ValidationError(e["row_number"], e["field"], e["code"], e["message"], e.get("raw_value", ""))
                for e in errs
            ]
            store.add(QuarantineEntry(row_number, source_file, file_hash, raw, errors))
        return store

    @property
    def audit(self) -> AuditLog:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT timestamp, action, source_file, file_hash, details "
                "FROM audit ORDER BY id"
            )
            rows = cur.fetchall()
        log = AuditLog()
        for ts, action, source_file, file_hash, details in rows:
            ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
            log.entries.append(AuditEntry(action, source_file, file_hash, details, timestamp=ts_str))
        return log
