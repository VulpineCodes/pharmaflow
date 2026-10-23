"""Store protocol — the contract ingestion runs against.

Two implementations exist:

* ``PharmaStore`` (in ``ingestion.py``) — in-memory, used by the original
  foundation tests and for fast unit tests. Data does not survive a process.
* ``SqliteStore`` (in ``sqlite_store.py``) — durable, stdlib ``sqlite3``.
  This is the M2 verified persistence path.

``PostgresStore`` (in ``postgres_store.py``) implements the same contract for
a future server deployment; it has NOT been run locally (no server available).

Ingestion must only use the methods declared here, never concrete storage
details (dicts / SQL tables), so swapping stores does not change the pipeline.
"""
from __future__ import annotations

from typing import Protocol, runtime_checkable

from .models import ImportResult, InventoryRecord, QuarantineEntry, SalesRecord
from .metrics import Metrics


@runtime_checkable
class StoreProtocol(Protocol):
    """Minimal interface ``ingest_bytes`` / ``ingest_file`` depend on."""

    metrics: Metrics

    # --- file / row idempotency -------------------------------------
    def has_file_hash(self, file_hash: str) -> bool: ...
    def add_file_hash(self, file_hash: str) -> None: ...
    def has_row_hash(self, row_hash: str) -> bool: ...
    def add_row_hash(self, row_hash: str, file_hash: str = "") -> None: ...

    # --- records ------------------------------------------------------
    def upsert_inventory(self, record: InventoryRecord) -> None: ...
    def append_sales(self, record: SalesRecord) -> None: ...
    def add_quarantine(self, entry: QuarantineEntry) -> None: ...
    def append_audit(self, action: str, source_file: str, file_hash: str, details: str): ...

    # --- reporting ----------------------------------------------------
    def record_import(self, result: ImportResult) -> None:
        """Persist a completed (non-duplicate) import summary for DQ reports."""
        ...
    def list_imports(self) -> list[ImportResult]: ...

    # --- read views used by CLI / API / tests -------------------------
    # ``inventory`` -> dict[str, InventoryRecord]
    # ``sales``     -> list[SalesRecord]
    # ``quarantine``-> object with ``.entries`` and ``__len__``
    # ``audit``     -> object with ``.entries``
    # ``file_hashes`` / ``row_hashes`` -> set[str]
