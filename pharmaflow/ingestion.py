from __future__ import annotations
import csv, hashlib, io
from pathlib import Path
from dataclasses import dataclass, field
from .models import InventoryRecord, SalesRecord, QuarantineEntry, ImportResult
from .validation import validate_inventory_row, validate_sales_row
from .quarantine import QuarantineStore
from .audit import AuditLog
from .metrics import Metrics

def file_hash_bytes(data: bytes) -> str: return hashlib.sha256(data).hexdigest()
def row_hash(raw: dict) -> str:
    norm = "|".join(f"{k}={(raw.get(k) or '').strip()}" for k in sorted(raw))
    return hashlib.sha256(norm.encode()).hexdigest()

def detect_kind(fieldnames) -> str:
    names=set(fieldnames or [])
    if "quantity_on_hand" in names: return "inventory"
    if "quantity_sold" in names: return "sales"
    raise ValueError(f"Cannot detect CSV kind from headers: {sorted(names)}")

@dataclass
class PharmaStore:
    """In-memory store — the original foundation implementation.

    Implements StoreProtocol (see store.py). Volatile by design: used for
    fast unit tests; use SqliteStore for durable persistence.
    """
    inventory: dict[str, InventoryRecord] = field(default_factory=dict)
    sales: list[SalesRecord] = field(default_factory=list)
    file_hashes: set[str] = field(default_factory=set)
    row_hashes: set[str] = field(default_factory=set)
    quarantine: QuarantineStore = field(default_factory=QuarantineStore)
    audit: AuditLog = field(default_factory=AuditLog)
    metrics: Metrics = field(default_factory=Metrics)
    imports: list[ImportResult] = field(default_factory=list)

    # --- StoreProtocol ------------------------------------------------
    def has_file_hash(self, file_hash: str) -> bool: return file_hash in self.file_hashes
    def add_file_hash(self, file_hash: str) -> None: self.file_hashes.add(file_hash)
    def has_row_hash(self, row_hash: str) -> bool: return row_hash in self.row_hashes
    def add_row_hash(self, row_hash: str, file_hash: str = "") -> None: self.row_hashes.add(row_hash)
    def upsert_inventory(self, record: InventoryRecord) -> None: self.inventory[record.sku] = record
    def append_sales(self, record: SalesRecord) -> None: self.sales.append(record)
    def add_quarantine(self, entry: QuarantineEntry) -> None: self.quarantine.add(entry)
    def append_audit(self, action, source_file, file_hash, details): return self.audit.append(action, source_file, file_hash, details)
    def record_import(self, result: ImportResult) -> None:
        if all(existing.file_hash != result.file_hash for existing in self.imports):
            self.imports.append(result)
    def list_imports(self) -> list[ImportResult]: return list(self.imports)

def ingest_bytes(data: bytes, source_name: str, store, kind: str | None = None) -> ImportResult:
    """Ingest CSV bytes into any StoreProtocol implementation."""
    fh = file_hash_bytes(data)
    text = data.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(text))
    if reader.fieldnames is None:
        raise ValueError("CSV has no header row")
    kind = kind or detect_kind(reader.fieldnames)
    rows = list(reader)
    result = ImportResult(source_file=source_name, file_hash=fh, kind=kind, rows_in=len(rows))
    store.metrics.inc("rows_in", len(rows))
    if store.has_file_hash(fh):
        result.already_imported = True
        result.duplicates_skipped = len(rows)
        store.metrics.inc("duplicates_skipped", len(rows)); store.metrics.inc("files_already_imported")
        store.append_audit("import_skipped_duplicate_file", source_name, fh, f"rows_in={len(rows)} no-op")
        return result
    # Process row-by-row: a bad row is quarantined, good rows before/after it are kept.
    # This is the partial-failure recovery contract.
    for idx, raw in enumerate(rows, start=2):  # row 1 is header
        # Normalise None keys/values from csv
        clean = {str(k): (v if v is not None else "") for k, v in raw.items() if k is not None}
        rh = row_hash(clean)
        if store.has_row_hash(rh):
            result.duplicates_skipped += 1; store.metrics.inc("duplicates_skipped"); continue
        if kind == "inventory":
            rec, errors = validate_inventory_row(clean, idx)
        else:
            rec, errors = validate_sales_row(clean, idx)
        if errors:
            result.rows_quarantined += 1; store.metrics.inc("rows_quarantined")
            store.add_quarantine(QuarantineEntry(row_number=idx, source_file=source_name, file_hash=fh, raw=clean, errors=errors))
            continue
        store.add_row_hash(rh, fh)
        if kind == "inventory": store.upsert_inventory(rec)  # type: ignore[arg-type]
        else: store.append_sales(rec)  # type: ignore[arg-type]
        result.rows_valid += 1; store.metrics.inc("rows_valid")
    store.add_file_hash(fh)
    store.metrics.inc("imports_total")
    store.append_audit("import_completed", source_name, fh, result.summary())
    store.record_import(result)
    return result

def ingest_file(path: str | Path, store, kind: str | None = None) -> ImportResult:
    p = Path(path); data = p.read_bytes()
    return ingest_bytes(data, str(p), store, kind=kind)
