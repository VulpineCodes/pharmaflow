from __future__ import annotations
from dataclasses import dataclass, field
from datetime import date
from typing import Any

@dataclass
class InventoryRecord:
    sku: str
    product_name: str
    quantity_on_hand: int
    reorder_point: int
    unit_cost: float = 0.0
    expiry_date: date | None = None
    supplier: str = ""
    batch: str = ""

@dataclass
class SalesRecord:
    sale_id: str
    sku: str
    quantity_sold: int
    sale_date: date | None = None
    unit_price: float = 0.0

@dataclass
class ValidationError:
    row_number: int
    field: str
    code: str  # required|type|business_rule|duplicate_schema
    message: str
    raw_value: str = ""
    def to_dict(self): return {"row_number": self.row_number, "field": self.field, "code": self.code, "message": self.message, "raw_value": self.raw_value}

@dataclass
class QuarantineEntry:
    row_number: int
    source_file: str
    file_hash: str
    raw: dict[str, str]
    errors: list[ValidationError]
    @property
    def reasons(self): return [e.message for e in self.errors]

@dataclass
class ImportResult:
    source_file: str
    file_hash: str
    kind: str
    rows_in: int = 0
    rows_valid: int = 0
    rows_quarantined: int = 0
    duplicates_skipped: int = 0
    already_imported: bool = False
    def summary(self):
        return (f"{self.kind} import from {self.source_file}: rows_in={self.rows_in} "
                f"valid={self.rows_valid} quarantined={self.rows_quarantined} "
                f"duplicates_skipped={self.duplicates_skipped} already_imported={self.already_imported} "
                f"file_hash={self.file_hash[:12]}...")
