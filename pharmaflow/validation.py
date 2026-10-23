from __future__ import annotations
import re
from datetime import date
from .models import InventoryRecord, SalesRecord, ValidationError

SKU_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_]*$")
INVENTORY_REQUIRED = ["sku", "product_name", "quantity_on_hand", "reorder_point"]
SALES_REQUIRED = ["sale_id", "sku", "quantity_sold"]

def _req(raw, field_name, row, errors):
    v = (raw.get(field_name) or "").strip()
    if not v:
        errors.append(ValidationError(row, field_name, "required", f"{field_name} is required", v))
    return v

def _int(raw, field_name, row, errors, min_v=None):
    v = (raw.get(field_name) or "").strip()
    if not v: return None
    try: iv = int(v)
    except ValueError:
        errors.append(ValidationError(row, field_name, "type", f"{field_name} must be an integer, got {v!r}", v)); return None
    if min_v is not None and iv < min_v:
        errors.append(ValidationError(row, field_name, "business_rule", f"{field_name} must be >= {min_v}, got {iv}", v))
    return iv

def _float(raw, field_name, row, errors, min_v=None):
    v = (raw.get(field_name) or "").strip()
    if not v: return 0.0
    try: fv = float(v)
    except ValueError:
        errors.append(ValidationError(row, field_name, "type", f"{field_name} must be a number, got {v!r}", v)); return None
    if min_v is not None and fv < min_v:
        errors.append(ValidationError(row, field_name, "business_rule", f"{field_name} must be >= {min_v}, got {fv}", v))
    return fv

def _date(raw, field_name, row, errors):
    v = (raw.get(field_name) or "").strip()
    if not v: return None
    try: return date.fromisoformat(v)
    except ValueError:
        errors.append(ValidationError(row, field_name, "type", f"{field_name} must be ISO date YYYY-MM-DD, got {v!r}", v)); return None

def validate_inventory_row(raw: dict, row_number: int) -> tuple[InventoryRecord | None, list[ValidationError]]:
    errors: list[ValidationError] = []
    for f in INVENTORY_REQUIRED: _req(raw, f, row_number, errors)
    sku = (raw.get("sku") or "").strip()
    if sku and not SKU_RE.match(sku):
        errors.append(ValidationError(row_number, "sku", "business_rule", f"sku has invalid characters: {sku!r}", sku))
    qty = _int(raw, "quantity_on_hand", row_number, errors, min_v=0)
    reorder = _int(raw, "reorder_point", row_number, errors, min_v=0)
    cost = _float(raw, "unit_cost", row_number, errors, min_v=0)
    expiry = _date(raw, "expiry_date", row_number, errors)
    if qty is not None and qty > 100000:
        errors.append(ValidationError(row_number, "quantity_on_hand", "business_rule", "quantity_on_hand implausibly large (>100000)", str(qty)))
    if errors: return None, errors
    return InventoryRecord(sku=sku, product_name=(raw.get("product_name") or "").strip(),
        quantity_on_hand=qty, reorder_point=reorder, unit_cost=cost or 0.0, expiry_date=expiry,
        supplier=(raw.get("supplier") or "").strip(), batch=(raw.get("batch") or "").strip()), []

def validate_sales_row(raw: dict, row_number: int) -> tuple[SalesRecord | None, list[ValidationError]]:
    errors: list[ValidationError] = []
    for f in SALES_REQUIRED: _req(raw, f, row_number, errors)
    qty = _int(raw, "quantity_sold", row_number, errors, min_v=1)
    price = _float(raw, "unit_price", row_number, errors, min_v=0)
    sdate = _date(raw, "sale_date", row_number, errors)
    if errors: return None, errors
    return SalesRecord(sale_id=(raw.get("sale_id") or "").strip(), sku=(raw.get("sku") or "").strip(),
        quantity_sold=qty, sale_date=sdate, unit_price=price or 0.0), []
