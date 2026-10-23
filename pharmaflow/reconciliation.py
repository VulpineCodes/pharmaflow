from __future__ import annotations
from dataclasses import dataclass
from .models import InventoryRecord, SalesRecord
@dataclass
class Discrepancy:
    sku: str; expected: int | None; actual: int | None; difference: int | None; kind: str; severity: str; message: str

def reconcile(expected: dict[str,int], actual: dict[str,int]) -> list[Discrepancy]:
    out=[]
    for sku in sorted(set(expected)|set(actual)):
        if sku not in actual:
            out.append(Discrepancy(sku, expected[sku], None, -expected[sku], "missing_sku", "high", f"{sku}: expected {expected[sku]}, not found in actual count"))
        elif sku not in expected:
            out.append(Discrepancy(sku, None, actual[sku], actual[sku], "unexpected_sku", "medium", f"{sku}: present ({actual[sku]}) but not expected"))
        elif actual[sku] != expected[sku]:
            diff=actual[sku]-expected[sku]
            kind="shrinkage" if diff<0 else "count_drift"
            sev="high" if abs(diff)>=10 else "medium"
            out.append(Discrepancy(sku, expected[sku], actual[sku], diff, kind, sev, f"{sku}: expected {expected[sku]}, actual {actual[sku]}, diff {diff} ({kind})"))
    return out


def expected_after_sales(starting: dict[str,int], sales: list[SalesRecord]) -> dict[str,int]:
    exp=dict(starting)
    for s in sales:
        exp[s.sku]=exp.get(s.sku,0)-s.quantity_sold
    return exp

def detect_overselling(starting: dict[str,int], sales: list[SalesRecord]) -> list[Discrepancy]:
    exp=expected_after_sales(starting, sales)
    return [Discrepancy(sku, starting.get(sku,0), qty, qty, "overselling", "high",
            f"{sku}: sales exceed available stock (expected remaining {qty})")
            for sku, qty in sorted(exp.items()) if qty < 0]

def reconcile_store(starting: dict[str,int], sales: list[SalesRecord], inventory: dict[str, InventoryRecord]) -> list[Discrepancy]:
    expected=expected_after_sales(starting, sales)
    actual={k: v.quantity_on_hand for k,v in inventory.items()}
    return reconcile(expected, actual) + detect_overselling(starting, sales)
