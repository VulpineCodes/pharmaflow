from __future__ import annotations
from dataclasses import dataclass
from datetime import date
from .models import InventoryRecord
@dataclass
class Alert:
    sku: str; product_name: str; kind: str; message: str
    quantity_on_hand: int = 0; reorder_point: int = 0; expiry_date: date | None = None; days_until_expiry: int | None = None

def generate_alerts(inventory: dict[str, InventoryRecord], today: date | None = None, expiry_window_days: int = 30) -> list[Alert]:
    today = today or date.today()
    alerts=[]
    for rec in sorted(inventory.values(), key=lambda r: r.sku):
        if rec.quantity_on_hand <= rec.reorder_point:
            alerts.append(Alert(rec.sku, rec.product_name, "low_stock",
                f"{rec.sku} low stock: {rec.quantity_on_hand} on hand <= reorder point {rec.reorder_point}",
                rec.quantity_on_hand, rec.reorder_point, rec.expiry_date))
        if rec.expiry_date is not None:
            days=(rec.expiry_date-today).days
            if days < 0:
                alerts.append(Alert(rec.sku, rec.product_name, "expired", f"{rec.sku} expired {abs(days)}d ago ({rec.expiry_date})", rec.quantity_on_hand, rec.reorder_point, rec.expiry_date, days))
            elif days <= expiry_window_days:
                alerts.append(Alert(rec.sku, rec.product_name, "expiring_soon", f"{rec.sku} expires in {days}d ({rec.expiry_date})", rec.quantity_on_hand, rec.reorder_point, rec.expiry_date, days))
    return alerts
