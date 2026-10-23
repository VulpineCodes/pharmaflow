"""DuckDB analytics — M3 executed analytical layer.

Loads inventory + sales from the durable SQLite store into an in-memory
DuckDB database and runs the aggregations *in DuckDB SQL* (not in Python
loops). Python only shapes the typed results and renders the report.

Honesty / definitions (these are the definitions the tests assert):

* **Category** — the source CSVs / SQLite schema have no category column,
  so category is *derived* deterministically from the product name by the
  CASE expression in ``_CATEGORY_SQL`` (keyword rules, ``Other`` fallback).
  It is a labelling convenience, not source data.
* **Inventory valuation** — ``quantity_on_hand * unit_cost`` per SKU,
  summed by derived category.
* **Sales velocity** — total units sold divided by the *observation
  window*: ``max(sale_date) - min(sale_date) + 1`` days across the whole
  sales table (so a single day of sales is a 1-day window). SKUs with no
  sales have velocity 0 and ``days_of_stock_remaining = None`` (unknown,
  not infinite stock). Otherwise ``quantity_on_hand / velocity_per_day``.
* **Expiry risk** — buckets by days from the reference date (``today``,
  injectable for reproducible tests) to ``expiry_date``:
  ``expired`` (<0), ``0-30``, ``31-90``, ``91-180``, ``>180``,
  ``no_expiry_date``. Value at risk is ``quantity_on_hand * unit_cost``
  summed per bucket.

DuckDB is a hard dependency of *this module only*; the ingestion core
remains stdlib-only. Import fails with a clear message if duckdb is absent.

M7 — bulk load: the M6 benchmark showed ``run_analytics`` at 50k rows
taking ~62 s, almost all of it the *load*: one ``con.execute(INSERT …)``
per row in Python (~59 s to load 50k inventory rows), while materialising
the rows from SQLite took ~0.3 s and the SQL aggregations are sub-second.
``_load_into_duckdb`` therefore bulk-loads with DuckDB's appender API
(``Appender.append_rows``, ~0.1 s for the same 50k rows in profiling):
for a ``SqliteStore`` it streams raw rows straight from the SQLite
connection in ``fetchmany`` chunks (no intermediate dataclass objects),
otherwise it builds tuples from the store's read views in chunks.
DuckDB's sqlite-scanner extension would be the other bulk path, but it
is *not* available offline (it tries to download the extension), and
``executemany`` measured just as slow as per-row ``execute`` (~51 s), so
neither is used. The original row-by-row loader is preserved verbatim as
``_load_into_duckdb_row_by_row`` for the M7 correctness gate, which
asserts both loaders produce identical analytics. The analytics SQL
itself is unchanged.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

try:
    import duckdb
except ImportError as exc:  # pragma: no cover - environment guard
    raise ImportError(
        "pharmaflow.analytics requires duckdb (pip install duckdb); "
        "the ingestion core does not."
    ) from exc

from .sqlite_store import SqliteStore

# Derived category: keyword rules over the lower-cased product name.
_CATEGORY_SQL = """
case
  when lower(product_name) like '%paracetamol%' or lower(product_name) like '%ibuprofen%' then 'Pain Relief'
  when lower(product_name) like '%vitamin%' or lower(product_name) like '%ors%' then 'Vitamins & Rehydration'
  when lower(product_name) like '%antiseptic%' or lower(product_name) like '%bandage%' then 'First Aid & Antiseptic'
  when lower(product_name) like '%cough%' then 'Respiratory'
  else 'Other'
end
"""

BUCKETS = ["expired", "0-30", "31-90", "91-180", ">180", "no_expiry_date"]


@dataclass
class CategoryValuation:
    category: str
    sku_count: int
    total_quantity: int
    total_value: float


@dataclass
class SkuVelocity:
    sku: str
    product_name: str
    quantity_on_hand: int
    total_sold: int
    velocity_per_day: float
    days_of_stock_remaining: float | None


@dataclass
class ExpiryRiskBucket:
    bucket: str
    sku_count: int
    total_quantity: int
    value_at_risk: float


@dataclass
class AnalyticsReport:
    reference_date: date
    sales_window_days: int | None
    total_inventory_value: float
    total_skus: int
    valuation_by_category: list[CategoryValuation] = field(default_factory=list)
    velocity: list[SkuVelocity] = field(default_factory=list)
    expiry_risk: list[ExpiryRiskBucket] = field(default_factory=list)

    @property
    def value_at_risk_30d(self) -> float:
        """Expired + expiring within 30 days — the actionable risk figure."""
        return round(sum(
            b.value_at_risk for b in self.expiry_risk if b.bucket in ("expired", "0-30")
        ), 2)

    def as_dict(self) -> dict:
        return {
            "reference_date": self.reference_date.isoformat(),
            "sales_window_days": self.sales_window_days,
            "total_inventory_value": self.total_inventory_value,
            "total_skus": self.total_skus,
            "value_at_risk_30d": self.value_at_risk_30d,
            "valuation_by_category": [vars(v) for v in self.valuation_by_category],
            "velocity": [vars(v) for v in self.velocity],
            "expiry_risk": [vars(b) for b in self.expiry_risk],
        }


_INVENTORY_DDL = (
    "CREATE TABLE inventory (sku VARCHAR, product_name VARCHAR, "
    "quantity_on_hand INTEGER, unit_cost DOUBLE, expiry_date DATE, "
    "reorder_point INTEGER, supplier VARCHAR, batch VARCHAR)"
)
_SALES_DDL = (
    "CREATE TABLE sales (sale_id VARCHAR, sku VARCHAR, quantity_sold INTEGER, "
    "sale_date DATE, unit_price DOUBLE)"
)

_CHUNK = 10_000


def _create_duckdb_tables(con: "duckdb.DuckDBPyConnection") -> None:
    con.execute(_INVENTORY_DDL)
    con.execute(_SALES_DDL)


def _append_chunked(con, table: str, chunks) -> None:
    """Bulk-append an iterable of row-tuple chunks via DuckDB's appender.

    The appender is DuckDB's bulk-ingest path: unlike ``execute`` /
    ``executemany`` per row, the rows cross into DuckDB in batches.
    ``flush()`` finalises the append; the appender object has no
    ``close()`` in duckdb 2.0, so it is released by reference drop.
    """
    appender = con.appender(table)
    try:
        for chunk in chunks:
            if chunk:
                appender.append_rows(chunk)
        appender.flush()
    finally:
        del appender


def _load_into_duckdb_row_by_row(store) -> "duckdb.DuckDBPyConnection":
    """The original (pre-M7) loader: one INSERT per row — SLOW at scale.

    Preserved verbatim as the reference implementation for the M7
    correctness gate (``tests/test_m7.py`` asserts the bulk loader
    produces identical analytics). Do not use in production paths:
    it measured ~59 s to load 50k inventory rows (see module docstring).
    """
    con = duckdb.connect(":memory:")
    con.execute(_INVENTORY_DDL)
    for rec in sorted(store.inventory.values(), key=lambda r: r.sku):
        con.execute(
            "INSERT INTO inventory VALUES (?,?,?,?,?,?,?,?)",
            [rec.sku, rec.product_name, rec.quantity_on_hand, rec.unit_cost,
             rec.expiry_date, rec.reorder_point, rec.supplier, rec.batch],
        )
    con.execute(_SALES_DDL)
    for s in store.sales:
        con.execute(
            "INSERT INTO sales VALUES (?,?,?,?,?)",
            [s.sale_id, s.sku, s.quantity_sold, s.sale_date, s.unit_price],
        )
    return con


def _load_into_duckdb(store) -> "duckdb.DuckDBPyConnection":
    """Bulk-load the store into in-memory DuckDB (M7).

    Two sources, one result — the same tables, column order and row
    order the row-by-row loader produced (inventory by SKU, sales in
    store order), so the (unchanged) analytics SQL sees identical data:

    * ``SqliteStore``: stream raw rows from its SQLite connection with
      ``fetchmany`` (no ``InventoryRecord``/``SalesRecord`` objects are
      materialised, and no per-row SQL is issued on either side).
      TEXT dates are converted to ``date`` here — that conversion is
      the only per-row Python work and is ~0.02 s at 50k rows.
    * Any other store (in-memory ``PharmaStore``, …): chunk the read
      views' records into tuple batches.

    Both feed DuckDB's appender (``append_rows``). If a future DuckDB
    drops the appender API this raises ``AttributeError`` loudly rather
    than silently falling back to the slow path.
    """
    con = duckdb.connect(":memory:")
    _create_duckdb_tables(con)
    raw_conn = getattr(store, "_conn", None)
    if isinstance(store, SqliteStore) and raw_conn is not None:
        inv_cursor = raw_conn.execute(
            "SELECT sku, product_name, quantity_on_hand, unit_cost, "
            "expiry_date, reorder_point, supplier, batch "
            "FROM inventory ORDER BY sku"
        )

        def inv_chunks():
            while True:
                chunk = inv_cursor.fetchmany(_CHUNK)
                if not chunk:
                    return
                yield [
                    (r[0], r[1], r[2], r[3],
                     date.fromisoformat(r[4]) if r[4] else None,
                     r[5], r[6], r[7])
                    for r in chunk
                ]

        _append_chunked(con, "inventory", inv_chunks())

        sales_cursor = raw_conn.execute(
            "SELECT sale_id, sku, quantity_sold, sale_date, unit_price "
            "FROM sales ORDER BY id"
        )

        def sales_chunks():
            while True:
                chunk = sales_cursor.fetchmany(_CHUNK)
                if not chunk:
                    return
                yield [
                    (r[0], r[1], r[2],
                     date.fromisoformat(r[3]) if r[3] else None, r[4])
                    for r in chunk
                ]

        _append_chunked(con, "sales", sales_chunks())
        return con

    inv_records = sorted(store.inventory.values(), key=lambda r: r.sku)
    _append_chunked(con, "inventory", (
        [(rec.sku, rec.product_name, rec.quantity_on_hand, rec.unit_cost,
          rec.expiry_date, rec.reorder_point, rec.supplier, rec.batch)
         for rec in inv_records[i:i + _CHUNK]]
        for i in range(0, len(inv_records), _CHUNK)
    ))
    sales_records = list(store.sales)
    _append_chunked(con, "sales", (
        [(s.sale_id, s.sku, s.quantity_sold, s.sale_date, s.unit_price)
         for s in sales_records[i:i + _CHUNK]]
        for i in range(0, len(sales_records), _CHUNK)
    ))
    return con


def run_analytics(store, today: date | None = None) -> AnalyticsReport:
    """Run all analytics in DuckDB over a store (SQLite or in-memory)."""
    today = today or date.today()
    con = _load_into_duckdb(store)
    try:
        val_rows = con.execute(f"""
            SELECT {_CATEGORY_SQL} AS category, count(*) AS sku_count,
                   sum(quantity_on_hand) AS total_quantity,
                   round(sum(quantity_on_hand * unit_cost), 2) AS total_value
            FROM inventory GROUP BY category ORDER BY total_value DESC, category
        """).fetchall()
        total_value, total_skus = con.execute(
            "SELECT round(coalesce(sum(quantity_on_hand * unit_cost),0),2), count(*) FROM inventory"
        ).fetchone()

        window_row = con.execute(
            "SELECT coalesce(max(sale_date) - min(sale_date) + 1, 0) FROM sales"
        ).fetchone()
        window_days = int(window_row[0]) if window_row and window_row[0] else None
        if window_days == 0:
            window_days = None

        vel_rows = con.execute("""
            SELECT i.sku, i.product_name, i.quantity_on_hand,
                   coalesce(sum(s.quantity_sold), 0) AS total_sold
            FROM inventory i LEFT JOIN sales s ON s.sku = i.sku
            GROUP BY i.sku, i.product_name, i.quantity_on_hand ORDER BY i.sku
        """).fetchall()
        velocity: list[SkuVelocity] = []
        for sku, name, on_hand, sold in vel_rows:
            sold = int(sold)
            vel = (sold / window_days) if (window_days and sold) else 0.0
            days = round(on_hand / vel, 4) if vel > 0 else None
            velocity.append(SkuVelocity(sku, name, int(on_hand), sold, vel, days))

        exp_rows = con.execute("""
            SELECT bucket, count(*), coalesce(sum(quantity_on_hand),0),
                   round(coalesce(sum(quantity_on_hand * unit_cost),0),2)
            FROM (
              SELECT *,
                CASE
                  WHEN expiry_date IS NULL THEN 'no_expiry_date'
                  WHEN expiry_date - ? < 0 THEN 'expired'
                  WHEN expiry_date - ? <= 30 THEN '0-30'
                  WHEN expiry_date - ? <= 90 THEN '31-90'
                  WHEN expiry_date - ? <= 180 THEN '91-180'
                  ELSE '>180'
                END AS bucket
              FROM inventory
            ) GROUP BY bucket
        """, [today, today, today, today]).fetchall()
        by_bucket = {r[0]: r for r in exp_rows}
        expiry = [
            ExpiryRiskBucket(
                bucket=b,
                sku_count=int(by_bucket[b][1]) if b in by_bucket else 0,
                total_quantity=int(by_bucket[b][2]) if b in by_bucket else 0,
                value_at_risk=float(by_bucket[b][3]) if b in by_bucket else 0.0,
            )
            for b in BUCKETS
        ]
        return AnalyticsReport(
            reference_date=today,
            sales_window_days=window_days,
            total_inventory_value=float(total_value or 0.0),
            total_skus=int(total_skus or 0),
            valuation_by_category=[
                CategoryValuation(r[0], int(r[1]), int(r[2]), float(r[3])) for r in val_rows
            ],
            velocity=velocity,
            expiry_risk=expiry,
        )
    finally:
        con.close()


def analytics_for_db(db_path: str | Path, today: date | None = None) -> AnalyticsReport:
    store = SqliteStore(db_path)
    try:
        return run_analytics(store, today=today)
    finally:
        store.close()


def render_analytics(report: AnalyticsReport) -> str:
    lines = [
        "PharmaFlow analytics (DuckDB)",
        "=" * 60,
        f"Reference date: {report.reference_date}  SKUs: {report.total_skus}  "
        f"Sales window: {report.sales_window_days} day(s)",
        f"Total inventory value: {report.total_inventory_value:.2f}",
        "",
        "Inventory valuation by category:",
    ]
    for v in report.valuation_by_category:
        lines.append(
            f"  {v.category}: skus={v.sku_count} quantity={v.total_quantity} value={v.total_value:.2f}"
        )
    lines.append("")
    lines.append("Sales velocity / days of stock remaining:")
    for v in report.velocity:
        days = f"{v.days_of_stock_remaining:.2f}" if v.days_of_stock_remaining is not None else "unknown (no sales)"
        lines.append(
            f"  {v.sku} {v.product_name}: on_hand={v.quantity_on_hand} sold={v.total_sold} "
            f"velocity={v.velocity_per_day:.4f}/day days_of_stock={days}"
        )
    lines.append("")
    lines.append("Expiry risk (value at risk by days-to-expiry bucket):")
    for b in report.expiry_risk:
        lines.append(
            f"  {b.bucket}: skus={b.sku_count} quantity={b.total_quantity} value_at_risk={b.value_at_risk:.2f}"
        )
    lines.append(f"Value at risk (expired + <=30d): {report.value_at_risk_30d:.2f}")
    return "\n".join(lines) + "\n"
