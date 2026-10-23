"""Executed transformation layer — M3.

Runs the *same* staging → mart SQL that lives in ``dbt/models/``:

* ``dbt/models/staging/stg_inventory.sql``
* ``dbt/models/staging/stg_sales.sql``
* ``dbt/models/marts/inventory_current.sql``

The Jinja ``{{ source('raw','...') }}`` / ``{{ ref('...') }}`` placeholders
are resolved to DuckDB table/view names, the SQL is executed in dependency
order, and the schema tests declared in ``dbt/models/marts/schema.yml``
(``not_null`` / ``unique`` on the declared columns) are run as queries.

This module is the execution path when dbt itself is not installed. When
dbt-duckdb *is* installed, ``dbt run`` / ``dbt test`` execute the identical
model files against the identical DuckDB engine (see STATUS.md for which
path was actually run, and when).

Raw tables: built either from the SQLite operational store (default) or
seeded directly from CSVs in ``data_dir`` (the dbt-seed equivalent). Both
carry the ``_file_hash`` / ``_imported_at`` lineage columns the staging
models select.
"""
from __future__ import annotations

import csv
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path

try:
    import duckdb
except ImportError as exc:  # pragma: no cover
    raise ImportError("pharmaflow.transform requires duckdb (pip install duckdb)") from exc

ROOT = Path(__file__).resolve().parents[1]
DBT_MODELS = ROOT / "dbt" / "models"

# model name -> file, in dependency order
_MODEL_FILES = [
    ("stg_inventory", DBT_MODELS / "staging" / "stg_inventory.sql"),
    ("stg_sales", DBT_MODELS / "staging" / "stg_sales.sql"),
    ("inventory_current", DBT_MODELS / "marts" / "inventory_current.sql"),
]
# Mirrors dbt_project.yml: staging = view, marts = table.
_MATERIALIZATION = {"stg_inventory": "view", "stg_sales": "view", "inventory_current": "table"}

# Mirrors dbt/models/marts/schema.yml (kept in sync deliberately; the dbt
# path runs the YAML itself, this path runs the same checks as SQL).
_SCHEMA_TESTS = [
    ("stg_inventory", "sku", "not_null"), ("stg_inventory", "sku", "unique"),
    ("stg_inventory", "quantity_on_hand", "not_null"),
    ("stg_sales", "sale_id", "not_null"), ("stg_sales", "sale_id", "unique"),
    ("inventory_current", "sku", "not_null"), ("inventory_current", "sku", "unique"),
    ("inventory_current", "quantity_on_hand", "not_null"),
]


@dataclass
class SchemaTestResult:
    name: str
    model: str
    column: str
    test: str
    passed: bool
    failures: int


@dataclass
class TransformResult:
    engine: str  # "duckdb-python (dbt model SQL)" — dbt path is reported separately
    model_row_counts: dict[str, int] = field(default_factory=dict)
    tests: list[SchemaTestResult] = field(default_factory=list)

    @property
    def all_tests_passed(self) -> bool:
        return all(t.passed for t in self.tests)


def resolve_model_sql(sql: str) -> str:
    """Replace dbt Jinja source()/ref() with DuckDB object names."""
    sql = re.sub(r"\{\{\s*source\(\s*'raw'\s*,\s*'([^']+)'\s*\)\s*\}\}", r"\1", sql)
    sql = re.sub(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}", r"\1", sql)
    if "{{" in sql or "}}" in sql:
        raise ValueError(f"unresolved dbt Jinja in model SQL: {sql!r}")
    return sql.strip().rstrip(";")


def _create_raw_from_store(con, db_path: Path) -> None:
    from .sqlite_store import SqliteStore
    store = SqliteStore(db_path)
    try:
        imported_at = datetime.now(timezone.utc).isoformat()
        con.execute(
            "CREATE TABLE raw_inventory (sku VARCHAR, product_name VARCHAR, "
            "quantity_on_hand INTEGER, unit_cost DOUBLE, expiry_date DATE, "
            "reorder_point INTEGER, supplier VARCHAR, batch VARCHAR, "
            "_file_hash VARCHAR, _imported_at TIMESTAMP)"
        )
        for rec in sorted(store.inventory.values(), key=lambda r: r.sku):
            con.execute("INSERT INTO raw_inventory VALUES (?,?,?,?,?,?,?,?,?,?)",
                        [rec.sku, rec.product_name, rec.quantity_on_hand, rec.unit_cost,
                         rec.expiry_date, rec.reorder_point, rec.supplier, rec.batch,
                         "sqlite-store", imported_at])
        con.execute(
            "CREATE TABLE raw_sales (sale_id VARCHAR, sku VARCHAR, "
            "quantity_sold INTEGER, sale_date DATE, unit_price DOUBLE, "
            "_file_hash VARCHAR, _imported_at TIMESTAMP)"
        )
        for s in store.sales:
            con.execute("INSERT INTO raw_sales VALUES (?,?,?,?,?,?,?)",
                        [s.sale_id, s.sku, s.quantity_sold, s.sale_date, s.unit_price,
                         "sqlite-store", imported_at])
    finally:
        store.close()


def _create_raw_from_csvs(con, data_dir: Path) -> None:
    """Seed-equivalent: load CSVs verbatim (no validation — bad data must
    be able to fail the transform's schema tests, as it would under dbt)."""
    inv = data_dir / "inventory.csv"
    sales = data_dir / "sales.csv"
    for path in (inv, sales):
        if not path.exists():
            raise FileNotFoundError(f"seed file not found: {path}")
    ts = datetime.now(timezone.utc).isoformat()
    # read_csv_auto keeps the raw values; staging models do the casting.
    con.execute(
        "CREATE TABLE raw_inventory AS SELECT *, 'csv-seed' AS _file_hash, "
        f"TIMESTAMP '{ts}' AS _imported_at FROM read_csv_auto(?, header=true)", [str(inv)]
    )
    con.execute(
        "CREATE TABLE raw_sales AS SELECT *, 'csv-seed' AS _file_hash, "
        f"TIMESTAMP '{ts}' AS _imported_at FROM read_csv_auto(?, header=true)", [str(sales)]
    )


def run_transform(db_path: str | Path | None = None,
                  duckdb_path: str | Path | None = None,
                  data_dir: str | Path | None = None) -> TransformResult:
    """Execute staging→mart models + schema tests. Raises on SQL errors."""
    if data_dir is None and db_path is None:
        raise ValueError("run_transform needs db_path (SQLite store) or data_dir (CSV seeds)")
    con = duckdb.connect(str(duckdb_path) if duckdb_path else ":memory:")
    try:
        # Fresh rebuild: re-running the transform is idempotent.
        for name in ("inventory_current", "stg_inventory", "stg_sales",
                     "raw_inventory", "raw_sales"):
            con.execute(f"DROP TABLE IF EXISTS {name}")
            con.execute(f"DROP VIEW IF EXISTS {name}")
        if data_dir is not None:
            _create_raw_from_csvs(con, Path(data_dir))
        else:
            _create_raw_from_store(con, Path(db_path))  # type: ignore[arg-type]

        result = TransformResult(engine="duckdb-python (dbt model SQL)")
        for name, path in _MODEL_FILES:
            sql = resolve_model_sql(path.read_text())
            kind = _MATERIALIZATION[name]
            con.execute(f"CREATE OR REPLACE {kind.upper()} {name} AS {sql}")
            count = con.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
            result.model_row_counts[name] = int(count)

        for model, column, test in _SCHEMA_TESTS:
            if test == "not_null":
                failures = con.execute(
                    f"SELECT count(*) FROM {model} WHERE {column} IS NULL"
                ).fetchone()[0]
            else:  # unique
                failures = con.execute(
                    f"SELECT count(*) FROM (SELECT {column} FROM {model} "
                    f"GROUP BY {column} HAVING count(*) > 1)"
                ).fetchone()[0]
            result.tests.append(SchemaTestResult(
                name=f"{test}_{model}_{column}", model=model, column=column,
                test=test, passed=int(failures) == 0, failures=int(failures),
            ))
        return result
    finally:
        con.close()
