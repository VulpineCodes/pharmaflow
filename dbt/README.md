# dbt project — executed in M3

Verified locally 2026-10-22 (dbt-core 1.12.6, dbt-duckdb 1.11.0, DuckDB,
Python 3.12 venv):

```bash
pip install dbt-duckdb          # one install; see ../STATUS.md
cd dbt
dbt seed --profiles-dir .       # PASS=2  (raw_inventory 5 rows, raw_sales 3 rows)
dbt run  --profiles-dir .       # PASS=3  (stg_inventory, stg_sales, inventory_current)
dbt test --profiles-dir .       # PASS=8  (all not_null / unique schema tests)
```

Seeds (`seeds/raw_inventory.csv`, `seeds/raw_sales.csv`) are the
`sample_data/` CSVs plus `_file_hash` / `_imported_at` lineage columns.
Sources (`models/staging/sources.yml`) point at the `main` schema, where
the DuckDB seeds land. `profiles.yml` is a local DuckDB profile (no
credentials).

The same model SQL is also executed without dbt by
`pharmaflow/transform.py` (used by the local daily-pipeline runner);
both paths were run green in M3 — see `../STATUS.md`.
