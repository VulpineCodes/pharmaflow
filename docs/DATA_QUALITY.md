# Data quality

Checks (implemented in `pharmaflow/validation.py`): required fields,
integer, float, and date typing, SKU character rule, non-negative
quantities and costs, sales quantity of at least 1, and an
implausible quantity guard (over 100,000). Every failure produces a
structured `ValidationError` (row, field, code, message) and a
quarantine entry. Bad records are never dropped without a record.
Metrics count rows_in, valid, quarantined, and duplicates. The dbt
`schema.yml` declares `not_null` and `unique` tests for the
warehouse layer. Those tests run locally with dbt against DuckDB
(M3, green, re-run 2026-10-28). The CI workflow that would run
them on GitHub exists only locally and has never run.

## Report (M2)

`pharmaflow/data_quality.py` builds a per-import data-quality
summary from persisted state (imports plus quarantine tables in the
SQLite store, or the in-memory store): rows in, valid, quarantined,
duplicates skipped, duplicate, valid, and quarantine rates, and a
quarantine reason breakdown by validation code and by message.
Render it against a database written by a durable ingest:

```bash
python -m pharmaflow ingest sample_data/inventory_with_errors.csv --db pharmaflow.db
python -m pharmaflow report --db pharmaflow.db
```
