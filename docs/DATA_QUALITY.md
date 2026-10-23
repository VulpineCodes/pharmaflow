# Data quality
Checks (implemented in pharmaflow/validation.py): required fields, integer/float/date
typing, sku character rule, non-negative quantities/costs, sales quantity >= 1,
implausible quantity guard (>100,000). Every failure produces a structured
ValidationError (row, field, code, message) and a quarantine entry — bad records are
never silently dropped. Metrics count rows_in / valid / quarantined / duplicates.
dbt schema.yml declares not_null/unique tests for the warehouse layer (M3, not yet run in CI).

## Report (M2)
`pharmaflow/data_quality.py` builds a per-import data-quality summary from
persisted state (imports + quarantine tables in the SQLite store, or the
in-memory store): rows in / valid / quarantined / duplicates skipped,
duplicate / valid / quarantine rates, and a quarantine reason breakdown by
validation code and by message. Render it against a database written by a
durable ingest:

```bash
python -m pharmaflow ingest sample_data/inventory_with_errors.csv --db pharmaflow.db
python -m pharmaflow report --db pharmaflow.db
```
