-- PharmaFlow initial schema (Postgres dialect) — migration 001.
--
-- NOT RUN LOCALLY: no Postgres server / Docker in the current build
-- environment. This file mirrors the verified SQLite schema in
-- pharmaflow/sqlite_store.py (same tables/columns) so both stores share
-- one data model. See docs/POSTGRES.md.
--
-- It is idempotent (CREATE TABLE IF NOT EXISTS) and is mounted into the
-- docker-compose postgres service as a docker-entrypoint-initdb.d script,
-- which Postgres runs once on first initialisation of a fresh volume.
-- PostgresStore._init_schema applies the same DDL, so either path is safe.

CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL
);

INSERT INTO schema_version (version, applied_at)
SELECT 1, now()
WHERE NOT EXISTS (SELECT 1 FROM schema_version);

CREATE TABLE IF NOT EXISTS file_hashes (
    file_hash TEXT PRIMARY KEY,
    imported_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS row_hashes (
    row_hash TEXT PRIMARY KEY,
    file_hash TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS imports (
    file_hash TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    kind TEXT NOT NULL,
    rows_in INTEGER NOT NULL DEFAULT 0,
    rows_valid INTEGER NOT NULL DEFAULT 0,
    rows_quarantined INTEGER NOT NULL DEFAULT 0,
    duplicates_skipped INTEGER NOT NULL DEFAULT 0,
    imported_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS inventory (
    sku TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    quantity_on_hand INTEGER NOT NULL,
    reorder_point INTEGER NOT NULL,
    unit_cost DOUBLE PRECISION NOT NULL DEFAULT 0,
    expiry_date DATE,
    supplier TEXT NOT NULL DEFAULT '',
    batch TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS sales (
    id BIGSERIAL PRIMARY KEY,
    sale_id TEXT NOT NULL,
    sku TEXT NOT NULL,
    quantity_sold INTEGER NOT NULL,
    sale_date DATE,
    unit_price DOUBLE PRECISION NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS quarantine (
    id BIGSERIAL PRIMARY KEY,
    row_number INTEGER NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    raw_json JSONB NOT NULL,
    errors_json JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS audit (
    id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL,
    action TEXT NOT NULL,
    source_file TEXT NOT NULL,
    file_hash TEXT NOT NULL,
    details TEXT NOT NULL
);
