# Postgres path: code only, NOT run locally

> **Honesty status (2026-10-22):** Postgres has **not** been executed for
> PharmaFlow. There is no Postgres server and no Docker in the current build
> environment. The verified durable persistence path at M2 is SQLite
> (`pharmaflow/sqlite_store.py`, stdlib `sqlite3`, covered by tests).
> This document describes the prepared Postgres path so it can be brought
> up and verified when a server is available. It is not evidence that it works yet.

## What exists

| Piece | Path | Status |
|---|---|---|
| Store implementation | `pharmaflow/postgres_store.py` | Code-complete skeleton, guarded `psycopg` import, **never connected to a server** |
| Schema migration | `db/migrations/001_initial_schema.sql` | Same tables/columns as the verified SQLite schema, Postgres dialect, idempotent |
| Local topology | `docker-compose.yml` (`postgres` service) | Declared, **untested locally (no Docker)**; mounts the migration into `/docker-entrypoint-initdb.d/` |

`PostgresStore` implements the same `StoreProtocol`
(`pharmaflow/store.py`) as `PharmaStore` (in-memory) and `SqliteStore`:
file/row hash idempotency, inventory upserts, sales, quarantine, audit,
and per-import summaries for the data-quality report. Ingestion code does
not change when the store changes. The migration inserts schema
version 1 and has no `pipeline_runs` table, so the Postgres store
does not persist pipeline runs (the M9 history is SQLite only).

## Dialect differences from SQLite

- Placeholders: `%s` (psycopg) instead of `?`.
- Auto-increment: `BIGSERIAL` instead of `INTEGER ... AUTOINCREMENT`.
- Quarantine payloads: `JSONB` instead of JSON text.
- Dates/timestamps: native `DATE` / `TIMESTAMPTZ` columns.
- Upserts / ignored inserts use the same `ON CONFLICT ... DO NOTHING /
  DO UPDATE` shape in both dialects.

## Bring-up checklist (to do when a server exists; do not skip verification)

1. Start Postgres, e.g. `docker compose up -d postgres` (fresh volume runs
   `001_initial_schema.sql` automatically), or apply the migration
   manually: `psql "$DATABASE_URL" -f db/migrations/001_initial_schema.sql`.
2. `pip install -r requirements.txt` (provides `psycopg[binary]`).
3. Point the app at the store via its DSN, e.g.
   `PostgresStore("postgresql://pharmaflow:...@localhost:5432/pharmaflow")`.
4. Run the SQLite store test-suite scenarios against Postgres: persistence
   across close/reopen, duplicate-file idempotency across restarts,
   quarantine + audit persistence, idempotent schema creation.
   Until those pass against a real server, do not claim Postgres support
   as verified in README/STATUS.
5. Only after step 4: update `STATUS.md` to mark the Postgres path verified,
   with the server version and test output recorded.

## Why SQLite first

The M2 requirement is durable persistence that is *verifiable in this
environment*. SQLite is a real, transactional, on-disk database in the
Python standard library: file hashes, inventory, quarantine and audit
genuinely survive process restarts here, and the tests prove it. Postgres
remains the intended multi-user / deployed store, prepared but honestly
labeled unverified.
