"""FastAPI serving layer — M5: verified against the durable SQLite store.

Run:  PHARMAFLOW_DB=pharmaflow.db uvicorn api.app:app --host 0.0.0.0 --port 8000

The app serves *real persisted state*: it is wired to ``SqliteStore``
(the M2 verified store) at the path from the ``PHARMAFLOW_DB``
environment variable (default ``pharmaflow.db`` in the working
directory). State therefore survives a process restart — verified by
``tests/test_api.py``, which creates a second app instance on the same
DB file and reads the inventory back.

``create_app(db_path)`` is the factory tests use to bind an app to a
temporary DB; the module-level ``app`` is what uvicorn serves.

If fastapi is not installed, importing this module raises a clear
ImportError; the ingestion core does not depend on it.
"""
from __future__ import annotations

import os
from datetime import date
from pathlib import Path

try:
    from fastapi import FastAPI, File, Query, UploadFile
    from fastapi.responses import PlainTextResponse
except ImportError as e:  # pragma: no cover
    raise ImportError(
        "FastAPI not installed — install requirements.txt to run the API. "
        "Core ingestion works without it."
    ) from e

from pharmaflow import SqliteStore, build_report, generate_alerts, ingest_bytes
from pharmaflow.reconciliation import reconcile


def create_app(db_path: str | Path | None = None) -> FastAPI:
    """Build a FastAPI app bound to a SQLite store at ``db_path``.

    Defaults to the ``PHARMAFLOW_DB`` env var, then ``pharmaflow.db``.
    """
    path = Path(db_path or os.environ.get("PHARMAFLOW_DB") or "pharmaflow.db")
    store = SqliteStore(path)

    app = FastAPI(title="PharmaFlow API", version="0.3.0")
    app.state.store = store
    app.state.db_path = str(path)

    @app.get("/health")
    def health():
        return {
            "status": "ok",
            "service": "pharmaflow",
            "db": str(path),
            "inventory_skus": len(store.inventory),
        }

    @app.get("/imports")
    def imports():
        return [
            {
                "timestamp": e.timestamp, "action": e.action,
                "source_file": e.source_file, "file_hash": e.file_hash,
                "details": e.details,
            }
            for e in store.audit.entries
        ]

    @app.post("/imports")
    async def create_import(file: UploadFile = File(...)):
        """Upload CSV bytes (multipart ``file``) → ingest result.

        Ingestion is idempotent: re-posting the identical bytes returns
        ``already_imported=True`` and changes no inventory.
        """
        data = await file.read()
        source = file.filename or "upload.csv"
        try:
            result = ingest_bytes(data, source, store)
        except ValueError as exc:
            from fastapi import HTTPException

            raise HTTPException(status_code=422, detail=str(exc)) from exc
        return {
            "source_file": result.source_file, "file_hash": result.file_hash,
            "kind": result.kind, "rows_in": result.rows_in,
            "rows_valid": result.rows_valid,
            "rows_quarantined": result.rows_quarantined,
            "duplicates_skipped": result.duplicates_skipped,
            "already_imported": result.already_imported,
        }

    @app.get("/inventory")
    def inventory():
        return [
            {
                "sku": r.sku, "product_name": r.product_name,
                "quantity_on_hand": r.quantity_on_hand,
                "reorder_point": r.reorder_point, "unit_cost": r.unit_cost,
                "expiry_date": str(r.expiry_date) if r.expiry_date else None,
                "supplier": r.supplier, "batch": r.batch,
            }
            for r in sorted(store.inventory.values(), key=lambda r: r.sku)
        ]

    @app.get("/alerts")
    def alerts(today: str | None = Query(default=None)):
        """Low-stock / expiry alerts. ``?today=YYYY-MM-DD`` pins the
        reference date (deterministic tests / reports); default today."""
        ref = date.fromisoformat(today) if today else date.today()
        return [
            {
                "sku": a.sku, "product_name": a.product_name, "kind": a.kind,
                "message": a.message, "quantity_on_hand": a.quantity_on_hand,
                "reorder_point": a.reorder_point,
                "expiry_date": str(a.expiry_date) if a.expiry_date else None,
                "days_until_expiry": a.days_until_expiry,
            }
            for a in generate_alerts(store.inventory, today=ref)
        ]

    @app.get("/reconciliation")
    def reconciliation(expected: str = ""):
        """expected as sku=qty,sku=qty, e.g. ?expected=MED-001=110,MED-002=8"""
        exp: dict[str, int] = {}
        for part in filter(None, expected.split(",")):
            k, v = part.split("=")
            exp[k.strip()] = int(v)
        actual = {k: v.quantity_on_hand for k, v in store.inventory.items()}
        return [
            {
                "sku": d.sku, "kind": d.kind, "expected": d.expected,
                "actual": d.actual, "difference": d.difference,
                "severity": d.severity, "message": d.message,
            }
            for d in reconcile(exp, actual)
        ]

    @app.get("/data-quality")
    def data_quality():
        return build_report(store).as_dict()

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        return store.metrics.render_prometheus()

    return app


app = create_app()
