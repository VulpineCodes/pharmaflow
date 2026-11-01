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
from pharmaflow.metrics import durable_metrics
from pharmaflow.reconciliation import reconcile


def create_app(db_path: str | Path | None = None,
               stream_log_dir: str | Path | None = None,
               stream_groups: list[str] | None = None) -> FastAPI:
    """Build a FastAPI app bound to a SQLite store at ``db_path``.

    Defaults to the ``PHARMAFLOW_DB`` env var, then ``pharmaflow.db``.

    M8 streaming gauges: ``/metrics`` also reports stream lag + DLQ
    size when a broker log dir is configured — via the arguments, or
    via ``PHARMAFLOW_STREAM_LOG_DIR`` + ``PHARMAFLOW_STREAM_GROUPS``
    (comma-separated). Gauges appear only when that directory exists,
    so a plain ``GET /metrics`` never creates broker files, and an
    unconfigured app keeps exactly its previous output.

    M9 pipeline runs: ``GET /pipeline-runs`` (newest first) and
    ``GET /pipeline-runs/latest`` serve the durable run history
    recorded by ``run_daily``; ``/metrics`` additionally renders the
    last stored run's pipeline gauges, labelled with its logical
    date. An app whose database has no recorded runs serves no
    pipeline gauges.

    M10 durable counters: ``/metrics`` renders ingestion counters
    derived from the store's persisted imports + audit trail
    (``durable_metrics``), so a fresh process on the same database
    serves the same numbers instead of restarting at zero. Each
    distinct file is counted once, at its first import; a
    duplicate-file re-import adds to ``files_already_imported`` only.
    """
    path = Path(db_path or os.environ.get("PHARMAFLOW_DB") or "pharmaflow.db")
    store = SqliteStore(path)
    stream_dir = stream_log_dir or os.environ.get("PHARMAFLOW_STREAM_LOG_DIR")
    if stream_groups is None:
        env_groups = os.environ.get("PHARMAFLOW_STREAM_GROUPS") or ""
        stream_groups = [g.strip() for g in env_groups.split(",") if g.strip()]
    app_state_stream = (Path(stream_dir), list(stream_groups)) if stream_dir else None

    app = FastAPI(title="PharmaFlow API", version="0.5.0")
    app.state.store = store
    app.state.db_path = str(path)

    def _run_response(record):
        """Serialise a StoredPipelineRun for the /pipeline-runs endpoints."""
        return {
            "run_id": record.run_id,
            "logical_date": record.logical_date.isoformat(),
            "recorded_at": record.recorded_at,
            "succeeded": record.succeeded,
            "failed_task": record.failed_task,
            "outcomes": record.result_dict.get("outcomes", []),
        }

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

    @app.get("/pipeline-runs")
    def pipeline_runs(limit: int = Query(default=20, ge=1, le=100)):
        """Recorded daily-pipeline runs, newest first (M9).

        Every completed ``run-daily`` execution is appended — successes,
        failures, and re-runs of the same logical date — so this is the
        run history, not just the last outcome.
        """
        return [_run_response(r) for r in store.list_pipeline_runs(limit=limit)]

    @app.get("/pipeline-runs/latest")
    def pipeline_runs_latest():
        """The most recently recorded run, or 404 when none exists yet."""
        from fastapi import HTTPException

        record = store.latest_pipeline_run()
        if record is None:
            raise HTTPException(status_code=404, detail="no pipeline runs recorded")
        return _run_response(record)

    @app.get("/metrics", response_class=PlainTextResponse)
    def metrics():
        # M10: ingestion counters are derived from the persisted record,
        # so they survive process restarts (the old per-process
        # store.metrics reset to zero on every restart).
        text = durable_metrics(store).render_prometheus()
        if app_state_stream is not None:
            log_dir, groups = app_state_stream
            if log_dir.exists():
                from pharmaflow.observability import (
                    render_streaming_prometheus,
                    snapshot_streaming,
                )
                from pharmaflow.streaming import FileLogBroker

                broker = FileLogBroker(log_dir)
                text += render_streaming_prometheus(
                    snapshot_streaming(broker, groups))
        # M9: pipeline gauges describe the last *stored* run, labelled
        # with its logical date, so they survive process restarts. No
        # stored run yet -> no pipeline gauges (same as before M9).
        latest = store.latest_pipeline_run()
        if latest is not None:
            from pharmaflow.observability import render_pipeline_prometheus
            from pharmaflow.orchestration import PipelineResult

            text += render_pipeline_prometheus(
                PipelineResult.from_dict(latest.result_dict),
                logical_date=latest.logical_date,
                runs_total=store.count_pipeline_runs(),
            )
        return text

    return app


app = create_app()
