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

    M11 stream consumption: ``GET /stream-consumption`` (newest first,
    optional ``?group=``) and ``GET /stream-consumption/latest``
    serve the durable consume-run history recorded by
    ``stream-consume``; ``/metrics`` additionally renders per-group
    summed consumption counters. An app whose database has no recorded
    consume runs serves no stream-consumption gauges.

    M12 dead letters: ``GET /dead-letters`` (newest first, optional
    ``?group=``) and ``GET /dead-letters/latest`` serve the durable
    dead-letter history recorded by ``stream-consume`` — one row per
    poison event a completed run dead-lettered, with its errors and
    source coordinates; ``/metrics`` additionally renders per-group
    dead-letter totals. An app whose database has no recorded dead
    letters serves no dead-letter gauge.

    M13 dead-letter replays: ``POST /dead-letters/{record_id}/replay``
    re-publishes a recorded dead letter with caller-supplied field
    corrections (``{"corrections": {...}}``; empty means re-publish
    the original payload unchanged) after validating it through the
    same ``parse_event`` rules the consumer applies; the corrected
    event keeps the original event id, so the consumer's dedupe
    makes a double replay a no-op. ``GET /dead-letter-replays``
    (newest first, optional ``?dead_letter_id=`` / ``?group=``) and
    ``GET /dead-letter-replays/latest`` serve the durable replay
    history; ``/metrics`` additionally renders per-group replay
    totals. Replay requires a broker log directory configured (the
    same ``stream_log_dir`` argument / ``PHARMAFLOW_STREAM_LOG_DIR``
    env var the streaming gauges use) and returns 503 without one.

    M14 replay outcomes: ``GET /replay-outcomes`` (newest first,
    optional ``?replay_id=`` / ``?group=`` / ``?outcome=``) and
    ``GET /replay-outcomes/latest`` serve the durable outcome
    history recorded by ``stream-consume`` — one row per (replay,
    group) saying what the group's next completed consume run did
    with the re-published record (applied / duplicate /
    dead-lettered again, with the new dead-letter row id when
    applicable, and the consumption run id). ``GET
    /dead-letter-replays/{replay_id}`` returns one replay with its
    outcomes embedded; an empty outcomes list means the replay has
    not been consumed yet (pending). ``/metrics`` additionally
    renders per-group, per-outcome totals.
    """
    path = Path(db_path or os.environ.get("PHARMAFLOW_DB") or "pharmaflow.db")
    store = SqliteStore(path)
    stream_dir = stream_log_dir or os.environ.get("PHARMAFLOW_STREAM_LOG_DIR")
    if stream_groups is None:
        env_groups = os.environ.get("PHARMAFLOW_STREAM_GROUPS") or ""
        stream_groups = [g.strip() for g in env_groups.split(",") if g.strip()]
    app_state_stream = (Path(stream_dir), list(stream_groups)) if stream_dir else None

    app = FastAPI(title="PharmaFlow API", version="0.9.0")
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

    @app.get("/stream-consumption")
    def stream_consumption(group: str | None = Query(default=None),
                           limit: int = Query(default=20, ge=1, le=100)):
        """Recorded stream-consume runs, newest first (M11).

        Every *completed* ``stream-consume`` execution is appended —
        including no-op catch-ups (read=0) and re-runs after a crash
        (whose duplicates column shows the redelivery). A run
        interrupted mid-consume is never recorded.
        """
        return [
            {
                "run_id": r.run_id, "group": r.group, "topic": r.topic,
                "records_read": r.records_read, "applied": r.applied,
                "duplicates": r.duplicates, "dlq": r.dlq,
                "lag_before": r.lag_before, "lag_after": r.lag_after,
                "recorded_at": r.recorded_at,
            }
            for r in store.list_stream_consumption(group=group, limit=limit)
        ]

    @app.get("/stream-consumption/latest")
    def stream_consumption_latest(group: str | None = Query(default=None)):
        """The most recently recorded consume run, or 404 when none exists."""
        from fastapi import HTTPException

        record = store.latest_stream_consumption(group=group)
        if record is None:
            raise HTTPException(status_code=404,
                                detail="no stream consumption runs recorded")
        return {
            "run_id": record.run_id, "group": record.group, "topic": record.topic,
            "records_read": record.records_read, "applied": record.applied,
            "duplicates": record.duplicates, "dlq": record.dlq,
            "lag_before": record.lag_before, "lag_after": record.lag_after,
            "recorded_at": record.recorded_at,
        }

    @app.get("/dead-letters")
    def dead_letters(group: str | None = Query(default=None),
                     limit: int = Query(default=20, ge=1, le=100)):
        """Recorded dead letters, newest first (M12).

        One row per poison event a *completed* ``stream-consume`` run
        dead-lettered: the consumer group, the event id (possibly
        empty for a malformed record), the validation errors, and the
        source topic/partition/offset where the poison record sits in
        the broker log. A run interrupted mid-consume records nothing.
        """
        return [
            {
                "record_id": r.record_id, "group": r.group,
                "event_id": r.event_id, "errors": r.errors,
                "topic": r.topic, "partition": r.partition,
                "offset": r.offset, "recorded_at": r.recorded_at,
            }
            for r in store.list_dead_letters(group=group, limit=limit)
        ]

    @app.get("/dead-letters/latest")
    def dead_letters_latest(group: str | None = Query(default=None)):
        """The most recently recorded dead letter, or 404 when none exists."""
        from fastapi import HTTPException

        record = store.latest_dead_letter(group=group)
        if record is None:
            raise HTTPException(status_code=404,
                                detail="no dead letters recorded")
        return {
            "record_id": record.record_id, "group": record.group,
            "event_id": record.event_id, "errors": record.errors,
            "topic": record.topic, "partition": record.partition,
            "offset": record.offset, "recorded_at": record.recorded_at,
        }

    def _replay_response(replay):
        """Serialise a DeadLetterReplay for the replay endpoints."""
        return {
            "replay_id": replay.replay_id,
            "dead_letter_id": replay.dead_letter_id,
            "group": replay.group, "event_id": replay.event_id,
            "topic": replay.topic, "partition": replay.partition,
            "offset": replay.offset,
            "corrected_event": replay.corrected_event,
            "replayed_at": replay.replayed_at,
        }

    @app.post("/dead-letters/{record_id}/replay", status_code=201)
    def dead_letter_replay(record_id: int, payload: dict | None = None):
        """Re-publish a recorded dead letter with corrections (M13).

        Body: ``{"corrections": {...}}`` — field overrides applied on
        top of the original payload (empty/omitted re-publishes the
        original unchanged). The corrected event is validated through
        the same ``parse_event`` rules the consumer applies and keeps
        the original event id, so a double replay is deduplicated by
        the consumer. Returns the recorded replay, including the
        broker coordinates of the re-published record. 404 for an
        unknown dead-letter id, 422 when the corrected payload still
        fails validation, 400 for a replay that cannot proceed
        (e.g. a correction changing the event id, or the original
        record missing from the broker log), 503 when no broker log
        directory is configured (``PHARMAFLOW_STREAM_LOG_DIR``).
        """
        from fastapi import HTTPException

        from pharmaflow.streaming import EventValidationError, replay_dead_letter

        if app_state_stream is None:
            raise HTTPException(
                status_code=503,
                detail="dead-letter replay requires a broker log directory "
                       "(set PHARMAFLOW_STREAM_LOG_DIR)")
        log_dir, _groups = app_state_stream
        body = payload or {}
        corrections = body.get("corrections")
        if corrections is not None and not isinstance(corrections, dict):
            raise HTTPException(status_code=422,
                                detail="corrections must be a JSON object")
        try:
            replay = replay_dead_letter(
                log_dir, path, record_id,
                corrections=corrections,
                group=body.get("group"),
            )
        except EventValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors) from exc
        except ValueError as exc:
            msg = str(exc)
            if msg.startswith("no dead letter recorded"):
                raise HTTPException(status_code=404, detail=msg) from exc
            raise HTTPException(status_code=400, detail=msg) from exc
        return _replay_response(replay)

    @app.get("/dead-letter-replays")
    def dead_letter_replays(dead_letter_id: int | None = Query(default=None),
                            group: str | None = Query(default=None),
                            limit: int = Query(default=20, ge=1, le=100)):
        """Recorded dead-letter replays, newest first (M13).

        One row per corrected re-publish: which dead letter it fixed,
        the group, the (unchanged) event id, the corrected payload as
        published, and the broker coordinates of the re-published
        record. Replaying the same dead letter twice appends two
        rows; the consumer's event-id dedupe is what makes the
        second copy a no-op.
        """
        return [
            _replay_response(r)
            for r in store.list_dead_letter_replays(
                dead_letter_id=dead_letter_id, group=group, limit=limit)
        ]

    @app.get("/dead-letter-replays/latest")
    def dead_letter_replays_latest(dead_letter_id: int | None = Query(default=None),
                                   group: str | None = Query(default=None)):
        """The most recently recorded replay, or 404 when none exists."""
        from fastapi import HTTPException

        record = store.latest_dead_letter_replay(
            dead_letter_id=dead_letter_id, group=group)
        if record is None:
            raise HTTPException(status_code=404,
                                detail="no dead-letter replays recorded")
        return _replay_response(record)

    def _outcome_response(outcome):
        """Serialise a StoredReplayOutcome for the outcome endpoints."""
        return {
            "outcome_id": outcome.outcome_id,
            "replay_id": outcome.replay_id,
            "dead_letter_id": outcome.dead_letter_id,
            "group": outcome.group,
            "event_id": outcome.event_id,
            "outcome": outcome.outcome,
            "new_dead_letter_id": outcome.new_dead_letter_id,
            "consumption_run_id": outcome.consumption_run_id,
            "recorded_at": outcome.recorded_at,
        }

    @app.get("/dead-letter-replays/{replay_id}")
    def dead_letter_replay_one(replay_id: int):
        """One recorded replay, with its outcomes embedded (M14).

        ``outcomes`` is empty while the replay has not been consumed
        yet (pending) — a replay's outcome is recorded by the next
        completed ``stream-consume`` run for its group, not at
        replay time. A ``dead_lettered`` outcome carries the new
        dead-letter row id, so the chain dead letter -> replay ->
        outcome -> new dead letter is traversable from this one
        response.
        """
        from fastapi import HTTPException

        record = store.get_dead_letter_replay(replay_id)
        if record is None:
            raise HTTPException(
                status_code=404,
                detail=f"no dead-letter replay recorded with id {replay_id}")
        body = _replay_response(record)
        body["outcomes"] = [
            _outcome_response(o)
            for o in store.list_replay_outcomes(replay_id=replay_id)
        ]
        return body

    @app.get("/replay-outcomes")
    def replay_outcomes(replay_id: int | None = Query(default=None),
                        group: str | None = Query(default=None),
                        outcome: str | None = Query(default=None),
                        limit: int = Query(default=20, ge=1, le=100)):
        """Recorded replay outcomes, newest first (M14).

        One row per (replay, group): what the group's next completed
        consume run did with the re-published record — applied,
        duplicate, or dead-lettered again. A replay with no row here
        has not been consumed yet. ``outcome`` filters to one of
        ``applied`` / ``duplicate`` / ``dead_lettered``.
        """
        return [
            _outcome_response(o)
            for o in store.list_replay_outcomes(
                replay_id=replay_id, group=group, outcome=outcome,
                limit=limit)
        ]

    @app.get("/replay-outcomes/latest")
    def replay_outcomes_latest(replay_id: int | None = Query(default=None),
                               group: str | None = Query(default=None)):
        """The most recently recorded replay outcome, or 404 when none exists."""
        from fastapi import HTTPException

        record = store.latest_replay_outcome(replay_id=replay_id, group=group)
        if record is None:
            raise HTTPException(status_code=404,
                                detail="no replay outcomes recorded")
        return _outcome_response(record)

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
        # M11: stream-consumption gauges sum over recorded, completed
        # consume runs, per group — so they survive process restarts.
        # No recorded runs yet -> no stream-consumption gauges (same
        # as the M9 pipeline gauges above). Live lag/DLQ still come
        # from the broker snapshot rendered above.
        if store.count_stream_consumption() > 0:
            from pharmaflow.observability import (
                render_stream_consumption_prometheus,
            )

            text += render_stream_consumption_prometheus(
                store.stream_consumption_totals_by_group()
            )
        # M12: dead-letter totals count poison events dead-lettered by
        # completed, recorded consume runs, per group. No recorded dead
        # letters yet -> no dead-letter gauge. The live DLQ size still
        # comes from the broker snapshot rendered above.
        if store.count_dead_letters() > 0:
            from pharmaflow.observability import render_dead_letters_prometheus

            text += render_dead_letters_prometheus(
                store.dead_letter_totals_by_group()
            )
        # M13: replay totals count corrected re-publishes of recorded
        # dead letters, per group. No recorded replays yet -> no
        # replay gauge. This gauge says the event was re-published,
        # not that it was applied — application is decided by the
        # next consume run and shows up in the consumption gauges.
        if store.count_dead_letter_replays() > 0:
            from pharmaflow.observability import (
                render_dead_letter_replays_prometheus,
            )

            text += render_dead_letter_replays_prometheus(
                store.dead_letter_replay_totals_by_group()
            )
        # M14: replay-outcome totals count, per group and per outcome,
        # what the next completed consume run did with each
        # re-published replay. No recorded outcomes yet -> no outcome
        # gauge. A replay with no outcome row (not consumed yet)
        # contributes to no count here.
        if store.count_replay_outcomes() > 0:
            from pharmaflow.observability import (
                render_replay_outcomes_prometheus,
            )

            text += render_replay_outcomes_prometheus(
                store.replay_outcome_totals_by_group()
            )
        return text

    return app


app = create_app()
