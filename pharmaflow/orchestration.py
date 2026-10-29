"""Local orchestration — M3.

The daily pipeline's task *logic* lives here as importable callables so the
same code runs under Airflow (``airflow/dags/pharmaflow_daily.py`` wires
these exact functions) and under the local runner in this module / the
``run-daily`` CLI subcommand — no Airflow server required.

Pipeline (DAG order): ``ingest_task`` → ``transform_task`` → ``report_task``

Runner semantics (explicit, tested):
* tasks run strictly in order;
* a failing task is retried up to ``retries`` extra times (so
  ``retries=2`` = up to 3 attempts, matching Airflow's ``retries``);
* a task that exhausts its retries **stops the pipeline**: the result
  records ``failed_task`` and downstream tasks are marked ``skipped``
  and are *not executed*;
* **recovery / backfill is safe by construction**: ingest is idempotent
  (M2 file/row hashes) and the transform rebuilds its DuckDB models from
  the operational store, so re-running the same logical date after a
  mid-pipeline failure completes without duplicating inventory.
* **run history is durable (M9)**: ``run_daily`` appends the completed
  result (success or failure) to the ``pipeline_runs`` table in the
  operational SQLite database, so the last run survives a restart and
  the API can serve it (``GET /pipeline-runs/latest``) and expose it
  as Prometheus gauges on ``/metrics``.
"""
from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from .ingestion import ingest_file
from .sqlite_store import SqliteStore

DEFAULT_FILES = ("inventory.csv", "sales.csv")


# --- tasks (importable callables; Airflow wires these) ----------------------

def ingest_task(db_path: str | Path, data_dir: str | Path,
                files: tuple[str, ...] = DEFAULT_FILES, **_ctx) -> dict:
    """Ingest the day's CSVs into the durable SQLite store. Idempotent."""
    data_dir = Path(data_dir)
    store = SqliteStore(db_path)
    try:
        results = []
        for name in files:
            path = data_dir / name
            if not path.exists():
                raise FileNotFoundError(f"input file not found: {path}")
            r = ingest_file(path, store)
            results.append({
                "source_file": name, "rows_in": r.rows_in, "rows_valid": r.rows_valid,
                "rows_quarantined": r.rows_quarantined,
                "duplicates_skipped": r.duplicates_skipped,
                "already_imported": r.already_imported,
            })
        return {"imports": results, "inventory_skus": len(store.inventory),
                "sales_records": len(store.sales)}
    finally:
        store.close()


def transform_task(db_path: str | Path, duckdb_path: str | Path | None = None,
                   data_dir: str | Path | None = None, **_ctx) -> dict:
    """Execute the staging→mart transformation (dbt model SQL via DuckDB).

    Delegates to :mod:`pharmaflow.transform`, which reads the actual SQL
    from ``dbt/models/``. A bad operational store (e.g. missing file, or a
    data_dir seed that fails validation) raises — the runner retries it.
    If ``data_dir`` is given, raw tables are seeded from its CSVs instead
    of the SQLite store (used to exercise the failure path in tests).
    """
    from .transform import run_transform
    result = run_transform(db_path=db_path, duckdb_path=duckdb_path, data_dir=data_dir)
    if not all(t.passed for t in result.tests):
        failed = [t.name for t in result.tests if not t.passed]
        raise RuntimeError(f"transform schema tests failed: {failed}")
    return {"models": {name: count for name, count in result.model_row_counts.items()},
            "tests_passed": len(result.tests),
            "engine": result.engine}


def report_task(db_path: str | Path, today: date | None = None, **_ctx) -> dict:
    """Build the DQ report + DuckDB analytics summary for the day."""
    from .analytics import analytics_for_db
    from .data_quality import build_report
    store = SqliteStore(db_path)
    try:
        dq = build_report(store)
        dq_totals = {"rows_in": dq.total_rows_in, "rows_valid": dq.total_rows_valid,
                     "rows_quarantined": dq.total_rows_quarantined,
                     "duplicates_skipped": dq.total_duplicates_skipped}
    finally:
        store.close()
    analytics = analytics_for_db(db_path, today=today)
    return {"data_quality": dq_totals,
            "total_inventory_value": analytics.total_inventory_value,
            "value_at_risk_30d": analytics.value_at_risk_30d,
            "total_skus": analytics.total_skus}


# --- local runner -------------------------------------------------------------

@dataclass
class TaskOutcome:
    name: str
    status: str  # success | failed | skipped
    attempts: int = 0
    result: dict | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "status": self.status,
            "attempts": self.attempts,
            "result": self.result,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "TaskOutcome":
        return cls(
            name=d["name"],
            status=d["status"],
            attempts=int(d.get("attempts", 0)),
            result=d.get("result"),
            error=d.get("error"),
        )


@dataclass
class PipelineResult:
    logical_date: date
    outcomes: list[TaskOutcome] = field(default_factory=list)
    # Storage id assigned by run_daily when the run is persisted (M9);
    # None for a purely in-memory result (persist=False) or one that
    # was rebuilt from a stored dict. Not part of the stored payload:
    # the id/recorded_at live in the pipeline_runs row, not the run.
    run_id: int | None = None

    def to_dict(self) -> dict:
        """JSON-safe payload for durable storage (M9).

        Dates serialise as ISO strings; outcome ``result`` values are
        left as-is here and JSON-encoded with ``default=str`` by the
        store, so a non-JSON-native value cannot break recording.
        """
        return {
            "logical_date": self.logical_date.isoformat(),
            "outcomes": [o.to_dict() for o in self.outcomes],
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PipelineResult":
        return cls(
            logical_date=date.fromisoformat(d["logical_date"]),
            outcomes=[TaskOutcome.from_dict(o) for o in d.get("outcomes", [])],
        )

    @property
    def succeeded(self) -> bool:
        return all(o.status == "success" for o in self.outcomes)

    @property
    def failed_task(self) -> str | None:
        for o in self.outcomes:
            if o.status == "failed":
                return o.name
        return None

    def status_of(self, name: str) -> str:
        for o in self.outcomes:
            if o.name == name:
                return o.status
        raise KeyError(name)


def run_daily(db_path: str | Path, data_dir: str | Path,
              logical_date: date | None = None, retries: int = 2,
              today: date | None = None,
              transform_data_dir: str | Path | None = None,
              persist: bool = True) -> PipelineResult:
    """Execute ingest → transform → report in DAG order, with retries.

    ``transform_data_dir`` overrides the transform's raw source (tests use
    it to inject a deliberately bad seed); production callers leave it
    ``None`` so the transform reads the operational store.

    Persistence (M9, ``persist=True``, the default): the completed
    result — success *or* failure — is appended to the ``pipeline_runs``
    table in the same SQLite database, and its row id is set on
    ``result.run_id``. Every run is appended, including re-runs of the
    same logical date; ingest idempotency is what prevents duplicate
    inventory, not overwriting run history. A recording failure raises
    rather than silently losing the run record. ``persist=False``
    keeps the historical in-memory-only behaviour (no row, ``run_id``
    stays ``None``).
    """
    logical_date = logical_date or date.today()
    duckdb_path = str(Path(db_path).with_suffix(".duckdb"))
    plan = [
        ("ingest", lambda: ingest_task(db_path, data_dir)),
        ("transform", lambda: transform_task(db_path, duckdb_path=duckdb_path,
                                             data_dir=transform_data_dir)),
        ("report", lambda: report_task(db_path, today=today or logical_date)),
    ]
    result = PipelineResult(logical_date=logical_date)
    blocked = False
    for name, fn in plan:
        if blocked:
            result.outcomes.append(TaskOutcome(name=name, status="skipped", attempts=0))
            continue
        outcome = TaskOutcome(name=name, status="failed", attempts=0)
        for attempt in range(1, retries + 2):
            outcome.attempts = attempt
            try:
                outcome.result = fn()
                outcome.status = "success"
                outcome.error = None
                break
            except Exception as exc:  # task failure: retry, then abort pipeline
                outcome.error = f"{type(exc).__name__}: {exc}"
                outcome.result = None
        if outcome.status != "success":
            blocked = True
            # Keep the traceback available to callers debugging locally.
            outcome.error = outcome.error or "unknown failure"
        result.outcomes.append(outcome)
    if persist:
        store = SqliteStore(db_path)
        try:
            result.run_id = store.record_pipeline_run(result.to_dict())
        finally:
            store.close()
    return result


def render_pipeline_result(result: PipelineResult) -> str:
    lines = [f"PharmaFlow daily pipeline — logical date {result.logical_date}",
             "=" * 60]
    for o in result.outcomes:
        line = f"  [{o.status}] {o.name} (attempts={o.attempts})"
        if o.error:
            line += f" error={o.error}"
        lines.append(line)
        if o.result:
            lines.append(f"      {o.result}")
    lines.append(f"Pipeline succeeded: {result.succeeded}"
                 + (f" (failed task: {result.failed_task})" if result.failed_task else ""))
    return "\n".join(lines) + "\n"
