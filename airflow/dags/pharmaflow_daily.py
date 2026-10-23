"""PharmaFlow daily DAG — M3: wiring only, logic lives in the package.

Task logic is in ``pharmaflow/orchestration.py`` as importable callables
(``ingest_task``, ``transform_task``, ``report_task``). This DAG wires
those exact callables with Airflow retries/backfill semantics; the same
callables are executed without a server by
``python -m pharmaflow run-daily --db <file> --data-dir sample_data``.

No Airflow server exists in the local portfolio environment, so the DAG
itself is *not* claimed as executed — the callables it wires are, via the
local runner and its tests (see STATUS.md).
"""
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
except ImportError:  # allows import/checkout without Airflow installed
    DAG = None

from datetime import datetime, timedelta
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pharmaflow.orchestration import ingest_task, report_task, transform_task

DB_PATH = "pharmaflow.db"
DATA_DIR = "sample_data"

if DAG is not None:
    with DAG("pharmaflow_daily", start_date=datetime(2026, 10, 22), schedule="@daily", catchup=False,
             default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "owner": "franklin"}) as dag:
        t_ingest = PythonOperator(
            task_id="ingest", python_callable=ingest_task,
            op_kwargs={"db_path": DB_PATH, "data_dir": DATA_DIR})
        t_transform = PythonOperator(
            task_id="transform", python_callable=transform_task,
            op_kwargs={"db_path": DB_PATH})
        t_report = PythonOperator(
            task_id="report", python_callable=report_task,
            op_kwargs={"db_path": DB_PATH})
        t_ingest >> t_transform >> t_report
