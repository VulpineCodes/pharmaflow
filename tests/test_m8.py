"""M8 — streaming + pipeline observability in Prometheus text.

The numbers here are derived from the broker's own bookkeeping, not
from a second counter: lag is end_offset - committed_offset per
partition, and the DLQ count is the DLQ topic's own size. The test
scenario is deliberately small and hand-checkable: 4 events produced,
one group consumes all 4 (lag 0), a second group consumes nothing
(lag 4), and one poison event dead-letters (DLQ 1).
"""
from pathlib import Path

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

from api.app import create_app
from pharmaflow.observability import (
    escape_label,
    render_pipeline_prometheus,
    render_streaming_prometheus,
    snapshot_streaming,
)
from pharmaflow.orchestration import PipelineResult, TaskOutcome
from pharmaflow.sqlite_store import SqliteStore
from pharmaflow.streaming import (
    EventProducer,
    FileLogBroker,
    StreamingConsumer,
    load_events_file,
)
from pharmaflow.__main__ import main as cli_main

ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "sample_data" / "stream_events.jsonl"
POISON = {
    "event_type": "SaleRecorded", "event_id": "evt-poison",
    "sale_id": "S-9999", "sku": "MED-001",
    "quantity_sold": -2, "unit_price": 6.00, "sale_date": "2026-10-22",
}


def _seed_broker(tmp_path):
    log_dir = tmp_path / "broker"
    broker = FileLogBroker(log_dir)
    producer = EventProducer(broker)
    for value in load_events_file(EVENTS):
        producer.publish_raw(value)
    producer.publish_raw(POISON)
    return broker, log_dir


def test_snapshot_lag_and_dlq(tmp_path):
    broker, _ = _seed_broker(tmp_path)
    store = SqliteStore(tmp_path / "obs.db")
    try:
        # One group consumes everything (4 valid + 1 poison -> DLQ);
        # a second group never consumes.
        result = StreamingConsumer(broker, store, group="reconciliation-v1").consume_until_caught_up()
        assert result.applied == 4 and result.dlq == 1 and result.lag_after == 0
        snap = snapshot_streaming(broker, ["reconciliation-v1", "alerts-v1"])
        assert snap.lag_for("reconciliation-v1") == 0
        assert snap.lag_for("alerts-v1") == 5
        assert sum(snap.groups[1].per_partition_lag.values()) == 5
        assert snap.dlq_records == 1
    finally:
        store.close()


def test_render_streaming_prometheus(tmp_path):
    broker, _ = _seed_broker(tmp_path)
    snap = snapshot_streaming(broker, ["alerts-v1"])
    text = render_streaming_prometheus(snap)
    assert 'pharmaflow_stream_lag_records{group="alerts-v1",topic="pharmaflow.inventory.events.v1"} 5' in text
    assert 'pharmaflow_stream_dlq_records{topic="pharmaflow.inventory.dlq"} 0' in text
    # Per-partition gauges sum to the group total (5 across 4 partitions).
    part_values = [
        int(line.rsplit(" ", 1)[1])
        for line in text.splitlines()
        if line.startswith("pharmaflow_stream_partition_lag_records")
    ]
    assert sum(part_values) == 5


def test_escape_label():
    assert escape_label('a"b\\c\nd') == 'a\\"b\\\\c\\nd'


def test_render_pipeline_prometheus_failed_run():
    result = PipelineResult(logical_date=__import__("datetime").date(2026, 10, 26))
    result.outcomes = [
        TaskOutcome(name="ingest", status="success", attempts=1),
        TaskOutcome(name="transform", status="failed", attempts=3, error="boom"),
        TaskOutcome(name="report", status="skipped", attempts=0),
    ]
    text = render_pipeline_prometheus(result)
    assert "pharmaflow_pipeline_succeeded 0" in text
    assert 'pharmaflow_pipeline_task_success{task="ingest"} 1' in text
    assert 'pharmaflow_pipeline_task_success{task="transform"} 0' in text
    # Skipped is not success: an aborted run must not look green.
    assert 'pharmaflow_pipeline_task_success{task="report"} 0' in text
    assert 'pharmaflow_pipeline_task_attempts{task="transform"} 3' in text
    assert 'pharmaflow_pipeline_task_attempts{task="report"} 0' in text


def test_cli_stream_metrics(tmp_path, capsys):
    _, log_dir = _seed_broker(tmp_path)
    rc = cli_main(["stream-metrics", "--log-dir", str(log_dir),
                   "--group", "alerts-v1"])
    assert rc == 0
    out = capsys.readouterr().out
    assert 'pharmaflow_stream_lag_records{group="alerts-v1",topic="pharmaflow.inventory.events.v1"} 5' in out
    assert 'pharmaflow_stream_dlq_records{topic="pharmaflow.inventory.dlq"} 0' in out


def test_api_metrics_includes_streaming_when_configured(tmp_path):
    _, log_dir = _seed_broker(tmp_path)
    app = create_app(tmp_path / "api.db", stream_log_dir=log_dir,
                     stream_groups=["alerts-v1"])
    with TestClient(app) as client:
        r = client.get("/metrics")
        assert r.status_code == 200
        assert 'pharmaflow_stream_lag_records{group="alerts-v1",topic="pharmaflow.inventory.events.v1"} 5' in r.text


def test_api_metrics_without_stream_config_has_no_stream_gauges(tmp_path):
    with TestClient(create_app(tmp_path / "api.db")) as client:
        r = client.get("/metrics")
        assert r.status_code == 200
        assert "pharmaflow_stream_lag_records" not in r.text
