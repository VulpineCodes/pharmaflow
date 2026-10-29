"""Observability for the pipeline and the event stream — M8.

This module turns state PharmaFlow already persists into Prometheus
text, so an operator can watch the two things that actually go wrong
in production without adding a new dependency or a new store:

* **streaming lag and dead letters** — derived from the broker itself
  (committed offsets vs partition ends, and the DLQ topic's size), so
  the numbers are the broker's own bookkeeping, not a second counter
  that can drift from it;
* **daily-pipeline outcome** — rendered from a :class:`PipelineResult`
  (M3), so a run's success, per-task success, and per-task attempts
  are visible as gauges after the run. Since M9 the run is also
  *stored* (``pipeline_runs`` table), so ``/metrics`` renders the
  last stored run — labelled with its logical date — even after the
  serving process restarts.

What this is NOT: a Prometheus server, a Grafana dashboard, or an
alert rule. The ``/metrics`` endpoint (M5) serves the ingestion
counters; with a stream log dir configured it also serves the
streaming gauges below. Dashboards/alerts remain compose declarations
(see ``docs/OBSERVABILITY.md``) until a real Prometheus scrapes them.

Label values are escaped per the Prometheus exposition format
(backslash, double-quote, newline); metric names are fixed strings,
never interpolated from user input.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .streaming import DEFAULT_TOPIC, DLQ_TOPIC

# ---------------------------------------------------------------------------
# Label escaping (Prometheus exposition format)
# ---------------------------------------------------------------------------

def escape_label(value: str) -> str:
    """Escape a label value: backslash, double-quote, newline."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _labels(**pairs: str) -> str:
    return ",".join(f'{k}="{escape_label(v)}"' for k, v in pairs.items())


# ---------------------------------------------------------------------------
# Streaming snapshot
# ---------------------------------------------------------------------------

@dataclass
class GroupLag:
    group: str
    lag: int
    per_partition_lag: dict[int, int] = field(default_factory=dict)


@dataclass
class StreamingSnapshot:
    topic: str
    dlq_topic: str
    dlq_records: int
    groups: list[GroupLag] = field(default_factory=list)

    def lag_for(self, group: str) -> int:
        for g in self.groups:
            if g.group == group:
                return g.lag
        raise KeyError(group)


def snapshot_streaming(broker, groups: list[str],
                       topic: str = DEFAULT_TOPIC,
                       dlq_topic: str = DLQ_TOPIC) -> StreamingSnapshot:
    """Read lag/DLQ state straight from the broker.

    ``broker`` is any :class:`BrokerProtocol` implementation; in practice
    this is ``FileLogBroker`` (the tested path). ``KafkaBroker`` raises
    ``NotImplementedError`` for ``lag`` — deliberately, because its
    group-offset surface is unverified (see ``streaming.py``), so a
    snapshot against it fails loudly instead of reporting zeros.
    """
    snapshot_groups: list[GroupLag] = []
    for group in groups:
        committed = broker.committed_offsets(group, topic)
        per_partition = {
            p: broker.end_offset(topic, p) - committed[p]
            for p in range(broker.num_partitions)
        }
        snapshot_groups.append(GroupLag(
            group=group,
            lag=sum(per_partition.values()),
            per_partition_lag=per_partition,
        ))
    dlq_records = sum(
        broker.end_offset(dlq_topic, p) for p in range(broker.num_partitions)
    )
    return StreamingSnapshot(
        topic=topic, dlq_topic=dlq_topic,
        dlq_records=dlq_records, groups=snapshot_groups,
    )


def render_streaming_prometheus(snapshot: StreamingSnapshot) -> str:
    """Render a snapshot as Prometheus gauge text."""
    lines: list[str] = []
    lines.append("# HELP pharmaflow_stream_lag_records Unconsumed records for a consumer group.")
    lines.append("# TYPE pharmaflow_stream_lag_records gauge")
    for g in snapshot.groups:
        lines.append(
            f"pharmaflow_stream_lag_records{{{_labels(group=g.group, topic=snapshot.topic)}}} {g.lag}"
        )
    lines.append("# HELP pharmaflow_stream_partition_lag_records Unconsumed records for a consumer group in one partition.")
    lines.append("# TYPE pharmaflow_stream_partition_lag_records gauge")
    for g in snapshot.groups:
        for partition in sorted(g.per_partition_lag):
            lines.append(
                f"pharmaflow_stream_partition_lag_records{{{_labels(group=g.group, topic=snapshot.topic, partition=str(partition))}}} {g.per_partition_lag[partition]}"
            )
    lines.append("# HELP pharmaflow_stream_dlq_records Records dead-lettered to the DLQ topic.")
    lines.append("# TYPE pharmaflow_stream_dlq_records gauge")
    lines.append(
        f"pharmaflow_stream_dlq_records{{{_labels(topic=snapshot.dlq_topic)}}} {snapshot.dlq_records}"
    )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Pipeline outcome
# ---------------------------------------------------------------------------

def render_pipeline_prometheus(result, logical_date=None, runs_total=None) -> str:
    """Render a :class:`PipelineResult` (M3) as Prometheus gauges.

    ``result`` is duck-typed (``succeeded`` + ``outcomes`` with
    ``name``/``status``/``attempts``) so tests can build it without
    running a full pipeline; ``run_daily`` results satisfy it directly.
    A skipped task is not a success: its success gauge is 0, which is
    exactly what an operator wants to see when a run aborts mid-way.

    M9: when ``logical_date`` is given (the API rendering a *stored*
    run), every gauge carries a ``logical_date`` label identifying
    which run it describes; without it the output is exactly the M8
    form, so existing consumers/scrapers keep parsing it. When
    ``runs_total`` is given, a ``pharmaflow_pipeline_runs_total``
    gauge records how many runs are stored, so "no gauges" (no runs
    yet) is distinguishable from a scrape failure.
    """
    lines: list[str] = []
    extra = {"logical_date": str(logical_date)} if logical_date is not None else {}
    lines.append("# HELP pharmaflow_pipeline_succeeded Whether the daily pipeline run succeeded (1) or not (0).")
    lines.append("# TYPE pharmaflow_pipeline_succeeded gauge")
    lines.append(
        f"pharmaflow_pipeline_succeeded{{{_labels(**extra)}}} {1 if result.succeeded else 0}"
        if extra else
        f"pharmaflow_pipeline_succeeded {1 if result.succeeded else 0}"
    )
    lines.append("# HELP pharmaflow_pipeline_task_success Whether a pipeline task succeeded (1) or not (0).")
    lines.append("# TYPE pharmaflow_pipeline_task_success gauge")
    for o in result.outcomes:
        lines.append(
            f"pharmaflow_pipeline_task_success{{{_labels(task=o.name, **extra)}}} {1 if o.status == 'success' else 0}"
        )
    lines.append("# HELP pharmaflow_pipeline_task_attempts Attempts used by a pipeline task (1 + retries).")
    lines.append("# TYPE pharmaflow_pipeline_task_attempts gauge")
    for o in result.outcomes:
        lines.append(
            f"pharmaflow_pipeline_task_attempts{{{_labels(task=o.name, **extra)}}} {o.attempts}"
        )
    if runs_total is not None:
        lines.append("# HELP pharmaflow_pipeline_runs_total Pipeline runs recorded in the operational store.")
        lines.append("# TYPE pharmaflow_pipeline_runs_total gauge")
        lines.append(f"pharmaflow_pipeline_runs_total {runs_total}")
    return "\n".join(lines) + "\n"
