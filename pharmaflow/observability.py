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
* **stream consumption history** — summed counters over completed,
  recorded consume runs (M11, ``stream_consumption_runs`` table), so
  what a group has consumed so far survives restarts. Live lag and
  DLQ size still come from the broker snapshot above; the two views
  answer different questions.
* **dead-letter history** — counts of poison events dead-lettered by
  completed, recorded consume runs (M12, ``dead_letters`` table), so
  an operator can see how many poison events each group has seen
  without reading the broker log. The per-event detail (errors plus
  source coordinates) lives in the same table and is served by the
  API; the gauge here is only the per-group total.
* **dead-letter replays** — counts of corrected re-publishes of
  recorded dead letters (M13, ``dead_letter_replays`` table), so an
  operator can see how many poison events each group has had fixed
  and re-driven. The replay record itself (which dead letter, the
  corrected payload, the broker coordinates of the re-published
  record) is served by the API; the gauge here is only the
  per-group total. Whether a replay was *applied* is answered by
  the next consume run's history (M11), not by this gauge.

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
# Stream consumption history (M11)
# ---------------------------------------------------------------------------

def render_stream_consumption_prometheus(totals_by_group: dict[str, dict[str, int]]) -> str:
    """Render recorded consume-run totals as Prometheus gauges.

    ``totals_by_group`` maps a consumer group to its summed counters
    (``records_read``/``applied``/``duplicates``/``dlq``/``runs``), as
    returned by ``SqliteStore.stream_consumption_totals`` per group.
    These are sums over *completed, recorded* runs only — the durable
    history, not live broker state. Live lag and DLQ size still come
    from the broker snapshot (M8); the two views answer different
    questions ("what has this group consumed so far?" vs "what is
    waiting right now?"). An empty mapping renders nothing, so an app
    with no recorded runs serves no stream-consumption gauges.
    """
    if not totals_by_group:
        return ""
    lines: list[str] = []
    lines.append("# HELP pharmaflow_stream_consumption_runs_total Completed consume runs recorded for a consumer group.")
    lines.append("# TYPE pharmaflow_stream_consumption_runs_total gauge")
    for group in sorted(totals_by_group):
        totals = totals_by_group[group]
        lines.append(
            f"pharmaflow_stream_consumption_runs_total{{{_labels(group=group)}}} {totals['runs']}"
        )
    for metric, key, help_text in [
        ("pharmaflow_stream_consumed_read_total", "records_read",
         "Records read by completed consume runs for a consumer group."),
        ("pharmaflow_stream_consumed_applied_total", "applied",
         "Events applied by completed consume runs for a consumer group."),
        ("pharmaflow_stream_consumed_duplicates_total", "duplicates",
         "Redelivered events recognised as duplicates by completed consume runs."),
        ("pharmaflow_stream_consumed_dlq_total", "dlq",
         "Poison events dead-lettered by completed consume runs."),
    ]:
        lines.append(f"# HELP {metric} {help_text}")
        lines.append(f"# TYPE {metric} gauge")
        for group in sorted(totals_by_group):
            lines.append(
                f"{metric}{{{_labels(group=group)}}} {totals_by_group[group][key]}"
            )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Dead-letter history (M12)
# ---------------------------------------------------------------------------

def render_dead_letters_prometheus(totals_by_group: dict[str, int]) -> str:
    """Render recorded dead letters as a Prometheus gauge.

    ``totals_by_group`` maps a consumer group to its recorded dead
    letters, as returned by
    ``SqliteStore.dead_letter_totals_by_group``. These are counts over
    the durable history — poison events dead-lettered by *completed,
    recorded* consume runs — not live broker state. The live DLQ size
    still comes from the broker snapshot (M8); the two views answer
    different questions ("which poison events have we seen, and why?"
    vs "how many records are waiting in the DLQ right now?"). An
    empty mapping renders nothing, so an app with no recorded dead
    letters serves no dead-letter gauge.
    """
    if not totals_by_group:
        return ""
    lines: list[str] = []
    lines.append("# HELP pharmaflow_dead_letters_total Poison events dead-lettered by completed consume runs for a consumer group.")
    lines.append("# TYPE pharmaflow_dead_letters_total gauge")
    for group in sorted(totals_by_group):
        lines.append(
            f"pharmaflow_dead_letters_total{{{_labels(group=group)}}} {totals_by_group[group]}"
        )
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Dead-letter replays (M13)
# ---------------------------------------------------------------------------

def render_dead_letter_replays_prometheus(totals_by_group: dict[str, int]) -> str:
    """Render recorded dead-letter replays as a Prometheus gauge.

    ``totals_by_group`` maps a consumer group to its recorded
    replays, as returned by
    ``SqliteStore.dead_letter_replay_totals_by_group``. These are
    counts over the durable history — corrected re-publishes of
    recorded dead letters — not live broker state, and not a claim
    that the replayed events were applied: application is decided
    by the next consume run and shows up in the consumption
    history (M11). An empty mapping renders nothing, so an app with
    no recorded replays serves no replay gauge.
    """
    if not totals_by_group:
        return ""
    lines: list[str] = []
    lines.append("# HELP pharmaflow_dead_letter_replays_total Corrected re-publishes of recorded dead letters for a consumer group.")
    lines.append("# TYPE pharmaflow_dead_letter_replays_total gauge")
    for group in sorted(totals_by_group):
        lines.append(
            f"pharmaflow_dead_letter_replays_total{{{_labels(group=group)}}} {totals_by_group[group]}"
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
