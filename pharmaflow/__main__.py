from __future__ import annotations
import argparse, sys
from datetime import date
from pathlib import Path
from .ingestion import PharmaStore, ingest_file
from .alerts import generate_alerts
from .sqlite_store import SqliteStore
from .data_quality import build_report, render_report
from .metrics import durable_metrics

def _open_store(db_path):
    """Durable SQLite store when --db is given, else in-memory (volatile)."""
    if db_path:
        return SqliteStore(db_path)
    return PharmaStore()

def _close_store(store):
    close = getattr(store, "close", None)
    if callable(close):
        close()

def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="pharmaflow", description="PharmaFlow ingestion CLI")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_ing = sub.add_parser("ingest", help="Ingest one or more CSV files end-to-end")
    p_ing.add_argument("files", nargs="+", help="CSV files (inventory or sales, auto-detected)")
    p_ing.add_argument("--kind", choices=["inventory","sales"], default=None)
    p_ing.add_argument("--db", default=None, help="SQLite database file for durable persistence (data survives restarts; re-ingesting the same file is a no-op)")
    p_ing.add_argument("--today", default=None, help="Override today (YYYY-MM-DD) for expiry alerts, for reproducible demos")
    p_rep = sub.add_parser("report", help="Data-quality report for imports recorded in a database")
    p_rep.add_argument("--db", required=True, help="SQLite database file written by a previous `ingest --db ...` run")
    p_an = sub.add_parser("analytics", help="DuckDB analytics over a SQLite database (M3)")
    p_an.add_argument("--db", required=True, help="SQLite database file written by a previous ingest")
    p_an.add_argument("--today", default=None, help="Reference date YYYY-MM-DD for expiry risk (default: today)")
    p_run = sub.add_parser("run-daily", help="Run the daily pipeline locally: ingest -> transform -> report (M3)")
    p_run.add_argument("--db", required=True, help="SQLite database file for the operational store")
    p_run.add_argument("--data-dir", default="sample_data", help="Directory containing inventory.csv / sales.csv")
    p_run.add_argument("--date", default=None, help="Logical date YYYY-MM-DD (default: today)")
    p_run.add_argument("--retries", type=int, default=2, help="Retries per task after the first attempt (default: 2)")
    p_pr = sub.add_parser("pipeline-runs", help="List recorded daily-pipeline runs, newest first (M9)")
    p_pr.add_argument("--db", required=True, help="SQLite database file written by a previous `run-daily` run")
    p_pr.add_argument("--latest", action="store_true", help="Show only the most recent run")
    p_pr.add_argument("--limit", type=int, default=20, help="Runs to show (default: 20, max: 100)")
    p_pr.add_argument("--json", action="store_true", help="Print runs as JSON instead of human-readable lines")
    p_sp = sub.add_parser("stream-produce", help="Publish events from a JSONL/JSON file to the file-log broker (M4)")
    p_sp.add_argument("--log-dir", required=True, help="Broker log directory (created if missing)")
    p_sp.add_argument("--events", required=True, help="Events file: JSON Lines or a JSON array of event objects")
    p_sp.add_argument("--topic", default=None, help="Topic (default: pharmaflow.inventory.events.v1)")
    p_sc = sub.add_parser("stream-consume", help="Consume events into a SQLite store until caught up (M4; completed runs are recorded, M11)")
    p_sc.add_argument("--log-dir", required=True, help="Broker log directory written by stream-produce")
    p_sc.add_argument("--db", required=True, help="SQLite database file for the operational store")
    p_sc.add_argument("--group", required=True, help="Consumer group name (offsets are tracked per group)")
    p_sc.add_argument("--topic", default=None, help="Topic (default: pharmaflow.inventory.events.v1)")
    p_scr = sub.add_parser("stream-consumption", help="List recorded stream-consume runs, newest first (M11)")
    p_scr.add_argument("--db", required=True, help="SQLite database file written by a previous `stream-consume` run")
    p_scr.add_argument("--group", default=None, help="Show only runs for this consumer group")
    p_scr.add_argument("--latest", action="store_true", help="Show only the most recent run")
    p_scr.add_argument("--limit", type=int, default=20, help="Runs to show (default: 20, max: 100)")
    p_scr.add_argument("--json", action="store_true", help="Print runs as JSON instead of human-readable lines")
    p_dl = sub.add_parser("dead-letters", help="List recorded dead letters, newest first (M12)")
    p_dl.add_argument("--db", required=True, help="SQLite database file written by a previous `stream-consume` run")
    p_dl.add_argument("--group", default=None, help="Show only dead letters for this consumer group")
    p_dl.add_argument("--latest", action="store_true", help="Show only the most recent dead letter")
    p_dl.add_argument("--limit", type=int, default=20, help="Dead letters to show (default: 20, max: 100)")
    p_dl.add_argument("--json", action="store_true", help="Print dead letters as JSON instead of human-readable lines")
    p_dlr = sub.add_parser("dead-letter-replay", help="Re-publish a recorded dead letter with corrections (M13)")
    p_dlr.add_argument("--db", required=True, help="SQLite database file holding the recorded dead letter")
    p_dlr.add_argument("--log-dir", required=True, help="Broker log directory holding the original record")
    p_dlr.add_argument("--id", type=int, required=True, help="Dead-letter record id (see `dead-letters`)")
    p_dlr.add_argument("--corrections", default=None, help="JSON object of field overrides applied to the original payload (omit to re-publish unchanged)")
    p_dlr.add_argument("--group", default=None, help="Consumer group to record the replay for (default: the dead letter's group)")
    p_dlrs = sub.add_parser("dead-letter-replays", help="List recorded dead-letter replays, newest first (M13)")
    p_dlrs.add_argument("--db", required=True, help="SQLite database file written by a previous `dead-letter-replay` run")
    p_dlrs.add_argument("--dead-letter-id", type=int, default=None, help="Show only replays of this dead letter")
    p_dlrs.add_argument("--group", default=None, help="Show only replays for this consumer group")
    p_dlrs.add_argument("--latest", action="store_true", help="Show only the most recent replay")
    p_dlrs.add_argument("--limit", type=int, default=20, help="Replays to show (default: 20, max: 100)")
    p_dlrs.add_argument("--json", action="store_true", help="Print replays as JSON instead of human-readable lines")
    p_ro = sub.add_parser("replay-outcomes", help="List recorded replay outcomes, newest first (M14)")
    p_ro.add_argument("--db", required=True, help="SQLite database file written by a previous `stream-consume` run")
    p_ro.add_argument("--replay-id", type=int, default=None, help="Show only outcomes for this replay")
    p_ro.add_argument("--group", default=None, help="Show only outcomes for this consumer group")
    p_ro.add_argument("--outcome", default=None, choices=["applied", "duplicate", "dead_lettered"], help="Show only outcomes with this result")
    p_ro.add_argument("--latest", action="store_true", help="Show only the most recent outcome")
    p_ro.add_argument("--limit", type=int, default=20, help="Outcomes to show (default: 20, max: 100)")
    p_ro.add_argument("--json", action="store_true", help="Print outcomes as JSON instead of human-readable lines")
    p_sm = sub.add_parser("stream-metrics", help="Prometheus text for stream lag + DLQ size, read from the broker (M8)")
    p_sm.add_argument("--log-dir", required=True, help="Broker log directory written by stream-produce")
    p_sm.add_argument("--group", action="append", default=[], help="Consumer group to report lag for (repeatable; omit for DLQ only)")
    p_sm.add_argument("--topic", default=None, help="Topic (default: pharmaflow.inventory.events.v1)")
    args = parser.parse_args(argv)
    if args.cmd == "stream-metrics":
        from .observability import render_streaming_prometheus, snapshot_streaming
        from .streaming import DEFAULT_TOPIC, FileLogBroker
        broker = FileLogBroker(args.log_dir)
        snap = snapshot_streaming(broker, args.group, topic=args.topic or DEFAULT_TOPIC)
        print(render_streaming_prometheus(snap), end="")
        return 0
    if args.cmd == "stream-produce":
        from .streaming import DEFAULT_TOPIC, EventProducer, FileLogBroker, load_events_file
        if not Path(args.events).exists():
            print(f"ERROR: events file not found: {args.events}"); return 2
        broker = FileLogBroker(args.log_dir)
        producer = EventProducer(broker, topic=args.topic or DEFAULT_TOPIC)
        events = load_events_file(args.events)
        for value in events:
            partition, offset = producer.publish_raw(value)
            print(f"  published key={value.get('sku') or value.get('event_id') or '?'} partition={partition} offset={offset} event_id={value.get('event_id')} type={value.get('event_type')}")
        print(f"Produced {len(events)} event(s) to topic {args.topic or DEFAULT_TOPIC} (partitions={broker.num_partitions})")
        return 0
    if args.cmd == "stream-consume":
        from .streaming import DEFAULT_TOPIC, FileLogBroker, consume_once
        broker = FileLogBroker(args.log_dir)
        topic = args.topic or DEFAULT_TOPIC
        print(f"Lag before: {broker.lag(args.group, topic)}")
        result = consume_once(args.log_dir, args.db, group=args.group, topic=topic)
        print(f"Consumed: read={result.records_read} applied={result.applied} skipped_dlq={result.dlq} duplicates={result.duplicates}")
        print(f"Lag after: {result.lag_after}")
        if result.record_id is not None:
            print(f"Recorded as stream consumption #{result.record_id} (group {args.group})")
        if result.dead_letter_ids:
            print(f"Recorded {len(result.dead_letter_ids)} dead letter(s) (group {args.group})")
        if result.replay_outcome_ids:
            print(f"Recorded {len(result.replay_outcome_ids)} replay outcome(s) (group {args.group})")
        return 0
    if args.cmd == "replay-outcomes":
        import json as _json
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        store = SqliteStore(args.db)
        try:
            if args.latest:
                latest = store.latest_replay_outcome(
                    replay_id=args.replay_id, group=args.group)
                outcomes = [latest] if latest is not None else []
            else:
                outcomes = store.list_replay_outcomes(
                    replay_id=args.replay_id, group=args.group,
                    outcome=args.outcome, limit=args.limit)
        finally:
            store.close()
        if args.json:
            print(_json.dumps([
                {"outcome_id": o.outcome_id, "replay_id": o.replay_id,
                 "dead_letter_id": o.dead_letter_id, "group": o.group,
                 "event_id": o.event_id, "outcome": o.outcome,
                 "new_dead_letter_id": o.new_dead_letter_id,
                 "consumption_run_id": o.consumption_run_id,
                 "recorded_at": o.recorded_at}
                for o in outcomes
            ], indent=2, default=str))
            return 0
        if not outcomes:
            print("No replay outcomes recorded.")
            return 0
        for o in outcomes:
            extra = (f" new_dead_letter=#{o.new_dead_letter_id}"
                     if o.new_dead_letter_id is not None else "")
            print(f"#{o.outcome_id} replay=#{o.replay_id} dead_letter=#{o.dead_letter_id} "
                  f"group={o.group} event_id={o.event_id or '(none)'} outcome={o.outcome} "
                  f"consumption_run=#{o.consumption_run_id}{extra} recorded_at={o.recorded_at}")
        return 0
    if args.cmd == "dead-letter-replay":
        import json as _json
        from pharmaflow.streaming import EventValidationError, replay_dead_letter
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        corrections = None
        if args.corrections:
            try:
                corrections = _json.loads(args.corrections)
            except _json.JSONDecodeError as exc:
                print(f"ERROR: --corrections is not valid JSON: {exc}"); return 2
            if not isinstance(corrections, dict):
                print("ERROR: --corrections must be a JSON object"); return 2
        try:
            replay = replay_dead_letter(
                args.log_dir, args.db, args.id,
                corrections=corrections, group=args.group)
        except EventValidationError as exc:
            print("ERROR: corrected event still fails validation:")
            for err in exc.errors:
                print(f"  - {err}")
            return 2
        except ValueError as exc:
            print(f"ERROR: {exc}"); return 2
        print(f"Replayed dead letter #{replay.dead_letter_id} "
              f"(event_id={replay.event_id}) as replay #{replay.replay_id}")
        print(f"  published to {replay.topic} partition={replay.partition} "
              f"offset={replay.offset} group={replay.group}")
        print("  outcome is decided by the next stream-consume run "
              "(applied / duplicate / dead-lettered again)")
        return 0
    if args.cmd == "dead-letter-replays":
        import json as _json
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        store = SqliteStore(args.db)
        try:
            if args.latest:
                latest = store.latest_dead_letter_replay(
                    dead_letter_id=args.dead_letter_id, group=args.group)
                replays = [latest] if latest is not None else []
            else:
                replays = store.list_dead_letter_replays(
                    dead_letter_id=args.dead_letter_id,
                    group=args.group, limit=args.limit)
        finally:
            store.close()
        if args.json:
            print(_json.dumps([
                {"replay_id": r.replay_id, "dead_letter_id": r.dead_letter_id,
                 "group": r.group, "event_id": r.event_id,
                 "topic": r.topic, "partition": r.partition,
                 "offset": r.offset, "corrected_event": r.corrected_event,
                 "replayed_at": r.replayed_at}
                for r in replays
            ], indent=2, default=str))
            return 0
        if not replays:
            print("No dead-letter replays recorded.")
            return 0
        for r in replays:
            print(f"#{r.replay_id} dead_letter=#{r.dead_letter_id} group={r.group} "
                  f"event_id={r.event_id or '(none)'} replayed_at={r.replayed_at} "
                  f"{r.topic} partition={r.partition} offset={r.offset}")
        return 0
    if args.cmd == "dead-letters":
        import json as _json
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        store = SqliteStore(args.db)
        try:
            if args.latest:
                latest = store.latest_dead_letter(group=args.group)
                letters = [latest] if latest is not None else []
            else:
                letters = store.list_dead_letters(group=args.group, limit=args.limit)
        finally:
            store.close()
        if args.json:
            print(_json.dumps([
                {"record_id": r.record_id, "group": r.group,
                 "event_id": r.event_id, "errors": r.errors,
                 "topic": r.topic, "partition": r.partition,
                 "offset": r.offset, "recorded_at": r.recorded_at}
                for r in letters
            ], indent=2, default=str))
            return 0
        if not letters:
            print("No dead letters recorded.")
            return 0
        for r in letters:
            errors = "; ".join(r.errors)
            print(f"#{r.record_id} group={r.group} event_id={r.event_id or '(none)'} "
                  f"recorded_at={r.recorded_at} {r.topic} partition={r.partition} offset={r.offset} "
                  f"errors: {errors}")
        return 0
    if args.cmd == "stream-consumption":
        import json as _json
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        store = SqliteStore(args.db)
        try:
            if args.latest:
                latest = store.latest_stream_consumption(group=args.group)
                runs = [latest] if latest is not None else []
            else:
                runs = store.list_stream_consumption(group=args.group, limit=args.limit)
        finally:
            store.close()
        if args.json:
            print(_json.dumps([
                {"run_id": r.run_id, "group": r.group, "topic": r.topic,
                 "records_read": r.records_read, "applied": r.applied,
                 "duplicates": r.duplicates, "dlq": r.dlq,
                 "lag_before": r.lag_before, "lag_after": r.lag_after,
                 "recorded_at": r.recorded_at}
                for r in runs
            ], indent=2, default=str))
            return 0
        if not runs:
            print("No stream consumption runs recorded.")
            return 0
        for r in runs:
            print(f"#{r.run_id} group={r.group} topic={r.topic} recorded_at={r.recorded_at} "
                  f"read={r.records_read} applied={r.applied} duplicates={r.duplicates} "
                  f"dlq={r.dlq} lag_before={r.lag_before} lag_after={r.lag_after}")
        return 0
    if args.cmd == "analytics":
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        from .analytics import analytics_for_db, render_analytics
        today = date.fromisoformat(args.today) if args.today else None
        print(render_analytics(analytics_for_db(args.db, today=today)), end="")
        return 0
    if args.cmd == "pipeline-runs":
        import json as _json
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        store = SqliteStore(args.db)
        try:
            if args.latest:
                latest = store.latest_pipeline_run()
                runs = [latest] if latest is not None else []
            else:
                runs = store.list_pipeline_runs(limit=args.limit)
        finally:
            store.close()
        if args.json:
            print(_json.dumps([
                {"run_id": r.run_id, "logical_date": r.logical_date.isoformat(),
                 "recorded_at": r.recorded_at, "succeeded": r.succeeded,
                 "failed_task": r.failed_task, "result": r.result_dict}
                for r in runs
            ], indent=2, default=str))
            return 0
        if not runs:
            print("No pipeline runs recorded.")
            return 0
        for r in runs:
            tasks = " ".join(
                f"{o['name']}:{o['status']}({o['attempts']})"
                for o in r.result_dict.get("outcomes", [])
            )
            print(f"#{r.run_id} logical_date={r.logical_date} recorded_at={r.recorded_at} "
                  f"succeeded={r.succeeded} failed_task={r.failed_task} {tasks}")
        return 0
    if args.cmd == "run-daily":
        from .orchestration import render_pipeline_result, run_daily
        logical = date.fromisoformat(args.date) if args.date else None
        result = run_daily(args.db, args.data_dir, logical_date=logical, retries=args.retries, today=logical)
        print(render_pipeline_result(result), end="")
        print(f"Recorded as pipeline run #{result.run_id} (logical date {result.logical_date})")
        return 0 if result.succeeded else 1
    if args.cmd == "report":
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        store = SqliteStore(args.db)
        try:
            print(render_report(build_report(store)), end="")
        finally:
            store.close()
        return 0
    if args.cmd == "ingest":
        store = _open_store(args.db)
        try:
            today = date.fromisoformat(args.today) if args.today else date.today()
            print("PharmaFlow ingestion report")
            if args.db:
                print(f"Store: SQLite (durable) at {args.db}")
            print("=" * 60)
            for f in args.files:
                if not Path(f).exists():
                    print(f"ERROR: file not found: {f}"); return 2
                result = ingest_file(f, store, kind=args.kind)
                print(result.summary())
            print("-" * 60)
            print(f"Inventory SKUs loaded: {len(store.inventory)}")
            print(f"Sales records loaded: {len(store.sales)}")
            print(f"Quarantined rows: {len(store.quarantine)}")
            for e in store.quarantine.entries:
                reasons = "; ".join(err.message for err in e.errors)
                print(f"  quarantine row {e.row_number} ({e.source_file}): {reasons}")
            alerts = generate_alerts(store.inventory, today=today)
            print(f"Alerts: {len(alerts)}")
            for a in alerts: print(f"  [{a.kind}] {a.message}")
            print("Metrics (Prometheus text, durable — derived from the recorded imports):")
            print(durable_metrics(store).render_prometheus(), end="")
            print(f"Audit entries: {len(store.audit.entries)}")
            for entry in store.audit.entries: print(f"  {entry.timestamp} {entry.action} {entry.details}")
            return 0
        finally:
            _close_store(store)
    return 0

if __name__ == "__main__": raise SystemExit(main())
