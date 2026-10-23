from __future__ import annotations
import argparse, sys
from datetime import date
from pathlib import Path
from .ingestion import PharmaStore, ingest_file
from .alerts import generate_alerts
from .sqlite_store import SqliteStore
from .data_quality import build_report, render_report

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
    p_sp = sub.add_parser("stream-produce", help="Publish events from a JSONL/JSON file to the file-log broker (M4)")
    p_sp.add_argument("--log-dir", required=True, help="Broker log directory (created if missing)")
    p_sp.add_argument("--events", required=True, help="Events file: JSON Lines or a JSON array of event objects")
    p_sp.add_argument("--topic", default=None, help="Topic (default: pharmaflow.inventory.events.v1)")
    p_sc = sub.add_parser("stream-consume", help="Consume events into a SQLite store until caught up (M4)")
    p_sc.add_argument("--log-dir", required=True, help="Broker log directory written by stream-produce")
    p_sc.add_argument("--db", required=True, help="SQLite database file for the operational store")
    p_sc.add_argument("--group", required=True, help="Consumer group name (offsets are tracked per group)")
    p_sc.add_argument("--topic", default=None, help="Topic (default: pharmaflow.inventory.events.v1)")
    args = parser.parse_args(argv)
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
        from .streaming import DEFAULT_TOPIC, FileLogBroker, StreamingConsumer
        broker = FileLogBroker(args.log_dir)
        topic = args.topic or DEFAULT_TOPIC
        print(f"Lag before: {broker.lag(args.group, topic)}")
        store = SqliteStore(args.db)
        try:
            result = StreamingConsumer(broker, store, group=args.group, topic=topic).consume_until_caught_up()
        finally:
            store.close()
        print(f"Consumed: read={result.records_read} applied={result.applied} skipped_dlq={result.dlq} duplicates={result.duplicates}")
        print(f"Lag after: {result.lag_after}")
        return 0
    if args.cmd == "analytics":
        if not Path(args.db).exists():
            print(f"ERROR: database not found: {args.db}"); return 2
        from .analytics import analytics_for_db, render_analytics
        today = date.fromisoformat(args.today) if args.today else None
        print(render_analytics(analytics_for_db(args.db, today=today)), end="")
        return 0
    if args.cmd == "run-daily":
        from .orchestration import render_pipeline_result, run_daily
        logical = date.fromisoformat(args.date) if args.date else None
        result = run_daily(args.db, args.data_dir, logical_date=logical, retries=args.retries, today=logical)
        print(render_pipeline_result(result), end="")
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
            print("Metrics (Prometheus text):")
            print(store.metrics.render_prometheus(), end="")
            print(f"Audit entries: {len(store.audit.entries)}")
            for entry in store.audit.entries: print(f"  {entry.timestamp} {entry.action} {entry.details}")
            return 0
        finally:
            _close_store(store)
    return 0

if __name__ == "__main__": raise SystemExit(main())
