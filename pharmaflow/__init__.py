"""PharmaFlow — inventory/operations data platform for small pharmacies (synthetic data only)."""
from .models import InventoryRecord, SalesRecord, ValidationError, QuarantineEntry, ImportResult
from .ingestion import PharmaStore, ingest_file, ingest_bytes, file_hash_bytes, row_hash
from .alerts import generate_alerts, Alert
from .reconciliation import reconcile, reconcile_store, Discrepancy
from .metrics import Metrics, durable_metrics
from .sqlite_store import SqliteStore
from .store import StoreProtocol
from .data_quality import DataQualityReport, ImportQuality, build_report, render_report
from .observability import render_dead_letter_replays_prometheus, render_dead_letters_prometheus, render_pipeline_prometheus, render_replay_outcomes_prometheus, render_stream_consumption_prometheus, render_streaming_prometheus, snapshot_streaming
__all__ = ["InventoryRecord","SalesRecord","ValidationError","QuarantineEntry","ImportResult","PharmaStore","SqliteStore","StoreProtocol","ingest_file","ingest_bytes","file_hash_bytes","row_hash","generate_alerts","Alert","reconcile","reconcile_store","Discrepancy","Metrics","durable_metrics","DataQualityReport","ImportQuality","build_report","render_report","snapshot_streaming","render_streaming_prometheus","render_pipeline_prometheus","render_stream_consumption_prometheus","render_dead_letters_prometheus","render_dead_letter_replays_prometheus","render_replay_outcomes_prometheus"]
__version__ = "0.2.0"
