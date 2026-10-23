"""Data-quality report — per-import summary built from persisted state.

Works against any store that implements ``list_imports()`` and exposes
``quarantine.entries`` (both the in-memory ``PharmaStore`` and the durable
``SqliteStore`` do). Rendered by the CLI ``report`` subcommand:

    python -m pharmaflow report --db pharmaflow.db
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .models import ImportResult, QuarantineEntry


@dataclass
class ImportQuality:
    source_file: str
    file_hash: str
    kind: str
    rows_in: int
    rows_valid: int
    rows_quarantined: int
    duplicates_skipped: int

    @property
    def duplicate_rate(self) -> float:
        return (self.duplicates_skipped / self.rows_in) if self.rows_in else 0.0

    @property
    def valid_rate(self) -> float:
        return (self.rows_valid / self.rows_in) if self.rows_in else 0.0

    @property
    def quarantine_rate(self) -> float:
        return (self.rows_quarantined / self.rows_in) if self.rows_in else 0.0


@dataclass
class DataQualityReport:
    imports: list[ImportQuality] = field(default_factory=list)
    # breakdown by ValidationError.code: required / type / business_rule / ...
    quarantine_reason_breakdown: dict[str, int] = field(default_factory=dict)
    # breakdown by error message, for human inspection
    quarantine_message_breakdown: dict[str, int] = field(default_factory=dict)

    @property
    def total_rows_in(self) -> int:
        return sum(i.rows_in for i in self.imports)

    @property
    def total_rows_valid(self) -> int:
        return sum(i.rows_valid for i in self.imports)

    @property
    def total_rows_quarantined(self) -> int:
        return sum(i.rows_quarantined for i in self.imports)

    @property
    def total_duplicates_skipped(self) -> int:
        return sum(i.duplicates_skipped for i in self.imports)

    @property
    def overall_duplicate_rate(self) -> float:
        return (self.total_duplicates_skipped / self.total_rows_in) if self.total_rows_in else 0.0

    def as_dict(self) -> dict:
        return {
            "imports": [
                {
                    "source_file": i.source_file,
                    "file_hash": i.file_hash,
                    "kind": i.kind,
                    "rows_in": i.rows_in,
                    "rows_valid": i.rows_valid,
                    "rows_quarantined": i.rows_quarantined,
                    "duplicates_skipped": i.duplicates_skipped,
                    "duplicate_rate": round(i.duplicate_rate, 4),
                    "valid_rate": round(i.valid_rate, 4),
                    "quarantine_rate": round(i.quarantine_rate, 4),
                }
                for i in self.imports
            ],
            "totals": {
                "rows_in": self.total_rows_in,
                "rows_valid": self.total_rows_valid,
                "rows_quarantined": self.total_rows_quarantined,
                "duplicates_skipped": self.total_duplicates_skipped,
                "duplicate_rate": round(self.overall_duplicate_rate, 4),
            },
            "quarantine_reason_breakdown": dict(self.quarantine_reason_breakdown),
            "quarantine_message_breakdown": dict(self.quarantine_message_breakdown),
        }


def build_report_from_imports(
    imports: list[ImportResult],
    quarantine_entries: list[QuarantineEntry],
) -> DataQualityReport:
    report = DataQualityReport(
        imports=[
            ImportQuality(
                source_file=r.source_file, file_hash=r.file_hash, kind=r.kind,
                rows_in=r.rows_in, rows_valid=r.rows_valid,
                rows_quarantined=r.rows_quarantined,
                duplicates_skipped=r.duplicates_skipped,
            )
            for r in imports
        ]
    )
    for entry in quarantine_entries:
        for err in entry.errors:
            report.quarantine_reason_breakdown[err.code] = (
                report.quarantine_reason_breakdown.get(err.code, 0) + 1
            )
            report.quarantine_message_breakdown[err.message] = (
                report.quarantine_message_breakdown.get(err.message, 0) + 1
            )
    return report


def build_report(store) -> DataQualityReport:
    """Build a report from any StoreProtocol store (memory or SQLite)."""
    return build_report_from_imports(store.list_imports(), list(store.quarantine.entries))


def render_report(report: DataQualityReport) -> str:
    lines = ["PharmaFlow data-quality report", "=" * 60]
    if not report.imports:
        lines.append("No imports recorded.")
    for i in report.imports:
        lines.append(
            f"{i.kind} import from {i.source_file}: rows_in={i.rows_in} "
            f"valid={i.rows_valid} quarantined={i.rows_quarantined} "
            f"duplicates_skipped={i.duplicates_skipped} "
            f"duplicate_rate={i.duplicate_rate:.2%} valid_rate={i.valid_rate:.2%} "
            f"quarantine_rate={i.quarantine_rate:.2%} file_hash={i.file_hash[:12]}..."
        )
    lines.append("-" * 60)
    lines.append(
        f"Totals: rows_in={report.total_rows_in} valid={report.total_rows_valid} "
        f"quarantined={report.total_rows_quarantined} "
        f"duplicates_skipped={report.total_duplicates_skipped} "
        f"duplicate_rate={report.overall_duplicate_rate:.2%}"
    )
    lines.append("Quarantine reason breakdown (by code):")
    if report.quarantine_reason_breakdown:
        for code in sorted(report.quarantine_reason_breakdown):
            lines.append(f"  {code}: {report.quarantine_reason_breakdown[code]}")
    else:
        lines.append("  (none)")
    if report.quarantine_message_breakdown:
        lines.append("Quarantine messages:")
        for msg in sorted(report.quarantine_message_breakdown):
            lines.append(f"  {msg}: {report.quarantine_message_breakdown[msg]}")
    return "\n".join(lines) + "\n"
