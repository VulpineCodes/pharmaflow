"""Ingestion counters — session (per-process) and durable (derived) forms.

Two views of the same events exist, and they deliberately differ in one
place:

* ``Metrics`` (the session counter) is incremented by ``ingest_bytes``
  on every ingest call, including duplicate-file re-imports: re-posting
  a file adds its rows to ``rows_in`` again. It is per-process state
  and resets on restart. It is what the foundation tests assert.
* ``durable_metrics(store)`` (M10) is derived at read time from the
  persisted record — the recorded imports plus the audit trail — so a
  fresh process on the same database serves the same numbers. Each
  distinct file is counted once, at its first import; a duplicate-file
  re-import adds to ``files_already_imported`` only, never to
  ``rows_in`` again. The four row counters plus ``imports_total`` are
  exactly the data-quality report totals (see ``data_quality.py``),
  which is asserted in ``tests/test_m10.py``.

No new table or migration is needed: the imports table (M2) and the
audit table already hold everything the durable counters need. The
session counters are kept — ingestion still increments them — because
the in-memory store has no other record, and because the difference
between the two views is itself the honest statement of what a
duplicate re-import did.
"""
from __future__ import annotations
from dataclasses import dataclass, field
@dataclass
class Metrics:
    counters: dict[str, int] = field(default_factory=lambda: {"rows_in":0,"rows_valid":0,"rows_quarantined":0,"duplicates_skipped":0,"imports_total":0,"files_already_imported":0})
    def inc(self, name: str, n: int = 1): self.counters[name] = self.counters.get(name, 0) + n
    def render_prometheus(self) -> str:
        lines=[]
        for k in sorted(self.counters):
            lines.append(f"# HELP pharmaflow_{k} Total {k} observed by PharmaFlow ingestion.")
            lines.append(f"# TYPE pharmaflow_{k} counter")
            lines.append(f"pharmaflow_{k}_total {self.counters[k]}")
        return "\n".join(lines)+"\n"

    @classmethod
    def from_imports(cls, imports, files_already_imported: int = 0) -> "Metrics":
        """Build counters from recorded (non-duplicate) import summaries.

        Duplicate-file re-imports are never recorded as imports (see
        ``ingest_bytes``), so their rows are not in these sums; pass
        their count separately — see ``durable_metrics``.
        """
        m = cls()
        for r in imports:
            m.counters["rows_in"] += r.rows_in
            m.counters["rows_valid"] += r.rows_valid
            m.counters["rows_quarantined"] += r.rows_quarantined
            m.counters["duplicates_skipped"] += r.duplicates_skipped
            m.counters["imports_total"] += 1
        m.counters["files_already_imported"] = files_already_imported
        return m


def durable_metrics(store) -> Metrics:
    """Ingestion counters derived from a store's persisted record.

    Works against any store exposing ``list_imports()`` and
    ``audit.entries`` (both ``PharmaStore`` and ``SqliteStore`` do).
    ``files_already_imported`` counts audit entries for duplicate-file
    re-imports, which are the only trace those attempts leave.
    """
    skipped = sum(
        1 for e in store.audit.entries
        if e.action == "import_skipped_duplicate_file"
    )
    return Metrics.from_imports(store.list_imports(), files_already_imported=skipped)
