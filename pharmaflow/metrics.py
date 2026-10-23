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
