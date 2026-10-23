from __future__ import annotations
from dataclasses import dataclass, field
from .models import QuarantineEntry
@dataclass
class QuarantineStore:
    entries: list[QuarantineEntry] = field(default_factory=list)
    def add(self, entry: QuarantineEntry): self.entries.append(entry)
    def __len__(self): return len(self.entries)
    def reasons_summary(self) -> dict[str,int]:
        out={}
        for e in self.entries:
            for err in e.errors: out[err.code]=out.get(err.code,0)+1
        return out
