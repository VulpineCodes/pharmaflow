from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
@dataclass
class AuditEntry:
    action: str
    source_file: str
    file_hash: str
    details: str
    timestamp: str = ""
    def __post_init__(self):
        if not self.timestamp: self.timestamp = datetime.now(timezone.utc).isoformat()
@dataclass
class AuditLog:
    entries: list[AuditEntry] = field(default_factory=list)
    def append(self, action, source_file, file_hash, details):
        e=AuditEntry(action, source_file, file_hash, details); self.entries.append(e); return e
    # append-only: no delete/update methods by design
