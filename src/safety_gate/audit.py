"""Tamper-evident audit log: a SHA-256 hash chain over gate events."""
import hashlib
import json
from dataclasses import asdict, dataclass

GENESIS = "0" * 64   # the "previous hash" of the very first entry


def canonical(event: dict) -> bytes:
    """Stable byte representation: same content always gives the same bytes."""
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def chain_hash(prev_hash: str, seq: int, event: dict) -> str:
    h = hashlib.sha256()
    h.update(prev_hash.encode("ascii"))
    h.update(str(seq).encode("ascii"))
    h.update(canonical(event))
    return h.hexdigest()


@dataclass(frozen=True)
class AuditEntry:
    seq: int
    prev_hash: str
    hash: str
    event: dict

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "AuditEntry":
        return cls(seq=d["seq"], prev_hash=d["prev_hash"], hash=d["hash"], event=d["event"])


def append(last: AuditEntry | None, event: dict) -> AuditEntry:
    """Build the next entry in the chain after `last` (None = empty chain)."""
    seq = 0 if last is None else last.seq + 1
    prev_hash = GENESIS if last is None else last.hash
    return AuditEntry(seq=seq, prev_hash=prev_hash, hash=chain_hash(prev_hash, seq, event), event=event)


def verify(entries: list[AuditEntry]) -> int | None:
    """Return the seq of the first broken entry, or None if the whole chain is intact."""
    expected_prev = GENESIS
    for i, entry in enumerate(entries):
        if entry.seq != i:
            return entry.seq
        if entry.prev_hash != expected_prev:
            return entry.seq
        if entry.hash != chain_hash(entry.prev_hash, entry.seq, entry.event):
            return entry.seq
        expected_prev = entry.hash
    return None
