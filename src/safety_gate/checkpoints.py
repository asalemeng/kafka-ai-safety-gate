"""Signed checkpoints of the audit chain, kept OUTSIDE Kafka.

The hash chain shows that nobody changed an entry in the middle. It cannot show
that somebody with full access deleted the whole topic and wrote a new, shorter
chain. So every N entries the audit sink writes a small signed note: "entry
number S has hash H". The notes go to a file that should live on storage Kafka
administrators cannot change (write-once storage, another team's bucket).

The verifier then checks the chain against the notes. The sink signs with a
private key; checking needs only the public key.

    python -m safety_gate.checkpoints keygen --private audit.key --public audit.pub
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey, Ed25519PublicKey

from safety_gate.audit import AuditEntry


@dataclass(frozen=True)
class Checkpoint:
    seq: int
    hash: str
    created_at: int          # seconds since 1970, informational
    signature: str = ""      # base64, over signed_bytes()

    def signed_bytes(self) -> bytes:
        return f"audit-checkpoint-v1|{self.seq}|{self.hash}|{self.created_at}".encode("ascii")

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, line: str) -> Checkpoint:
        data = json.loads(line)
        return cls(seq=int(data["seq"]), hash=str(data["hash"]),
                   created_at=int(data["created_at"]), signature=str(data["signature"]))


def generate_keys() -> tuple[bytes, bytes]:
    """Return (private key PEM, public key PEM)."""
    key = Ed25519PrivateKey.generate()
    private = key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                serialization.NoEncryption())
    public = key.public_key().public_bytes(serialization.Encoding.PEM,
                                           serialization.PublicFormat.SubjectPublicKeyInfo)
    return private, public


def load_private_key(path: str | os.PathLike) -> Ed25519PrivateKey:
    key = serialization.load_pem_private_key(Path(path).read_bytes(), password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError(f"{path} is not an Ed25519 private key")
    return key


def load_public_key(path: str | os.PathLike) -> Ed25519PublicKey:
    key = serialization.load_pem_public_key(Path(path).read_bytes())
    if not isinstance(key, Ed25519PublicKey):
        raise ValueError(f"{path} is not an Ed25519 public key")
    return key


def read_checkpoints(path: str | os.PathLike) -> list[Checkpoint]:
    file = Path(path)
    if not file.exists():
        return []
    return [Checkpoint.from_json(line) for line in file.read_text(encoding="utf-8").splitlines() if line.strip()]


class CheckpointWriter:
    """Appends one signed checkpoint for every `every`-th audit entry."""

    def __init__(self, path: str | os.PathLike, private_key: Ed25519PrivateKey, every: int = 100,
                 clock=time.time) -> None:
        if every < 1:
            raise ValueError("checkpoint interval must be at least 1")
        self.path = Path(path)
        self.key = private_key
        self.every = every
        self.clock = clock
        existing = read_checkpoints(self.path)
        self.last_seq = existing[-1].seq if existing else -1

    def offer(self, entry: AuditEntry) -> Checkpoint | None:
        """Write a checkpoint if this entry is due and not covered yet. Safe to call again."""
        if (entry.seq + 1) % self.every != 0 or entry.seq <= self.last_seq:
            return None
        unsigned = Checkpoint(entry.seq, entry.hash, int(self.clock()))
        signature = base64.b64encode(self.key.sign(unsigned.signed_bytes())).decode("ascii")
        checkpoint = Checkpoint(unsigned.seq, unsigned.hash, unsigned.created_at, signature)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8") as file:
            file.write(checkpoint.to_json() + "\n")
            file.flush()
            os.fsync(file.fileno())
        self.last_seq = entry.seq
        return checkpoint

    def catch_up(self, chain: list[AuditEntry]) -> int:
        """Write checkpoints that are missing, e.g. after a crash between commit and file write."""
        return sum(1 for entry in chain if self.offer(entry) is not None)


def check(chain: list[AuditEntry], checkpoints: list[Checkpoint], public_key: Ed25519PublicKey) -> list[str]:
    """Compare a chain with its checkpoints. Returns the problems found (empty = all good)."""
    problems: list[str] = []
    previous = -1
    for cp in checkpoints:
        try:
            public_key.verify(base64.b64decode(cp.signature), cp.signed_bytes())
        except (InvalidSignature, ValueError):
            problems.append(f"checkpoint for seq {cp.seq}: signature is not valid")
            continue
        if cp.seq <= previous:
            problems.append(f"checkpoint for seq {cp.seq}: out of order")
        previous = max(previous, cp.seq)
        if cp.seq >= len(chain):
            problems.append(f"checkpoint for seq {cp.seq}: the chain has only {len(chain)} entries (history was cut)")
        elif chain[cp.seq].hash != cp.hash:
            problems.append(f"checkpoint for seq {cp.seq}: the chain has a different entry here (history was rewritten)")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser(description="Keys for signed audit checkpoints.")
    sub = parser.add_subparsers(dest="command", required=True)
    keygen = sub.add_parser("keygen", help="create a new key pair")
    keygen.add_argument("--private", required=True, help="file for the private key (audit sink only)")
    keygen.add_argument("--public", required=True, help="file for the public key (anyone who verifies)")
    args = parser.parse_args()

    for target in (args.private, args.public):
        if os.path.exists(target):
            print(f"refusing to overwrite {target}", file=sys.stderr)
            return 1
    private, public = generate_keys()
    fd = os.open(args.private, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as file:
        file.write(private)
    Path(args.public).write_bytes(public)
    print(f"private key: {args.private} (keep it with the audit sink only)\npublic key:  {args.public}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
