"""Verify the audit chain. Exit code 0 = intact, 1 = broken.

    python -m safety_gate.verify
    python -m safety_gate.verify --checkpoints checkpoints.jsonl --public-key audit.pub
"""
import argparse
import sys

from safety_gate.audit import verify
from safety_gate.audit_sink import read_chain
from safety_gate.checkpoints import check, load_public_key, read_checkpoints
from safety_gate.config import Settings


def main(argv: list[str] | None = None, settings: Settings | None = None) -> int:
    parser = argparse.ArgumentParser(description="Check the audit chain for tampering.")
    parser.add_argument("--checkpoints", help="file with signed checkpoints kept outside Kafka")
    parser.add_argument("--public-key", help="public key the checkpoints were signed for")
    args = parser.parse_args(argv)
    if bool(args.checkpoints) != bool(args.public_key):
        parser.error("--checkpoints and --public-key must be given together")

    chain = read_chain(settings or Settings.from_env())
    broken = verify(chain)
    if broken is not None:
        print(f"AUDIT CHAIN BROKEN: first break at seq {broken} ({len(chain)} entries read)")
        return 1

    if args.checkpoints:
        checkpoints = read_checkpoints(args.checkpoints)
        problems = check(chain, checkpoints, load_public_key(args.public_key))
        if problems:
            print(f"AUDIT CHAIN DOES NOT MATCH ITS CHECKPOINTS ({len(problems)} problem(s)):")
            for problem in problems:
                print(f"  - {problem}")
            return 1
        print(f"audit chain intact: {len(chain)} entries, {len(checkpoints)} signed checkpoints match")
        return 0

    print(f"audit chain intact: {len(chain)} entries")
    return 0


if __name__ == "__main__":
    sys.exit(main())
