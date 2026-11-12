"""Verify the audit chain. Exit code 0 = intact, 1 = broken."""
import sys

from safety_gate.audit import verify
from safety_gate.audit_sink import read_chain


def main() -> int:
    chain = read_chain()
    broken = verify(chain)
    if broken is None:
        print(f"audit chain intact: {len(chain)} entries")
        return 0
    print(f"AUDIT CHAIN BROKEN: first break at seq {broken} ({len(chain)} entries read)")
    return 1


if __name__ == "__main__":
    sys.exit(main())
