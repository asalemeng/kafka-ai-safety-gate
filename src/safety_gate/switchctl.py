"""Operator command for the kill switch.

    python -m safety_gate.switchctl disable --reason "injection wave" --by oncall
    python -m safety_gate.switchctl enable  --reason "all clear"      --by oncall
"""
import argparse
import json
import sys

from confluent_kafka import Producer

from safety_gate.config import BOOTSTRAP, CONTROL_TOPIC
from safety_gate.killswitch import DISABLED, ENABLED


def send_command(bootstrap: str, mode: str, reason: str = "", issued_by: str = "") -> None:
    """Write one command to the control topic and wait until the broker confirms it."""
    errors: list = []
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
    producer.produce(
        CONTROL_TOPIC,
        partition=0,
        key="gate",
        value=json.dumps({"mode": mode, "reason": reason, "issued_by": issued_by}).encode("utf-8"),
        on_delivery=lambda err, _msg: errors.append(err) if err else None,
    )
    if producer.flush(15) != 0:
        raise RuntimeError("kill switch command was NOT confirmed by the broker")
    if errors:
        raise RuntimeError(f"kill switch command failed: {errors[0]}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Turn the gate's kill switch on or off.")
    parser.add_argument("action", choices=["disable", "enable"])
    parser.add_argument("--reason", default="")
    parser.add_argument("--by", default="", help="who issued the command")
    args = parser.parse_args()

    mode = DISABLED if args.action == "disable" else ENABLED
    send_command(BOOTSTRAP, mode, args.reason, args.by)
    print(f"kill switch command confirmed: {mode}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
