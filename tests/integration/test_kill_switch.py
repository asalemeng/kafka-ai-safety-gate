import json
import threading
import uuid

import pytest

from safety_gate import gate
from safety_gate.config import CLEAN_TOPIC, DLQ_TOPIC
from safety_gate.killswitch import DISABLED, ENABLED
from safety_gate.switchctl import send_command
from tests.integration.helpers import new_request, send_raw, wait_for

pytestmark = pytest.mark.integration


def start_gate(bootstrap):
    stop = threading.Event()
    thread = threading.Thread(
        target=gate.run,
        kwargs={"bootstrap": bootstrap, "group_id": f"gate-{uuid.uuid4()}", "stop_event": stop},
        daemon=True,
    )
    thread.start()
    return stop, thread


def in_clean(request):
    return lambda p: p.get("request_id") == request["request_id"]


def in_dlq(request):
    return lambda p: isinstance(p.get("request"), dict) and p["request"].get("request_id") == request["request_id"]


def test_switch_blocks_harmless_requests_until_re_enabled(bootstrap):
    stop, thread = start_gate(bootstrap)
    try:
        # 1. Normal operation: a harmless request passes.
        before = new_request("What is the weather in Karlsruhe?")
        send_raw(bootstrap, json.dumps(before).encode())
        assert wait_for(bootstrap, CLEAN_TOPIC, in_clean(before)) == before

        # 2. Operator pulls the switch. The very next request is rejected,
        #    although the validator would have passed it.
        send_command(bootstrap, DISABLED, reason="incident drill", issued_by="test")
        during = new_request("Another perfectly harmless question")
        send_raw(bootstrap, json.dumps(during).encode())

        blocked = wait_for(bootstrap, DLQ_TOPIC, in_dlq(during))
        assert blocked is not None
        assert blocked["reasons"] == ["kill_switch"]
        assert blocked["kill_switch_reason"] == "incident drill"
        assert blocked["request"] == during                       # kept for review, not lost
        assert wait_for(bootstrap, CLEAN_TOPIC, in_clean(during), timeout=8) is None

        # 3. Operator re-enables. Traffic flows again.
        send_command(bootstrap, ENABLED, reason="drill over", issued_by="test")
        after = new_request("Back to normal?")
        send_raw(bootstrap, json.dumps(after).encode())
        assert wait_for(bootstrap, CLEAN_TOPIC, in_clean(after)) == after
    finally:
        stop.set()
        thread.join(timeout=15)


def test_restarted_gate_remembers_it_was_disabled(bootstrap):
    # The switch is pulled while no gate is running...
    send_command(bootstrap, DISABLED, reason="disabled before start", issued_by="test")

    # ...so a gate that starts afterwards must come up disabled.
    stop, thread = start_gate(bootstrap)
    try:
        request = new_request("Is anyone there?")
        send_raw(bootstrap, json.dumps(request).encode())

        blocked = wait_for(bootstrap, DLQ_TOPIC, in_dlq(request))
        assert blocked is not None
        assert blocked["reasons"] == ["kill_switch"]
        assert wait_for(bootstrap, CLEAN_TOPIC, in_clean(request), timeout=8) is None
    finally:
        stop.set()
        thread.join(timeout=15)
