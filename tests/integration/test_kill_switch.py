import time

import pytest

from safety_gate import gate, model_stub
from safety_gate.killswitch import DISABLED, ENABLED
from safety_gate.switchctl import send_command
from tests.integration.helpers import Running, in_clean, in_dlq, new_request, send_request, wait_for

pytestmark = pytest.mark.integration


def test_switch_blocks_harmless_requests_until_re_enabled(make_settings):
    settings = make_settings()                                    # strict mode: checked before every batch
    running = Running(gate.run, settings)
    try:
        # 1. Normal operation: a harmless request passes.
        before = new_request("What is the weather in Karlsruhe?")
        send_request(settings, before)
        assert wait_for(settings, settings.topics.clean, in_clean(before)) == before

        # 2. Operator pulls the switch. The very next request is rejected,
        #    although the validator would have passed it.
        send_command(settings, DISABLED, reason="incident drill", issued_by="test")
        during = new_request("Another perfectly harmless question")
        send_request(settings, during)

        blocked = wait_for(settings, settings.topics.dlq, in_dlq(during))
        assert blocked is not None
        assert blocked["reasons"] == ["kill_switch"]
        assert blocked["kill_switch_reason"] == "incident drill"
        assert blocked["request"] == during                       # kept for review, not lost
        assert wait_for(settings, settings.topics.clean, in_clean(during), timeout=8) is None

        # 3. Operator re-enables. Traffic flows again.
        send_command(settings, ENABLED, reason="drill over", issued_by="test")
        after = new_request("Back to normal?")
        send_request(settings, after)
        assert wait_for(settings, settings.topics.clean, in_clean(after)) == after
        running.assert_alive()
    finally:
        running.shutdown()


def test_restarted_gate_remembers_it_was_disabled(make_settings):
    settings = make_settings()
    # The switch is pulled while no gate is running...
    send_command(settings, DISABLED, reason="disabled before start", issued_by="test")

    # ...so a gate that starts afterwards must come up disabled.
    running = Running(gate.run, settings)
    try:
        request = new_request("Is anyone there?")
        send_request(settings, request)

        blocked = wait_for(settings, settings.topics.dlq, in_dlq(request))
        assert blocked is not None
        assert blocked["reasons"] == ["kill_switch"]
        assert wait_for(settings, settings.topics.clean, in_clean(request), timeout=8) is None
    finally:
        running.shutdown()


# --- task 8: interval mode ---------------------------------------------------------
def test_interval_mode_reacts_within_seconds_and_then_lets_nothing_through(make_settings):
    settings = make_settings(switch_check_s=0.2)                  # the default: re-read every 200 ms
    running = Running(gate.run, settings)
    try:
        warm_up = new_request("warm up")
        send_request(settings, warm_up)
        assert wait_for(settings, settings.topics.clean, in_clean(warm_up)) == warm_up

        send_command(settings, DISABLED, reason="interval test", issued_by="test")
        time.sleep(1.0)                                           # five times the interval: generous

        late = [new_request(f"sent after the switch {i}") for i in range(5)]
        for request in late:
            send_request(settings, request)

        for request in late:
            assert wait_for(settings, settings.topics.dlq, in_dlq(request)) is not None
        ids = {r["request_id"] for r in late}
        assert wait_for(settings, settings.topics.clean, lambda p: p.get("request_id") in ids, timeout=8) is None
    finally:
        running.shutdown()


# --- task 19: the model service honours the switch too ---------------------------------
def test_model_service_refuses_requests_that_passed_the_gate_before_the_switch(make_settings):
    settings = make_settings()
    calls: list[str] = []

    def model(request: dict) -> str:
        calls.append(request["request_id"])
        return "an answer"

    gate_running = Running(gate.run, settings)
    service = None
    try:
        # The request passes the gate and waits in the clean topic...
        request = new_request("A harmless question that is already on its way")
        send_request(settings, request)
        assert wait_for(settings, settings.topics.clean, in_clean(request)) == request

        # ...then the operator pulls the switch, and only then the model service gets to it.
        send_command(settings, DISABLED, reason="model misbehaving", issued_by="test")
        service = Running(model_stub.run, settings, call_model=model)

        response = wait_for(settings, settings.topics.responses, in_clean(request))
        assert response is not None
        assert response["status"] == "REJECTED" and response["reason"] == "kill_switch"
        assert calls == []                                        # the model was never called

        send_command(settings, ENABLED, reason="fixed", issued_by="test")
        after = new_request("And now?")
        send_request(settings, after)
        answered = wait_for(settings, settings.topics.responses, in_clean(after))
        assert answered is not None and answered["status"] == "OK"
        assert calls == [after["request_id"]]
    finally:
        if service is not None:
            service.shutdown()
        gate_running.shutdown()
