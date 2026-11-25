"""Tasks 10 and 17 on a real broker: poison messages, restart after failure, duplicates."""
import json
import threading

import pytest

from safety_gate import gate
from safety_gate.engine import Engine
from tests.integration.helpers import (
    Running,
    count_visible,
    in_clean,
    in_dlq,
    new_request,
    send_raw,
    send_request,
    wait_for,
)

pytestmark = pytest.mark.integration


class ExplodingEngine(Engine):
    """Fails for one specific prompt, like a bug in a rule would."""

    def evaluate(self, request):
        if isinstance(request, dict) and request.get("prompt") == "boom":
            raise ZeroDivisionError("bug in a check")
        return super().evaluate(request)


def test_poison_message_is_parked_and_the_requests_behind_it_still_flow(make_settings):
    settings = make_settings(max_attempts=3, batch_size=10)
    running = Running(gate.run, settings, engine=ExplodingEngine())
    try:
        poison = new_request("boom")
        good = new_request("I am stuck behind the poison message")
        # same key = same partition, so `good` really is behind `poison`
        for request in (poison, good):
            send_raw(settings, json.dumps(request).encode(), key="same-partition")

        parked = wait_for(settings, settings.topics.dlq, lambda p: p.get("reasons") == ["processing_error"])
        assert parked is not None
        assert parked["error"] == "ZeroDivisionError" and parked["attempts"] == 3
        assert wait_for(settings, settings.topics.clean, in_clean(good)) == good
        assert count_visible(settings, settings.topics.clean, in_clean(good)) == 1   # retried, yet delivered once
        assert wait_for(settings, settings.topics.clean, in_clean(poison), timeout=5) is None
        running.assert_alive()
    finally:
        running.shutdown()


def test_supervisor_starts_a_new_gate_after_a_failure(make_settings, monkeypatch):
    settings = make_settings()
    failures = {"left": 1}
    real_build = gate.build_gate

    def flaky_build(*args, **kwargs):
        built = real_build(*args, **kwargs)
        if failures["left"]:
            failures["left"] -= 1

            def crash(_msg):
                raise RuntimeError("simulated crash before commit")

            built.before_commit = crash
        return built

    stop = threading.Event()
    monkeypatch.setattr(gate, "build_gate", flaky_build)
    thread = threading.Thread(target=gate.run_forever, args=(settings, stop), daemon=True)
    try:
        request = new_request("survive one crash")
        send_request(settings, request)
        thread.start()

        assert wait_for(settings, settings.topics.clean, in_clean(request), timeout=60) == request
        assert count_visible(settings, settings.topics.clean, in_clean(request)) == 1
        assert failures["left"] == 0 and thread.is_alive()
    finally:
        stop.set()
        thread.join(timeout=20)


def test_resent_request_is_forwarded_once_even_across_a_restart(make_settings):
    settings = make_settings()
    request = new_request("please do not charge me twice")

    running = Running(gate.run, settings)
    try:
        send_request(settings, request)
        send_request(settings, request)                           # the client retries
        assert wait_for(settings, settings.topics.clean, in_clean(request)) == request
        duplicate = wait_for(settings, settings.topics.dlq, in_dlq(request))
        assert duplicate is not None and duplicate["reasons"] == ["duplicate"]
    finally:
        running.shutdown()

    restarted = Running(gate.run, settings.with_(instance_id="after-restart"))   # empty memory
    try:
        marker = new_request("processed after the restart")
        send_request(settings, request)                           # ...and retries again later
        send_request(settings, marker)
        assert wait_for(settings, settings.topics.clean, in_clean(marker)) == marker
        assert count_visible(settings, settings.topics.clean, in_clean(request)) == 1
        assert count_visible(settings, settings.topics.dlq, in_dlq(request), window=6) == 2
    finally:
        restarted.shutdown()
