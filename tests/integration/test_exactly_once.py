import threading

import pytest

from safety_gate import gate
from tests.integration.helpers import Running, count_visible, in_clean, new_request, send_request, wait_for

pytestmark = pytest.mark.integration


def test_crash_between_write_and_commit_produces_no_visible_duplicate(make_settings):
    settings = make_settings()
    request = new_request("deliver me exactly once")
    send_request(settings, request)

    # 1. First gate instance crashes AFTER writing the output, BEFORE committing.
    def crash(msg):
        raise RuntimeError("simulated crash before commit")

    with pytest.raises(RuntimeError, match="simulated crash"):
        gate.run(settings, threading.Event(), before_commit=crash)

    # 2. Whatever happened to the half-done write, it is invisible to normal consumers.
    #    (librdkafka may have discarded it client-side, or the broker rolled it back.)
    assert wait_for(settings, settings.topics.clean, in_clean(request), timeout=8) is None

    # 3. A new gate instance with the same group resumes from the last committed offset.
    running = Running(gate.run, settings.with_(instance_id="second-life"))
    try:
        assert wait_for(settings, settings.topics.clean, in_clean(request)) == request
        # 4. Exactly one committed copy, not two.
        assert count_visible(settings, settings.topics.clean, in_clean(request)) == 1
        running.assert_alive()
    finally:
        running.shutdown()
