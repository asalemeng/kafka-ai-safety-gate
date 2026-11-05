import json
import threading
import uuid

import pytest

from safety_gate import gate
from safety_gate.config import CLEAN_TOPIC
from tests.integration.helpers import count_visible, new_request, send_raw, wait_for

pytestmark = pytest.mark.integration


def test_crash_between_write_and_commit_produces_no_visible_duplicate(bootstrap):
    group = f"gate-{uuid.uuid4()}"
    request = new_request("deliver me exactly once")
    is_ours = lambda p: p.get("request_id") == request["request_id"]
    send_raw(bootstrap, json.dumps(request).encode())

    # 1. First gate instance crashes AFTER writing the output, BEFORE committing.
    def crash(msg):
        raise RuntimeError("simulated crash before commit")

    with pytest.raises(RuntimeError, match="simulated crash"):
        gate.run(bootstrap=bootstrap, group_id=group, stop_event=threading.Event(), before_commit=crash)

    # 2. Whatever happened to the half-done write, it is invisible to normal consumers.
    #    (librdkafka may have discarded it client-side, or the broker rolled it back.)
    assert wait_for(bootstrap, CLEAN_TOPIC, is_ours, timeout=8) is None

    # 3. A new gate instance with the same group resumes from the last committed offset.
    stop = threading.Event()
    thread = threading.Thread(
        target=gate.run,
        kwargs={"bootstrap": bootstrap, "group_id": group, "stop_event": stop},
        daemon=True,
    )
    thread.start()
    try:
        assert wait_for(bootstrap, CLEAN_TOPIC, is_ours) == request
        # 4. Exactly one committed copy, not two.
        assert count_visible(bootstrap, CLEAN_TOPIC, is_ours) == 1
    finally:
        stop.set()
        thread.join(timeout=15)
