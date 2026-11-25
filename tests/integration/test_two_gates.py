"""Task 1: two gates in the same consumer group must work side by side."""
import json
import time

import pytest
from confluent_kafka.admin import AdminClient

from safety_gate import gate
from tests.integration.helpers import Running, collect, new_request, send_request

pytestmark = pytest.mark.integration

REQUESTS = 60


def wait_for_members(settings, expected: int, timeout: float = 60) -> None:
    admin = AdminClient(settings.client_config())
    deadline = time.time() + timeout
    members = 0
    while time.time() < deadline:
        group = admin.describe_consumer_groups([settings.group_id])[settings.group_id].result(timeout=15)
        members = len(group.members)
        if members == expected and all(m.assignment and m.assignment.topic_partitions for m in group.members):
            return
        time.sleep(1)
    pytest.fail(f"consumer group has {members} member(s) with partitions, expected {expected}")


def test_two_gates_in_one_group_share_the_work_and_neither_crashes(make_settings):
    settings = make_settings(batch_size=5)
    first = Running(gate.run, settings.with_(instance_id="gate-a"))
    second = Running(gate.run, settings.with_(instance_id="gate-b"))
    try:
        wait_for_members(settings, 2)

        requests = [new_request(f"question number {i}") for i in range(REQUESTS)]
        for request in requests:
            send_request(settings, request)             # keyed: spread over all three partitions

        collect(settings, settings.topics.clean, lambda seen: len(seen) >= REQUESTS, timeout=90)
        # Read everything again over a fixed window: a late duplicate would show up here.
        messages = collect(settings, settings.topics.clean, lambda seen: False, timeout=12)

        ids = [json.loads(m.value())["request_id"] for m in messages]
        assert sorted(ids) == sorted(r["request_id"] for r in requests)      # every request, exactly once
        handled_by = {dict(m.headers())["gate-instance"] for m in messages}
        assert handled_by == {b"gate-a", b"gate-b"}                         # both really did work
        first.assert_alive()                                                 # neither was fenced
        second.assert_alive()
    finally:
        first.shutdown()
        second.shutdown()
