"""The audit sink's logic against the in-memory stand-in (no Docker needed)."""
import json

import pytest
from tests.unit.fakes import FakeBroker, FakeConsumer, FakeMessage, FakeProducer

from safety_gate.audit import AuditEntry, verify
from safety_gate.audit_sink import AuditSink, SinkFenced, describe
from safety_gate.checkpoints import (
    CheckpointWriter,
    check,
    generate_keys,
    load_private_key,
    load_public_key,
    read_checkpoints,
)
from safety_gate.config import Settings


def chain_of(broker, topics) -> list[AuditEntry]:
    return [AuditEntry.from_dict(json.loads(v)) for v in broker.values(topics.audit)]


def make_sink(broker, settings=None, group=None, checkpoints=None):
    settings = settings or Settings(batch_size=10)
    sink = AuditSink(settings, FakeConsumer(broker, group or settings.audit_group_id),
                     FakeProducer(broker, f"audit-writer-{settings.topics.audit}"),
                     lambda: chain_of(broker, settings.topics), checkpoints)
    sink.start()
    return sink, settings.topics


def gate_output(broker, topics, request_id, blocked=False):
    if blocked:
        payload = {"verdict": "BLOCK", "reasons": ["pii"], "request": {"request_id": request_id, "prompt": "x"}}
        broker.append(topics.dlq, key=request_id, value=json.dumps(payload))
    else:
        broker.append(topics.clean, key=request_id, value=json.dumps({"request_id": request_id, "prompt": "secret text"}))


def drain(sink):
    while sink.step():
        pass


def test_every_gate_output_gets_one_chained_entry_without_the_prompt():
    broker = FakeBroker()
    sink, topics = make_sink(broker)
    gate_output(broker, topics, "r-1")
    gate_output(broker, topics, "r-2", blocked=True)
    drain(sink)

    chain = chain_of(broker, topics)
    assert verify(chain) is None
    assert {(e.event["request_id"], e.event["verdict"]) for e in chain} == {("r-1", "PASS"), ("r-2", "BLOCK")}
    assert "secret text" not in json.dumps([e.to_dict() for e in chain])


def test_event_records_policy_version_and_redactions_from_the_headers():
    msg = FakeMessage("clean", 0, 7, b"r-1", b'{"request_id": "r-1"}',
                      headers=[("policy-version", b"v3"), ("gate-redactions", b"pii,customer-numbers")])
    event = describe(msg, dlq_topic="dlq")
    assert event["policy_version"] == "v3" and event["redactions"] == ["pii", "customer-numbers"]


# --- task 4: resume at the real end of the topic ---------------------------------
def test_restarted_sink_continues_the_chain_without_a_fork():
    broker = FakeBroker()
    sink, topics = make_sink(broker)
    for i in range(5):
        gate_output(broker, topics, f"r-{i}")
    drain(sink)

    restarted, _ = make_sink(broker)
    for i in range(5, 8):
        gate_output(broker, topics, f"r-{i}")
    drain(restarted)

    chain = chain_of(broker, topics)
    assert [e.seq for e in chain] == list(range(8))             # no gap, no repeated number
    assert verify(chain) is None


def test_sink_refuses_to_extend_a_chain_that_was_tampered_with():
    broker = FakeBroker()
    sink, topics = make_sink(broker)
    gate_output(broker, topics, "r-1")
    drain(sink)
    forged = chain_of(broker, topics)[0].to_dict()
    forged["event"]["verdict"] = "BLOCK"
    broker.log(topics.audit, 0)[0]._value = json.dumps(forged).encode()

    with pytest.raises(RuntimeError, match="broken at seq 0"):
        make_sink(broker)


# --- task 5: only one sink writes ----------------------------------------------------
def test_second_sink_cuts_off_the_first_and_the_chain_does_not_fork():
    broker = FakeBroker()
    first, topics = make_sink(broker)
    gate_output(broker, topics, "r-1")
    drain(first)

    second, _ = make_sink(broker)                               # same transactional id: takes over
    gate_output(broker, topics, "r-2")
    with pytest.raises(SinkFenced):
        first.step()                                            # the old sink can no longer write
    gate_output(broker, topics, "r-3")
    drain(second)

    chain = chain_of(broker, topics)
    assert verify(chain) is None
    assert [e.seq for e in chain] == [0, 1, 2]
    assert [e.event["request_id"] for e in chain] == ["r-1", "r-2", "r-3"]   # each output exactly once


def test_sink_with_a_new_group_id_does_not_audit_old_outputs_twice():
    broker = FakeBroker()
    sink, topics = make_sink(broker)
    gate_output(broker, topics, "r-1")
    drain(sink)

    other, _ = make_sink(broker, group="a-different-group")     # would start reading from offset 0 again
    gate_output(broker, topics, "r-2")
    drain(other)

    assert [e.event["request_id"] for e in chain_of(broker, topics)] == ["r-1", "r-2"]


# --- task 16: signed checkpoints outside Kafka -------------------------------------------
@pytest.fixture
def keys(tmp_path):
    private, public = generate_keys()
    (tmp_path / "audit.key").write_bytes(private)
    (tmp_path / "audit.pub").write_bytes(public)
    return load_private_key(tmp_path / "audit.key"), load_public_key(tmp_path / "audit.pub")


def run_with_checkpoints(tmp_path, keys, outputs=7, every=3):
    broker = FakeBroker()
    file = tmp_path / "checkpoints.jsonl"
    sink, topics = make_sink(broker, checkpoints=CheckpointWriter(file, keys[0], every=every))
    for i in range(outputs):
        gate_output(broker, topics, f"r-{i}")
    drain(sink)
    return broker, topics, file


def test_a_signed_checkpoint_is_written_every_n_entries(tmp_path, keys):
    broker, topics, file = run_with_checkpoints(tmp_path, keys)
    checkpoints = read_checkpoints(file)

    assert [c.seq for c in checkpoints] == [2, 5]
    assert check(chain_of(broker, topics), checkpoints, keys[1]) == []


def test_missing_checkpoints_are_written_on_the_next_start(tmp_path, keys):
    broker, topics, file = run_with_checkpoints(tmp_path, keys)
    file.write_text(file.read_text().splitlines()[0] + "\n")    # as if the sink crashed before writing the 2nd

    make_sink(broker, checkpoints=CheckpointWriter(file, keys[0], every=3))
    assert [c.seq for c in read_checkpoints(file)] == [2, 5]


def test_rewritten_history_is_caught_even_if_the_new_chain_is_consistent(tmp_path, keys):
    """The attack a hash chain alone cannot see: delete everything, write a new clean chain."""
    broker, topics, file = run_with_checkpoints(tmp_path, keys)
    fresh = FakeBroker()
    sink, _ = make_sink(fresh)
    for i in range(7):
        gate_output(fresh, topics, f"other-{i}")
    drain(sink)
    replaced = chain_of(fresh, topics)

    assert verify(replaced) is None                             # looks perfectly fine on its own
    problems = check(replaced, read_checkpoints(file), keys[1])
    assert len(problems) == 2 and "rewritten" in problems[0]


def test_shortened_history_is_caught(tmp_path, keys):
    broker, topics, file = run_with_checkpoints(tmp_path, keys)
    problems = check(chain_of(broker, topics)[:4], read_checkpoints(file), keys[1])
    assert len(problems) == 1 and "history was cut" in problems[0]


def test_forged_checkpoint_is_caught(tmp_path, keys):
    broker, topics, file = run_with_checkpoints(tmp_path, keys)
    line = json.loads(file.read_text().splitlines()[0])
    line["hash"] = "0" * 64
    file.write_text(json.dumps(line) + "\n")
    assert "signature is not valid" in check(chain_of(broker, topics), read_checkpoints(file), keys[1])[0]


def test_checkpoint_signed_with_another_key_is_caught(tmp_path, keys):
    broker, topics, file = run_with_checkpoints(tmp_path, keys)
    (tmp_path / "other.pub").write_bytes(generate_keys()[1])
    problems = check(chain_of(broker, topics), read_checkpoints(file), load_public_key(tmp_path / "other.pub"))
    assert len(problems) == 2
