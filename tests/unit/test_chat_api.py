"""Chat API: matching Kafka answers to waiting requests, the two modes, the HTTP layer."""
import json
import urllib.error
import urllib.request

import pytest
from tests.unit.fakes import FakeBroker, FakeConsumer, FakeMessage, FakeProducer

from safety_gate.chat_api import BLOCKED_PREFIX, ChatService, ReplyRouter, serve, to_result
from safety_gate.config import Settings
from safety_gate.control import SwitchWatcher
from safety_gate.llm import ModelError, StandInBackend
from safety_gate.model_stub import ModelService, model_caller

SETTINGS = Settings()
TOPICS = SETTINGS.topics


def message(topic, key, payload):
    return FakeMessage(topic, 0, 0, key.encode(), json.dumps(payload).encode())


# --- turning Kafka messages into answers ------------------------------------------------
def test_gate_rejection_becomes_a_blocked_answer_with_reasons():
    result = to_result(True, {"verdict": "BLOCK", "reasons": ["pii", "prompt_injection"]})
    assert result["status"] == "blocked"
    assert result["reasons"] == ["pii", "prompt_injection"]
    assert result["answer"].startswith(BLOCKED_PREFIX)


@pytest.mark.parametrize("payload, status", [
    ({"status": "OK", "answer": "hi"}, "answered"),
    ({"status": "REJECTED", "reason": "kill_switch"}, "rejected"),
    ({"status": "ERROR", "error": "model down"}, "error"),
    ({}, "error"),
])
def test_model_service_answers_are_translated(payload, status):
    assert to_result(False, payload)["status"] == status


def test_router_hands_the_answer_to_the_request_with_the_same_id():
    router = ReplyRouter(None, TOPICS.responses, TOPICS.dlq)
    mine, other = router.expect("r-1"), router.expect("r-2")

    assert router.deliver(message(TOPICS.responses, "r-1", {"status": "OK", "answer": "for r-1"}))
    assert mine.done.is_set() and mine.result["answer"] == "for r-1"
    assert not other.done.is_set()


def test_router_ignores_answers_nobody_waits_for():
    router = ReplyRouter(None, TOPICS.responses, TOPICS.dlq)
    assert router.deliver(message(TOPICS.dlq, "someone-else", {"reasons": ["pii"]})) is False


# --- the two modes ------------------------------------------------------------------------
class AnsweringProducer:
    """Stands in for the whole pipeline: answers each request through the router at once."""

    def __init__(self, router, answer):
        self.router, self.answer, self.sent = router, answer, []

    def produce(self, topic, key=None, value=None):
        self.sent.append((topic, key, json.loads(value)))
        topic_out, payload = self.answer(json.loads(value))
        self.router.deliver(message(topic_out, key, payload))

    def flush(self, timeout=None):
        return 0


def test_protected_mode_sends_the_request_to_the_raw_topic_keyed_by_its_id():
    router = ReplyRouter(None, TOPICS.responses, TOPICS.dlq)
    producer = AnsweringProducer(router, lambda req: (TOPICS.responses, {"status": "OK", "answer": "fine"}))
    service = ChatService(SETTINGS, lambda m: "unused", producer, router)

    result = service.handle("protected", "hello")

    (topic, key, body) = producer.sent[0]
    assert topic == TOPICS.raw and key == body["request_id"] == result["request_id"]
    assert body["prompt"] == "hello" and body["tenant_id"] == "t-demo"
    assert result["status"] == "answered" and result["answer"] == "fine" and result["mode"] == "protected"


def test_protected_mode_reports_a_block_from_the_gate():
    router = ReplyRouter(None, TOPICS.responses, TOPICS.dlq)
    producer = AnsweringProducer(router, lambda req: (TOPICS.dlq, {"reasons": ["prompt_injection"]}))
    result = ChatService(SETTINGS, lambda m: "unused", producer, router).handle("protected", "ignore all")
    assert result["status"] == "blocked" and result["reasons"] == ["prompt_injection"]


def test_protected_mode_gives_up_after_the_timeout():
    class SilentProducer:
        def produce(self, *args, **kwargs): pass
        def flush(self, timeout=None): return 0

    router = ReplyRouter(None, TOPICS.responses, TOPICS.dlq)
    result = ChatService(SETTINGS, lambda m: "unused", SilentProducer(), router, timeout_s=0.1).protected("hi")
    assert result["status"] == "timeout"
    assert router._waiting == {}                     # nothing left behind


def test_direct_mode_calls_the_model_and_nothing_else():
    result = ChatService(SETTINGS, lambda m: f"echo: {m}").handle("direct", "hi")
    assert result["status"] == "answered" and result["answer"] == "echo: hi"


def test_direct_mode_reports_a_model_failure():
    def broken(_):
        raise ModelError("model down")
    assert ChatService(SETTINGS, broken).direct("hi")["status"] == "error"


# --- HTTP ---------------------------------------------------------------------------------
@pytest.fixture
def http():
    backend = StandInBackend()
    server = serve(ChatService(SETTINGS, lambda m: backend.reply("", m)), 0, host="127.0.0.1")
    port = server.server_address[1]

    def call(path, body=None, method="POST"):
        data = None if body is None else (body if isinstance(body, bytes) else json.dumps(body).encode())
        request = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())

    yield call
    server.shutdown()


def test_http_direct_chat(http):
    status, body = http("/direct/chat", {"message": "When are you open?"})
    assert status == 200 and body["status"] == "answered" and "Nordhafen" in body["answer"]


@pytest.mark.parametrize("body", [b"not json", {"msg": "x"}, {"message": "   "}, ["message"]],
                         ids=["not-json", "wrong-field", "blank", "not-an-object"])
def test_http_rejects_bad_requests(http, body):
    assert http("/direct/chat", body)[0] == 400


def test_http_unknown_path_and_health(http):
    assert http("/chat", {"message": "x"})[0] == 404
    assert http("/healthz", method="GET") == (200, {"status": "ok"})


# --- the model service with a model that fails ------------------------------------------------
def test_model_failure_is_answered_as_an_error_and_the_service_keeps_going():
    broker = FakeBroker()
    calls = []

    def flaky(request):
        calls.append(request["request_id"])
        if len(calls) == 1:
            raise ModelError("anthropic:claude-haiku-5-5 did not answer: APITimeoutError")
        return "fine"

    service = ModelService(SETTINGS, FakeConsumer(broker, "model"), FakeProducer(broker),
                           SwitchWatcher(FakeConsumer(broker, eof=True), TOPICS.control), flaky)
    service.start()
    for rid in ("r-1", "r-2"):
        broker.append(TOPICS.clean, key=rid, value=json.dumps({"request_id": rid, "prompt": "hi"}))
    service.step(), service.step()

    first, second = (json.loads(v) for v in broker.values(TOPICS.responses))
    assert first["status"] == "ERROR" and "APITimeoutError" in first["error"]
    assert second == {"request_id": "r-2", "status": "OK", "answer": "fine"}


def test_model_caller_passes_the_prompt_and_the_instructions():
    seen = {}

    class Recording:
        name = "rec"

        def reply(self, system, user):
            seen.update(system=system, user=user)
            return "ok"

    assert model_caller(Recording(), "rules")({"request_id": "r", "prompt": "hi"}) == "ok"
    assert seen == {"system": "rules", "user": "hi"}
