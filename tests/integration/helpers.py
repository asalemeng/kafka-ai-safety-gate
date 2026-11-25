import json
import threading
import time
import uuid

from confluent_kafka import Consumer, Producer

from safety_gate.config import Settings
from safety_gate.kafka_io import consumer_config


class Running:
    """A component running in a background thread. Remembers why it stopped."""

    def __init__(self, target, *args, **kwargs) -> None:
        self.stop = threading.Event()
        self.error: BaseException | None = None

        def body() -> None:
            try:
                target(*args, stop_event=self.stop, **kwargs)
            except BaseException as exc:   # noqa: B902 - the test wants to see every reason
                self.error = exc

        self.thread = threading.Thread(target=body, daemon=True)
        self.thread.start()

    @property
    def alive(self) -> bool:
        return self.thread.is_alive()

    def assert_alive(self) -> None:
        assert self.alive, f"component stopped unexpectedly: {self.error!r}"

    def wait_stopped(self, timeout: float = 30) -> bool:
        self.thread.join(timeout)
        return not self.alive

    def shutdown(self, timeout: float = 20) -> None:
        self.stop.set()
        self.thread.join(timeout)


def send_raw(settings: Settings, value: bytes, key: str | None = None) -> None:
    producer = Producer(settings.client_config())
    producer.produce(settings.topics.raw, key=key, value=value)
    producer.flush()


def send_request(settings: Settings, request: dict) -> None:
    """Send like a well-behaved client: the request id is the message key."""
    send_raw(settings, json.dumps(request).encode(), key=request["request_id"])


def _consumer(settings: Settings, topic: str, isolation: str) -> Consumer:
    consumer = Consumer(consumer_config(settings, f"test-{uuid.uuid4()}", **{"isolation.level": isolation}))
    consumer.subscribe([topic])
    return consumer


def wait_for(settings, topic, matches, timeout=30, isolation="read_committed"):
    """Return the first message payload on `topic` for which matches(payload) is True, or None."""
    consumer = _consumer(settings, topic, isolation)
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value())
            if matches(payload):
                return payload
        return None
    finally:
        consumer.close()


def collect(settings, topic, enough, timeout=60, isolation="read_committed"):
    """Read messages (as Kafka messages) until enough(list) is True or the deadline passes."""
    consumer = _consumer(settings, topic, isolation)
    deadline = time.time() + timeout
    messages = []
    try:
        while time.time() < deadline and not enough(messages):
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            messages.append(msg)
        return messages
    finally:
        consumer.close()


def count_visible(settings, topic, matches, window=8, isolation="read_committed"):
    """Count matching messages seen within a fixed time window."""
    consumer = _consumer(settings, topic, isolation)
    deadline = time.time() + window
    count = 0
    try:
        while time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            if matches(json.loads(msg.value())):
                count += 1
        return count
    finally:
        consumer.close()


def new_request(prompt, tenant="t-1"):
    return {"request_id": f"r-{uuid.uuid4()}", "tenant_id": tenant, "prompt": prompt}


def in_clean(request):
    return lambda p: p.get("request_id") == request["request_id"]


def in_dlq(request):
    return lambda p: isinstance(p.get("request"), dict) and p["request"].get("request_id") == request["request_id"]
