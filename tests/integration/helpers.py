import json
import time
import uuid

from confluent_kafka import Consumer, Producer

from safety_gate.config import RAW_TOPIC


def send_raw(bootstrap, value: bytes) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    producer.produce(RAW_TOPIC, value=value)
    producer.flush()


def _consumer(bootstrap, topic, isolation):
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"test-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
        "isolation.level": isolation,
    })
    consumer.subscribe([topic])
    return consumer


def wait_for(bootstrap, topic, matches, timeout=30, isolation="read_committed"):
    """Return the first message on `topic` for which matches(payload) is True, or None."""
    consumer = _consumer(bootstrap, topic, isolation)
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


def count_visible(bootstrap, topic, matches, window=8, isolation="read_committed"):
    """Count matching messages seen within a fixed time window."""
    consumer = _consumer(bootstrap, topic, isolation)
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


def new_request(prompt):
    return {"request_id": f"r-{uuid.uuid4()}", "tenant_id": "t-1", "prompt": prompt}
