import json
import logging
import signal
import threading
from typing import Callable

from confluent_kafka import Consumer, Message, Producer

from safety_gate.config import BOOTSTRAP, CLEAN_TOPIC, DLQ_TOPIC, GROUP_ID, RAW_TOPIC
from safety_gate.validator import validate

log = logging.getLogger("gate")


def route(raw_value: bytes) -> tuple[str, str | None, dict]:
    """Decide where a raw message goes. Returns (topic, key, payload)."""
    try:
        request = json.loads(raw_value)
    except (ValueError, UnicodeDecodeError):
        payload = {
            "verdict": "BLOCK",
            "reasons": ["malformed_json"],
            "raw": raw_value.decode("utf-8", errors="replace"),
        }
        return DLQ_TOPIC, None, payload

    result = validate(request)
    key = request.get("request_id") if isinstance(request, dict) else None

    if result.verdict == "PASS":
        return CLEAN_TOPIC, key, request

    payload = {"verdict": "BLOCK", "reasons": list(result.reasons), "request": request}
    return DLQ_TOPIC, key, payload


def run(
    bootstrap: str = BOOTSTRAP,
    group_id: str = GROUP_ID,
    stop_event: threading.Event | None = None,
    before_commit: Callable[[Message], None] | None = None,
) -> None:
    """Process messages exactly once until stop_event is set.

    Each input message is handled in one Kafka transaction: the output write and
    the input offset commit succeed together or not at all. `before_commit` is a
    test hook called between the write and the commit (used to simulate crashes).
    """
    stop_event = stop_event or threading.Event()

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,          # offsets are committed inside the transaction
    })
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "transactional.id": f"gate-{group_id}",   # one id per gate instance
        "enable.idempotence": True,
    })
    producer.init_transactions(30)   # seconds; fail loudly instead of hanging

    consumer.subscribe([RAW_TOPIC])
    log.info("gate started (exactly-once): %s -> %s | %s", RAW_TOPIC, CLEAN_TOPIC, DLQ_TOPIC)

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            producer.begin_transaction()
            try:
                topic, key, payload = route(msg.value())
                producer.produce(topic, key=key, value=json.dumps(payload).encode("utf-8"))
                if before_commit:
                    before_commit(msg)
                producer.send_offsets_to_transaction(
                    consumer.position(consumer.assignment()),
                    consumer.consumer_group_metadata(),
                )
                producer.commit_transaction()
            except Exception:
                log.exception("transaction failed, aborting")
                producer.abort_transaction()
                raise

            log.info("%s -> %s key=%s", RAW_TOPIC, topic, key)
    finally:
        consumer.close()
        log.info("gate stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    run(stop_event=stop)


if __name__ == "__main__":
    main()
