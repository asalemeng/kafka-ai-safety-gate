import json
import logging
import signal
import threading

from confluent_kafka import Consumer, Producer

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
) -> None:
    """Process messages until stop_event is set."""
    stop_event = stop_event or threading.Event()

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,      # we commit only after the output is written
    })
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,       # no duplicates if a send is retried
    })

    consumer.subscribe([RAW_TOPIC])
    log.info("gate started: %s -> %s | %s", RAW_TOPIC, CLEAN_TOPIC, DLQ_TOPIC)

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            topic, key, payload = route(msg.value())
            producer.produce(topic, key=key, value=json.dumps(payload).encode("utf-8"))
            producer.flush()                 # wait until Kafka confirms the write...
            consumer.commit(message=msg)     # ...only then mark the input as done
            log.info("%s -> %s key=%s", RAW_TOPIC, topic, key)
    finally:
        consumer.close()
        producer.flush()
        log.info("gate stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    run(stop_event=stop)


if __name__ == "__main__":
    main()
