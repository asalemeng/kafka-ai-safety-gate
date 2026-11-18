import json
import logging
import signal
import threading
import time
import uuid
from typing import Callable

from confluent_kafka import OFFSET_BEGINNING, Consumer, Message, Producer, TopicPartition

from safety_gate.config import BOOTSTRAP, CLEAN_TOPIC, CONTROL_TOPIC, DLQ_TOPIC, GROUP_ID, RAW_TOPIC
from safety_gate.killswitch import KillSwitch
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


def reject_all(raw_value: bytes, why: str) -> tuple[str, str | None, dict]:
    """Kill switch is on: send the message to the DLQ without running the validator."""
    payload: dict = {"verdict": "BLOCK", "reasons": ["kill_switch"], "kill_switch_reason": why}
    try:
        request = json.loads(raw_value)
    except (ValueError, UnicodeDecodeError):
        payload["raw"] = raw_value.decode("utf-8", errors="replace")
        return DLQ_TOPIC, None, payload

    payload["request"] = request
    key = request.get("request_id") if isinstance(request, dict) else None
    return DLQ_TOPIC, key, payload


def sync_switch(control: Consumer, switch: KillSwitch, next_offset: int, timeout: float = 10.0) -> int:
    """Apply every control command written so far. Returns the next offset to read.

    Fails closed: if the state of the switch cannot be confirmed, the gate stops
    rather than guess.
    """
    partition = TopicPartition(CONTROL_TOPIC, 0)
    marks = control.get_watermark_offsets(partition, timeout=timeout, cached=False)
    if marks is None:
        raise RuntimeError("kill switch state unknown: control topic did not answer")
    low, high = marks
    next_offset = max(next_offset, low)

    deadline = time.monotonic() + timeout
    while next_offset < high:
        if time.monotonic() > deadline:
            raise RuntimeError("kill switch state unknown: could not read the control topic to its end")
        msg = control.poll(0.5)
        if msg is None or msg.error():
            continue
        was_disabled = switch.disabled
        switch.apply(msg.value())
        next_offset = msg.offset() + 1
        if switch.disabled != was_disabled:
            log.warning("kill switch %s (%s)", "ON: rejecting everything" if switch.disabled else "OFF", switch.last_reason or "no reason given")
    return next_offset


def run(
    bootstrap: str = BOOTSTRAP,
    group_id: str = GROUP_ID,
    stop_event: threading.Event | None = None,
    before_commit: Callable[[Message], None] | None = None,
) -> None:
    """Process messages exactly once until stop_event is set.

    Each input message is handled in one Kafka transaction: the output write and
    the input offset commit succeed together or not at all. Before each message
    the gate re-reads the kill switch, so a command written before a request is
    processed is always honoured. `before_commit` is a test hook called between
    the write and the commit (used to simulate crashes).
    """
    stop_event = stop_event or threading.Event()

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,          # offsets are committed inside the transaction
    })
    # The switch is rebuilt from the whole control topic on every start, so this
    # consumer never commits offsets: a restarted gate must remember it was disabled.
    control = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"gate-control-{uuid.uuid4()}",
        "enable.auto.commit": False,
    })
    control.assign([TopicPartition(CONTROL_TOPIC, 0, OFFSET_BEGINNING)])
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "transactional.id": f"gate-{group_id}",   # one id per gate instance
        "enable.idempotence": True,
    })

    switch = KillSwitch()
    try:
        producer.init_transactions(30)            # seconds; fail loudly instead of hanging
        control_offset = sync_switch(control, switch, 0)

        consumer.subscribe([RAW_TOPIC])
        log.info("gate started (exactly-once, kill switch %s): %s -> %s | %s",
                 "ON" if switch.disabled else "off", RAW_TOPIC, CLEAN_TOPIC, DLQ_TOPIC)

        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            control_offset = sync_switch(control, switch, control_offset)

            producer.begin_transaction()
            try:
                if switch.disabled:
                    topic, key, payload = reject_all(msg.value(), switch.reason)
                else:
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
        control.close()
        log.info("gate stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    run(stop_event=stop)


if __name__ == "__main__":
    main()
