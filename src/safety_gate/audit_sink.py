"""Audit sink: chains every gate output (clean or DLQ) into a tamper-evident log."""
import hashlib
import json
import logging
import signal
import threading
import uuid

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

from safety_gate.audit import AuditEntry, append
from safety_gate.config import AUDIT_TOPIC, BOOTSTRAP, CLEAN_TOPIC, DLQ_TOPIC

log = logging.getLogger("audit")
SINK_GROUP = "audit-sink"


def read_chain(bootstrap: str = BOOTSTRAP, idle_polls: int = 3) -> list[AuditEntry]:
    """Read the whole audit topic, in order. Stops after `idle_polls` empty polls."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"audit-reader-{uuid.uuid4()}",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
    })
    consumer.assign([TopicPartition(AUDIT_TOPIC, 0, OFFSET_BEGINNING)])
    entries: list[AuditEntry] = []
    idle = 0
    try:
        while idle < idle_polls:
            msg = consumer.poll(1.0)
            if msg is None:
                idle += 1
                continue
            if msg.error():
                continue
            idle = 0
            entries.append(AuditEntry.from_dict(json.loads(msg.value())))
        return entries
    finally:
        consumer.close()


def describe(msg) -> dict:
    """What is recorded about one gate output. The prompt is never stored, only a hash."""
    value = msg.value()
    try:
        payload = json.loads(value)
    except ValueError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    blocked = msg.topic() == DLQ_TOPIC
    request = payload.get("request", {}) if blocked else payload
    if not isinstance(request, dict):
        request = {}

    return {
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "request_id": request.get("request_id"),
        "verdict": "BLOCK" if blocked else "PASS",
        "reasons": payload.get("reasons", []) if blocked else [],
        "payload_sha256": hashlib.sha256(value).hexdigest(),
    }


def run(
    bootstrap: str = BOOTSTRAP,
    group_id: str = SINK_GROUP,
    stop_event: threading.Event | None = None,
) -> None:
    """Append one chained audit entry per gate output, exactly once."""
    stop_event = stop_event or threading.Event()

    chain = read_chain(bootstrap)                 # resume the chain where it ended
    last = chain[-1] if chain else None

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "isolation.level": "read_committed",      # never audit an aborted gate write
    })
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "transactional.id": f"audit-{group_id}",
        "enable.idempotence": True,
    })
    producer.init_transactions(30)

    consumer.subscribe([CLEAN_TOPIC, DLQ_TOPIC])
    log.info("audit sink started, next seq=%d", 0 if last is None else last.seq + 1)

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.error("consumer error: %s", msg.error())
                continue

            entry = append(last, describe(msg))
            producer.begin_transaction()
            try:
                producer.produce(
                    AUDIT_TOPIC,
                    partition=0,
                    key=str(entry.seq),
                    value=json.dumps(entry.to_dict()).encode("utf-8"),
                )
                producer.send_offsets_to_transaction(
                    consumer.position(consumer.assignment()),
                    consumer.consumer_group_metadata(),
                )
                producer.commit_transaction()
            except Exception:
                log.exception("audit transaction failed, aborting")
                producer.abort_transaction()
                raise

            last = entry                          # only advance the chain after the commit
            log.info("audit seq=%d %s %s", entry.seq, entry.event["verdict"], entry.event["request_id"])
    finally:
        consumer.close()
        log.info("audit sink stopped")


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    run(stop_event=stop)


if __name__ == "__main__":
    main()
