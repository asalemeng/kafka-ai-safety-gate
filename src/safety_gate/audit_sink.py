"""Audit sink: chains every gate output (clean or DLQ) into a tamper-evident log.

Only one sink may write the chain. All sinks use the same transactional id, so
when a second one starts, Kafka cuts the first one off ("fencing"): the old sink
can no longer commit anything and stops. Two writers can therefore never fork
the chain.
"""
from __future__ import annotations

import hashlib
import json
import logging
import signal
import sys
import threading

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

from safety_gate.audit import AuditEntry, append, verify
from safety_gate.checkpoints import CheckpointWriter, load_private_key
from safety_gate.config import Settings
from safety_gate.kafka_io import (
    can_abort_and_continue,
    consumer_config,
    is_fenced,
    read_to_end,
    reader_config,
    transactional_producer_config,
)

log = logging.getLogger("audit")


class SinkFenced(RuntimeError):
    """Another audit sink has taken over. This one must stop and must not restart by itself."""


def read_chain(settings: Settings, idle_timeout: float = 30.0) -> list[AuditEntry]:
    """Read the whole audit topic in order, up to its real end."""
    consumer = Consumer(reader_config(settings, "audit-reader"))
    try:
        return [AuditEntry.from_dict(json.loads(msg.value()))
                for msg in read_to_end(consumer, settings.topics.audit, 0, idle_timeout)]
    finally:
        consumer.close()


def _header(msg, name: str) -> str | None:
    for key, value in msg.headers() or ():
        if key == name and value is not None:
            return value.decode("utf-8", errors="replace")
    return None


def describe(msg, dlq_topic: str) -> dict:
    """What is recorded about one gate output. The prompt is never stored, only a hash."""
    value = msg.value() or b""
    try:
        payload = json.loads(value)
    except ValueError:
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    blocked = msg.topic() == dlq_topic
    request = payload.get("request", {}) if blocked else payload
    if not isinstance(request, dict):
        request = {}

    redactions = _header(msg, "gate-redactions")
    return {
        "source_topic": msg.topic(),
        "source_partition": msg.partition(),
        "source_offset": msg.offset(),
        "request_id": request.get("request_id"),
        "verdict": "BLOCK" if blocked else "PASS",
        "reasons": payload.get("reasons", []) if blocked else [],
        "redactions": redactions.split(",") if redactions else [],
        "policy_version": _header(msg, "policy-version"),
        "payload_sha256": hashlib.sha256(value).hexdigest(),
    }


class AuditSink:
    """The Kafka clients are passed in, so tests can use stand-ins."""

    def __init__(self, settings: Settings, consumer, producer, chain_reader,
                 checkpoints: CheckpointWriter | None = None) -> None:
        self.settings = settings
        self.topics = settings.topics
        self.consumer = consumer
        self.producer = producer
        self.chain_reader = chain_reader          # () -> list[AuditEntry]
        self.checkpoints = checkpoints
        self.last: AuditEntry | None = None
        self.audited: dict[tuple[str, int], int] = {}   # (topic, partition) -> highest offset in the chain

    def start(self) -> None:
        # Order matters. First become the only writer (this cuts off any older
        # sink and rolls back its unfinished transaction), THEN read the chain:
        # nothing can be appended behind our back after that.
        self.producer.init_transactions(30)
        chain = self.chain_reader()
        broken = verify(chain)
        if broken is not None:
            # Never build on a chain that was tampered with: a human has to look first.
            raise RuntimeError(f"audit chain is broken at seq {broken}; refusing to extend it")
        self.last = chain[-1] if chain else None
        for entry in chain:
            self._note(entry)
        if self.checkpoints is not None:
            self.checkpoints.catch_up(chain)
        self.consumer.subscribe([self.topics.clean, self.topics.dlq])
        log.info("audit sink started, next seq=%d", 0 if self.last is None else self.last.seq + 1)

    def _note(self, entry: AuditEntry) -> None:
        event = entry.event
        try:
            source = (event["source_topic"], int(event["source_partition"]))
            offset = int(event["source_offset"])
        except (KeyError, TypeError, ValueError):
            return                                # an entry without a source (should not happen)
        self.audited[source] = max(self.audited.get(source, -1), offset)

    def step(self) -> int:
        """Chain one batch of gate outputs in one transaction. Returns how many entries were added."""
        first = self.consumer.poll(1.0)
        if first is None:
            return 0
        batch = [first]
        if self.settings.batch_size > 1:
            batch += self.consumer.consume(self.settings.batch_size - 1, timeout=self.settings.batch_linger_s)
        messages = [m for m in batch if not m.error()]
        for m in batch:
            if m.error():
                log.error("consumer error: %s", m.error())
        if not messages:
            return 0

        entries: list[AuditEntry] = []
        last = self.last
        positions: dict[tuple[str, int], int] = {}
        for msg in messages:
            source = (msg.topic(), msg.partition())
            positions[source] = max(positions.get(source, -1), msg.offset())
            if msg.offset() <= self.audited.get(source, -1):
                continue                          # already in the chain (e.g. the group id was changed)
            last = append(last, describe(msg, self.topics.dlq))
            entries.append(last)

        try:
            self.producer.begin_transaction()
            for entry in entries:
                self.producer.produce(self.topics.audit, partition=0, key=str(entry.seq),
                                      value=json.dumps(entry.to_dict()).encode("utf-8"))
            self.producer.send_offsets_to_transaction(
                [TopicPartition(topic, partition, offset + 1) for (topic, partition), offset in positions.items()],
                self.consumer.consumer_group_metadata())
            self.producer.commit_transaction()
        except Exception as exc:
            if is_fenced(exc):
                raise SinkFenced("another audit sink has taken over; this one stops") from exc
            try:
                self.producer.abort_transaction()
                if can_abort_and_continue(exc):
                    # e.g. the consumer group was rebalancing. Nothing was written and the
                    # chain in memory was not advanced: go back and read the batch again.
                    log.warning("audit transaction failed (%s); rolled back, reading the batch again", exc)
                    self._rewind()
                    return 0
            except Exception as abort_exc:
                if is_fenced(abort_exc):
                    raise SinkFenced("another audit sink has taken over; this one stops") from abort_exc
                log.exception("could not roll back the audit transaction")
            log.exception("audit transaction failed")
            raise

        for entry in entries:                     # only advance the chain after the commit
            self.last = entry
            self._note(entry)
            if self.checkpoints is not None:
                self.checkpoints.offer(entry)
            log.debug("audit seq=%d %s %s", entry.seq, entry.event["verdict"], entry.event["request_id"])
        return len(entries)

    def _rewind(self) -> None:
        assignment = self.consumer.assignment()
        if not assignment:
            return
        for tp in self.consumer.committed(assignment, timeout=10):
            if tp.offset < 0:
                tp = TopicPartition(tp.topic, tp.partition, OFFSET_BEGINNING)
            self.consumer.seek(tp)

    def close(self) -> None:
        try:
            self.consumer.close()
        except Exception:
            log.exception("error while closing the consumer")
        log.info("audit sink stopped")


def build_sink(settings: Settings) -> AuditSink:
    consumer = Consumer(consumer_config(
        settings, settings.audit_group_id,
        **{"isolation.level": "read_committed"}))          # never audit an aborted gate write
    # One fixed id per audit topic, whatever the group id: this is what makes a
    # second sink cut off the first one.
    producer = Producer(transactional_producer_config(settings, f"audit-writer-{settings.topics.audit}"))
    checkpoints = None
    if settings.checkpoint_file:
        if not settings.checkpoint_private_key:
            raise ValueError("AUDIT_CHECKPOINT_FILE is set but AUDIT_CHECKPOINT_PRIVATE_KEY is not")
        checkpoints = CheckpointWriter(settings.checkpoint_file,
                                       load_private_key(settings.checkpoint_private_key),
                                       settings.checkpoint_every)
    return AuditSink(settings, consumer, producer, lambda: read_chain(settings), checkpoints)


def run(settings: Settings | None = None, stop_event: threading.Event | None = None) -> None:
    """Append one chained audit entry per gate output, exactly once, until stop_event is set."""
    settings = settings or Settings.from_env()
    stop_event = stop_event or threading.Event()
    sink = build_sink(settings)
    try:
        sink.start()
        while not stop_event.is_set():
            sink.step()
    finally:
        sink.close()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    try:
        run(stop_event=stop)
    except SinkFenced as exc:
        log.error("%s", exc)
        return 3                                  # distinct exit code: do not restart automatically
    return 0


if __name__ == "__main__":
    sys.exit(main())
