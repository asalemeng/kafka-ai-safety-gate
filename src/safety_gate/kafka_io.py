"""Small helpers around the Kafka client, shared by all components."""
from __future__ import annotations

import time
import uuid
from collections.abc import Iterator

from confluent_kafka import OFFSET_BEGINNING, KafkaError, KafkaException, TopicPartition

from safety_gate.config import Settings

_FENCED_CODES = {
    KafkaError._FENCED,
    KafkaError.PRODUCER_FENCED,
    KafkaError.INVALID_PRODUCER_EPOCH,
    KafkaError.FENCED_INSTANCE_ID,
}


def consumer_config(settings: Settings, group_id: str, **extra) -> dict:
    return {
        **settings.client_config(),
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        **extra,
    }


def reader_config(settings: Settings, name: str) -> dict:
    """A consumer that reads one partition from the start to its end and never commits."""
    return consumer_config(
        settings, f"{name}-{uuid.uuid4()}",
        **{"isolation.level": "read_committed", "enable.partition.eof": True},
    )


def transactional_producer_config(settings: Settings, transactional_id: str) -> dict:
    return {
        **settings.client_config(),
        "transactional.id": transactional_id,
        "enable.idempotence": True,
        "transaction.timeout.ms": int(settings.transaction_timeout_s * 1000),
    }


def kafka_error(exc: BaseException) -> KafkaError | None:
    if isinstance(exc, KafkaException) and exc.args and isinstance(exc.args[0], KafkaError):
        return exc.args[0]
    return None


def is_fenced(exc: BaseException) -> bool:
    """True if a newer producer with the same transactional id has taken over."""
    err = kafka_error(exc)
    if err is None:
        return False
    # The client reports fencing as a fatal error; the exact code differs
    # between the calls that notice it, so the message is checked as well.
    return err.code() in _FENCED_CODES or (err.fatal() and "fenc" in (err.str() or "").lower())


def can_abort_and_continue(exc: BaseException) -> bool:
    """True if the transaction failed but this producer may abort it and carry on."""
    err = kafka_error(exc)
    return err is not None and not err.fatal() and not is_fenced(exc)


def read_to_end(consumer, topic: str, partition: int = 0, idle_timeout: float = 30.0) -> Iterator:
    """Yield every committed message of one partition, from the start to its current end.

    The consumer must be built with `reader_config`. "End" is what the broker
    reports, not a guess based on silence: the read stops when the position
    reaches the end offset asked for at the start, or when the broker signals
    "end of partition". If the broker stays silent for `idle_timeout` seconds
    the read fails instead of returning a partial result.
    """
    marks = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=idle_timeout, cached=False)
    if marks is None:
        raise RuntimeError(f"could not ask for the end of {topic}[{partition}]")
    low, end = marks
    if end <= max(low, 0):
        return                                    # nothing in the partition

    consumer.assign([TopicPartition(topic, partition, OFFSET_BEGINNING)])
    deadline = time.monotonic() + idle_timeout
    while True:
        msg = consumer.poll(0.5)
        if msg is None:
            # Transaction markers take up offsets but are never delivered, so
            # the position can reach the end without a last message arriving.
            position = consumer.position([TopicPartition(topic, partition)])[0].offset
            if position >= end:
                return
            if time.monotonic() > deadline:
                raise RuntimeError(f"could not read {topic}[{partition}] to its end within {idle_timeout:.0f}s")
            continue
        error = msg.error()
        if error is not None:
            if error.code() == KafkaError._PARTITION_EOF:
                return
            if error.fatal():
                raise KafkaException(error)
            continue
        deadline = time.monotonic() + idle_timeout
        yield msg
        if msg.offset() + 1 >= end:
            return
