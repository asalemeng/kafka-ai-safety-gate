"""A tiny in-memory stand-in for Kafka, enough to test the gate's logic without Docker.

It models what the logic depends on: ordered partitions, consumer positions,
committed group offsets, transactions that are all-or-nothing, and fencing of an
older producer with the same transactional id. It does not model timing,
rebalancing or the network; the integration tests cover those on a real broker.
"""
from __future__ import annotations

import zlib

from confluent_kafka import OFFSET_BEGINNING, KafkaError, KafkaException, TopicPartition


class FakeMessage:
    def __init__(self, topic, partition, offset, key, value, headers=None, error=None, timestamp_ms=0):
        self._topic, self._partition, self._offset = topic, partition, offset
        self._key, self._value, self._headers = key, value, headers
        self._error, self._timestamp_ms = error, timestamp_ms

    def topic(self): return self._topic
    def partition(self): return self._partition
    def offset(self): return self._offset
    def key(self): return self._key
    def value(self): return self._value
    def headers(self): return self._headers
    def error(self): return self._error
    def timestamp(self): return (1 if self._timestamp_ms else 0, self._timestamp_ms)


def _bytes(value):
    return value.encode("utf-8") if isinstance(value, str) else value


class FakeBroker:
    def __init__(self, partitions: dict[str, int] | None = None) -> None:
        self.partitions = dict(partitions or {})
        self.logs: dict[tuple[str, int], list[FakeMessage]] = {}
        self.group_offsets: dict[tuple[str, str, int], int] = {}
        self.epochs: dict[str, int] = {}
        self.commits = 0                 # number of committed transactions

    def partition_count(self, topic: str) -> int:
        return self.partitions.setdefault(topic, 1)

    def log(self, topic: str, partition: int = 0) -> list[FakeMessage]:
        return self.logs.setdefault((topic, partition), [])

    def append(self, topic, key=None, value=None, partition=None, headers=None, timestamp_ms=0) -> FakeMessage:
        key, value = _bytes(key), _bytes(value)
        if partition is None:
            partition = zlib.crc32(key) % self.partition_count(topic) if key is not None else 0
        log = self.log(topic, partition)
        msg = FakeMessage(topic, partition, len(log), key, value, headers, timestamp_ms=timestamp_ms)
        log.append(msg)
        return msg

    def values(self, topic: str) -> list[bytes]:
        return [m.value() for p in range(self.partition_count(topic)) for m in self.log(topic, p)]


class FakeConsumer:
    def __init__(self, broker: FakeBroker, group: str = "g", eof: bool = False) -> None:
        self.broker, self.group, self.eof = broker, group, eof
        self.positions: dict[tuple[str, int], int] = {}
        self._eof_sent: set[tuple[str, int]] = set()
        self.closed = False
        self.fail_watermarks = False

    def subscribe(self, topics, on_assign=None, on_revoke=None, on_lost=None) -> None:
        assigned = []
        for topic in topics:
            for partition in range(self.broker.partition_count(topic)):
                self.positions[(topic, partition)] = self.broker.group_offsets.get((self.group, topic, partition), 0)
                assigned.append(TopicPartition(topic, partition))
        if on_assign:
            on_assign(self, assigned)

    def assign(self, partitions) -> None:
        self.positions = {(tp.topic, tp.partition): 0 if tp.offset in (OFFSET_BEGINNING, -1001) else tp.offset
                          for tp in partitions}
        self._eof_sent.clear()

    def poll(self, timeout=None):
        for (topic, partition), position in self.positions.items():
            log = self.broker.log(topic, partition)
            if position < len(log):
                self.positions[(topic, partition)] = position + 1
                self._eof_sent.discard((topic, partition))
                return log[position]
        if self.eof:
            for source in self.positions:
                if source not in self._eof_sent:
                    self._eof_sent.add(source)
                    return FakeMessage(*source, self.positions[source], None, None,
                                       error=KafkaError(KafkaError._PARTITION_EOF))
        return None

    def consume(self, num_messages=1, timeout=None) -> list:
        out = []
        while len(out) < num_messages:
            msg = self.poll(0)
            if msg is None:
                break
            out.append(msg)
        return out

    def get_watermark_offsets(self, partition, timeout=None, cached=False):
        if self.fail_watermarks:
            return None
        return 0, len(self.broker.log(partition.topic, partition.partition))

    def position(self, partitions):
        return [TopicPartition(tp.topic, tp.partition, self.positions.get((tp.topic, tp.partition), -1001))
                for tp in partitions]

    def assignment(self):
        return [TopicPartition(t, p) for t, p in self.positions]

    def committed(self, partitions, timeout=None):
        return [TopicPartition(tp.topic, tp.partition,
                               self.broker.group_offsets.get((self.group, tp.topic, tp.partition), -1001))
                for tp in partitions]

    def seek(self, partition) -> None:
        offset = 0 if partition.offset == OFFSET_BEGINNING else partition.offset
        self.positions[(partition.topic, partition.partition)] = offset

    def commit(self, message=None, asynchronous=True) -> None:
        self.broker.group_offsets[(self.group, message.topic(), message.partition())] = message.offset() + 1

    def consumer_group_metadata(self):
        return self.group

    def close(self) -> None:
        self.closed = True


class FakeProducer:
    def __init__(self, broker: FakeBroker, transactional_id: str | None = None) -> None:
        self.broker, self.transactional_id = broker, transactional_id
        self.epoch: int | None = None
        self._buffer: list[dict] | None = None
        self._offsets: list[tuple[str, list]] = []
        self.fail_next_commit: Exception | None = None

    def _check_fenced(self) -> None:
        if self.transactional_id and self.broker.epochs.get(self.transactional_id) != self.epoch:
            raise KafkaException(KafkaError(KafkaError._FENCED, "fenced by a newer producer", fatal=True))

    def init_transactions(self, timeout=None) -> None:
        self.epoch = self.broker.epochs.get(self.transactional_id, 0) + 1
        self.broker.epochs[self.transactional_id] = self.epoch

    def begin_transaction(self) -> None:
        self._check_fenced()
        self._buffer, self._offsets = [], []

    def produce(self, topic, value=None, key=None, partition=None, headers=None, on_delivery=None) -> None:
        record = dict(topic=topic, key=key, value=value, partition=partition, headers=headers)
        if self._buffer is not None:
            self._buffer.append(record)
        else:
            self.broker.append(**record)

    def send_offsets_to_transaction(self, offsets, group_metadata) -> None:
        self._offsets.append((group_metadata, list(offsets)))

    def commit_transaction(self, timeout=None) -> None:
        self._check_fenced()
        if self.fail_next_commit is not None:
            error, self.fail_next_commit = self.fail_next_commit, None
            raise error
        for record in self._buffer or []:
            self.broker.append(**record)
        for group, offsets in self._offsets:
            for tp in offsets:
                self.broker.group_offsets[(group, tp.topic, tp.partition)] = tp.offset
        self._buffer, self._offsets = None, []
        self.broker.commits += 1

    def abort_transaction(self, timeout=None) -> None:
        self._check_fenced()
        self._buffer, self._offsets = None, []

    def flush(self, timeout=None) -> int:
        return 0

    def poll(self, timeout=None) -> int:
        return 0
