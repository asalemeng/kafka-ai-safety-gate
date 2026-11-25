"""Recognising a request the client sent twice (same tenant, same request_id).

A request that was forwarded to the model is remembered. A second copy is not
forwarded again; it goes to the DLQ with the reason "duplicate".

The memory is a compacted Kafka topic with the same number of partitions as the
raw topic. The gate writes the id in the same transaction that forwards the
request, so "forwarded" and "remembered" always happen together. After a
restart or a rebalance the new owner of a partition reads that partition's ids
back before it decides anything.

Limit: two copies are only recognised if they land on the same raw partition,
so clients must use the request id as the message key.
"""
from __future__ import annotations

from collections import OrderedDict
from collections.abc import Callable
from typing import Any

from safety_gate.kafka_io import read_to_end


def dedup_key(request: dict) -> str:
    return f"{request['tenant_id']}\x1f{request['request_id']}"


class DedupStore:
    def __init__(self, topic: str, make_reader: Callable[[], Any], max_ids: int = 100_000) -> None:
        self.topic = topic
        self.make_reader = make_reader
        self.max_ids = max_ids
        self._ids: dict[int, OrderedDict[str, None]] = {}

    def ensure_loaded(self, partition: int) -> None:
        """Read the remembered ids of one partition, once per ownership."""
        if partition in self._ids:
            return
        ids: OrderedDict[str, None] = OrderedDict()
        reader = self.make_reader()
        try:
            for msg in read_to_end(reader, self.topic, partition):
                key = msg.key()
                if key is None:
                    continue
                key = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
                if msg.value() is None:          # tombstone
                    ids.pop(key, None)
                else:
                    self._remember(ids, key)
        finally:
            reader.close()
        self._ids[partition] = ids

    def forget_partitions(self, partitions) -> None:
        """Called when partitions are taken away: another gate owns their memory now."""
        for partition in partitions:
            self._ids.pop(partition, None)

    def seen(self, partition: int, key: str) -> bool:
        return key in self._ids.get(partition, ())

    def remember(self, partition: int, key: str) -> None:
        self._remember(self._ids.setdefault(partition, OrderedDict()), key)

    def _remember(self, ids: OrderedDict, key: str) -> None:
        ids[key] = None
        ids.move_to_end(key)
        while len(ids) > self.max_ids:
            ids.popitem(last=False)
