"""The gate: reads raw requests, decides, and writes each one to the clean topic or the DLQ.

Exactly once: the outputs of a batch and the "I have read up to here" offsets
are written in one Kafka transaction. Either all of it happens or none of it.
"""
from __future__ import annotations

import hashlib
import json
import logging
import signal
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from confluent_kafka import OFFSET_BEGINNING, Consumer, Producer, TopicPartition

from safety_gate.config import Settings
from safety_gate.control import SwitchWatcher
from safety_gate.dedup import DedupStore, dedup_key
from safety_gate.engine import Engine
from safety_gate.kafka_io import can_abort_and_continue, consumer_config, reader_config, transactional_producer_config
from safety_gate.metrics import GateMetrics, serve
from safety_gate.policy import PolicyError, PolicyStore

log = logging.getLogger("gate")

MAX_RESTART_DELAY_S = 30.0


class RetryBatch(Exception):
    """Checking one request failed; the whole batch is rolled back and tried again."""


@dataclass(frozen=True)
class Output:
    topic: str
    key: str | None
    payload: dict
    verdict: str
    reasons: tuple[str, ...] = ()
    headers: tuple[tuple[str, bytes], ...] = ()
    remember: str | None = None          # request id to store for duplicate detection


class Gate:
    """One gate instance. The Kafka clients are passed in, so tests can use stand-ins."""

    def __init__(
        self,
        settings: Settings,
        consumer,
        producer,
        watcher: SwitchWatcher,
        engine: Engine | None = None,
        dedup: DedupStore | None = None,
        metrics: GateMetrics | None = None,
        before_commit: Callable | None = None,
        attempts: dict | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.settings = settings
        self.topics = settings.topics
        self.consumer = consumer
        self.producer = producer
        self.watcher = watcher
        self.engine = engine or Engine()
        self.dedup = dedup
        self.metrics = metrics or GateMetrics()
        self.before_commit = before_commit      # test hook: called between the write and the commit
        self.attempts = attempts if attempts is not None else {}   # (partition, offset) -> failed tries
        self.sleep = sleep
        self._batch_ids: set[tuple[int, str]] = set()

    # --- life cycle ---------------------------------------------------------
    def start(self) -> None:
        self.producer.init_transactions(30)       # seconds; fail loudly instead of hanging
        switch = self.watcher.refresh()           # fail closed: no known switch state, no start
        self.metrics.kill_switch.set(1 if switch.disabled else 0)
        self.consumer.subscribe([self.topics.raw], on_assign=self._on_assign,
                                on_revoke=self._on_revoke, on_lost=self._on_revoke)
        log.info("gate %s started (exactly-once, kill switch %s, policy %s): %s -> %s | %s",
                 self.settings.instance_id, "ON" if switch.disabled else "off", self.engine.policy_version,
                 self.topics.raw, self.topics.clean, self.topics.dlq)

    def close(self) -> None:
        self.metrics.stopped()
        for client in (self.consumer, self.watcher.consumer):
            try:
                client.close()
            except Exception:
                log.exception("error while closing a Kafka client")
        log.info("gate %s stopped", self.settings.instance_id)

    def run(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            self.step()

    def _on_assign(self, _consumer, partitions) -> None:
        if self.dedup is not None:                # read the remembered ids again before deciding
            self.dedup.forget_partitions(tp.partition for tp in partitions)

    def _on_revoke(self, _consumer, partitions) -> None:
        if self.dedup is not None:
            self.dedup.forget_partitions(tp.partition for tp in partitions)

    # --- one batch ----------------------------------------------------------
    def step(self) -> int:
        """Read one batch and handle it in one transaction. Returns how many requests were decided."""
        self.metrics.heartbeat()
        messages = self._read_batch()
        if not messages:
            return 0

        switch = self.watcher.refresh_if_older(self.settings.switch_check_s)
        self.metrics.kill_switch.set(1 if switch.disabled else 0)
        self._batch_ids = set()
        if self.dedup is not None and not switch.disabled:
            # A failure to read the remembered ids is a Kafka problem, not a bad
            # request: it stops this gate (nothing was committed, nothing is lost).
            for partition in {msg.partition() for msg in messages}:
                self.dedup.ensure_loaded(partition)

        self.producer.begin_transaction()
        try:
            outputs = [self.decide(msg) for msg in messages]
            for msg, output in zip(messages, outputs, strict=True):
                self._write(msg, output)
            if self.before_commit:
                for msg in messages:
                    self.before_commit(msg)
            self.producer.send_offsets_to_transaction(
                self._next_offsets(messages), self.consumer.consumer_group_metadata())
            self.producer.commit_transaction()
        except RetryBatch as exc:
            log.warning("%s; rolling the batch back and trying again", exc)
            self._roll_back()
            self.metrics.retries.inc()
            self.sleep(0.1)
            return 0
        except Exception as exc:
            if can_abort_and_continue(exc):
                log.warning("transaction failed (%s); rolling back and reading the batch again", exc)
                self._roll_back()
                return 0
            log.exception("transaction failed, aborting")
            self._abort_quietly()
            raise

        self._after_commit(messages, outputs)
        return len(messages)

    def _read_batch(self) -> list:
        first = self.consumer.poll(1.0)
        if first is None:
            return []
        batch = [first]
        if self.settings.batch_size > 1:
            batch += self.consumer.consume(self.settings.batch_size - 1, timeout=self.settings.batch_linger_s)
        messages = []
        for msg in batch:
            if msg.error():
                log.error("consumer error: %s", msg.error())
            else:
                messages.append(msg)
        return messages

    def _write(self, msg, output: Output) -> None:
        self.producer.produce(
            output.topic,
            key=output.key,
            value=json.dumps(output.payload, ensure_ascii=False).encode("utf-8"),
            headers=list(output.headers),
        )
        if output.remember is not None:
            self.producer.produce(self.topics.dedup, partition=msg.partition(),
                                  key=output.remember, value=str(int(time.time())).encode("ascii"))

    def _next_offsets(self, messages) -> list[TopicPartition]:
        latest: dict[int, int] = {}
        for msg in messages:
            latest[msg.partition()] = max(latest.get(msg.partition(), -1), msg.offset())
        return [TopicPartition(self.topics.raw, partition, offset + 1) for partition, offset in latest.items()]

    def _after_commit(self, messages, outputs) -> None:
        now = time.time()
        self.metrics.batch_size.observe(len(messages))
        for msg, output in zip(messages, outputs, strict=True):
            self.attempts.pop((msg.partition(), msg.offset()), None)
            if output.remember is not None and self.dedup is not None:
                self.dedup.remember(msg.partition(), output.remember)
            kind, stamp_ms = msg.timestamp()
            if kind != 0 and stamp_ms > 0:
                self.metrics.latency_seconds.observe(max(0.0, now - stamp_ms / 1000))
            log.debug("%s -> %s key=%s", self.topics.raw, output.topic, output.key)
        for tp in self._next_offsets(messages):
            try:
                marks = self.consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), cached=True)
            except Exception:
                marks = None
            if marks is not None and marks[1] >= 0:
                self.metrics.lag.labels(str(tp.partition)).set(max(0, marks[1] - tp.offset))
        self.metrics.policy_errors.set(self.engine.store.reload_errors)

    # --- failure handling ---------------------------------------------------
    def _abort_quietly(self) -> None:
        try:
            self.producer.abort_transaction()
        except Exception:
            log.exception("could not abort the transaction (the broker will time it out)")

    def _roll_back(self) -> None:
        """Abort the transaction and go back to the last committed position.

        If going back is not possible this raises: continuing would skip the
        requests of the failed batch.
        """
        self.metrics.aborted.inc()
        self.producer.abort_transaction()
        assignment = self.consumer.assignment()
        if not assignment:
            return
        for tp in self.consumer.committed(assignment, timeout=10):
            if tp.offset < 0:                     # nothing committed yet: start of the partition
                tp = TopicPartition(tp.topic, tp.partition, OFFSET_BEGINNING)
            self.consumer.seek(tp)

    # --- the decision for one request ---------------------------------------
    def decide(self, msg) -> Output:
        started = time.perf_counter()
        raw = msg.value() or b""
        try:
            output = self._decide(msg.partition(), raw)
        except Exception as exc:
            key = (msg.partition(), msg.offset())
            tries = self.attempts[key] = self.attempts.get(key, 0) + 1
            if tries < self.settings.max_attempts:
                raise RetryBatch(
                    f"check failed for partition {key[0]} offset {key[1]} "
                    f"(attempt {tries} of {self.settings.max_attempts}): {type(exc).__name__}: {exc}"
                ) from exc
            log.error("giving up on partition %d offset %d after %d attempts: %s", *key, tries, exc)
            output = self._poison(raw, exc, tries)
        self.metrics.record_decision(output.verdict, output.reasons, time.perf_counter() - started)
        return output

    def _decide(self, partition: int, raw: bytes) -> Output:
        if self.watcher.switch.disabled:
            return self._reject_all(raw, self.watcher.switch.reason)

        try:
            request = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            payload = {"verdict": "BLOCK", "reasons": ["malformed_json"],
                       "raw": self.engine.for_review(raw.decode("utf-8", errors="replace"))}
            return Output(self.topics.dlq, None, payload, "BLOCK", ("malformed_json",), self._headers())

        decision = self.engine.evaluate(request)
        if decision.verdict != "PASS":
            return self._blocked(request, decision.reasons, decision.policy_version, decision.details)

        remember = None
        if self.dedup is not None:
            key = dedup_key(request)
            if self.dedup.seen(partition, key) or (partition, key) in self._batch_ids:
                return self._blocked(request, ("duplicate",), decision.policy_version, {})
            self._batch_ids.add((partition, key))
            remember = key

        headers = self._headers(decision.policy_version)
        if decision.redactions:
            headers += (("gate-redactions", ",".join(decision.redactions).encode("utf-8")),)
        assert decision.forward is not None
        return Output(self.topics.clean, request["request_id"], decision.forward, "PASS", (), headers, remember)

    def _headers(self, policy_version: str | None = None) -> tuple[tuple[str, bytes], ...]:
        version = policy_version or self.engine.policy_version
        return (("gate-instance", self.settings.instance_id.encode("utf-8")),
                ("policy-version", version.encode("utf-8")))

    @staticmethod
    def _key(request) -> str | None:
        key = request.get("request_id") if isinstance(request, dict) else None
        return key if isinstance(key, str) else None

    def _blocked(self, request, reasons: tuple[str, ...], policy_version: str, details: dict) -> Output:
        payload = {"verdict": "BLOCK", "reasons": list(reasons),
                   "request": self.engine.for_review(request, reasons), "policy_version": policy_version}
        if details:
            payload["details"] = details
        return Output(self.topics.dlq, self._key(request), payload, "BLOCK", reasons, self._headers(policy_version))

    def _reject_all(self, raw: bytes, why: str) -> Output:
        """Kill switch is on: straight to the DLQ, the checks are not even run."""
        payload: dict = {"verdict": "BLOCK", "reasons": ["kill_switch"], "kill_switch_reason": why}
        try:
            request = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            payload["raw"] = self.engine.for_review(raw.decode("utf-8", errors="replace"))
            return Output(self.topics.dlq, None, payload, "BLOCK", ("kill_switch",), self._headers())
        payload["request"] = self.engine.for_review(request)
        return Output(self.topics.dlq, self._key(request), payload, "BLOCK", ("kill_switch",), self._headers())

    def _poison(self, raw: bytes, exc: Exception, tries: int) -> Output:
        """A request the gate cannot check is never forwarded. It is parked in the DLQ."""
        payload: dict = {"verdict": "BLOCK", "reasons": ["processing_error"],
                         "error": type(exc).__name__, "attempts": tries,
                         "raw_sha256": hashlib.sha256(raw).hexdigest()}
        try:
            payload["raw"] = self.engine.for_review(raw.decode("utf-8", errors="replace"))
        except Exception:
            pass                                  # masking failed too: keep the hash only
        return Output(self.topics.dlq, None, payload, "BLOCK", ("processing_error",), self._headers())


# --- wiring ------------------------------------------------------------------
def transactional_id(settings: Settings) -> str:
    """Unique per running gate.

    Two gates sharing one id would cut each other off ("fencing"). Stale work
    after a rebalance is still rejected: Kafka checks the consumer group
    generation that is sent along with the offsets.
    """
    return f"gate-{settings.group_id}-{settings.instance_id}"


def build_gate(
    settings: Settings,
    before_commit: Callable | None = None,
    attempts: dict | None = None,
    metrics: GateMetrics | None = None,
    engine: Engine | None = None,
) -> Gate:
    """Create the Kafka clients for one gate instance."""
    engine = engine or Engine(PolicyStore(settings.policy_path))
    consumer = Consumer(consumer_config(settings, settings.group_id))
    control = Consumer(reader_config(settings, "gate-control"))
    producer = Producer(transactional_producer_config(settings, transactional_id(settings)))
    dedup = None
    if settings.dedup_enabled:
        dedup = DedupStore(settings.topics.dedup,
                           lambda: Consumer(reader_config(settings, "gate-dedup")), settings.dedup_max_ids)
    watcher = SwitchWatcher(control, settings.topics.control)
    return Gate(settings, consumer, producer, watcher, engine, dedup, metrics, before_commit, attempts)


def run(
    settings: Settings | None = None,
    stop_event: threading.Event | None = None,
    before_commit: Callable | None = None,
    attempts: dict | None = None,
    metrics: GateMetrics | None = None,
    engine: Engine | None = None,
) -> None:
    """Run one gate until stop_event is set. Any unexpected failure is raised."""
    settings = settings or Settings.from_env()
    stop_event = stop_event or threading.Event()
    gate = build_gate(settings, before_commit, attempts, metrics, engine)
    try:
        gate.start()
        gate.run(stop_event)
    finally:
        gate.close()


def run_forever(
    settings: Settings,
    stop_event: threading.Event,
    metrics: GateMetrics | None = None,
    engine: Engine | None = None,
    sleep: Callable[[float], None] | None = None,
) -> None:
    """Keep a gate running: after a failure, wait a little and start a new one.

    The count of failed tries per request survives the restart, so a request
    that makes the gate fail again and again is parked in the DLQ after
    `max_attempts` tries instead of blocking the partition for ever.
    """
    metrics = metrics or GateMetrics()
    attempts: dict = {}
    delay = 1.0
    wait = sleep or (lambda seconds: stop_event.wait(seconds))
    while not stop_event.is_set():
        started = time.monotonic()
        try:
            run(settings, stop_event, attempts=attempts, metrics=metrics, engine=engine)
        except PolicyError:
            raise                                  # a broken policy file needs a human, not a retry
        except Exception:
            if time.monotonic() - started > 60:
                delay = 1.0                        # it ran fine for a while: start the back-off again
            metrics.restarts.inc()
            log.exception("gate failed; restarting in %.0f s", delay)
            wait(delay)
            delay = min(delay * 2, MAX_RESTART_DELAY_S)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    metrics = GateMetrics()
    if settings.metrics_port is not None:
        serve(settings.metrics_port, metrics)
        log.info("metrics on :%d/metrics, health on :%d/healthz", settings.metrics_port, settings.metrics_port)
    run_forever(settings, stop, metrics)


if __name__ == "__main__":
    main()
