"""A stand-in for the model service, to show the kill switch is honoured after the gate too.

Why it matters: a request can already be in the clean topic at the moment an
operator pulls the switch. The gate cannot take it back. So the service that
calls the model reads the same control topic and refuses to call the model
while the switch is on.

This is a stub. It "answers" by echoing, and it delivers at least once (a
response can be repeated after a crash). A real model service would replace
`call_model` and keep the kill switch check exactly as it is here.
"""
from __future__ import annotations

import json
import logging
import signal
import threading
from collections.abc import Callable

from confluent_kafka import Consumer, Producer

from safety_gate.config import Settings
from safety_gate.control import SwitchWatcher
from safety_gate.kafka_io import consumer_config, reader_config

log = logging.getLogger("model")


def echo_model(request: dict) -> str:
    return f"(stub answer to request {request.get('request_id')})"


class ModelService:
    def __init__(self, settings: Settings, consumer, producer, watcher: SwitchWatcher,
                 call_model: Callable[[dict], str] = echo_model) -> None:
        self.settings = settings
        self.topics = settings.topics
        self.consumer = consumer
        self.producer = producer
        self.watcher = watcher
        self.call_model = call_model

    def start(self) -> None:
        self.watcher.refresh()                    # fail closed, like the gate
        self.consumer.subscribe([self.topics.clean])
        log.info("model service stub started (kill switch %s)", "ON" if self.watcher.switch.disabled else "off")

    def step(self) -> int:
        msg = self.consumer.poll(1.0)
        if msg is None or msg.error():
            return 0
        switch = self.watcher.refresh()           # last line of defence: checked for every single request
        try:
            request = json.loads(msg.value())
        except ValueError:
            request = {}
        if not isinstance(request, dict):
            request = {}

        response: dict = {"request_id": request.get("request_id")}
        if switch.disabled:
            response.update(status="REJECTED", reason="kill_switch", kill_switch_reason=switch.reason)
        else:
            response.update(status="OK", answer=self.call_model(request))

        self.producer.produce(self.topics.responses, key=msg.key(),
                              value=json.dumps(response, ensure_ascii=False).encode("utf-8"))
        if self.producer.flush(15) != 0:
            raise RuntimeError("response was not confirmed by the broker")
        self.consumer.commit(message=msg, asynchronous=False)
        return 1

    def close(self) -> None:
        for client in (self.consumer, self.watcher.consumer):
            try:
                client.close()
            except Exception:
                log.exception("error while closing a Kafka client")


def build_service(settings: Settings, call_model: Callable[[dict], str] = echo_model) -> ModelService:
    consumer = Consumer(consumer_config(
        settings, settings.model_group_id, **{"isolation.level": "read_committed"}))
    producer = Producer({**settings.client_config(), "acks": "all", "enable.idempotence": True})
    watcher = SwitchWatcher(Consumer(reader_config(settings, "model-control")), settings.topics.control)
    return ModelService(settings, consumer, producer, watcher, call_model)


def run(settings: Settings | None = None, stop_event: threading.Event | None = None,
        call_model: Callable[[dict], str] = echo_model) -> None:
    settings = settings or Settings.from_env()
    stop_event = stop_event or threading.Event()
    service = build_service(settings, call_model)
    try:
        service.start()
        while not stop_event.is_set():
            service.step()
    finally:
        service.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    run(stop_event=stop)


if __name__ == "__main__":
    main()
