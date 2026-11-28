"""Chat API: one HTTP endpoint for the demo chatbot, with and without the safety gate.

    POST /direct/chat      {"message": "..."}   straight to the model, no checks
    POST /protected/chat   {"message": "..."}   through Kafka: raw topic -> gate -> model service
    GET  /healthz
    GET  /                 the demo console (see console.py for its /api/... routes)

Both answer with the same shape:

    {"status": "answered" | "blocked" | "rejected" | "error" | "timeout",
     "answer": "...", "reasons": [...], "request_id": "...", "latency_ms": 123}

Protected mode turns Kafka into a normal request and answer: the request is
written to the raw topic with its request id as the key, and a background
consumer waits for a message with that key on the responses topic (answered by
the model) or the dead-letter topic (stopped by the gate).

    python -m safety_gate.chat_api          # port CHAT_API_PORT, default 8080
"""
from __future__ import annotations

import json
import logging
import signal
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from confluent_kafka import Consumer, Producer, TopicPartition

from safety_gate.config import Settings
from safety_gate.console import ActivityLog, Console, KillSwitchControl, page
from safety_gate.control import SwitchWatcher
from safety_gate.kafka_io import consumer_config, reader_config
from safety_gate.llm import ModelConfig, ModelError, build_backend
from safety_gate.switchctl import send_command

log = logging.getLogger("chat")

MAX_BODY_BYTES = 64 * 1024
BLOCKED_PREFIX = "[blocked by safety gate"


def blocked_answer(reasons: list[str]) -> str:
    return f"{BLOCKED_PREFIX}: {', '.join(reasons) or 'no reason given'}]"


@dataclass
class Waiter:
    done: threading.Event = field(default_factory=threading.Event)
    result: dict | None = None


class ReplyRouter:
    """Matches answers arriving on Kafka to the HTTP requests waiting for them."""

    def __init__(self, consumer, responses_topic: str, dlq_topic: str,
                 on_message: Callable[[bool, str | None, dict], None] | None = None) -> None:
        self.consumer = consumer
        self.on_message = on_message          # sees every message, waited for or not (the console's feed)
        self.responses_topic = responses_topic
        self.dlq_topic = dlq_topic
        self.ready = threading.Event()
        self._waiting: dict[str, Waiter] = {}
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # --- life cycle ---------------------------------------------------------
    def start(self, timeout: float = 30.0) -> None:
        self.consumer.subscribe([self.responses_topic, self.dlq_topic], on_assign=self._on_assign)
        self._thread = threading.Thread(target=self._loop, name="reply-router", daemon=True)
        self._thread.start()
        if not self.ready.wait(timeout):
            raise RuntimeError("could not join the reply topics in time")

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        self.consumer.close()

    def _on_assign(self, consumer, partitions) -> None:
        # Start at the current end of each partition, fixed now: anything written
        # after this moment is seen, nothing older is replayed.
        for tp in partitions:
            low, high = consumer.get_watermark_offsets(TopicPartition(tp.topic, tp.partition), timeout=10, cached=False)
            tp.offset = high
        consumer.assign(partitions)
        self.ready.set()

    def _loop(self) -> None:
        while not self._stop.is_set():
            msg = self.consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                log.warning("reply consumer: %s", msg.error())
                continue
            self.deliver(msg)

    # --- matching -------------------------------------------------------------
    def expect(self, request_id: str) -> Waiter:
        waiter = Waiter()
        with self._lock:
            self._waiting[request_id] = waiter
        return waiter

    def forget(self, request_id: str) -> None:
        with self._lock:
            self._waiting.pop(request_id, None)

    def deliver(self, msg) -> bool:
        """Hand one Kafka message to the request waiting for it. False if nobody waits."""
        key = msg.key()
        request_id = key.decode("utf-8", errors="replace") if isinstance(key, bytes) else key
        try:
            payload = json.loads(msg.value())
        except (TypeError, ValueError):
            payload = {}
        payload = payload if isinstance(payload, dict) else {}
        from_dlq = msg.topic() == self.dlq_topic
        if self.on_message is not None:
            try:
                self.on_message(from_dlq, request_id, payload)
            except Exception:
                log.exception("console feed failed")
        with self._lock:
            waiter = self._waiting.pop(request_id, None) if request_id else None
        if waiter is None:
            return False
        waiter.result = to_result(from_dlq, payload)
        waiter.done.set()
        return True


def to_result(from_dlq: bool, payload: dict) -> dict:
    """Translate a gate or model service message into the API's answer."""
    if from_dlq:
        reasons = [str(r) for r in payload.get("reasons", [])]
        return {"status": "blocked", "answer": blocked_answer(reasons), "reasons": reasons}
    status = payload.get("status")
    if status == "OK":
        return {"status": "answered", "answer": str(payload.get("answer", "")), "reasons": []}
    if status == "REJECTED":
        reason = str(payload.get("reason", "kill_switch"))
        return {"status": "rejected", "answer": blocked_answer([reason]), "reasons": [reason]}
    return {"status": "error", "answer": "(the model service could not answer)",
            "reasons": [str(payload.get("error", "unknown error"))]}


class ChatService:
    """The two ways to reach the demo chatbot. HTTP is a thin layer on top."""

    def __init__(self, settings: Settings, direct: Callable[[str], str], producer=None,
                 router: ReplyRouter | None = None, timeout_s: float = 60.0, tenant_id: str = "t-demo",
                 console: Console | None = None) -> None:
        self.settings = settings
        self.console = console or Console()
        self.direct_model = direct
        self.producer = producer
        self.router = router
        self.timeout_s = timeout_s
        self.tenant_id = tenant_id

    def direct(self, message: str) -> dict:
        try:
            return {"status": "answered", "answer": self.direct_model(message), "reasons": []}
        except ModelError as exc:
            return {"status": "error", "answer": "(the model could not answer)", "reasons": [str(exc)]}

    def protected(self, message: str) -> dict:
        if self.producer is None or self.router is None:
            return {"status": "error", "answer": "(protected mode is not connected to Kafka)", "reasons": []}
        request_id = f"chat-{uuid.uuid4()}"
        waiter = self.router.expect(request_id)            # register BEFORE sending: no race
        body = {"request_id": request_id, "tenant_id": self.tenant_id, "prompt": message}
        try:
            self.producer.produce(self.settings.topics.raw, key=request_id,
                                  value=json.dumps(body, ensure_ascii=False).encode("utf-8"))
            if self.producer.flush(10) != 0:
                raise RuntimeError("the broker did not confirm the request")
        except Exception as exc:
            self.router.forget(request_id)
            return {"status": "error", "answer": "(could not hand the request to the gate)",
                    "reasons": [type(exc).__name__], "request_id": request_id}
        if not waiter.done.wait(self.timeout_s):
            self.router.forget(request_id)
            return {"status": "timeout", "answer": f"(no answer within {self.timeout_s:.0f} s)",
                    "reasons": [], "request_id": request_id}
        return {**(waiter.result or {}), "request_id": request_id}

    def handle(self, mode: str, message: str) -> dict:
        started = time.perf_counter()
        result = self.direct(message) if mode == "direct" else self.protected(message)
        return {**result, "mode": mode, "latency_ms": round((time.perf_counter() - started) * 1000)}


def make_handler(service: ChatService):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:   # noqa: N802 (name required by http.server)
            body = self._read_json()
            if body is _BAD:
                return
            mode = {"/direct/chat": "direct", "/protected/chat": "protected"}.get(self.path)
            if mode is None:
                handled = service.console.post(self.path, body)
                if handled is None:
                    return self._send(404, {"error": "use POST /direct/chat or POST /protected/chat"})
                return self._send(*handled)
            message = body.get("message") if isinstance(body, dict) else None
            if not isinstance(message, str) or not message.strip():
                return self._send(400, {"error": 'send {"message": "<text>"}'})
            self._send(200, service.handle(mode, message))

        def do_GET(self) -> None:    # noqa: N802
            if self.path in ("/", "/index.html"):
                return self._send_bytes(200, page(), "text/html; charset=utf-8")
            if self.path == "/healthz":
                ok = service.router is None or service.router.ready.is_set()
                return self._send(200 if ok else 503, {"status": "ok" if ok else "starting"})
            handled = service.console.get(self.path)
            if handled is None:
                return self._send(404, {"error": "not found"})
            self._send(*handled)

        def _read_json(self):
            length = int(self.headers.get("Content-Length") or 0)
            if length <= 0 or length > MAX_BODY_BYTES:
                self._send(400, {"error": f"body must be 1 to {MAX_BODY_BYTES} bytes of JSON"})
                return _BAD
            try:
                return json.loads(self.rfile.read(length))
            except ValueError:
                self._send(400, {"error": "body is not valid JSON"})
                return _BAD

        def _send(self, status: int, payload: dict) -> None:
            self._send_bytes(status, json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                             "application/json; charset=utf-8")

        def _send_bytes(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            # the page loads nothing from outside: no scripts, fonts or images from other sites
            self.send_header("Content-Security-Policy",
                             "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; "
                             "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    return Handler


_BAD = object()


def serve(service: ChatService, port: int, host: str = "0.0.0.0") -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(service))
    server.daemon_threads = True
    threading.Thread(target=server.serve_forever, name="chat-http", daemon=True).start()
    return server


def build_console(settings: Settings, metrics_url: str | None, summaries_dir: Path | None,
                  model_name: str = "") -> Console:
    watcher = SwitchWatcher(Consumer(reader_config(settings, "console-control")), settings.topics.control)

    def read_state() -> dict:
        switch = watcher.refresh()
        return {"disabled": switch.disabled, "reason": switch.reason, "last_reason": switch.last_reason}

    def send(mode: str, reason: str, issued_by: str) -> None:
        send_command(settings, mode, reason, issued_by)

    return Console(ActivityLog(), metrics_url, summaries_dir, KillSwitchControl(read_state, send), model_name)


def build_service(settings: Settings, model: ModelConfig | None = None, timeout_s: float = 60.0,
                  metrics_url: str | None = None, summaries_dir: Path | None = None) -> ChatService:
    model = model or ModelConfig.from_env()
    backend = build_backend(model)
    log.info("model backend: %s", backend.name)
    console = build_console(settings, metrics_url, summaries_dir, backend.name)
    consumer = Consumer(consumer_config(
        settings, f"chat-api-{uuid.uuid4().hex[:8]}", **{"isolation.level": "read_committed"}))
    router = ReplyRouter(consumer, settings.topics.responses, settings.topics.dlq, console.activity.record)
    producer = Producer({**settings.client_config(), "acks": "all", "enable.idempotence": True, "linger.ms": 0})
    return ChatService(settings, lambda text: backend.reply(model.system_prompt, text),
                       producer, router, timeout_s, console=console)


def main() -> None:
    import os
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    port = int(os.environ.get("CHAT_API_PORT", "8080"))
    summaries = os.environ.get("REDTEAM_SUMMARIES_DIR", "redteam/summaries")
    service = build_service(settings, timeout_s=float(os.environ.get("CHAT_TIMEOUT_S", "60")),
                            metrics_url=os.environ.get("GATE_METRICS_URL") or None,
                            summaries_dir=Path(summaries) if summaries else None)
    assert service.router is not None
    service.router.start()
    server = serve(service, port)
    log.info("chat API and console on :%d (open http://localhost:%d/)", port, port)
    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    stop.wait()
    server.shutdown()
    service.router.stop()


if __name__ == "__main__":
    main()
