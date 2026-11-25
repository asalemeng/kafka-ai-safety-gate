"""Metrics (Prometheus format) and a health check, served over HTTP.

    GET /metrics   numbers for dashboards and alerts
    GET /healthz   200 while the gate is working, 503 when it is stuck or stopped
"""
from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

_LATENCY_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)


class GateMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry()
        r = self.registry
        self.messages = Counter("gate_messages_total", "Requests decided, by verdict", ["verdict"], registry=r)
        self.blocked = Counter("gate_blocked_total", "Blocked requests, by reason", ["reason"], registry=r)
        self.decision_seconds = Histogram(
            "gate_decision_seconds", "Time to check one request", buckets=_LATENCY_BUCKETS, registry=r)
        self.latency_seconds = Histogram(
            "gate_end_to_end_seconds", "From arrival in the raw topic to the committed decision",
            buckets=_LATENCY_BUCKETS, registry=r)
        self.batch_size = Histogram(
            "gate_batch_size", "Requests per Kafka transaction",
            buckets=(1, 2, 5, 10, 25, 50, 100, 250, 500), registry=r)
        self.lag = Gauge("gate_consumer_lag", "Requests waiting in the raw topic", ["partition"], registry=r)
        self.kill_switch = Gauge("gate_kill_switch_on", "1 while the kill switch rejects everything", registry=r)
        self.aborted = Counter("gate_transactions_aborted_total", "Transactions rolled back", registry=r)
        self.retries = Counter("gate_message_retries_total", "Requests whose check failed and was retried", registry=r)
        self.restarts = Counter("gate_restarts_total", "Restarts after an unexpected failure", registry=r)
        self.policy_errors = Gauge(
            "gate_policy_reload_errors", "Rejected policy file versions since start", registry=r)
        self._last_loop = time.monotonic()
        self._running = False

    # --- health -------------------------------------------------------------
    def heartbeat(self) -> None:
        self._last_loop = time.monotonic()
        self._running = True

    def stopped(self) -> None:
        self._running = False

    def health(self, max_silence_s: float = 30.0) -> tuple[bool, dict]:
        silence = time.monotonic() - self._last_loop
        ok = self._running and silence < max_silence_s
        return ok, {"status": "ok" if ok else "unhealthy", "running": self._running,
                    "seconds_since_last_loop": round(silence, 1)}

    # --- recording ----------------------------------------------------------
    def record_decision(self, verdict: str, reasons, seconds: float) -> None:
        self.messages.labels(verdict).inc()
        for reason in reasons:
            self.blocked.labels(reason).inc()
        self.decision_seconds.observe(seconds)


def serve(port: int, metrics: GateMetrics, host: str = "0.0.0.0") -> ThreadingHTTPServer:
    """Start the HTTP endpoint in a background thread. Port 0 picks a free port."""
    health: Callable[[], tuple[bool, dict]] = metrics.health

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:   # noqa: N802 (name required by http.server)
            if self.path == "/metrics":
                self._send(200, generate_latest(metrics.registry), "text/plain; version=0.0.4; charset=utf-8")
            elif self.path == "/healthz":
                ok, body = health()
                self._send(200 if ok else 503, json.dumps(body).encode(), "application/json")
            else:
                self._send(404, b"not found\n", "text/plain")

        def _send(self, status: int, body: bytes, content_type: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args) -> None:
            pass

    server = ThreadingHTTPServer((host, port), Handler)
    threading.Thread(target=server.serve_forever, name="metrics-http", daemon=True).start()
    return server
