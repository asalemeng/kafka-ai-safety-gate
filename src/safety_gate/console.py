"""Demo console: the data behind the web page served by the Chat API.

    GET  /                  the console page
    GET  /api/activity      the latest gate decisions and model answers seen on Kafka
    GET  /api/stats         the gate's own counters (from its /metrics endpoint)
    GET  /api/killswitch    current state of the kill switch
    POST /api/killswitch    {"action": "disable" | "enable", "reason": "..."}
    GET  /api/redteam       red-team summaries, one per model (redteam/summaries/*.json)
    GET  /api/info          which model answers the demo chatbot

The console has no login. It is meant for a laptop demo and is published on
127.0.0.1 only; see the README before exposing it anywhere else.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import urllib.request
from collections import deque
from collections.abc import Callable
from importlib import resources
from pathlib import Path

log = logging.getLogger("console")

ACTIVITY_SIZE = 200
PREVIEW_CHARS = 160
_METRIC = re.compile(r'^(gate_[a-z_]+?)(?:_total)?\{(\w+)="([^"]*)"\}\s+([0-9.eE+-]+)$')
_PLAIN = re.compile(r"^(gate_[a-z_]+)\s+([0-9.eE+-]+)$")


def page() -> bytes:
    return resources.files("safety_gate").joinpath("web", "console.html").read_bytes()


# --- live activity -------------------------------------------------------------------
class ActivityLog:
    """The last decisions seen on the responses and dead-letter topics, newest first.

    Only what the gate already stored is shown: dead-letter entries are masked
    by the gate, and model answers are cut to a short preview.
    """

    def __init__(self, size: int = ACTIVITY_SIZE, clock: Callable[[], float] = time.time) -> None:
        self._items: deque[dict] = deque(maxlen=size)
        self._lock = threading.Lock()
        self.clock = clock

    def record(self, from_dlq: bool, key: str | None, payload: dict) -> None:
        if from_dlq:
            raw_request = payload.get("request")
            request: dict = raw_request if isinstance(raw_request, dict) else {}
            text = request.get("prompt") if isinstance(request.get("prompt"), str) else payload.get("raw")
            item = {"outcome": "blocked", "reasons": [str(r) for r in payload.get("reasons", [])],
                    "text": _preview(text), "policy_version": payload.get("policy_version")}
        else:
            status = payload.get("status")
            outcome = {"OK": "answered", "REJECTED": "rejected"}.get(str(status), "error")
            reasons = [str(payload["reason"])] if payload.get("reason") else []
            item = {"outcome": outcome, "reasons": reasons, "text": _preview(payload.get("answer")),
                    "policy_version": None}
        item.update(time=self.clock(), request_id=key)
        with self._lock:
            self._items.appendleft(item)

    def latest(self, limit: int = 50) -> list[dict]:
        with self._lock:
            return list(self._items)[:max(0, limit)]


def _preview(text) -> str:
    if not isinstance(text, str):
        return ""
    text = " ".join(text.split())
    return text if len(text) <= PREVIEW_CHARS else text[:PREVIEW_CHARS - 1] + "…"


# --- the gate's counters -----------------------------------------------------------------
def parse_metrics(text: str) -> dict:
    """Pick the counters the console shows out of the gate's Prometheus text."""
    verdicts: dict[str, float] = {}
    reasons: dict[str, float] = {}
    single: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        match = _METRIC.match(line.strip())
        if match:
            name, label, value, number = match.groups()
            if name == "gate_messages" and label == "verdict":
                verdicts[value] = float(number)
            elif name == "gate_blocked" and label == "reason":
                reasons[value] = float(number)
            continue
        plain = _PLAIN.match(line.strip())
        if plain:
            single[plain.group(1)] = float(plain.group(2))
    return {
        "decided": int(sum(verdicts.values())),
        "passed": int(verdicts.get("PASS", 0)),
        "blocked": int(verdicts.get("BLOCK", 0)),
        "reasons": {k: int(v) for k, v in sorted(reasons.items(), key=lambda kv: -kv[1])},
        "kill_switch_on": bool(single.get("gate_kill_switch_on", 0)),
        "restarts": int(single.get("gate_restarts_total", 0)),
    }


def fetch_metrics(url: str | None, timeout: float = 2.0) -> dict:
    if not url:
        return {"available": False, "why": "GATE_METRICS_URL is not set"}
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:   # noqa: S310 (fixed, configured URL)
            return {"available": True, **parse_metrics(response.read().decode("utf-8", errors="replace"))}
    except Exception as exc:
        return {"available": False, "why": f"gate metrics not reachable ({type(exc).__name__})"}


# --- red-team summaries -------------------------------------------------------------------
def load_summaries(directory: Path | None) -> list[dict]:
    if directory is None or not directory.is_dir():
        return []
    runs = []
    for path in sorted(directory.glob("*.json")):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            log.warning("skipping unreadable red-team summary %s", path)
            continue
        if isinstance(data, dict) and data.get("kind") == "redteam-summary":
            runs.append(data)
    return sorted(runs, key=lambda r: str(r.get("created", "")), reverse=True)


# --- kill switch -----------------------------------------------------------------------
class KillSwitchControl:
    """Reads and sets the kill switch. Both sides are passed in, so tests need no Kafka."""

    def __init__(self, read_state: Callable[[], dict], send: Callable[[str, str, str], None]) -> None:
        self.read_state = read_state
        self.send = send
        self._lock = threading.Lock()

    def state(self) -> dict:
        with self._lock:
            try:
                return {"known": True, **self.read_state()}
            except Exception as exc:
                return {"known": False, "why": f"state unknown ({type(exc).__name__})"}

    def set(self, action: str, reason: str) -> dict:
        from safety_gate.killswitch import DISABLED, ENABLED
        mode = {"disable": DISABLED, "enable": ENABLED}.get(action)
        if mode is None:
            raise ValueError('action must be "disable" or "enable"')
        reason = reason.strip()[:200]
        if mode == DISABLED and not reason:
            raise ValueError("a reason is required to pull the kill switch")
        self.send(mode, reason, "console")
        return self.state()


class Console:
    def __init__(self, activity: ActivityLog | None = None, metrics_url: str | None = None,
                 summaries_dir: Path | None = None, switch: KillSwitchControl | None = None,
                 model_name: str = "") -> None:
        self.activity = activity or ActivityLog()
        self.model_name = model_name
        self.metrics_url = metrics_url
        self.summaries_dir = summaries_dir
        self.switch = switch

    def get(self, path: str) -> tuple[int, dict] | None:
        """Answer a GET for /api/...; None if the path is not the console's."""
        route, _, query = path.partition("?")
        if route == "/api/info":
            return 200, {"model": self.model_name, "metrics": bool(self.metrics_url), "kafka": self.switch is not None}
        if route == "/api/activity":
            limit = 50
            match = re.search(r"(?:^|&)limit=(\d{1,3})(?:&|$)", query)
            if match:
                limit = int(match.group(1))
            return 200, {"items": self.activity.latest(limit)}
        if route == "/api/stats":
            return 200, fetch_metrics(self.metrics_url)
        if route == "/api/redteam":
            return 200, {"runs": load_summaries(self.summaries_dir)}
        if route == "/api/killswitch":
            if self.switch is None:
                return 200, {"known": False, "why": "not connected to Kafka"}
            return 200, self.switch.state()
        return None

    def post(self, path: str, body) -> tuple[int, dict] | None:
        if path != "/api/killswitch":
            return None
        if self.switch is None:
            return 503, {"error": "not connected to Kafka"}
        if not isinstance(body, dict):
            return 400, {"error": 'send {"action": "disable" | "enable", "reason": "..."}'}
        try:
            return 200, self.switch.set(str(body.get("action", "")), str(body.get("reason", "")))
        except ValueError as exc:
            return 400, {"error": str(exc)}
        except Exception as exc:
            return 502, {"error": f"command not confirmed ({type(exc).__name__})"}
