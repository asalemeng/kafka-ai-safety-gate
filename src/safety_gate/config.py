"""Settings for every component.

Nothing is read from the environment at import time. Each program calls
`Settings.from_env()` once at start-up and passes the result down, so a test can
run with its own topic names, group id and limits.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field, replace


@dataclass(frozen=True)
class Topics:
    raw: str = "raw.inference.requests.v1"
    clean: str = "clean.inference.requests.v1"
    dlq: str = "dlq.inference.rejected.v1"
    audit: str = "audit.gate-log.v1"
    control: str = "control.killswitch.v1"
    dedup: str = "state.gate-dedup.v1"
    responses: str = "responses.inference.v1"

    def with_prefix(self, prefix: str) -> Topics:
        """The same topics under a prefix, e.g. one isolated set per test."""
        return Topics(**{name: f"{prefix}.{value}" for name, value in self.__dict__.items()})

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> Topics:
        default = cls()
        return cls(
            raw=env.get("RAW_TOPIC", default.raw),
            clean=env.get("CLEAN_TOPIC", default.clean),
            dlq=env.get("DLQ_TOPIC", default.dlq),
            audit=env.get("AUDIT_TOPIC", default.audit),
            control=env.get("CONTROL_TOPIC", default.control),
            dedup=env.get("DEDUP_TOPIC", default.dedup),
            responses=env.get("RESPONSES_TOPIC", default.responses),
        )


def _new_instance_id() -> str:
    return uuid.uuid4().hex[:12]


@dataclass(frozen=True)
class Settings:
    bootstrap: str = "localhost:9092"
    topics: Topics = field(default_factory=Topics)

    # --- gate ---
    group_id: str = "input-validator"
    # Unique per running gate. Set GATE_INSTANCE_ID to something stable (a pod
    # name) so a restarted gate aborts its own unfinished transaction at once.
    instance_id: str = field(default_factory=_new_instance_id)
    batch_size: int = 100              # messages per Kafka transaction
    batch_linger_s: float = 0.02       # how long to wait for a batch to fill
    switch_check_s: float = 0.2        # how often the kill switch is re-read (0 = before every batch)
    max_attempts: int = 3              # a message failing this often goes to the DLQ
    transaction_timeout_s: float = 15.0
    dedup_enabled: bool = True
    dedup_max_ids: int = 100_000       # remembered request ids per partition
    policy_path: str | None = None     # company rules; None = built-in checks only
    metrics_port: int | None = None    # serves /metrics and /healthz; None = off

    # --- audit sink ---
    audit_group_id: str = "audit-sink"
    checkpoint_every: int = 100
    checkpoint_file: str | None = None         # signed checkpoints, kept outside Kafka
    checkpoint_private_key: str | None = None  # Ed25519 private key (PEM file)

    # --- model service stub ---
    model_group_id: str = "model-service"

    # --- connection security (empty = plaintext, for local development) ---
    security: Mapping[str, str] = field(default_factory=dict)

    def client_config(self) -> dict:
        """Settings shared by every Kafka client: where the broker is and how to log in."""
        return {"bootstrap.servers": self.bootstrap, **self.security}

    def with_(self, **changes) -> Settings:
        return replace(self, **changes)

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        default = cls()

        security: dict[str, str] = {}
        for var, key in (
            ("KAFKA_SECURITY_PROTOCOL", "security.protocol"),
            ("KAFKA_SASL_MECHANISM", "sasl.mechanism"),
            ("KAFKA_SASL_USERNAME", "sasl.username"),
            ("KAFKA_SASL_PASSWORD", "sasl.password"),
            ("KAFKA_SSL_CA_LOCATION", "ssl.ca.location"),
        ):
            if env.get(var):
                security[key] = env[var]

        def number(name: str, fallback, kind=int):
            raw = env.get(name)
            if raw is None or raw == "":
                return fallback
            try:
                return kind(raw)
            except ValueError:
                raise ValueError(f"{name} must be a number, got {raw!r}") from None

        port = number("METRICS_PORT", None)
        return cls(
            bootstrap=env.get("KAFKA_BOOTSTRAP", default.bootstrap),
            topics=Topics.from_env(env),
            group_id=env.get("GATE_GROUP_ID", default.group_id),
            instance_id=env.get("GATE_INSTANCE_ID") or _new_instance_id(),
            batch_size=number("GATE_BATCH_SIZE", default.batch_size),
            batch_linger_s=number("GATE_BATCH_LINGER_MS", default.batch_linger_s * 1000, float) / 1000,
            switch_check_s=number("GATE_SWITCH_CHECK_MS", default.switch_check_s * 1000, float) / 1000,
            max_attempts=number("GATE_MAX_ATTEMPTS", default.max_attempts),
            transaction_timeout_s=number("GATE_TRANSACTION_TIMEOUT_MS", default.transaction_timeout_s * 1000, float) / 1000,
            dedup_enabled=env.get("GATE_DEDUP", "on").lower() not in ("off", "0", "false", "no"),
            dedup_max_ids=number("GATE_DEDUP_MAX_IDS", default.dedup_max_ids),
            policy_path=env.get("GATE_POLICY_FILE") or None,
            metrics_port=port,
            audit_group_id=env.get("AUDIT_GROUP_ID", default.audit_group_id),
            checkpoint_every=number("AUDIT_CHECKPOINT_EVERY", default.checkpoint_every),
            checkpoint_file=env.get("AUDIT_CHECKPOINT_FILE") or None,
            checkpoint_private_key=env.get("AUDIT_CHECKPOINT_PRIVATE_KEY") or None,
            model_group_id=env.get("MODEL_GROUP_ID", default.model_group_id),
            security=security,
        )
