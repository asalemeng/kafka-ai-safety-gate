"""Task 6: the access rules of the secure stack, checked from the outside.

These tests need the stack from docker-compose.secure.yml and are skipped
otherwise. To run them:

    scripts/secure/make-certs.sh
    docker compose -f docker-compose.secure.yml --env-file secure/.env up -d kafka
    scripts/secure/create-acls.sh
    SECURE_KAFKA_BOOTSTRAP=localhost:9095 pytest -m secure -v
"""
import os
from pathlib import Path

import pytest
from confluent_kafka import KafkaError, Producer

from safety_gate.config import Topics

BOOTSTRAP = os.environ.get("SECURE_KAFKA_BOOTSTRAP")
SECRETS = Path(os.environ.get("SECURE_DIR", "secure"))

pytestmark = [
    pytest.mark.secure,
    pytest.mark.skipif(not BOOTSTRAP, reason="set SECURE_KAFKA_BOOTSTRAP to run against the secure stack"),
]
TOPICS = Topics()


def password(user: str) -> str:
    for line in (SECRETS / ".env").read_text().splitlines():
        name, _, value = line.partition("=")
        if name == f"{user.upper()}_PASSWORD":
            return value
    raise KeyError(user)


def try_to_write(topic: str, user: str | None, secret: str | None = None) -> KafkaError | None:
    """Write one message as `user`. Returns None on success, else the error."""
    config = {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "ssl.ca.location": str(SECRETS / "ca.crt"),
        "sasl.username": user or "nobody",
        "sasl.password": secret if secret is not None else (password(user) if user else "wrong"),
        "message.timeout.ms": 10_000,
        "socket.timeout.ms": 10_000,
    }
    result: list = []
    producer = Producer(config)
    producer.produce(topic, value=b"{}", on_delivery=lambda err, _msg: result.append(err))
    producer.flush(20)
    return result[0] if result else KafkaError(KafkaError._TIMED_OUT)


def test_wrong_password_gets_nothing_done():
    assert try_to_write(TOPICS.raw, "client", secret="not-the-password") is not None


def test_client_may_write_raw_requests():
    assert try_to_write(TOPICS.raw, "client") is None


def test_client_cannot_skip_the_gate_by_writing_to_the_clean_topic():
    error = try_to_write(TOPICS.clean, "client")
    assert error is not None and error.code() == KafkaError.TOPIC_AUTHORIZATION_FAILED


def test_only_the_gate_writes_the_clean_topic():
    assert try_to_write(TOPICS.clean, "gate") is None
    for user in ("client", "audit", "operator", "model"):
        assert try_to_write(TOPICS.clean, user) is not None, f"{user} could write to the clean topic"


def test_only_operators_write_the_kill_switch():
    assert try_to_write(TOPICS.control, "operator") is None
    for user in ("client", "gate", "audit", "model"):
        assert try_to_write(TOPICS.control, user) is not None, f"{user} could flip the kill switch"


def test_only_the_audit_sink_writes_the_audit_chain():
    for user in ("client", "gate", "operator", "model"):
        assert try_to_write(TOPICS.audit, user) is not None, f"{user} could write to the audit chain"
