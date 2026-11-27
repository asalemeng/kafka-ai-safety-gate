import os
import uuid

import pytest

from safety_gate.config import Settings, Topics
from safety_gate.topology import create_topics


@pytest.fixture(scope="session")
def bootstrap():
    """One single-node Kafka for the whole test run, configured for transactions.

    Set KAFKA_BOOTSTRAP_TEST=host:port to use a broker that is already running
    instead of starting one in Docker.
    """
    existing = os.environ.get("KAFKA_BOOTSTRAP_TEST")
    if existing:
        yield existing
        return

    from testcontainers.community.kafka import KafkaContainer

    container = (
        KafkaContainer()
        # single broker: the internal transaction topic can only have 1 replica
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    )
    with container as kafka:
        yield kafka.get_bootstrap_server()


def _fresh(bootstrap: str, **overrides) -> Settings:
    """Settings with their own topics and group ids, so tests never see each other's messages."""
    run = uuid.uuid4().hex[:8]
    values: dict = dict(
        bootstrap=bootstrap,
        topics=Topics().with_prefix(f"t{run}"),
        group_id=f"gate-{run}",
        audit_group_id=f"audit-{run}",
        model_group_id=f"model-{run}",
        switch_check_s=0,                # strict: the switch is re-read before every batch
    )
    values.update(overrides)
    settings = Settings(**values)
    create_topics(settings)
    return settings


@pytest.fixture(scope="module")
def settings(bootstrap):
    """Shared by the tests of one module."""
    return _fresh(bootstrap)


@pytest.fixture
def make_settings(bootstrap):
    """For tests that need topics nobody else has touched: make_settings(batch_size=1, ...)."""
    return lambda **overrides: _fresh(bootstrap, **overrides)


@pytest.fixture(scope="module")
def make_settings_module(bootstrap):
    """Own topics for one test module, strict about the kill switch."""
    return _fresh(bootstrap)
