import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.kafka import KafkaContainer

from safety_gate.config import CLEAN_TOPIC, DLQ_TOPIC, RAW_TOPIC


@pytest.fixture(scope="module")
def bootstrap():
    """A fresh single-node Kafka per test module, configured for transactions."""
    container = (
        KafkaContainer()
        # single broker: the internal transaction topic can only have 1 replica
        .with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    )
    with container as kafka:
        servers = kafka.get_bootstrap_server()
        admin = AdminClient({"bootstrap.servers": servers})
        topics = [NewTopic(t, num_partitions=3, replication_factor=1)
                  for t in (RAW_TOPIC, CLEAN_TOPIC, DLQ_TOPIC)]
        for future in admin.create_topics(topics).values():
            future.result()
        yield servers
