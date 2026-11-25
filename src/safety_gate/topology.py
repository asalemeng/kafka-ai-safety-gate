"""The topics the system needs, with their settings. One definition for scripts and tests.

    python -m safety_gate.topology          # create missing topics on KAFKA_BOOTSTRAP
"""
from __future__ import annotations

import sys
from dataclasses import dataclass, field

from safety_gate.config import Settings, Topics

DAY_MS = 24 * 60 * 60 * 1000
RAW_RETENTION_MS = 1 * DAY_MS         # unchecked prompts: kept as briefly as replay needs
CLEAN_RETENTION_MS = 1 * DAY_MS
DLQ_RETENTION_MS = 7 * DAY_MS         # rejected requests (masked): time for a human review
RESPONSES_RETENTION_MS = 1 * DAY_MS
DEDUP_RETENTION_MS = 1 * DAY_MS       # how long a re-sent request id is recognised
PARTITIONS = 3


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    config: dict[str, str] = field(default_factory=dict)


def topic_specs(topics: Topics, partitions: int = PARTITIONS) -> list[TopicSpec]:
    return [
        TopicSpec(topics.raw, partitions, {"retention.ms": str(RAW_RETENTION_MS)}),
        TopicSpec(topics.clean, partitions, {"retention.ms": str(CLEAN_RETENTION_MS)}),
        TopicSpec(topics.dlq, partitions, {"retention.ms": str(DLQ_RETENTION_MS)}),
        # one partition = one ordered hash chain; never deleted by the broker
        TopicSpec(topics.audit, 1, {"retention.ms": "-1"}),
        # compacted: the latest kill switch command is always kept
        TopicSpec(topics.control, 1, {"cleanup.policy": "compact"}),
        # request ids already forwarded; same partition count as the raw topic
        TopicSpec(topics.dedup, partitions,
                  {"cleanup.policy": "compact,delete", "retention.ms": str(DEDUP_RETENTION_MS)}),
        TopicSpec(topics.responses, partitions, {"retention.ms": str(RESPONSES_RETENTION_MS)}),
    ]


def create_topics(settings: Settings, replication_factor: int = 1, timeout: float = 30.0) -> list[str]:
    """Create the topics that do not exist yet. Returns the names that were created."""
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient(settings.client_config())
    new = [NewTopic(s.name, num_partitions=s.partitions, replication_factor=replication_factor, config=s.config)
           for s in topic_specs(settings.topics)]
    created = []
    for name, future in admin.create_topics(new, operation_timeout=timeout).items():
        try:
            future.result(timeout=timeout + 5)
            created.append(name)
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return created


def main() -> int:
    settings = Settings.from_env()
    created = create_topics(settings)
    for spec in topic_specs(settings.topics):
        state = "created" if spec.name in created else "exists "
        print(f"{state}  {spec.name}  partitions={spec.partitions}  {spec.config}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
