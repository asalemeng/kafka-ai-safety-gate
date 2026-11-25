#!/usr/bin/env bash
# Creates the topics with the Kafka command line tools inside the broker container.
# (`python -m safety_gate.topology` does the same from Python and is what
#  `docker compose up` and the tests use. Keep the two in step.)
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_INSIDE:-localhost:9092}"
DAY_MS=86400000

create() {   # create <topic> <partitions> [extra kafka-topics.sh options]
  local topic="$1" partitions="$2"
  shift 2
  docker exec "$CONTAINER" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 "$@"
}

# Topics that hold prompts are deleted by the broker after a fixed time.
create raw.inference.requests.v1   3 --config retention.ms=$((1 * DAY_MS))   # unchecked prompts: 1 day
create clean.inference.requests.v1 3 --config retention.ms=$((1 * DAY_MS))
create dlq.inference.rejected.v1   3 --config retention.ms=$((7 * DAY_MS))   # rejected, masked: 7 days for review
create responses.inference.v1      3 --config retention.ms=$((1 * DAY_MS))
create audit.gate-log.v1 1 --config retention.ms=-1          # single partition: one ordered hash chain, kept
create control.killswitch.v1 1 --config cleanup.policy=compact   # kill switch: latest command is kept
create state.gate-dedup.v1 3 --config cleanup.policy=compact,delete --config retention.ms=$((1 * DAY_MS))
