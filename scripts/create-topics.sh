#!/usr/bin/env bash
set -euo pipefail

create() {   # create <topic> <partitions> [extra kafka-topics.sh options]
  local topic="$1" partitions="$2"
  shift 2
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1 "$@"
}

create raw.inference.requests.v1 3
create clean.inference.requests.v1 3
create dlq.inference.rejected.v1 3
create audit.gate-log.v1 1          # single partition: one ordered hash chain
create control.killswitch.v1 1 --config cleanup.policy=compact   # kill switch: latest command is kept
