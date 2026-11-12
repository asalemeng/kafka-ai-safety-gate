#!/usr/bin/env bash
set -euo pipefail

create() {   # create <topic> <partitions>
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1
}

create raw.inference.requests.v1 3
create clean.inference.requests.v1 3
create dlq.inference.rejected.v1 3
create audit.gate-log.v1 1          # single partition: one ordered hash chain
