#!/usr/bin/env bash
set -euo pipefail

TOPICS=(
  raw.inference.requests.v1
  clean.inference.requests.v1
  dlq.inference.rejected.v1
)

for topic in "${TOPICS[@]}"; do
  docker exec kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic "$topic" --partitions 3 --replication-factor 1
done