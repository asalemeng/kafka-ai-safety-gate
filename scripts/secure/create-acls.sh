#!/usr/bin/env bash
# Topics and access rules for the secure stack. Run once after
#   docker compose -f docker-compose.secure.yml --env-file secure/.env up -d kafka
#
# The rule of thumb: every program may do exactly what it needs, nothing else.
#
#   client      writes raw requests                      (the chatbot backend)
#   gate        reads raw, writes clean + DLQ, reads the kill switch
#   audit       reads clean + DLQ, is the ONLY writer of the audit chain
#   operator    is the ONLY writer of the kill switch
#   model       reads clean and the kill switch, writes responses
set -euo pipefail

CONTAINER="${KAFKA_CONTAINER:-kafka-secure}"
# The admin tools talk to a listener that only exists inside the broker container.
INSIDE="localhost:19092"

KAFKA_CONTAINER="$CONTAINER" KAFKA_BOOTSTRAP_INSIDE="$INSIDE" "$(dirname "$0")/../create-topics.sh"

acl() { docker exec "$CONTAINER" /opt/kafka/bin/kafka-acls.sh --bootstrap-server "$INSIDE" --add "$@" > /dev/null; }

RAW=raw.inference.requests.v1
CLEAN=clean.inference.requests.v1
DLQ=dlq.inference.rejected.v1
AUDIT=audit.gate-log.v1
CONTROL=control.killswitch.v1
DEDUP=state.gate-dedup.v1
RESPONSES=responses.inference.v1

# client
acl --allow-principal User:client --operation Write --operation Describe --topic "$RAW"

# gate
acl --allow-principal User:gate --operation Read --operation Describe --topic "$RAW"
acl --allow-principal User:gate --operation Write --operation Describe --topic "$CLEAN"
acl --allow-principal User:gate --operation Write --operation Describe --topic "$DLQ"
acl --allow-principal User:gate --operation Read --operation Describe --topic "$CONTROL"
acl --allow-principal User:gate --operation Read --operation Write --operation Describe --topic "$DEDUP"
acl --allow-principal User:gate --operation Read --operation Describe --group input-validator
acl --allow-principal User:gate --operation Read --operation Describe --group gate- --resource-pattern-type prefixed
acl --allow-principal User:gate --operation Write --operation Describe \
    --transactional-id gate- --resource-pattern-type prefixed

# audit sink
acl --allow-principal User:audit --operation Read --operation Describe --topic "$CLEAN"
acl --allow-principal User:audit --operation Read --operation Describe --topic "$DLQ"
acl --allow-principal User:audit --operation Read --operation Write --operation Describe --topic "$AUDIT"
acl --allow-principal User:audit --operation Read --operation Describe --group audit- --resource-pattern-type prefixed
acl --allow-principal User:audit --operation Write --operation Describe \
    --transactional-id audit-writer- --resource-pattern-type prefixed

# operator
acl --allow-principal User:operator --operation Write --operation Describe --topic "$CONTROL"

# model service
acl --allow-principal User:model --operation Read --operation Describe --topic "$CLEAN"
acl --allow-principal User:model --operation Read --operation Describe --topic "$CONTROL"
acl --allow-principal User:model --operation Write --operation Describe --topic "$RESPONSES"
acl --allow-principal User:model --operation Read --operation Describe --group model- --resource-pattern-type prefixed

echo "topics and access rules created"
