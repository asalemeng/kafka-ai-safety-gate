import os

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
GROUP_ID = os.getenv("GATE_GROUP_ID", "input-validator")

RAW_TOPIC = os.getenv("RAW_TOPIC", "raw.inference.requests.v1")
CLEAN_TOPIC = os.getenv("CLEAN_TOPIC", "clean.inference.requests.v1")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "dlq.inference.rejected.v1")
AUDIT_TOPIC = os.getenv("AUDIT_TOPIC", "audit.gate-log.v1")   # single partition: one ordered chain
