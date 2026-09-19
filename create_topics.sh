#!/usr/bin/env bash
# Creates every topic in libs/common/topics.py with the right partition count
# and retention. Idempotent — safe to re-run.
set -euo pipefail
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
RF="${REPLICATION_FACTOR:-1}"

create () {
  local name=$1 parts=$2 retention=$3
  kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor "$RF" \
    --config retention.ms="$retention" \
    --config min.insync.replicas=$(( RF > 1 ? 2 : 1 ))
  echo "  ok  $name  parts=$parts  retention=${retention}ms"
}

echo "creating topics on $BOOTSTRAP"
create txn.raw.v1              12 604800000
create txn.enriched.v1         12 604800000
create analysis.rules.v1       12 259200000
create analysis.ml.v1          12 259200000
create analysis.graph.v1       12 259200000
create decision.final.v1       12 2592000000
create notification.outbound.v1 6 259200000
create case.created.v1          6 7776000000
create audit.event.v1           6 31536000000
create dlq.v1                   6 2592000000
echo "done"
