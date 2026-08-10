#!/bin/bash
# Creates PulseGuard's Kafka topics with sensible partition counts.
# market-data gets more partitions since it's the high-throughput topic and
# partition count is what lets us scale consumer concurrency across
# ingestion / anomaly-detection / monitoring consumer groups.
set -euo pipefail

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"

create_topic() {
  local name="$1"
  local partitions="$2"
  local retention_ms="$3"
  echo "Ensuring topic '$name' (partitions=$partitions, retention.ms=$retention_ms)..."
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$name" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --config retention.ms="$retention_ms"
}

create_topic "market-data" 6 604800000
create_topic "market-data-dead-letter" 3 1209600000
create_topic "alerts" 3 1209600000

echo "Topics ready:"
/opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --list
