#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVER="kafka:9092"

create_topic() {
    local topic_name="$1"
    local partitions="$2"
    local retention_ms="$3"

    /opt/kafka/bin/kafka-topics.sh \
     --bootstrap-server "${BOOTSTRAP_SERVER}" \
     --create --if-not-exists \
     --topic "${topic_name}" \
     --partitions "${partitions}" \
     --replication-factor 1 \
     --config "retention.ms=${retention_ms}"
}

create_topic "kitchen.sensor.raw" 3 604800000
create_topic "kitchen.sensor.quarantine" 1 2592000000
create_topic "kitchen.alerts.temperature-breach" 3 2592000000
create_topic "kitchen.twin.state-changelog" 3 604800000
create_topic "kitchen.sensor.retry" 3 2592000000
create_topic "kitchen.sensor.dlq" 1 2592000000