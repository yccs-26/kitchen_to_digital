#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"
KAFKA_READY_MAX_ATTEMPTS="${KAFKA_READY_MAX_ATTEMPTS:-30}"

if ! [[ "$KAFKA_READY_MAX_ATTEMPTS" =~ ^[1-9][0-9]*$ ]]; then
    echo "KAFKA_READY_MAX_ATTEMPTS must be a positive integer" >&2
    exit 1
fi

echo "Waiting for Kafka.."

attempt=1
until /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list > /dev/null 2>&1
do
    if (( attempt >= KAFKA_READY_MAX_ATTEMPTS )); then
        echo "Kafka readiness failed after $attempt attempts; topic initialization aborted" >&2
        exit 1
    fi
    echo "Kafka not ready (attempt $attempt/$KAFKA_READY_MAX_ATTEMPTS); retrying in 2s" >&2
    sleep 2
    attempt=$((attempt + 1))
done

echo "Kafka is ready"

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic kitchen.sensor.raw \
    --partitions 3 \
    --replication-factor 1

echo "Kafka topics initialized"

/opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe \
    --topic kitchen.sensor.raw
