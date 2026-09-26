#!/usr/bin/env bash

set -e

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka:29092}"

echo "Waiting for Kafka.."

until /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --list > /dev/null 2>&1
do
    sleep 2
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