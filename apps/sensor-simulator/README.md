# Sensor Simulator

## Prerequisites
- Docker Compose 기반 Kafka 실행
- uv 설치

## Setup
uv sync
cp .env.example .env

## Run: normal mode
uv run --package kitchen-sensor-simulator python -m kitchen_simulator.main

## Run: temperature breach mode
SIMULATION_MODE=temperature_breach \
uv run --package kitchen-sensor-simulator \
python -m kitchen_simulator.main

## Verify
docker exec -it ktd-kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic kitchen.sensor.raw \
  --from-beginning \
  --property print.key=true \
  --property key.separator=" | "