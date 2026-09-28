import json
import os

from confluent_kafka import Consumer, KafkaError
from streaming.validator import validate_event

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092",
)

TOPIC = os.getenv(
    "KAFKA_TOPIC_SENSOR_RAW",
    "kitchen.sensor.raw",
)

GROUP_ID = os.getenv(
    "KAFKA_CONSUMER_GROUP",
    "ktd-raw-consumer",
)

def create_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
        }
    )

def main():
    consumer = create_consumer()

    consumer.subscribe([TOPIC])

    print(f"Consuming Topic: {TOPIC}")

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue

                raise RuntimeError(message.error())

            key = (
                message.key().decode("utf-8")
                if message.key()
                else None
            )

            value = json.loads(
                message.value().decode("utf-8")
            )

            is_valid, reason = validate_event(value)

            if is_valid:
                print(
                    f"[VALID] "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} "
                    f"key={key}"
                )
            else:
                print(
                    f"[INVALID] "
                    f"partition={message.partition()} "
                    f"offset={message.offset()} "
                    f"key={key} "
                    f"reason={reason}"
                )

            print(
                f"partition={message.partition()} "
                f"offset={message.offset()} "
                f"key={key} "
                f"value={value}"
            )

    except KeyboardInterrupt:
        print("Consumer stopped")

    finally:
        consumer.close()

if __name__ == "__main__":
    main()