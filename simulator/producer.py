import os
from pathlib import Path

from dotenv import load_dotenv
from confluent_kafka import KafkaError, Message, Producer

from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)

load_dotenv()

class KafkaEventProducer:
    def __init__(self):
        self.bootstrap_servers = os.getenv(
            "KAFKA_BOOTSTRAP_SERVERS",
            "localhost:9092",
        )

        self.topic = os.getenv(
            "KAFKA_TOPIC_SENSOR_RAW",
            "kitchen.sensor.raw",
        )

        self.producer = Producer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "message.timeout.ms": 10000,
            }
        )
        self.registry = SchemaRegistryClient({
            "url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
            "timeout": 10,
        })
        schema_path = Path(__file__).resolve().parents[1] / "schemas/avro/sensor_metric_event.avsc"
        self.value_serializer = AvroSerializer(
            self.registry,
            schema_path.read_text(encoding="utf-8"),
            conf={
                "auto.register.schemas": False,
                "use.latest.version": False,
                "subject.name.strategy": topic_subject_name_strategy,
            },
        )
        self.key_serializer = StringSerializer("utf_8")
        self.delivered = 0
        self.failed = 0

    def _delivery_callback(
            self,
            err: KafkaError | None,
            msg: Message,
    ) -> None:

        if err is not None:
            self.failed += 1
            print(f"[KAFKA ERROR] {err}")
            return

        self.delivered += 1
        print(
            "[KAFKA DELIVERED] "
            f"topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )

    # key 생성 책임 -> main.py
    def send(
        self,
        key: str,
        payload: dict,
    ) -> None:
        if not key or key != payload.get("equipment_id"):
            raise ValueError("Kafka key must equal payload equipment_id")

        metric_value = payload.get("metric_value")

        if isinstance(metric_value, bool) or not isinstance(metric_value, (int, float)):
            raise ValueError("metric_value must be numeric, not boolean or string")

        value = self.value_serializer(
            payload, SerializationContext(self.topic, MessageField.VALUE)
        )
        serialized_key = self.key_serializer(key)

        try:
            self.producer.produce(
                topic=self.topic,
                key=serialized_key,
                value=value,
                on_delivery=self._delivery_callback,
            )

        except BufferError:
            self.producer.poll(1.0)

            self.producer.produce(
                topic=self.topic,
                key=serialized_key,
                value=value,
                on_delivery=self._delivery_callback,
            )

        self.producer.poll(0)

    def flush(self, timeout: float = 15.0) -> None:
        remaining = self.producer.flush(timeout)
        print(f"[KAFKA FLUSH] delivered={self.delivered} failed={self.failed} remaining={remaining}")
        if remaining or self.failed:
            raise RuntimeError(f"Kafka delivery incomplete: failed={self.failed}, remaining={remaining}")
