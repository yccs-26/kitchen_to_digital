import json
from typing import Any

from confluent_kafka import Producer
from kitchen_simulator.config import Settings
from kitchen_simulator.models import SensorEvent


class KafkaEventProducer:
    def __init__(self, settings: Settings) -> None:
        self._topic = settings.kafka_topic_raw
        self._producer = Producer(
            {
                "bootstrap.servers": settings.kafka_bootstrap_servers,
                "client.id": settings.kafka_client_id,
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "delivery.timeout.ms": 30_000,
            }
        )

    def publish(self, event: SensorEvent) -> None:
        payload: dict[str, Any] = event.model_dump(mode="json")

        self._producer.produce(
            topic=self._topic,
            key=event.equipment_id.encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=self._delivery_report,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        return self._producer.flush(timeout)

    @staticmethod
    def _delivery_report(error:Exception | None, message: Any) -> None:
        if error is not None:
            print(f"delivery failed: {error}")
            return

        print(
            "delivered "
            f"topic={message.topic()}"
            f"partition={message.partition()}"
            f"offset={message.offset()}"
        )