import json
import os

from dotenv import load_dotenv
from confluent_kafka import KafkaError, Message, Producer

from models import SensorEvent

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
            }
        )

    def _delivery_callback(
            self,
            err: KafkaError | None,
            msg: Message,
    ) -> None:
        if err is not None:
            print(f"[KAFKA ERROR] {err}")
            return

        print(
            "[KAFKA DELIVERED] "
            f"topic={msg.topic()}"
            f"partition={msg.partition()}"
            f"offset={msg.offset()}"
        )


    def send(self, event: SensorEvent) -> None:
        key = f"{event.store_id}:{event.equipment_id}"

        payload = json.dumps(
            event.to_dict(),
            ensure_ascii=False,
        )

        try:
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=payload,
                on_delivery=self._delivery_callback,
            )
        except BufferError:
            print("[KAFKA BUFFER FULL] waiting for queued messages")

            self.producer.poll(1.0)

            self.producer.produce(
                topic=self.topic,
                key=key,
                value=payload,
                on_delivery=self._delivery_callback,
            )

        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()
