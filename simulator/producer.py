import json

from confluent_kafka import KafkaError, Message, Producer

from models import SensorEvent


class KafkaEventProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": "localhost:9092",
        })

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

        self.producer.produce(
            topic="kitchen.sensor.raw",
            key=key,
            value=json.dumps(
                event.to_dict(),
                ensure_ascii=False
            ),
            on_delivery=self._delivery_callback,
        )

        self.producer.poll(0)

    def flush(self) -> None:
        self.producer.flush()