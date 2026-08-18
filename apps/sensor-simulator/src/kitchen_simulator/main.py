import asyncio
import random
from datetime import UTC, datetime
from uuid import uuid4

from kitchen_simulator.config import Settings
from kitchen_simulator.kafka_producer import KafkaEventProducer
from kitchen_simulator.models import SensorEvent


async def simulate_fridge(
        producer: KafkaEventProducer,
        settings: Settings,
) -> None:
    while True:
        temperature = round(random.uniform(2.0, 5.0), 2)

        is_fault = settings.simulation_mode == "temperature_breach"
        if is_fault:
            temperature = round(random.uniform(5.5, 8.0), 2)

        now = datetime.now(UTC)
        event = SensorEvent(
            event_id=uuid4(),
            event_time=now,
            ingested_at=now,
            store_id=settings.store_id,
            equipment_id="fridge-001",
            equipment_type="refrigerator",
            metric_name="temperature_celsius",
            metric_value=temperature,
            unit="celsius",
            simulated_fault=is_fault,
        )

        producer.publish(event)
        await asyncio.sleep(settings.default_interval_seconds)

async def main() -> None:
    settings = Settings()
    producer = KafkaEventProducer(settings)

    try:
        await simulate_fridge(producer, settings)
    finally:
        remaining = producer.flush()
        print(f"producer shutdown complete, remaining_messages={remaining}")

if __name__ == "__main__":
    asyncio.run(main())