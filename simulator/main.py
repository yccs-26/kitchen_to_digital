import asyncio
import json
import uuid
from datetime import datetime, timezone

from simulator.equipment import EQUIPMENTS, EquipmentConfig
from simulator.models import SensorEvent
from simulator.generator import generate_metric_value
from simulator.producer import KafkaEventProducer
from simulator.fault_injection import inject_fault

def generate_event(equipment: EquipmentConfig) -> SensorEvent:
    return SensorEvent(
        event_id=str(uuid.uuid4()),
        event_time=datetime.now(timezone.utc).isoformat(),
        store_id=equipment.store_id,
        equipment_id=equipment.equipment_id,
        equipment_type=equipment.equipment_type,
        metric_name=equipment.metric_name,
        metric_value=generate_metric_value(equipment),
        unit=equipment.unit,
        schema_version="1.0.0",
        source="simulator",
    )


async def simulate_equipment(
        equipment: EquipmentConfig,
        producer: KafkaEventProducer,
        interval_seconds: float = 1.0,
):
    while True:
        event = generate_event(equipment)

        payload = event.to_dict()

        payload = inject_fault(
            payload,
            fault_rate=0.1,
        )

        key = f"{event.store_id}:{event.equipment_id}"

        producer.send(
            key=key,
            payload=payload
        )

        print(
            json.dumps(
                event.to_dict(),
                ensure_ascii=False,
            )
        )

        await asyncio.sleep(interval_seconds)



async def main():
    producer = KafkaEventProducer()

    tasks = [
        asyncio.create_task(
            simulate_equipment(
                equipment,
                producer,
            )
        )
        for equipment in EQUIPMENTS
    ]
    try:
        await asyncio.gather(*tasks)

    finally:
        for task in tasks:
            task.cancel()

        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )
        
        producer.flush()


if __name__ == "__main__":
    asyncio.run(main())