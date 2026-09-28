import asyncio
import argparse
import json
import uuid
from datetime import datetime, timezone

from simulator.equipment import EQUIPMENTS, EquipmentConfig
from simulator.models import SensorEvent
from simulator.generator import generate_metric_value
from simulator.producer import KafkaEventProducer
from simulator.fault_injection import inject_fault

def generate_event(equipment: EquipmentConfig) -> SensorEvent:
    if equipment.metric_type != "numeric" or not equipment.unit:
        raise ValueError("SensorMetricEvent requires numeric equipment with a unit")
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
        count: int | None = None,
        fault_rate: float = 0.0,
):
    sent = 0
    while count is None or sent < count:
        event = generate_event(equipment)

        payload = event.to_dict()

        payload = inject_fault(
            payload,
            fault_rate=fault_rate,
        )

        key = event.equipment_id

        producer.send(
            key=key,
            payload=payload
        )

        print(
            json.dumps(
                payload,
                ensure_ascii=False,
            )
        )

        sent += 1
        if count is None or sent < count:
            await asyncio.sleep(interval_seconds)



async def main(count: int | None = None):
    if count is not None and count < 1:
        raise ValueError("count must be positive")
    producer = KafkaEventProducer()

    tasks = [
        asyncio.create_task(
            simulate_equipment(
                equipment,
                producer,
                count=count,
            )
        )
        for equipment in EQUIPMENTS
        if equipment.metric_type == "numeric"
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
    parser = argparse.ArgumentParser(description="Publish numeric SensorMetricEvent records as Avro")
    parser.add_argument("--count", type=int, help="Events per numeric equipment; omit to run continuously")
    args = parser.parse_args()
    if args.count is not None and args.count < 1:
        parser.error("--count must be positive")
    asyncio.run(main(count=args.count))
