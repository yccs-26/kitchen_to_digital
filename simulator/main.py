import json
import random
import uuid
from datetime import datetime, timezone

from equipment import EQUIPMENTS, EquipmentConfig
from models import SensorEvent
from generators import generate_metric_value

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

def main():
    for equipment in EQUIPMENTS:
        event = generate_event(equipment)

        print(
            json.dumps(
                event.to_dict(),
                ensure_ascii=False,
            )
        )

if __name__ == "__main__":
    main()