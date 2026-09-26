import json
import random
import uuid
from datetime import datetime, timezone

from models import SensorEvent

def generate_temperature_event() -> SensorEvent:
    return SensorEvent(
        event_id=str(uuid.uuid4()),
        event_time=datetime.now(timezone.utc).isoformat(),
        store_id="store-001",
        equipment_id="fridge-001",
        equipment_type="refrigerator",
        metric_name="temperature_celsius",
        metric_value=round(random.uniform(2.0, 6.0), 2),
        unit="celsius", 
        schema_version="1.0.0", 
        source="simulator",
        )

def main():
    event = generate_temperature_event()

    print(
        json.dumps(
            event.to_dict(),
            ensure_ascii=False,
            indent=2,
        )
    )

if __name__ == "__main__":
    main()