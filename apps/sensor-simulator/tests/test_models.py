from datetime import UTC, datetime
from uuid import uuid4

from kitchen_simulator.models import SensorEvent


def test_sensor_event_is_serializable() -> None:
    event = SensorEvent(
        event_id=uuid4(),
        event_time=datetime.now(UTC),
        ingested_at=datetime.now(UTC),
        store_id="store-001",
        equipment_id="fridge-001",
        equipment_type="refrigerator",
        metric_name="temperature_celsius",
        metric_value=4.2,
        unit="selsius",
    )

    payload = event.model_dump(mode="json")

    assert payload["equipment_id"] == "fridge-001"
    assert payload["metric_value"] == 4.2
    assert payload["schema_version"] == "1.0"