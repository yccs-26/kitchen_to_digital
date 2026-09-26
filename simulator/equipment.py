from dataclasses import dataclass

@dataclass(frozen=True)
class EquipmentConfig:
    store_id: str
    equipment_id: str
    equipment_type: str
    metric_name: str
    metric_type: str
    unit: str | None = None
    min_value: float | None = None
    max_value: float | None = None

EQUIPMENTS = [
    EquipmentConfig(
        store_id="store-001",
        equipment_id="fridge-01",
        equipment_type="refrigerator",
        metric_name="temperature_celsius",
        metric_type="numeric",
        unit="celsius",
        min_value=2.0,
        max_value=6.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="freezer-01",
        equipment_type="freezer",
        metric_name="temperature_celsius",
        metric_type="numeric",
        unit="celsius",
        min_value=-22.0,
        max_value=-16.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="fryer-01",
        equipment_type="fryer",
        metric_name="oil_temperature_celsius",
        metric_type="numeric",
        unit="celsius",
        min_value=160.0,
        max_value=190.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="hood-01",
        equipment_type="hood",
        metric_name="fan_rpm",
        metric_type="numeric",
        unit="rpm",
        min_value=800.0,
        max_value=1800.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="fridge-door-01",
        equipment_type="refrigerator",
        metric_name="door_open",
        metric_type="boolean",
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="dishwasher-01",
        equipment_type="dishwasher",
        metric_name="state",
        metric_type="categorical",
    )
]