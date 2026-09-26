from dataclasses import dataclass

@dataclass(frozen=True)
class EquipmentConfig:
    store_id: str
    equipment_id: str
    equipment_type: str
    metric_name: str
    unit: str
    min_value: float
    max_value: float

EQUIPMENTS = [
    EquipmentConfig(
        store_id="store-001",
        equipment_id="fridge-01",
        equipment_type="refrigerator",
        metric_name="temperature_celsius",
        unit="celsius",
        min_value=2.0,
        max_value=6.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="freezer-01",
        equipment_type="freezer",
        metric_name="temperature_celsius",
        unit="celsius",
        min_value=-22.0,
        max_value=-16.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="fryer-01",
        equipment_type="fryer",
        metric_name="oil_temperature_celsius",
        unit="celsius",
        min_value=160.0,
        max_value=190.0,
    ),
    EquipmentConfig(
        store_id="store-001",
        equipment_id="hood-01",
        equipment_type="hood",
        metric_name="fan_rpm",
        unit="rpm",
        min_value=800.0,
        max_value=1800.0,
    ),
]