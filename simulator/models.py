from dataclasses import dataclass, asdict

@dataclass
class SensorEvent:
    event_id: str
    event_time: str
    store_id: str
    equipment_id: str
    equipment_type: str
    metric_name: str
    metric_value: float
    unit: str
    schema_version: str
    source: str

    def to_dict(self) -> dict:
        return asdict(self)
