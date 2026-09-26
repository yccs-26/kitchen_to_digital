from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Union

@dataclass
class SensorEvent:
    event_id: str
    event_time: str
    store_id: str
    equipment_id: str
    equipment_type: str
    metric_name: str
    metric_value: Union[int, float, bool]
    unit: str
    schema_version: str
    source: str

    def to_dict(self) -> dict:
        return asdict(self)