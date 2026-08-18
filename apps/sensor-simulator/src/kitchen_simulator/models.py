# 초기 : 모든 주방 장비 이벤트를 models.py로 표현

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, Field


class SensorEvent(BaseModel):
    event_id: UUID
    event_time: datetime
    ingested_at: datetime

    store_id: str = Field(min_length=1)
    equipment_id: str = Field(min_length=1)
    equipment_type: Literal["refrigerator", "stove", "hood"]

    metric_name: str = Field(min_length=1)
    metric_value: float
    unit: str = Field(min_length=1)

    schema_version: str = "1.0"
    simulated_fault: bool = False