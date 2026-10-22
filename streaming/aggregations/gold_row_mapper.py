"""확정 집계를 검증하고 저장 작업 없이 Gold 12개 물리 컬럼으로 매핑한다."""

from collections.abc import Mapping
from dataclasses import fields
from datetime import datetime, timedelta, timezone
import math
from typing import TypedDict

from streaming.aggregations.minute_aggregation import MinuteAggregate


class GoldRowMappingError(ValueError):
    """논리 집계 행이 Gold 물리 행 계약을 만족하지 않는다."""


class GoldPhysicalRow(TypedDict):
    """저장 전 검증된 12컬럼이며 timestamp는 UTC aware datetime이다."""

    window_start: datetime
    window_type: str
    store_id: str
    equipment_id: str
    metric_name: str
    window_end: datetime
    unit: str
    sample_count: int
    sum_value: float
    min_value: float
    max_value: float
    avg_value: float


_LOGICAL_FIELDS = frozenset({
    "window_start", "window_type", "store_id", "equipment_id",
    "metric_name", "window_end", "unit", "count", "sum", "min", "max",
    "avg",
})
_WINDOW_MINUTES = {"1m_tumbling": 1, "5m_sliding": 5}


def _string(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise GoldRowMappingError(f"Expected nonblank string: {field}")
    return value


def _timestamp(value: object, field: str) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise GoldRowMappingError(f"Expected aware datetime: {field}")
    try:
        return value.astimezone(timezone.utc)
    except (OverflowError, ValueError) as exc:
        raise GoldRowMappingError(f"Invalid UTC timestamp: {field}") from exc


def _double(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GoldRowMappingError(f"Expected finite number: {field}")
    try:
        converted = float(value)
    except OverflowError as exc:
        raise GoldRowMappingError(f"Expected finite number: {field}") from exc
    if not math.isfinite(converted):
        raise GoldRowMappingError(f"Expected finite number: {field}")
    if isinstance(value, int) and converted != value:
        raise GoldRowMappingError(
            "DOUBLE conversion would lose integer precision"
        )
    return 0.0 if converted == 0.0 else converted


def map_gold_row(
    logical: Mapping[str, object] | MinuteAggregate,
) -> GoldPhysicalRow:
    """논리 행 또는 기존 1분 모델을 매핑하며 입력과 저장소를 변경하지 않는다.

    5분 행은 평탄한 Mapping으로 받는다. 닫힌 window인지, 기존 자연키가
    존재하는지는 판단하지 않으며 호출자가 확정 집계만 전달해야 한다.
    """
    if isinstance(logical, MinuteAggregate):
        row = dict(logical.key._asdict())
        row.update({
            field.name: getattr(logical, field.name)
            for field in fields(MinuteAggregate) if field.name != "key"
        })
    elif isinstance(logical, Mapping):
        row = dict(logical)
    else:
        raise GoldRowMappingError(
            "Expected logical mapping or MinuteAggregate"
        )
    if set(row) != _LOGICAL_FIELDS:
        raise GoldRowMappingError("Unexpected logical fields")
    for field, value in row.items():
        if value is None:
            raise GoldRowMappingError(f"NULL is forbidden: {field}")

    window_type = _string(row["window_type"], "window_type")
    if window_type not in _WINDOW_MINUTES:
        raise GoldRowMappingError("Unsupported window_type")
    start = _timestamp(row["window_start"], "window_start")
    end = _timestamp(row["window_end"], "window_end")
    if start.second != 0 or start.microsecond != 0:
        raise GoldRowMappingError(
            "window_start must be on UTC minute boundary"
        )
    if end - start != timedelta(minutes=_WINDOW_MINUTES[window_type]):
        raise GoldRowMappingError("window_end must match window_type duration")

    count = row["count"]
    if type(count) is not int or not 0 < count <= 2**63 - 1:
        raise GoldRowMappingError("Expected positive signed BIGINT count")
    total = _double(row["sum"], "sum")
    minimum = _double(row["min"], "min")
    maximum = _double(row["max"], "max")
    average = _double(row["avg"], "avg")
    if average != total / count:
        raise GoldRowMappingError("avg must equal sum / count")

    return {
        "window_start": start,
        "window_type": window_type,
        "store_id": _string(row["store_id"], "store_id"),
        "equipment_id": _string(row["equipment_id"], "equipment_id"),
        "metric_name": _string(row["metric_name"], "metric_name"),
        "window_end": end,
        "unit": _string(row["unit"], "unit"),
        "sample_count": count,
        "sum_value": total,
        "min_value": minimum,
        "max_value": maximum,
        "avg_value": average,
    }
