"""검증된 수치 이벤트의 집계 키와 UTC 1분 경계만 정의한다.

입력은 기존 decoder가 해석한 이벤트이며 wire 시각도 기존 정규화기로
처리한다. 전체 도메인 재검증, 중복 제거, 수치 집계와 저장은 수행하지 않는다.
unit은 키에 추가하지 않고 주입된 metric 규칙과 정확히 일치해야 한다.
한 규칙 묶음에서 같은 metric의 단위가 다르면 키 충돌 위험으로 거부한다.
규칙 버전 간 단위 변경과 기존 결과의 이관 정책은 후속 결정 사항이다.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal, NamedTuple

from streaming.validation.event_time_normalizer import normalize_event_time


class AggregationKey(NamedTuple):
    """Gold 논리 키의 순서를 보존하며 unit은 포함하지 않는다."""

    window_start: datetime
    window_type: Literal["1m_tumbling"]
    store_id: str
    equipment_id: str
    metric_name: str


@dataclass(frozen=True)
class MinuteWindow:
    """UTC 반개구간 [start, end): end 시각은 다음 구간에 속한다."""

    start: datetime
    end: datetime


@dataclass(frozen=True)
class MetricWindowAssignment:
    """집계 실행 없이 키, 종료 경계와 검증된 단위를 전달한다."""

    key: AggregationKey
    window_end: datetime
    unit: str


def minute_window(event_time: str | datetime) -> MinuteWindow:
    """시각을 UTC로 정규화하고 해당 1분의 포함·제외 경계를 반환한다."""
    if isinstance(event_time, str):
        utc_time = normalize_event_time(event_time)
    elif (
        isinstance(event_time, datetime)
        and event_time.utcoffset() is not None
    ):
        utc_time = event_time.astimezone(timezone.utc)
    else:
        raise ValueError("event_time must include timezone information")
    start = utc_time.replace(second=0, microsecond=0)
    return MinuteWindow(start, start + timedelta(minutes=1))


def _required_string(event: Mapping[str, object], field: str) -> str:
    """키와 단위 필드의 누락·빈 값은 명시적으로 거부한다."""
    value = event.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")
    return value


def assign_metric_window(
    event: Mapping[str, object],
    *,
    metric_units: Mapping[str, Mapping[str, str]],
) -> MetricWindowAssignment:
    """기존 장비 유형별 규칙을 주입받아 단위 혼합 없는 키를 만든다.

    단위를 변환하거나 metric 이름을 축약하지 않는다. 장비 등록·값 검증은
    upstream의 책임이며 이 함수는 입력 mapping을 변경하지 않는다.
    """
    store_id = _required_string(event, "store_id")
    equipment_id = _required_string(event, "equipment_id")
    equipment_type = _required_string(event, "equipment_type")
    metric_name = _required_string(event, "metric_name")
    unit = _required_string(event, "unit")
    expected = metric_units.get(equipment_type, {}).get(metric_name)
    if not isinstance(expected, str) or not expected.strip():
        raise ValueError("Unsupported equipment type or metric")
    if any(
        rules[metric_name] != expected
        for rules in metric_units.values()
        if metric_name in rules
    ):
        raise ValueError("Conflicting units configured for the same metric")
    if unit != expected:
        raise ValueError(f"Metric requires unit {expected!r}")
    event_time = event.get("event_time")
    if not isinstance(event_time, (str, datetime)):
        raise ValueError("event_time must be a string or datetime")
    window = minute_window(event_time)
    key = AggregationKey(
        window.start, "1m_tumbling", store_id, equipment_id, metric_name,
    )
    return MetricWindowAssignment(key, window.end, unit)
