"""유한 입력 전체에서 ID 중복을 제거한 뒤 UTC 1분 수치를 집계한다.

상태는 호출 안에서만 유지한다. 충돌이면 부분 결과를 반환하지 않으며,
호출 간 중복 제거·watermark·영속 상태·sink 쓰기는 제공하지 않는다.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import math

from streaming.aggregations.windows import (
    AggregationKey,
    assign_metric_window,
)
from streaming.validation.event_identity import (
    PAYLOAD_FIELDS,
    EventIdentityStatus,
    classify_event_identity,
)
from streaming.validation.event_time_normalizer import normalize_event_time


class EventConflictError(ValueError):
    """같은 ID의 payload 충돌 필드를 전달하며 전체 집계 호출을 중단한다."""

    def __init__(self, event_id: str, differing_fields: tuple[str, ...]):
        self.event_id = event_id
        self.differing_fields = differing_fields
        super().__init__(
            f"Conflicting event_id {event_id!r}: {differing_fields}"
        )


@dataclass(frozen=True)
class MinuteAggregate:
    """자연키별 수치 결과이며 Gold 물리 스키마를 확정하지 않는다."""

    key: AggregationKey
    window_end: datetime
    unit: str
    count: int
    sum: float
    avg: float
    min: float
    max: float


def _canonical_payload(event: Mapping[str, object]) -> dict[str, object]:
    """기존 10필드만 복사하고 identity 비교 전 시각을 UTC로 통일한다."""
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id.strip():
        raise ValueError("event_id must be a non-blank string")
    payload = {
        field: event[field] for field in ("event_id", *PAYLOAD_FIELDS)
    }
    event_time = payload["event_time"]
    if isinstance(event_time, str):
        payload["event_time"] = normalize_event_time(event_time)
    elif (
        isinstance(event_time, datetime)
        and event_time.utcoffset() is not None
    ):
        payload["event_time"] = event_time.astimezone(timezone.utc)
    else:
        raise ValueError("event_time must include timezone information")
    value = payload["metric_value"]
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
    ):
        raise ValueError("metric_value must be a finite number")
    return payload


def aggregate_minutes(
    events: Iterable[Mapping[str, object]],
    *,
    metric_units: Mapping[str, Mapping[str, str]],
) -> tuple[MinuteAggregate, ...]:
    """동일 ID/동일 payload는 한 번만 반영하고 충돌은 예외로 반환한다.

    ID 비교 범위는 window와 무관하게 호출 전체다. 모든 입력을 먼저 비교하므로
    충돌 payload를 임의로 선택하지 않는다. 다른 ID의 같은 값은 별개 샘플이다.
    입력과 규칙은 변경하지 않으며 결과는 자연키 순서로 정렬한다.
    """
    canonical = {}
    for event in events:
        payload = _canonical_payload(event)
        event_id = payload["event_id"]
        identity = classify_event_identity(payload, canonical.get(event_id))
        if identity.status is EventIdentityStatus.CONFLICT:
            raise EventConflictError(event_id, identity.differing_fields)
        if identity.status is EventIdentityStatus.NEW:
            canonical[event_id] = payload

    groups = {}
    assignments = {}
    for payload in canonical.values():
        assignment = assign_metric_window(payload, metric_units=metric_units)
        assignments[assignment.key] = assignment
        groups.setdefault(assignment.key, []).append(payload["metric_value"])

    results = []
    for key in sorted(groups):
        values = sorted(groups[key])
        assignment = assignments[key]
        # 합산 순서를 고정하고 fsum을 사용해 부동소수점 누적 오차를 줄인다.
        total = math.fsum(values)
        results.append(MinuteAggregate(
            key, assignment.window_end, assignment.unit, len(values),
            total, total / len(values), min(values), max(values),
        ))
    return tuple(results)
