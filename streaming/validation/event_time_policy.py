"""오프라인 지연 이벤트 분류와 watermark 관측 후보 판정."""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

from streaming.validation.domain_validator import validate_event_time


class EventTimeStatus(Enum):
    ON_TIME = "ON_TIME"
    LATE = "LATE"
    FUTURE_INVALID = "FUTURE_INVALID"


@dataclass(frozen=True)
class EventTimeClassification:
    status: EventTimeStatus
    is_watermark_candidate: bool


def classify_event_time(
    event_time: datetime,
    *,
    reference_time: datetime,
    allowed_lateness: timedelta,
    max_future_skew: timedelta,
) -> EventTimeClassification:
    """명시적으로 주입한 기준 시각으로 검증된 이벤트를 분류한다.

    reference_time은 호출자가 제공하는 신뢰할 수 있는 처리 시각 기준이며,
    domain validation에도 같은 값을 사용한다. Spark watermark나 관측된
    최대 이벤트 시각과는 다르며, 검증되지 않은 입력 시각에서 도출하면 안 된다.
    이 함수는 시스템 시각, 상태, watermark 계산 또는 sink에 접근하지 않는다.

    표준 입력은 UTC datetime이다. 다른 시간대의 aware datetime도 허용하며
    UTC로 변환해 비교한다. naive 또는 잘못된 시각과 음수 기간은 즉시 실패한다.
    기존 domain 시각 규칙을 방어적으로 재사용하며, max_future_skew를 초과하면
    FUTURE_INVALID를 반환하고 watermark 후보에서 제외한다.

    이벤트의 지연 시간이 allowed_lateness를 초과할 때만 LATE이며,
    경계와 같으면 ON_TIME이다. domain 규칙상 유효한 시각은 지연 이벤트와
    허용 범위 내 미래 시각도 관측 후보가 된다. 후보 여부가 True여도 실제
    watermark 전진을 뜻하지 않는다. 후속 통합에서 관측된 최대 시각과 비교하고
    Spark의 동작을 검증해야 한다. 이 분류 함수 자체는 행을 필터링하지 않는다.
    """
    if allowed_lateness < timedelta(0):
        raise ValueError("allowed_lateness must be non-negative.")
    validation = validate_event_time(
        event_time,
        reference_time=reference_time,
        max_future_skew=max_future_skew,
    )
    for error in validation.errors:
        if error.code == "FUTURE_EVENT_TIME":
            return EventTimeClassification(
                EventTimeStatus.FUTURE_INVALID, False
            )
        raise ValueError(error.details)

    age = (
        reference_time.astimezone(timezone.utc)
        - event_time.astimezone(timezone.utc)
    )
    status = (
        EventTimeStatus.LATE
        if age > allowed_lateness else EventTimeStatus.ON_TIME
    )
    return EventTimeClassification(status, True)
