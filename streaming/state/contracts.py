"""전이 실행이나 실제 wire 입력 연결이 없는 로컬 후보 계약.

현재 센서 Avro에는 아래 명시적 신호가 없다. 후보 신호는 synthetic
테스트에서만 사용하며 온도에서 상태나 장애를 추론하지 않는다.
"""

from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Literal


class OperationalState(Enum):
    """장비 동작 상태이며 관측 신뢰도와 독립적이다."""

    IDLE = "IDLE"
    PREHEATING = "PREHEATING"
    COOKING = "COOKING"
    CLEANING = "CLEANING"


class HealthStatus(Enum):
    """STALE은 관측 부족이며 명시적 장애인 FAULT와 구분한다."""

    HEALTHY = "HEALTHY"
    STALE = "STALE"
    FAULT = "FAULT"


@dataclass(frozen=True)
class StateObservation:
    """이미 알려진 두 상태를 담으며 초기 상태나 전이를 결정하지 않는다."""

    operational_state: OperationalState
    health_status: HealthStatus

    def __post_init__(self) -> None:
        if not isinstance(self.operational_state, OperationalState):
            raise TypeError("operational_state must be OperationalState")
        if not isinstance(self.health_status, HealthStatus):
            raise TypeError("health_status must be HealthStatus")


class ReportedMode(Enum):
    """제어기가 보고했다고 가정한 모드 후보이며 전이 명령이 아니다."""

    IDLE = "IDLE"
    PREHEATING = "PREHEATING"
    COOKING = "COOKING"


@dataclass(frozen=True)
class ModeSignal:
    """모드 보고 후보이며 청소 여부는 별도 신호로 표현한다."""

    mode: ReportedMode

    def __post_init__(self) -> None:
        if not isinstance(self.mode, ReportedMode):
            raise TypeError("mode must be ReportedMode")


@dataclass(frozen=True)
class CleaningSignal:
    """명시적 청소 활성/비활성 보고 후보이며 완료 판정은 하지 않는다."""

    active: bool

    def __post_init__(self) -> None:
        if type(self.active) is not bool:
            raise TypeError("active must be bool")


@dataclass(frozen=True)
class CleaningCompletedSignal:
    """제어기가 청소의 정상 완료를 명시적으로 보고한 synthetic 신호."""


def _require_text(value: object, field: str) -> None:
    """식별자는 빈 문자열을 허용하지 않고 원문을 보존한다."""
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a non-blank string")


@dataclass(frozen=True)
class FaultSignal:
    """특정 장애 코드의 발생/해제 후보이며 전체 health를 결정하지 않는다."""

    code: str
    active: bool

    def __post_init__(self) -> None:
        _require_text(self.code, "code")
        if type(self.active) is not bool:
            raise TypeError("active must be bool")


@dataclass(frozen=True)
class SyntheticSignal:
    """하나의 명시적 신호를 담는 synthetic 전용 입력 후보.

    시각은 내부 UTC로 정규화한다. 식별자·source는 후보 추적 정보일 뿐
    실제 장비 등록, 신호 권한, 순서, 중복 또는 전달 보장을 검증하지 않는다.
    """

    signal_id: str
    event_time: datetime
    store_id: str
    equipment_id: str
    source: str
    payload: (
        ModeSignal | CleaningSignal | CleaningCompletedSignal | FaultSignal
    )
    provenance: Literal["synthetic_test_only"]

    def __post_init__(self) -> None:
        for field in ("signal_id", "store_id", "equipment_id", "source"):
            _require_text(getattr(self, field), field)
        if self.provenance != "synthetic_test_only":
            raise ValueError("Only synthetic_test_only signals are allowed")
        if not isinstance(
            self.payload,
            (ModeSignal, CleaningSignal, CleaningCompletedSignal, FaultSignal),
        ):
            raise TypeError("payload must be an explicit candidate signal")
        if (
            not isinstance(self.event_time, datetime)
            or self.event_time.utcoffset() is None
        ):
            raise ValueError("event_time must be a timezone-aware datetime")
        object.__setattr__(
            self, "event_time", self.event_time.astimezone(timezone.utc)
        )
