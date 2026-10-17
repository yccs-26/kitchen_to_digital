"""기존 Fryer FSM과 Hood qualifier를 그대로 호출하는 순수 adapter."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from streaming.state.bindings import (
    FRYER_REFERENCE, HOOD_PROFILE, InputKind, ProfileKey,
)
from streaming.state.contracts import SyntheticSignal
from streaming.state.fsm import FsmResult, TwinState, transition
from streaming.state.hood import (
    HoodResult, HoodRpmEvent, HoodSnapshot, qualify_rpm,
)


@dataclass(frozen=True)
class FryerReferenceAdapter:
    """synthetic 명시적 신호 전용이며 수치 telemetry를 변환하지 않는다."""

    profile: ClassVar[ProfileKey] = FRYER_REFERENCE
    input_kind: ClassVar[InputKind] = InputKind.SYNTHETIC_SIGNAL
    snapshot_type: ClassVar[type[TwinState]] = TwinState

    def accepts(self, event: object) -> bool:
        """수치 입력을 명시적 조리 신호로 변환하지 않는다."""
        return isinstance(event, SyntheticSignal)

    def evaluate(
        self,
        previous: TwinState,
        event: SyntheticSignal,
        *,
        reference_time: datetime,
    ) -> FsmResult:
        """원래 함수의 판정·출력·전이 ID를 그대로 반환한다."""
        return transition(previous, event, reference_time=reference_time)


@dataclass(frozen=True)
class HoodRpmAdapter:
    """RPM 입력 계약을 선택하며 후보·정책·출력을 변환하지 않는다."""

    profile: ClassVar[ProfileKey] = HOOD_PROFILE
    input_kind: ClassVar[InputKind] = InputKind.NUMERIC_TELEMETRY
    snapshot_type: ClassVar[type[HoodSnapshot]] = HoodSnapshot

    def accepts(self, event: object) -> bool:
        """Registry 경계의 계약 검사. 음수 RPM 판정은 qualifier에 위임."""
        return (
            isinstance(event, HoodRpmEvent)
            and event.equipment_type == "hood"
            and event.metric_name == "fan_rpm"
            and event.unit == "rpm"
            and event.schema_version == "1.0.0"
        )

    def evaluate(
        self,
        previous: HoodSnapshot,
        event: HoodRpmEvent,
        *,
        reference_time: datetime,
    ) -> HoodResult:
        """후보 시작·원장·전이 ID를 포함해 원본 계산 결과를 반환한다."""
        return qualify_rpm(previous, event, reference_time=reference_time)


# 지원하는 두 adapter만 허용한다. 임의 handler의 duck typing/fallback은 없다.
EquipmentAdapter = FryerReferenceAdapter | HoodRpmAdapter
