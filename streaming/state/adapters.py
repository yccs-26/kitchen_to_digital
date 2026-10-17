"""기존 조리 Reference FSM을 변경하지 않고 호출하는 순수 adapter."""

from dataclasses import dataclass
from datetime import datetime
from typing import ClassVar

from streaming.state.bindings import FRYER_REFERENCE, InputKind, ProfileKey
from streaming.state.contracts import SyntheticSignal
from streaming.state.fsm import FsmResult, TwinState, transition


@dataclass(frozen=True)
class FryerReferenceAdapter:
    """synthetic 명시적 신호 전용이며 수치 telemetry를 변환하지 않는다."""

    profile: ClassVar[ProfileKey] = FRYER_REFERENCE
    input_kind: ClassVar[InputKind] = InputKind.SYNTHETIC_SIGNAL

    def evaluate(
        self,
        previous: TwinState,
        event: SyntheticSignal,
        *,
        reference_time: datetime,
    ) -> FsmResult:
        """원래 함수의 판정·출력·전이 ID를 그대로 반환한다."""
        return transition(previous, event, reference_time=reference_time)
