"""외부 I/O 없이 synthetic 명시적 보고를 계산하는 reference FSM."""

from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json

from streaming.state.contracts import (
    CleaningCompletedSignal,
    CleaningSignal,
    FaultSignal,
    ModeSignal,
    OperationalState,
    StateObservation,
    SyntheticSignal,
    _require_text,
)


POLICY_VERSION = "synthetic-reference-v1"


class Decision(Enum):
    """전이·무변경·입력 거부를 구별하는 계산 결과."""

    TRANSITION = "TRANSITION"
    NO_CHANGE = "NO_CHANGE"
    FORBIDDEN = "FORBIDDEN"
    DUPLICATE = "DUPLICATE"
    ID_CONFLICT = "ID_CONFLICT"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    FUTURE_EVENT = "FUTURE_EVENT"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    SAME_TIME = "SAME_TIME"
    REFERENCE_REVERSED = "REFERENCE_REVERSED"
    HEALTH_DEFERRED = "HEALTH_DEFERRED"


def _utc(value: datetime) -> datetime:
    """벽시계를 읽지 않고 명시적 aware 시각을 UTC로 바꾼다."""
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("time must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


@dataclass(frozen=True)
class TwinState:
    """호출자가 제공하는 초기 상태와 로컬 재실행 원장.

    receipts는 판정한 신호 원문을 보관한다. 운영용 보존 기간·checkpoint
    직렬화·크기 제한은 별도 구현 전까지 이 모델을 서비스에 사용하지 않는다.
    """

    store_id: str
    equipment_id: str
    observation: StateObservation
    last_event_time: datetime | None = None
    receipts: tuple[SyntheticSignal, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.store_id, "store_id")
        _require_text(self.equipment_id, "equipment_id")
        if not isinstance(self.observation, StateObservation):
            raise TypeError("observation must be StateObservation")
        if type(self.receipts) is not tuple or any(
            not isinstance(item, SyntheticSignal) for item in self.receipts
        ):
            raise TypeError("receipts must be a tuple of SyntheticSignal")
        if self.last_event_time is not None:
            object.__setattr__(
                self, "last_event_time", _utc(self.last_event_time)
            )
        identities = {item.signal_id for item in self.receipts}
        if len(identities) != len(self.receipts):
            raise ValueError("receipt signal_id must be unique")
        for item in self.receipts:
            if (item.store_id, item.equipment_id) != (
                self.store_id, self.equipment_id
            ):
                raise ValueError("receipt identity must match state")
        if self.receipts and (
            self.last_event_time is None
            or any(
                item.event_time > self.last_event_time
                for item in self.receipts
            )
        ):
            raise ValueError("receipt time must not exceed last_event_time")


@dataclass(frozen=True)
class StateChangeIntent:
    """미전달 출력 의도이며 wire schema나 sink ACK를 의미하지 않는다."""

    state_change_id: str
    signal_id: str
    store_id: str
    equipment_id: str
    event_time: datetime
    previous: OperationalState
    current: OperationalState
    policy_version: str = POLICY_VERSION


@dataclass(frozen=True)
class FsmResult:
    """변경 불가능한 다음 snapshot, 판정 이유와 출력 의도."""

    state: TwinState
    decision: Decision
    intents: tuple[StateChangeIntent, ...] = ()


def _target(signal: SyntheticSignal) -> OperationalState | None:
    """단일 보고에서만 목표 상태를 읽고 수치 telemetry는 받지 않는다."""
    payload = signal.payload
    if isinstance(payload, ModeSignal):
        return OperationalState(payload.mode.value)
    if isinstance(payload, CleaningSignal) and payload.active:
        return OperationalState.CLEANING
    if isinstance(payload, CleaningCompletedSignal):
        return OperationalState.IDLE
    return None


# IDLE 보고만으로 청소 완료를 추론하지 않는다.
_ALLOWED = {
    (OperationalState.IDLE, OperationalState.PREHEATING),
    (OperationalState.PREHEATING, OperationalState.COOKING),
    (OperationalState.COOKING, OperationalState.CLEANING),
    (OperationalState.CLEANING, OperationalState.IDLE),
}


def transition(
    previous: TwinState,
    event: SyntheticSignal,
    *,
    reference_time: datetime,
) -> FsmResult:
    """이전 snapshot과 단일 신호를 계산하며 입력 객체를 변경하지 않는다.

    identity → 중복/충돌 → 기준 시각 → 미래/역순/동시각 → 전이 순으로
    판정한다. 시간 거부는 원장에 넣지 않아 이후 정상 시각으로 재시도한다.
    시간 검사를 통과한 금지 전이도 소비하여 나중에 유효 전이로 재해석하지
    않는다. health는 보존하고 별도 계산 대상으로 명시한다.
    """
    if not isinstance(previous, TwinState):
        raise TypeError("previous must be TwinState")
    if not isinstance(event, SyntheticSignal):
        raise TypeError("event must be SyntheticSignal")
    now = _utc(reference_time)
    if (event.store_id, event.equipment_id) != (
        previous.store_id, previous.equipment_id
    ):
        return FsmResult(previous, Decision.IDENTITY_MISMATCH)
    for receipt in previous.receipts:
        if receipt.signal_id == event.signal_id:
            decision = (
                Decision.DUPLICATE
                if receipt == event else Decision.ID_CONFLICT
            )
            return FsmResult(previous, decision)
    last_time = previous.last_event_time
    if last_time is not None and now < last_time:
        return FsmResult(previous, Decision.REFERENCE_REVERSED)
    if event.event_time > now:
        return FsmResult(previous, Decision.FUTURE_EVENT)
    if last_time is not None:
        if event.event_time < last_time:
            return FsmResult(previous, Decision.OUT_OF_ORDER)
        if event.event_time == last_time:
            return FsmResult(previous, Decision.SAME_TIME)

    consumed = replace(
        previous,
        last_event_time=event.event_time,
        receipts=(*previous.receipts, event),
    )
    if isinstance(event.payload, FaultSignal):
        return FsmResult(consumed, Decision.HEALTH_DEFERRED)
    target = _target(event)
    current = previous.observation.operational_state
    if target is None or target is current:
        return FsmResult(consumed, Decision.NO_CHANGE)
    permitted = (current, target) in _ALLOWED
    if current is OperationalState.CLEANING:
        permitted = isinstance(event.payload, CleaningCompletedSignal)
    if isinstance(event.payload, CleaningCompletedSignal):
        permitted = current is OperationalState.CLEANING
    if not permitted:
        return FsmResult(consumed, Decision.FORBIDDEN)

    observation = replace(previous.observation, operational_state=target)
    next_state = replace(consumed, observation=observation)
    # 기준 처리 시각과 무관하게 같은 논리 전이는 동일 identity를 갖는다.
    identity = json.dumps(
        [
            POLICY_VERSION, event.store_id, event.equipment_id,
            event.signal_id, event.event_time.isoformat(),
            current.value, target.value,
        ],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    intent = StateChangeIntent(
        state_change_id=hashlib.sha256(identity).hexdigest(),
        signal_id=event.signal_id,
        store_id=event.store_id,
        equipment_id=event.equipment_id,
        event_time=event.event_time,
        previous=current,
        current=target,
    )
    return FsmResult(next_state, Decision.TRANSITION, (intent,))
