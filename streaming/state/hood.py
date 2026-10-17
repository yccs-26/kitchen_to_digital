"""외부 I/O 없는 Hood RPM 관측 qualifier. Health·Registry와 독립적이다."""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from math import isfinite

from streaming.state.bindings import HOOD_PROFILE
from streaming.state.contracts import _require_text


POLICY_VERSION = "hood-rpm-qualifier-v1"


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("time must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _number(value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError("RPM must be numeric, excluding bool")
    if not isfinite(value):
        raise ValueError("RPM must be finite")


class HoodState(Enum):
    """RPM 관측 기반 상태이며 전원 명령·풍량·장애를 뜻하지 않는다."""

    STOPPED = "STOPPED"
    ROTATING = "ROTATING"


class HoodDecision(Enum):
    """초기 관측 확정과 이후 상태 변경을 구별한다."""

    INITIALIZED = "INITIALIZED"
    TRANSITION = "TRANSITION"
    PENDING = "PENDING"
    NO_CHANGE = "NO_CHANGE"
    INVALID_SAMPLE = "INVALID_SAMPLE"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    DUPLICATE = "DUPLICATE"
    ID_CONFLICT = "ID_CONFLICT"
    REFERENCE_REVERSED = "REFERENCE_REVERSED"
    FUTURE_EVENT = "FUTURE_EVENT"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    SAME_TIME = "SAME_TIME"


@dataclass(frozen=True)
class HoodRpmEvent:
    """기존 validated 10필드의 내부 모델. wire 시각 파싱은 caller 책임.

    numeric 구조를 검사한다. metric/unit/version 및 음수 RPM은 qualifier가
    방어적으로 거부하고 후보를 취소한다. 실제 장비 등록·source 권한 검증이나
    validated topic 소비를 수행하는 adapter가 아니다.
    """

    event_id: str
    event_time: datetime
    store_id: str
    equipment_id: str
    equipment_type: str
    metric_name: str
    metric_value: float
    unit: str
    schema_version: str
    source: str

    def __post_init__(self) -> None:
        for name in (
            "event_id", "store_id", "equipment_id", "equipment_type",
            "metric_name", "unit", "schema_version", "source",
        ):
            _require_text(getattr(self, name), name)
        _number(self.metric_value)
        object.__setattr__(self, "event_time", _utc(self.event_time))


@dataclass(frozen=True)
class HoodPolicy:
    """호출자가 명시하는 초기 실험 파라미터이며 장비 사양이 아니다."""

    enter_rpm: float
    exit_rpm: float
    enter_duration: timedelta
    exit_duration: timedelta
    max_sample_gap: timedelta

    def __post_init__(self) -> None:
        _number(self.enter_rpm)
        _number(self.exit_rpm)
        if not 0 <= self.exit_rpm < self.enter_rpm:
            raise ValueError("require 0 <= exit_rpm < enter_rpm")
        for name in ("enter_duration", "exit_duration", "max_sample_gap"):
            value = getattr(self, name)
            if not isinstance(value, timedelta) or value <= timedelta(0):
                raise ValueError(f"{name} must be a positive timedelta")


@dataclass(frozen=True)
class HoodSnapshot:
    """정책과 identity가 고정된 로컬 snapshot. 처음에는 current=None.

    receipts는 전체 소비 원문이며 무제한 로컬 참조 원장이다. 운영 보존·복구는
    별도 설계한다. last_event_time은 invalid sample도 포함하는 순서 cursor다.
    Health last_seen으로 사용하지 않는다.
    """

    store_id: str
    equipment_id: str
    policy: HoodPolicy
    current: HoodState | None = None
    candidate: HoodState | None = None
    candidate_since: datetime | None = None
    last_event_time: datetime | None = None
    receipts: tuple[HoodRpmEvent, ...] = ()

    def __post_init__(self) -> None:
        _require_text(self.store_id, "store_id")
        _require_text(self.equipment_id, "equipment_id")
        if not isinstance(self.policy, HoodPolicy):
            raise TypeError("policy must be HoodPolicy")
        for name in ("current", "candidate"):
            value = getattr(self, name)
            if value is not None and not isinstance(value, HoodState):
                raise TypeError(f"{name} must be HoodState or None")
        for name in ("candidate_since", "last_event_time"):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
        if (self.candidate is None) != (self.candidate_since is None):
            raise ValueError("candidate and candidate_since must coexist")
        if self.candidate is not None and (
            self.candidate is self.current
            or self.last_event_time is None
            or self.candidate_since > self.last_event_time
        ):
            raise ValueError("invalid candidate interval or direction")
        if type(self.receipts) is not tuple or any(
            not isinstance(item, HoodRpmEvent) for item in self.receipts
        ):
            raise TypeError("receipts must be a tuple of HoodRpmEvent")
        if len({e.event_id for e in self.receipts}) != len(self.receipts):
            raise ValueError("receipt event_id must be unique")
        for index, item in enumerate(self.receipts):
            if (item.store_id, item.equipment_id) != (
                self.store_id, self.equipment_id
            ):
                raise ValueError("receipt identity must match snapshot")
            if self.last_event_time is None or (
                item.event_time > self.last_event_time
                or (index and item.event_time <= (
                    self.receipts[index - 1].event_time
                ))
            ):
                raise ValueError("invalid receipt time order")


@dataclass(frozen=True)
class HoodStateChange:
    """순수 출력 의도. 초기화는 가상의 이전 상태/전이를 만들지 않는다."""

    state_change_id: str
    event_id: str
    store_id: str
    equipment_id: str
    event_time: datetime
    previous: HoodState
    current: HoodState
    policy: HoodPolicy
    policy_version: str = POLICY_VERSION


@dataclass(frozen=True)
class HoodResult:
    state: HoodSnapshot
    decision: HoodDecision
    intents: tuple[HoodStateChange, ...] = ()


def _policy_identity(policy: HoodPolicy) -> list:
    """숫자는 600과 600.0이 동일하도록, 기간은 정확한 정수 μs로 표현."""
    def micros(value: timedelta) -> int:
        return value // timedelta(microseconds=1)

    return [
        float(policy.enter_rpm), float(policy.exit_rpm),
        micros(policy.enter_duration), micros(policy.exit_duration),
        micros(policy.max_sample_gap),
    ]


def qualify_rpm(
    previous: HoodSnapshot,
    event: HoodRpmEvent,
    *,
    reference_time: datetime,
) -> HoodResult:
    """새 시간순 샘플만 후보·cursor를 진행한다. 벽시계를 읽지 않는다.

    identity → 원장 → 기준 역전 → 미래 → 역순/동시각 → 입력 적합성 →
    공백·구간·지속시간 순서다. 시간/identity 거부는 후보도 그대로 보존한다.
    정책 변경에는 명시적인 snapshot migration 또는 재계산이 필요하다.
    """
    if not isinstance(previous, HoodSnapshot):
        raise TypeError("previous must be HoodSnapshot")
    if not isinstance(event, HoodRpmEvent):
        raise TypeError("event must be HoodRpmEvent")
    now = _utc(reference_time)
    if (event.store_id, event.equipment_id) != (
        previous.store_id, previous.equipment_id
    ):
        return HoodResult(previous, HoodDecision.IDENTITY_MISMATCH)
    for receipt in previous.receipts:
        if receipt.event_id == event.event_id:
            decision = (
                HoodDecision.DUPLICATE if receipt == event
                else HoodDecision.ID_CONFLICT
            )
            return HoodResult(previous, decision)
    last = previous.last_event_time
    if last is not None and now < last:
        return HoodResult(previous, HoodDecision.REFERENCE_REVERSED)
    if event.event_time > now:
        return HoodResult(previous, HoodDecision.FUTURE_EVENT)
    if last is not None:
        if event.event_time < last:
            return HoodResult(previous, HoodDecision.OUT_OF_ORDER)
        if event.event_time == last:
            return HoodResult(previous, HoodDecision.SAME_TIME)

    consumed = replace(
        previous, last_event_time=event.event_time,
        receipts=(*previous.receipts, event),
    )
    cleared = replace(consumed, candidate=None, candidate_since=None)
    if (
        event.equipment_type != "hood" or event.metric_name != "fan_rpm"
        or event.unit != "rpm" or event.schema_version != "1.0.0"
        or event.metric_value < 0
    ):
        return HoodResult(cleared, HoodDecision.INVALID_SAMPLE)

    policy = previous.policy
    target = None
    if event.metric_value >= policy.enter_rpm:
        target = HoodState.ROTATING
    elif event.metric_value <= policy.exit_rpm:
        target = HoodState.STOPPED
    if target is None or target is previous.current:
        return HoodResult(cleared, HoodDecision.NO_CHANGE)

    since = previous.candidate_since
    if (
        previous.candidate is not target or last is None
        or event.event_time - last > policy.max_sample_gap
    ):
        since = event.event_time
    duration = (
        policy.enter_duration if target is HoodState.ROTATING
        else policy.exit_duration
    )
    if event.event_time - since < duration:
        pending = replace(consumed, candidate=target, candidate_since=since)
        return HoodResult(pending, HoodDecision.PENDING)

    confirmed = replace(cleared, current=target)
    if previous.current is None:
        return HoodResult(confirmed, HoodDecision.INITIALIZED)
    # Trigger event_id를 보존하며 처리 지연과 Python hash에 의존하지 않는다.
    identity = json.dumps([
        POLICY_VERSION, HOOD_PROFILE.equipment_type, HOOD_PROFILE.profile_id,
        HOOD_PROFILE.version, _policy_identity(policy),
        event.store_id, event.equipment_id, event.event_id,
        event.event_time.isoformat(timespec="microseconds"),
        previous.current.value, target.value, since.isoformat(),
    ], ensure_ascii=False, separators=(",", ":"))
    intent = HoodStateChange(
        hashlib.sha256(identity.encode("utf-8")).hexdigest(),
        event.event_id, event.store_id, event.equipment_id, event.event_time,
        previous.current, target, policy,
    )
    return HoodResult(confirmed, HoodDecision.TRANSITION, (intent,))
