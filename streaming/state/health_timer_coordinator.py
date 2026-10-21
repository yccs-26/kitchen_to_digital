"""명시적 시각으로 Health timer 요청을 조정하는 순수 로컬 계층.

snapshot·queue·clock을 전역 보관하지 않는다. 관측 순서와 다음 snapshot의
확정/저장은 caller 책임이며, 이 모듈은 실제 예약·ACK·CAS를 구현하지 않는다.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

from streaming.state.health import (
    HEALTH_CONTRACT_VERSION, SNAPSHOT_SCHEMA_VERSION,
    HealthChange, HealthDecision, HealthPolicy, HealthResult, HealthSnapshot,
    HealthTick, HealthTimerToken, reduce_health, timer_token,
)
from streaming.state.health_codec import decode_health_snapshot


class TimerStatus(Enum):
    """요청의 실행 가능성만 나타내며 Equipment Health 상태가 아니다."""

    WAITING = "WAITING"
    DUE = "DUE"
    ALREADY_EVALUATED = "ALREADY_EVALUATED"
    REFERENCE_REVERSED = "REFERENCE_REVERSED"
    OBSOLETE = "OBSOLETE"
    EVALUATED = "EVALUATED"


class HealthTimerCoordinatorError(ValueError):
    def __init__(self, code: HealthDecision) -> None:
        self.code = code
        super().__init__(code.value)


@dataclass(frozen=True)
class HealthTimerRequest:
    """기존 token과 요청 상태. 예약 ID나 별도 deadline을 생성하지 않는다."""

    token: HealthTimerToken
    status: TimerStatus


@dataclass(frozen=True)
class HealthTimerResult:
    snapshot: HealthSnapshot
    status: TimerStatus
    reduction: HealthResult | None = None

    @property
    def changes(self) -> tuple[HealthChange, ...]:
        return self.reduction.changes if self.reduction is not None else ()


@dataclass(frozen=True)
class RestoredHealthTimer:
    """복원된 상태와 재구성한 요청. 재등록/실행은 caller가 결정한다."""

    snapshot: HealthSnapshot
    request: HealthTimerRequest


def _context(snapshot: HealthSnapshot, policy: HealthPolicy) -> None:
    if not isinstance(snapshot, HealthSnapshot):
        raise TypeError("snapshot must be HealthSnapshot")
    if not isinstance(policy, HealthPolicy):
        raise TypeError("policy must be HealthPolicy")
    if (
        snapshot.policy != policy
        or snapshot.snapshot_schema_version != SNAPSHOT_SCHEMA_VERSION
        or snapshot.health_contract_version != HEALTH_CONTRACT_VERSION
    ):
        raise HealthTimerCoordinatorError(HealthDecision.INCOMPATIBLE_SNAPSHOT)


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("evaluation_time must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _status(snapshot: HealthSnapshot, now: datetime) -> TimerStatus:
    # 평가 시각으로 timer의 실행 필요성만 구분한다. Health 판정은 하지 않는다.
    if now < snapshot.last_evaluated_at:
        return TimerStatus.REFERENCE_REVERSED
    if snapshot.last_evaluated_at >= snapshot.freshness_deadline:
        return TimerStatus.ALREADY_EVALUATED
    if now < snapshot.freshness_deadline:
        return TimerStatus.WAITING
    return TimerStatus.DUE


def plan_health_timer(
    snapshot: HealthSnapshot,
    *,
    policy: HealthPolicy,
    evaluation_time: datetime,
) -> HealthTimerRequest:
    """현재 token을 재사용해 대기/만료/평가 완료를 구분한다. 상태는 불변이다."""
    _context(snapshot, policy)
    now = _utc(evaluation_time)
    return HealthTimerRequest(timer_token(snapshot), _status(snapshot, now))


def coordinate_health_timer(
    snapshot: HealthSnapshot,
    token: HealthTimerToken,
    *,
    policy: HealthPolicy,
    evaluation_time: datetime,
    reducer: Callable[..., HealthResult] = reduce_health,
) -> HealthTimerResult:
    """현재 대기 요청은 미실행하고 전달된 만료/중복/거부 tick은 Core에 위임한다.

    reducer 주입은 동기 실패/재시도 경계 테스트용이다. 함수가 성공해도
    snapshot 확정·Health 변화 발행·timer ACK가 완료되었다는 의미는 아니다.
    """
    _context(snapshot, policy)
    if not isinstance(token, HealthTimerToken):
        raise TypeError("token must be HealthTimerToken")
    now = _utc(evaluation_time)
    current = timer_token(snapshot)
    status = _status(snapshot, now)
    # Core의 obsolete-token → reference-reversal 우선순위를 보존한다.
    if token != current or status is TimerStatus.REFERENCE_REVERSED:
        result = reducer(
            snapshot, HealthTick(token), policy=policy, reference_time=now,
        )
        status = (
            TimerStatus.OBSOLETE
            if result.decision is HealthDecision.OBSOLETE_TIMER
            else TimerStatus.REFERENCE_REVERSED
        )
        return HealthTimerResult(result.snapshot, status, result)
    if status is TimerStatus.WAITING:
        return HealthTimerResult(snapshot, status)
    result = reducer(
        snapshot, HealthTick(token), policy=policy, reference_time=now,
    )
    delivery = (
        TimerStatus.ALREADY_EVALUATED
        if status is TimerStatus.ALREADY_EVALUATED else TimerStatus.EVALUATED
    )
    return HealthTimerResult(result.snapshot, delivery, result)


def restore_health_timer(
    payload: bytes,
    *,
    policy: HealthPolicy,
    evaluation_time: datetime,
) -> RestoredHealthTimer:
    """기존 Codec으로 복원한 뒤 현재 요청을 재구성한다. timer는 실행하지 않는다."""
    snapshot = decode_health_snapshot(payload, policy=policy)
    request = plan_health_timer(
        snapshot, policy=policy, evaluation_time=evaluation_time,
    )
    return RestoredHealthTimer(snapshot, request)
