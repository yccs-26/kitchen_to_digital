"""명시적 정책·관측·tick만 사용하는 단일 채널 Health 참조 코어.

시계·scheduler·Registry·외부 I/O를 사용하지 않는다. 원문 tuple 원장은 로컬
참조용이며 영속 저장·TTL·fault lifecycle은 별도 계약/구현이 필요하다.
"""

from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json
from math import isfinite

from streaming.state.bindings import EquipmentBinding, InputKind, ProfileKey
from streaming.state.contracts import HealthStatus, _require_text


HEALTH_CONTRACT_VERSION = "health-freshness-v1"
SNAPSHOT_SCHEMA_VERSION = "1"


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("time must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _stamp(value: datetime) -> str:
    return value.isoformat(timespec="microseconds")


def _digest(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _profile_key(key: ProfileKey) -> list[str]:
    return [key.equipment_type, key.profile_id, key.version]


def _finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return isfinite(value)
    except OverflowError:
        return False


class HealthDecision(Enum):
    INITIALIZED = "INITIALIZED"
    TRANSITION = "TRANSITION"
    NO_CHANGE = "NO_CHANGE"
    DUPLICATE = "DUPLICATE"
    ID_CONFLICT = "ID_CONFLICT"
    IDENTITY_MISMATCH = "IDENTITY_MISMATCH"
    INVALID_OBSERVATION = "INVALID_OBSERVATION"
    UNSUPPORTED_INPUT = "UNSUPPORTED_INPUT"
    INCOMPATIBLE_SNAPSHOT = "INCOMPATIBLE_SNAPSHOT"
    REFERENCE_REVERSED = "REFERENCE_REVERSED"
    FUTURE_EVENT = "FUTURE_EVENT"
    BEFORE_ACTIVATION = "BEFORE_ACTIVATION"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    SAME_TIME = "SAME_TIME"
    TOO_OLD_FOR_FRESHNESS = "TOO_OLD_FOR_FRESHNESS"
    OBSOLETE_TIMER = "OBSOLETE_TIMER"


@dataclass(frozen=True)
class HealthObservation:
    """기존 numeric 10필드의 내부 입력. 시각 파싱은 caller 책임.

    primitive 부적합 수치도 담아 reducer의 거부를 표현한다. 수용 원장에는
    정책 검사를 통과한 관측만 들어간다. mutable payload는 허용하지 않는다.
    """

    event_id: str
    event_time: datetime
    store_id: str
    equipment_id: str
    equipment_type: str
    metric_name: str
    metric_value: int | float | bool | str | None
    unit: str
    schema_version: str
    source: str

    def __post_init__(self) -> None:
        for name in (
            "event_id", "store_id", "equipment_id", "equipment_type",
            "metric_name", "unit", "schema_version", "source",
        ):
            _require_text(getattr(self, name), name)
        if self.metric_value is not None and type(self.metric_value) not in (
            int, float, bool, str,
        ):
            raise TypeError("metric_value must be an immutable primitive")
        object.__setattr__(self, "event_time", _utc(self.event_time))


@dataclass(frozen=True)
class HealthPolicy:
    """장비 등록·정책·dependency를 명시적으로 주입한다. 파일을 읽지 않는다."""

    binding: EquipmentBinding
    binding_revision: str
    input_contract_revision: str
    profile: ProfileKey
    freshness_timeout: timedelta
    metric_name: str
    unit: str
    schema_versions: tuple[str, ...]
    sources: tuple[str, ...]
    fault_supported: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.binding, EquipmentBinding):
            raise TypeError("binding must be EquipmentBinding")
        if not isinstance(self.profile, ProfileKey):
            raise TypeError("profile must be ProfileKey")
        if (
            self.binding.input_kind is not InputKind.NUMERIC_TELEMETRY
            or self.profile.equipment_type != (
                self.binding.profile.equipment_type
            )
        ):
            raise ValueError("Health requires a matching numeric binding")
        for name in (
            "binding_revision", "input_contract_revision",
            "metric_name", "unit",
        ):
            _require_text(getattr(self, name), name)
        if not isinstance(self.freshness_timeout, timedelta) or (
            self.freshness_timeout <= timedelta(0)
        ):
            raise ValueError("freshness_timeout must be a positive timedelta")
        for name in ("schema_versions", "sources"):
            values = getattr(self, name)
            if type(values) is not tuple or not values:
                raise TypeError(f"{name} must be a nonempty tuple")
            for value in values:
                _require_text(value, name)
            if len(set(values)) != len(values):
                raise ValueError(f"{name} must be unique")
            object.__setattr__(self, name, tuple(sorted(values)))
        supported = (
            ("refrigerator", "temperature_celsius", "celsius"),
            ("freezer", "temperature_celsius", "celsius"),
            ("fryer", "oil_temperature_celsius", "celsius"),
            ("hood", "fan_rpm", "rpm"),
        )
        if (self.profile.equipment_type, self.metric_name, self.unit) not in (
            supported
        ) or self.schema_versions != ("1.0.0",):
            raise ValueError("unsupported current numeric input contract")
        if type(self.fault_supported) is not bool or self.fault_supported:
            raise ValueError("fault input contract is not implemented")

    @property
    def fingerprint(self) -> str:
        """정렬한 allowlist와 정수 μs로 정책 fingerprint를 고정한다."""
        binding = self.binding
        return _digest([
            HEALTH_CONTRACT_VERSION, _profile_key(self.profile),
            self.freshness_timeout // timedelta(microseconds=1),
            self.metric_name, self.unit, self.schema_versions, self.sources,
            "finite-number-excluding-bool", self.profile.equipment_type == (
                "hood"
            ), self.fault_supported,
            binding.store_id, binding.equipment_id,
            _profile_key(binding.profile), binding.input_kind.value,
            self.binding_revision, self.input_contract_revision,
        ])


def _identity(event: HealthObservation) -> tuple[str, str, str]:
    return event.store_id, event.equipment_id, event.equipment_type


def _binding_identity(policy: HealthPolicy) -> tuple[str, str, str]:
    return (
        policy.binding.store_id, policy.binding.equipment_id,
        policy.profile.equipment_type,
    )


def _valid_observation(event: HealthObservation, policy: HealthPolicy) -> bool:
    return (
        event.metric_name == policy.metric_name and event.unit == policy.unit
        and event.schema_version in policy.schema_versions
        and event.source in policy.sources
        and _finite_number(event.metric_value)
        and (event.equipment_type != "hood" or event.metric_value >= 0)
    )


@dataclass(frozen=True)
class HealthSnapshot:
    """정책·활성화·전체 수용 원장을 보존하는 immutable 로컬 snapshot.

    generation은 last_accepted_event_id 또는 boot epoch에서 재구성한다.
    active_faults는 현재 미지원이므로 빈 tuple만 허용한다.
    """

    policy: HealthPolicy
    monitoring_epoch: str
    monitoring_started_at: datetime
    freshness_deadline: datetime
    last_evaluated_at: datetime
    last_seen: datetime | None = None
    last_accepted_event_id: str | None = None
    health_status: HealthStatus | None = None
    receipts: tuple[HealthObservation, ...] = ()
    health_revision: int = 0
    snapshot_revision: int = 0
    last_health_change_id: str | None = None
    active_faults: tuple[str, ...] = ()
    snapshot_schema_version: str = SNAPSHOT_SCHEMA_VERSION
    health_contract_version: str = HEALTH_CONTRACT_VERSION

    def __post_init__(self) -> None:
        if not isinstance(self.policy, HealthPolicy):
            raise TypeError("policy must be HealthPolicy")
        for name in (
            "monitoring_epoch", "snapshot_schema_version",
            "health_contract_version",
        ):
            _require_text(getattr(self, name), name)
        for name in (
            "monitoring_started_at", "freshness_deadline", "last_evaluated_at",
            "last_seen",
        ):
            value = getattr(self, name)
            if value is not None:
                object.__setattr__(self, name, _utc(value))
            elif name != "last_seen":
                raise ValueError(f"{name} must not be None")
        if self.last_evaluated_at < self.monitoring_started_at:
            raise ValueError("evaluation precedes monitoring start")
        if self.freshness_deadline != (
            self.last_seen or self.monitoring_started_at
        ) + self.policy.freshness_timeout:
            raise ValueError("deadline must equal observation/start + timeout")
        if type(self.receipts) is not tuple or any(
            not isinstance(item, HealthObservation) for item in self.receipts
        ):
            raise TypeError("receipts must be a tuple of HealthObservation")
        identities = {item.event_id for item in self.receipts}
        if len(identities) != len(self.receipts):
            raise ValueError("receipt event_id must be unique")
        for index, item in enumerate(self.receipts):
            if _identity(item) != _binding_identity(self.policy) or not (
                _valid_observation(item, self.policy)
            ):
                raise ValueError("receipt violates identity or policy")
            if not self.monitoring_started_at <= item.event_time <= (
                self.last_evaluated_at
            ) or (index and item.event_time <= (
                self.receipts[index - 1].event_time
            )):
                raise ValueError("invalid receipt time order")
        if self.receipts:
            latest = self.receipts[-1]
            if (self.last_seen, self.last_accepted_event_id) != (
                latest.event_time, latest.event_id
            ):
                raise ValueError("last accepted observation must match ledger")
        elif (self.last_seen, self.last_accepted_event_id) != (None, None):
            raise ValueError("last accepted observation requires a ledger")
        if type(self.active_faults) is not tuple or self.active_faults:
            raise ValueError("active fault restoration is not supported")
        expected = (
            HealthStatus.STALE if self.last_evaluated_at >= (
                self.freshness_deadline
            ) else HealthStatus.HEALTHY if self.last_seen is not None else None
        )
        if self.health_status is not expected:
            raise ValueError("health status is inconsistent with evaluation")
        for name in ("health_revision", "snapshot_revision"):
            value = getattr(self, name)
            if type(value) is not int or value < 0:
                raise ValueError(f"{name} must be a nonnegative integer")
        if self.snapshot_revision < self.health_revision:
            raise ValueError("snapshot_revision must include health changes")
        if self.health_status is None:
            if (self.health_revision, self.last_health_change_id) != (0, None):
                raise ValueError("uninitialized health has no change identity")
        elif self.health_revision < 1 or not (
            isinstance(self.last_health_change_id, str)
            and len(self.last_health_change_id) == 64
            and all(
                c in "0123456789abcdef" for c in self.last_health_change_id
            )
        ):
            raise ValueError("initialized health requires revision/change ID")


def initialize_health(
    policy: HealthPolicy,
    *,
    monitoring_epoch: str,
    monitoring_started_at: datetime,
) -> HealthSnapshot:
    """호출자가 제공한 활성화 기록을 사용하며 현재 시각을 생성하지 않는다."""
    if not isinstance(policy, HealthPolicy):
        raise TypeError("policy must be HealthPolicy")
    start = _utc(monitoring_started_at)
    return HealthSnapshot(
        policy, monitoring_epoch, start,
        start + policy.freshness_timeout, start,
    )


@dataclass(frozen=True)
class HealthTimerToken:
    store_id: str
    equipment_id: str
    profile: ProfileKey
    policy_fingerprint: str
    monitoring_epoch: str
    observation_generation: tuple[str, str]
    expected_deadline: datetime

    def __post_init__(self) -> None:
        for name in (
            "store_id", "equipment_id",
            "policy_fingerprint", "monitoring_epoch",
        ):
            _require_text(getattr(self, name), name)
        if not isinstance(self.profile, ProfileKey):
            raise TypeError("profile must be ProfileKey")
        if type(self.observation_generation) is not tuple or (
            len(self.observation_generation) != 2
            or self.observation_generation[0] not in ("boot", "event")
        ):
            raise ValueError("generation must be a tagged boot/event tuple")
        _require_text(self.observation_generation[1], "generation identity")
        object.__setattr__(
            self, "expected_deadline", _utc(self.expected_deadline)
        )


@dataclass(frozen=True)
class HealthTick:
    token: HealthTimerToken

    def __post_init__(self) -> None:
        if not isinstance(self.token, HealthTimerToken):
            raise TypeError("token must be HealthTimerToken")


def timer_token(snapshot: HealthSnapshot) -> HealthTimerToken:
    """snapshot에서 논리 token만 재구성하며 실제 예약은 하지 않는다."""
    if not isinstance(snapshot, HealthSnapshot):
        raise TypeError("snapshot must be HealthSnapshot")
    event_id = snapshot.last_accepted_event_id
    return HealthTimerToken(
        snapshot.policy.binding.store_id, snapshot.policy.binding.equipment_id,
        snapshot.policy.profile, snapshot.policy.fingerprint,
        snapshot.monitoring_epoch,
        ("event", event_id) if event_id is not None else (
            "boot", snapshot.monitoring_epoch
        ), snapshot.freshness_deadline,
    )


@dataclass(frozen=True)
class HealthChange:
    health_change_id: str
    store_id: str
    equipment_id: str
    equipment_type: str
    profile: ProfileKey
    policy_fingerprint: str
    monitoring_epoch: str
    health_revision: int
    previous_health_change_id: str | None
    previous: HealthStatus | None
    current: HealthStatus
    record_kind: str
    cause: tuple[str, ...]
    health_contract_version: str = HEALTH_CONTRACT_VERSION


@dataclass(frozen=True)
class HealthResult:
    snapshot: HealthSnapshot
    decision: HealthDecision
    changes: tuple[HealthChange, ...] = ()


def _commit(
    previous: HealthSnapshot,
    now: datetime,
    target: HealthStatus | None,
    cause: tuple[str, ...],
    event: HealthObservation | None = None,
) -> HealthResult:
    """검증된 다음 snapshot과 표시 변화 identity를 한 번에 구성한다."""
    values = dict(
        last_evaluated_at=now,
        snapshot_revision=previous.snapshot_revision + 1,
    )
    if event is not None:
        values.update(
            last_seen=event.event_time, last_accepted_event_id=event.event_id,
            freshness_deadline=(
                event.event_time + previous.policy.freshness_timeout
            ),
            receipts=(*previous.receipts, event),
        )
    if target is previous.health_status:
        if event is None and now == previous.last_evaluated_at:
            return HealthResult(previous, HealthDecision.NO_CHANGE)
        return HealthResult(
            replace(previous, **values), HealthDecision.NO_CHANGE,
        )

    policy = previous.policy
    kind = "INITIALIZED" if previous.health_status is None else "TRANSITION"
    revision = previous.health_revision + 1
    root = _digest([
        "equipment-health-boot", policy.fingerprint,
        previous.monitoring_epoch, _stamp(previous.monitoring_started_at),
    ])
    identity = _digest([
        "equipment-health", HEALTH_CONTRACT_VERSION,
        _profile_key(policy.profile), policy.fingerprint,
        *_binding_identity(policy), previous.monitoring_epoch, revision,
        previous.last_health_change_id or root,
        previous.health_status.value if previous.health_status else None,
        target.value, kind, cause,
    ])
    change = HealthChange(
        identity, *_binding_identity(policy),
        policy.profile, policy.fingerprint,
        previous.monitoring_epoch, revision, previous.last_health_change_id,
        previous.health_status, target, kind, cause,
    )
    values.update(
        health_status=target, health_revision=revision,
        last_health_change_id=identity,
    )
    return HealthResult(
        replace(previous, **values), HealthDecision(kind), (change,),
    )


def reduce_health(
    previous: HealthSnapshot,
    item: HealthObservation | HealthTick,
    *,
    policy: HealthPolicy,
    reference_time: datetime,
) -> HealthResult:
    """계약의 순서대로 판정하며 거부 입력은 snapshot 전체를 유지한다."""
    if not isinstance(previous, HealthSnapshot):
        raise TypeError("previous must be HealthSnapshot")
    if not isinstance(policy, HealthPolicy):
        raise TypeError("policy must be HealthPolicy")
    now = _utc(reference_time)
    if (
        previous.policy != policy
        or previous.snapshot_schema_version != SNAPSHOT_SCHEMA_VERSION
        or previous.health_contract_version != HEALTH_CONTRACT_VERSION
    ):
        return HealthResult(previous, HealthDecision.INCOMPATIBLE_SNAPSHOT)
    if isinstance(item, HealthTick):
        if item.token != timer_token(previous):
            return HealthResult(previous, HealthDecision.OBSOLETE_TIMER)
        if now < previous.last_evaluated_at:
            return HealthResult(previous, HealthDecision.REFERENCE_REVERSED)
        target = (
            HealthStatus.STALE if now >= previous.freshness_deadline
            else previous.health_status
        )
        cause = (
            "timeout", *item.token.observation_generation,
            _stamp(item.token.expected_deadline),
        )
        return _commit(previous, now, target, cause)
    if not isinstance(item, HealthObservation):
        return HealthResult(previous, HealthDecision.UNSUPPORTED_INPUT)
    if _identity(item) != _binding_identity(policy):
        return HealthResult(previous, HealthDecision.IDENTITY_MISMATCH)
    for receipt in previous.receipts:
        if item.event_id == receipt.event_id:
            # bool/string/NaN 등 부적합 값은 수용 원문과 동등할 수 없다.
            same = _finite_number(item.metric_value) and item == receipt
            decision = (
                HealthDecision.DUPLICATE if same
                else HealthDecision.ID_CONFLICT
            )
            return HealthResult(previous, decision)
    if now < previous.last_evaluated_at:
        return HealthResult(previous, HealthDecision.REFERENCE_REVERSED)
    if item.event_time > now:
        return HealthResult(previous, HealthDecision.FUTURE_EVENT)
    if item.event_time < previous.monitoring_started_at:
        return HealthResult(previous, HealthDecision.BEFORE_ACTIVATION)
    if previous.last_seen is not None:
        if item.event_time < previous.last_seen:
            return HealthResult(previous, HealthDecision.OUT_OF_ORDER)
        if item.event_time == previous.last_seen:
            return HealthResult(previous, HealthDecision.SAME_TIME)
    if not _valid_observation(item, policy):
        return HealthResult(previous, HealthDecision.INVALID_OBSERVATION)
    if now >= item.event_time + policy.freshness_timeout:
        return HealthResult(previous, HealthDecision.TOO_OLD_FOR_FRESHNESS)
    cause = ("observation", item.event_id, _stamp(item.event_time))
    return _commit(previous, now, HealthStatus.HEALTHY, cause, item)
