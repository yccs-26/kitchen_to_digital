"""Fryer/Hood Health 정책 선택과 기존 Core/Timer/Codec의 순수 연결 계층.

HealthPolicy 안의 EquipmentBinding/ProfileKey를 재사용한다. 운영 설정·권한을
읽거나 자동 부여하지 않으며 Operational Registry의 공개 API를 변경하지 않는다.
"""

from dataclasses import dataclass
from datetime import datetime

from streaming.state.bindings import FRYER_REFERENCE, HOOD_PROFILE, ProfileKey
from streaming.state.contracts import _require_text
from streaming.state.health import (
    HEALTH_CONTRACT_VERSION, SNAPSHOT_SCHEMA_VERSION,
    HealthObservation, HealthPolicy, HealthResult, HealthSnapshot,
    HealthTimerToken, reduce_health,
)
from streaming.state.health_timer_coordinator import (
    HealthTimerRequest, HealthTimerResult, RestoredHealthTimer,
    coordinate_health_timer, plan_health_timer, restore_health_timer,
)
from streaming.state.hood import HoodRpmEvent


class HealthRegistryError(ValueError):
    """선택/입력 권한 실패이며 Core HealthDecision과 구분한다."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _observation(event: HealthObservation | HoodRpmEvent) -> HealthObservation:
    if isinstance(event, HealthObservation):
        return event
    if isinstance(event, HoodRpmEvent):
        # 기존 numeric 10필드를 보존한다. mode/fault/wire string은 변환하지 않는다.
        return HealthObservation(
            event.event_id, event.event_time,
            event.store_id, event.equipment_id,
            event.equipment_type, event.metric_name, event.metric_value,
            event.unit, event.schema_version, event.source,
        )
    raise HealthRegistryError("UNSUPPORTED_INPUT")


@dataclass(frozen=True)
class EquipmentHealthRegistry:
    """명시적 장비별 정책 tuple만 보유하는 Health 선택 adapter.

    policy.binding은 기존 EquipmentBinding이고 policy.profile은 별도 Health
    ProfileKey다. 두 객체를 별도 설정 catalog에 중복 등록하지 않는다.
    """

    policies: tuple[HealthPolicy, ...]

    def __post_init__(self) -> None:
        if type(self.policies) is not tuple or any(
            not isinstance(policy, HealthPolicy) for policy in self.policies
        ):
            raise TypeError("policies must be a tuple of HealthPolicy")
        ids = {policy.binding.equipment_id for policy in self.policies}
        if len(ids) != len(self.policies):
            raise HealthRegistryError("DUPLICATE_BINDING")
        for policy in self.policies:
            if policy.profile.equipment_type not in ("fryer", "hood"):
                raise HealthRegistryError("UNSUPPORTED_EQUIPMENT")
            expected = (
                FRYER_REFERENCE if policy.profile.equipment_type == "fryer"
                else HOOD_PROFILE
            )
            if policy.binding.profile != expected:
                raise HealthRegistryError("UNSUPPORTED_OPERATIONAL_PROFILE")

    def resolve(
        self,
        *,
        store_id: str,
        equipment_id: str,
        equipment_type: str,
        profile: ProfileKey,
    ) -> HealthPolicy:
        """장비 key → store/type → Health key 전체를 exact-match한다."""
        for name, value in (
            ("store_id", store_id), ("equipment_id", equipment_id),
            ("equipment_type", equipment_type),
        ):
            _require_text(value, name)
        if not isinstance(profile, ProfileKey):
            raise TypeError("profile must be ProfileKey")
        policy = next((
            item for item in self.policies
            if item.binding.equipment_id == equipment_id
        ), None)
        if policy is None:
            raise HealthRegistryError("UNKNOWN_EQUIPMENT")
        if policy.binding.store_id != store_id:
            raise HealthRegistryError("STORE_MISMATCH")
        if policy.profile.equipment_type != equipment_type:
            raise HealthRegistryError("EQUIPMENT_TYPE_MISMATCH")
        if policy.profile != profile:
            raise HealthRegistryError("PROFILE_MISMATCH")
        return policy

    def _validate_snapshot(
        self, previous: HealthSnapshot, policy: HealthPolicy,
    ) -> None:
        if not isinstance(previous, HealthSnapshot):
            raise TypeError("previous must be HealthSnapshot")
        if (
            previous.policy.binding.store_id,
            previous.policy.binding.equipment_id,
            previous.policy.profile.equipment_type,
        ) != (
            policy.binding.store_id, policy.binding.equipment_id,
            policy.profile.equipment_type,
        ):
            raise HealthRegistryError("SNAPSHOT_IDENTITY_MISMATCH")
        if (
            previous.policy != policy
            or previous.policy.fingerprint != policy.fingerprint
            or previous.snapshot_schema_version != SNAPSHOT_SCHEMA_VERSION
            or previous.health_contract_version != HEALTH_CONTRACT_VERSION
        ):
            raise HealthRegistryError("INCOMPATIBLE_SNAPSHOT")

    def _snapshot_policy(
        self, previous: HealthSnapshot, profile: ProfileKey,
    ) -> HealthPolicy:
        if not isinstance(previous, HealthSnapshot):
            raise TypeError("previous must be HealthSnapshot")
        policy = self.resolve(
            store_id=previous.policy.binding.store_id,
            equipment_id=previous.policy.binding.equipment_id,
            equipment_type=previous.policy.profile.equipment_type,
            profile=profile,
        )
        self._validate_snapshot(previous, policy)
        return policy

    def evaluate(
        self,
        previous: HealthSnapshot,
        event: HealthObservation | HoodRpmEvent,
        *,
        profile: ProfileKey,
        reference_time: datetime,
    ) -> HealthResult:
        """입력 권한만 검사하고 수치·시간·중복/전이 판정은 Core에 위임한다."""
        if not isinstance(previous, HealthSnapshot):
            raise TypeError("previous must be HealthSnapshot")
        sample = _observation(event)
        policy = self.resolve(
            store_id=sample.store_id, equipment_id=sample.equipment_id,
            equipment_type=sample.equipment_type, profile=profile,
        )
        self._validate_snapshot(previous, policy)
        for actual, expected, code in (
            (sample.metric_name, policy.metric_name, "METRIC_MISMATCH"),
            (sample.unit, policy.unit, "UNIT_MISMATCH"),
        ):
            if actual != expected:
                raise HealthRegistryError(code)
        if sample.schema_version not in policy.schema_versions:
            raise HealthRegistryError("SCHEMA_VERSION_MISMATCH")
        if sample.source not in policy.sources:
            raise HealthRegistryError("SOURCE_NOT_ALLOWED")
        return reduce_health(
            previous, sample, policy=policy, reference_time=reference_time,
        )

    def plan_timer(
        self,
        previous: HealthSnapshot,
        *,
        profile: ProfileKey,
        evaluation_time: datetime,
    ) -> HealthTimerRequest:
        policy = self._snapshot_policy(previous, profile)
        return plan_health_timer(
            previous, policy=policy, evaluation_time=evaluation_time,
        )

    def coordinate_timer(
        self,
        previous: HealthSnapshot,
        token: HealthTimerToken,
        *,
        profile: ProfileKey,
        evaluation_time: datetime,
    ) -> HealthTimerResult:
        policy = self._snapshot_policy(previous, profile)
        # token의 장비가 아닌 요청 snapshot의 장비를 선택한다. 교차/옛 token
        # 처리와 deadline/평가 시각 규칙은 기존 Coordinator/Core 책임이다.
        return coordinate_health_timer(
            previous, token, policy=policy, evaluation_time=evaluation_time,
        )

    def restore(
        self,
        payload: bytes,
        *,
        store_id: str,
        equipment_id: str,
        equipment_type: str,
        profile: ProfileKey,
        evaluation_time: datetime,
    ) -> RestoredHealthTimer:
        policy = self.resolve(
            store_id=store_id, equipment_id=equipment_id,
            equipment_type=equipment_type, profile=profile,
        )
        return restore_health_timer(
            payload, policy=policy, evaluation_time=evaluation_time,
        )
