"""외부 I/O·fallback·가변 전역 등록이 없는 Equipment FSM Registry."""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from streaming.state.adapters import FryerReferenceAdapter
from streaming.state.bindings import (
    DISHWASHER_PROFILE,
    FREEZER_PROFILE,
    FRYER_REFERENCE,
    HOOD_PROFILE,
    REFRIGERATOR_PROFILE,
    EquipmentBinding,
    ProfileKey,
)
from streaming.state.contracts import SyntheticSignal, _require_text
from streaming.state.fsm import FsmResult, TwinState


class RegistryErrorCode(Enum):
    """Registry 선택 실패이며 기존 FSM의 Decision과 별도다."""

    UNKNOWN_EQUIPMENT = "UNKNOWN_EQUIPMENT"
    STORE_MISMATCH = "STORE_MISMATCH"
    EQUIPMENT_TYPE_MISMATCH = "EQUIPMENT_TYPE_MISMATCH"
    PROFILE_MISMATCH = "PROFILE_MISMATCH"
    PROFILE_NOT_FOUND = "PROFILE_NOT_FOUND"
    HANDLER_NOT_IMPLEMENTED = "HANDLER_NOT_IMPLEMENTED"
    INPUT_CONTRACT_UNAVAILABLE = "INPUT_CONTRACT_UNAVAILABLE"
    UNSUPPORTED_INPUT = "UNSUPPORTED_INPUT"
    SNAPSHOT_IDENTITY_MISMATCH = "SNAPSHOT_IDENTITY_MISMATCH"
    HANDLER_PROFILE_MISMATCH = "HANDLER_PROFILE_MISMATCH"
    DUPLICATE_BINDING = "DUPLICATE_BINDING"
    DUPLICATE_PROFILE = "DUPLICATE_PROFILE"


class RegistryError(ValueError):
    """실패 사유를 코드로 제공하고 FSM 호출 전에 거부한다."""

    def __init__(self, code: RegistryErrorCode) -> None:
        self.code = code
        super().__init__(code.value)


@dataclass(frozen=True)
class ProfileSpec:
    """handler가 None이면 알려진 profile이지만 계산은 미구현이다."""

    key: ProfileKey
    handler: FryerReferenceAdapter | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.key, ProfileKey):
            raise TypeError("key must be ProfileKey")
        if self.handler is not None:
            # 이번에는 이 adapter만 허용한다. 새 장비 handler는 별도 작업이다.
            if type(self.handler) is not FryerReferenceAdapter:
                raise TypeError("only FryerReferenceAdapter is implemented")
            if self.key != self.handler.profile:
                raise RegistryError(RegistryErrorCode.HANDLER_PROFILE_MISMATCH)


@dataclass(frozen=True)
class ProfileSnapshot:
    """snapshot이 계산된 profile을 보존해 다른 버전으로의 암묵 이동을 막는다.

    현재 state 타입은 Reference 전용이다. 다른 장비용 snapshot을 미리
    구현한 것이 아니며 resolve 단계에서 미구현 handler를 구별한다.
    """

    profile: ProfileKey
    state: TwinState

    def __post_init__(self) -> None:
        if not isinstance(self.profile, ProfileKey):
            raise TypeError("profile must be ProfileKey")
        if not isinstance(self.state, TwinState):
            raise TypeError("state must be TwinState")


@dataclass(frozen=True)
class RegistryResult:
    """다음 profile snapshot과 원본 FSM 결과를 함께 돌려준다."""

    snapshot: ProfileSnapshot
    fsm: FsmResult


@dataclass(frozen=True)
class EquipmentFsmRegistry:
    """명시적으로 주입한 immutable 목록만 사용하며 파일을 읽지 않는다."""

    bindings: tuple[EquipmentBinding, ...]
    profiles: tuple[ProfileSpec, ...]

    def __post_init__(self) -> None:
        for name, expected in (
            ("bindings", EquipmentBinding), ("profiles", ProfileSpec)
        ):
            values = getattr(self, name)
            if type(values) is not tuple or any(
                not isinstance(item, expected) for item in values
            ):
                raise TypeError(
                    f"{name} must be a tuple of {expected.__name__}"
                )
        if len({b.equipment_id for b in self.bindings}) != len(self.bindings):
            raise RegistryError(RegistryErrorCode.DUPLICATE_BINDING)
        if len({p.key for p in self.profiles}) != len(self.profiles):
            raise RegistryError(RegistryErrorCode.DUPLICATE_PROFILE)

    def resolve(
        self,
        *,
        store_id: str,
        equipment_id: str,
        equipment_type: str,
        profile: ProfileKey,
    ) -> FryerReferenceAdapter:
        """identity・type・profile・실행 능력 순으로 검사한다."""
        for name, value in (
            ("store_id", store_id), ("equipment_id", equipment_id),
            ("equipment_type", equipment_type),
        ):
            _require_text(value, name)
        if not isinstance(profile, ProfileKey):
            raise TypeError("profile must be ProfileKey")
        binding = next(
            (b for b in self.bindings if b.equipment_id == equipment_id), None
        )
        if binding is None:
            raise RegistryError(RegistryErrorCode.UNKNOWN_EQUIPMENT)
        if binding.store_id != store_id:
            raise RegistryError(RegistryErrorCode.STORE_MISMATCH)
        if binding.profile.equipment_type != equipment_type:
            raise RegistryError(RegistryErrorCode.EQUIPMENT_TYPE_MISMATCH)
        if binding.profile != profile:
            raise RegistryError(RegistryErrorCode.PROFILE_MISMATCH)
        spec = next((p for p in self.profiles if p.key == profile), None)
        if spec is None:
            raise RegistryError(RegistryErrorCode.PROFILE_NOT_FOUND)
        if spec.handler is None:
            raise RegistryError(RegistryErrorCode.HANDLER_NOT_IMPLEMENTED)
        if binding.input_kind is not spec.handler.input_kind:
            raise RegistryError(RegistryErrorCode.INPUT_CONTRACT_UNAVAILABLE)
        return spec.handler

    def evaluate(
        self,
        previous: ProfileSnapshot,
        event: SyntheticSignal,
        *,
        equipment_type: str,
        reference_time: datetime,
    ) -> RegistryResult:
        """선택을 검증한 뒤 기존 FSM을 호출하고 profile key를 유지한다."""
        if not isinstance(previous, ProfileSnapshot):
            raise TypeError("previous must be ProfileSnapshot")
        if not isinstance(event, SyntheticSignal):
            raise RegistryError(RegistryErrorCode.UNSUPPORTED_INPUT)
        handler = self.resolve(
            store_id=event.store_id,
            equipment_id=event.equipment_id,
            equipment_type=equipment_type,
            profile=previous.profile,
        )
        if (previous.state.store_id, previous.state.equipment_id) != (
            event.store_id, event.equipment_id
        ):
            raise RegistryError(RegistryErrorCode.SNAPSHOT_IDENTITY_MISMATCH)
        result = handler.evaluate(
            previous.state, event, reference_time=reference_time
        )
        snapshot = ProfileSnapshot(previous.profile, result.state)
        return RegistryResult(snapshot, result)


def build_registry(
    bindings: tuple[EquipmentBinding, ...],
) -> EquipmentFsmRegistry:
    """기존 Reference 하나만 실행 가능하게 하는 새 로컬 Registry를 만든다.

    장비 ID나 입력 능력은 자동 생성하지 않는다. 나머지는 알려진 profile
    자리만 등록하며 냉장고·후드·세척기 FSM 구현을 포함하지 않는다.
    """
    return EquipmentFsmRegistry(
        bindings=bindings,
        profiles=(
            ProfileSpec(FRYER_REFERENCE, FryerReferenceAdapter()),
            ProfileSpec(REFRIGERATOR_PROFILE),
            ProfileSpec(FREEZER_PROFILE),
            ProfileSpec(HOOD_PROFILE),
            ProfileSpec(DISHWASHER_PROFILE),
        ),
    )
