"""순수 Registry의 장비 binding과 버전이 고정된 profile 식별자."""

from dataclasses import dataclass
from enum import Enum

from streaming.state.contracts import _require_text


class InputKind(Enum):
    """현재 입력 능력이며 실제 제어기 wire 계약을 선언하지 않는다."""

    NUMERIC_TELEMETRY = "numeric_telemetry"
    SYNTHETIC_SIGNAL = "synthetic_signal"


@dataclass(frozen=True)
class ProfileKey:
    """유형·정책 이름·버전 전체가 일치해야 하는 선택 key."""

    equipment_type: str
    profile_id: str
    version: str

    def __post_init__(self) -> None:
        for field in ("equipment_type", "profile_id", "version"):
            _require_text(getattr(self, field), field)


FRYER_REFERENCE = ProfileKey("fryer", "explicit-cooking-reference", "1")
REFRIGERATOR_PROFILE = ProfileKey("refrigerator", "cooling-controller", "1")
FREEZER_PROFILE = ProfileKey("freezer", "cooling-controller", "1")
HOOD_PROFILE = ProfileKey("hood", "rpm-observation", "1")
DISHWASHER_PROFILE = ProfileKey("dishwasher", "wash-cycle", "1")


@dataclass(frozen=True)
class EquipmentBinding:
    """호출자가 주입한 장비 등록·정책·입력 능력, 자동 초기값은 없다.

    equipment_id는 전역 유일 key이고 store_id와 profile의 equipment_type을
    함께 대조한다. synthetic binding은 실제 장비의 입력 확보 증거가 아니다.
    """

    store_id: str
    equipment_id: str
    profile: ProfileKey
    input_kind: InputKind

    def __post_init__(self) -> None:
        _require_text(self.store_id, "store_id")
        _require_text(self.equipment_id, "equipment_id")
        if not isinstance(self.profile, ProfileKey):
            raise TypeError("profile must be ProfileKey")
        if not isinstance(self.input_kind, InputKind):
            raise TypeError("input_kind must be InputKind")
