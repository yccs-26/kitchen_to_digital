"""장비 선택의 실패 경계와 기존 Reference의 결과·ID 보존을 검증한다."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.state.adapters import FryerReferenceAdapter
from streaming.state.bindings import (
    DISHWASHER_PROFILE,
    FREEZER_PROFILE,
    FRYER_REFERENCE,
    HOOD_PROFILE,
    REFRIGERATOR_PROFILE,
    EquipmentBinding,
    InputKind,
)
from streaming.state.contracts import (
    CleaningCompletedSignal,
    CleaningSignal,
    HealthStatus,
    ModeSignal,
    OperationalState,
    ReportedMode,
    StateObservation,
    SyntheticSignal,
)
from streaming.state.fsm import Decision, TwinState, transition
from streaming.state.registry import (
    EquipmentFsmRegistry,
    ProfileSnapshot,
    ProfileSpec,
    RegistryError,
    RegistryErrorCode,
    build_registry,
)


NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def binding():
    # 실제 registry의 identity를 쓰되 명시적 입력 능력은 synthetic 가정이다.
    equipment = next(
        item for item in json.loads(
            (ROOT / "config/validation_rules.json").read_text()
        )["equipment_registry"] if item["equipment_type"] == "fryer"
    )
    assert equipment["equipment_type"] == FRYER_REFERENCE.equipment_type
    return EquipmentBinding(
        equipment["store_id"], equipment["equipment_id"], FRYER_REFERENCE,
        InputKind.SYNTHETIC_SIGNAL,
    )


@pytest.fixture
def registry(binding):
    return build_registry((binding,))


@pytest.fixture
def snapshot(binding):
    return ProfileSnapshot(
        FRYER_REFERENCE,
        TwinState(
            binding.store_id, binding.equipment_id,
            StateObservation(OperationalState.IDLE, HealthStatus.STALE),
        ),
    )


@pytest.fixture
def event(binding):
    return SyntheticSignal(
        "registry-test-1", NOW, binding.store_id, binding.equipment_id,
        "synthetic-controller", ModeSignal(ReportedMode.PREHEATING),
        "synthetic_test_only",
    )


def evaluate(registry, snapshot, event, **changes):
    arguments = dict(equipment_type="fryer", reference_time=NOW)
    arguments.update(changes)
    return registry.evaluate(snapshot, event, **arguments)


@pytest.mark.parametrize("health", list(HealthStatus))
def test_fryer_cycle_matches_direct_fsm_and_preserves_ids(
    registry, snapshot, event, health
):
    initial = replace(snapshot, state=replace(
        snapshot.state,
        observation=StateObservation(OperationalState.IDLE, health),
    ))
    payloads = (
        ModeSignal(ReportedMode.PREHEATING), ModeSignal(ReportedMode.COOKING),
        CleaningSignal(True), CleaningCompletedSignal(),
    )

    def run():
        previous = initial
        results = []
        for index, payload in enumerate(payloads):
            signal = replace(
                event, signal_id=f"registry-cycle-{index}", payload=payload,
                event_time=NOW + timedelta(seconds=index),
            )
            reference = signal.event_time + timedelta(seconds=1)
            selected = evaluate(
                registry, previous, signal, reference_time=reference
            )
            direct = transition(
                previous.state, signal, reference_time=reference
            )
            assert selected.fsm == direct
            assert selected.fsm.decision is Decision.TRANSITION
            assert selected.snapshot.profile == FRYER_REFERENCE
            assert selected.snapshot.state is selected.fsm.state
            assert selected.fsm.intents[0].state_change_id == (
                direct.intents[0].state_change_id
            )
            assert selected.snapshot.state.observation.health_status is health
            retry = evaluate(
                registry, previous, signal,
                reference_time=reference + timedelta(days=1),
            )
            assert selected == retry
            duplicate = evaluate(
                registry, selected.snapshot, signal, reference_time=reference
            )
            assert duplicate.fsm.decision is Decision.DUPLICATE
            assert duplicate.snapshot.state is selected.snapshot.state
            assert not duplicate.fsm.intents
            results.append(selected)
            previous = selected.snapshot
        return results

    first = run()
    assert first == run()
    assert len({r.fsm.intents[0].state_change_id for r in first}) == 4
    assert initial.state.receipts == ()


def test_only_fryer_has_a_handler_and_no_binding_is_inferred(binding):
    registry = build_registry((binding,))
    implemented = [p.key for p in registry.profiles if p.handler is not None]
    assert implemented == [FRYER_REFERENCE]
    assert registry.resolve(
        store_id=binding.store_id, equipment_id=binding.equipment_id,
        equipment_type="fryer", profile=FRYER_REFERENCE,
    ) == FryerReferenceAdapter()
    with pytest.raises(RegistryError) as raised:
        build_registry(()).resolve(
            store_id=binding.store_id, equipment_id=binding.equipment_id,
            equipment_type="fryer", profile=FRYER_REFERENCE,
        )
    assert raised.value.code is RegistryErrorCode.UNKNOWN_EQUIPMENT


@pytest.mark.parametrize(
    "changes, code",
    [
        ({"equipment_id": "unknown"}, RegistryErrorCode.UNKNOWN_EQUIPMENT),
        ({"store_id": "other-store"}, RegistryErrorCode.STORE_MISMATCH),
    ],
)
def test_identity_failure_never_calls_adapter(
    registry, snapshot, event, monkeypatch, changes, code
):
    def forbidden_call(*args, **kwargs):
        pytest.fail("adapter must not run on selection failure")

    monkeypatch.setattr(FryerReferenceAdapter, "evaluate", forbidden_call)
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, snapshot, replace(event, **changes))
    assert raised.value.code is code
    assert snapshot.state.receipts == ()


@pytest.mark.parametrize("equipment_type", ["hood", "refrigerator", "Fryer"])
def test_input_type_mismatch_does_not_fallback(
    registry, snapshot, event, equipment_type
):
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, snapshot, event, equipment_type=equipment_type)
    assert raised.value.code is RegistryErrorCode.EQUIPMENT_TYPE_MISMATCH
    assert snapshot.state.receipts == ()


@pytest.mark.parametrize(
    "key",
    [
        replace(FRYER_REFERENCE, profile_id="different"),
        replace(FRYER_REFERENCE, version="2"),
        replace(FRYER_REFERENCE, equipment_type="hood"),
    ],
)
def test_snapshot_profile_mismatch_never_selects_another_handler(
    registry, snapshot, event, key
):
    # 원래 handler가 등록되어 있어도 snapshot의 다른 key를 무시하지 않는다.
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, replace(snapshot, profile=key), event)
    assert raised.value.code is RegistryErrorCode.PROFILE_MISMATCH


@pytest.mark.parametrize(
    "key",
    [
        replace(FRYER_REFERENCE, profile_id="unknown"),
        replace(FRYER_REFERENCE, version="2"),
    ],
)
def test_bound_but_unknown_profile_is_distinct_from_unimplemented(
    binding, snapshot, event, key
):
    registry = build_registry((replace(binding, profile=key),))
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, replace(snapshot, profile=key), event)
    assert raised.value.code is RegistryErrorCode.PROFILE_NOT_FOUND


@pytest.mark.parametrize(
    "key, equipment_id",
    [
        (REFRIGERATOR_PROFILE, "fridge-001"),
        (FREEZER_PROFILE, "freezer-001"),
        (HOOD_PROFILE, "hood-001"),
        # 세척기는 simulator 설정에만 있다. 테스트 binding은 live 등록이 아니다.
        (DISHWASHER_PROFILE, "dishwasher-001"),
    ],
)
def test_known_non_fryer_profiles_have_no_handler(key, equipment_id):
    binding = EquipmentBinding(
        "store-001", equipment_id, key, InputKind.NUMERIC_TELEMETRY
    )
    with pytest.raises(RegistryError) as raised:
        build_registry((binding,)).resolve(
            store_id=binding.store_id, equipment_id=binding.equipment_id,
            equipment_type=key.equipment_type, profile=key,
        )
    assert raised.value.code is RegistryErrorCode.HANDLER_NOT_IMPLEMENTED


def test_numeric_only_fryer_cannot_run_reference(binding, snapshot, event):
    registry = build_registry((replace(
        binding, input_kind=InputKind.NUMERIC_TELEMETRY
    ),))
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, snapshot, event)
    assert raised.value.code is RegistryErrorCode.INPUT_CONTRACT_UNAVAILABLE


@pytest.mark.parametrize("payload", [None, {}, {"metric_value": 180.0}])
def test_numeric_or_untyped_payload_is_not_implicitly_converted(
    registry, snapshot, payload
):
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, snapshot, payload)
    assert raised.value.code is RegistryErrorCode.UNSUPPORTED_INPUT


@pytest.mark.parametrize("field", ["equipment_id", "store_id"])
def test_snapshot_cannot_be_reused_for_another_identity(
    registry, snapshot, event, field
):
    foreign = replace(snapshot, state=replace(
        snapshot.state, **{field: "other"}
    ))
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, foreign, event)
    assert raised.value.code is RegistryErrorCode.SNAPSHOT_IDENTITY_MISMATCH
    assert foreign.state.receipts == ()


@pytest.mark.parametrize(
    "key",
    [
        HOOD_PROFILE,
        replace(FRYER_REFERENCE, profile_id="other"),
        replace(FRYER_REFERENCE, version="2"),
    ],
)
def test_handler_cannot_be_registered_under_different_key(key):
    with pytest.raises(RegistryError) as raised:
        ProfileSpec(key, FryerReferenceAdapter())
    assert raised.value.code is RegistryErrorCode.HANDLER_PROFILE_MISMATCH


def test_duplicate_device_ids_cannot_shadow_one_another(binding):
    for duplicate in (binding, replace(binding, store_id="other-store")):
        with pytest.raises(RegistryError) as raised:
            build_registry((binding, duplicate))
        assert raised.value.code is RegistryErrorCode.DUPLICATE_BINDING


def test_duplicate_profile_cannot_shadow_handler():
    with pytest.raises(RegistryError) as raised:
        EquipmentFsmRegistry(
            (), (ProfileSpec(FRYER_REFERENCE, FryerReferenceAdapter()),
                 ProfileSpec(FRYER_REFERENCE)),
        )
    assert raised.value.code is RegistryErrorCode.DUPLICATE_PROFILE


def test_configuration_and_snapshot_are_immutable(
    registry, binding, snapshot, event
):
    for target, field, value in (
        (registry, "bindings", ()),
        (binding, "input_kind", InputKind.NUMERIC_TELEMETRY),
        (snapshot, "profile", HOOD_PROFILE),
        (registry.profiles[0], "handler", None),
    ):
        with pytest.raises(FrozenInstanceError):
            setattr(target, field, value)
    first = evaluate(registry, snapshot, event)
    assert first == evaluate(build_registry((binding,)), snapshot, event)


@pytest.mark.parametrize("field", ["equipment_type", "profile_id", "version"])
@pytest.mark.parametrize("value", [None, "", " ", 1])
def test_profile_key_requires_nonblank_exact_strings(field, value):
    with pytest.raises(ValueError):
        replace(FRYER_REFERENCE, **{field: value})


@pytest.mark.parametrize("field", ["store_id", "equipment_id"])
@pytest.mark.parametrize("value", [None, "", " ", 1])
def test_binding_requires_identity(binding, field, value):
    with pytest.raises(ValueError):
        replace(binding, **{field: value})


def test_invalid_configuration_types_are_rejected(binding, snapshot):
    for factory in (
        lambda: replace(binding, profile="fryer"),
        lambda: replace(binding, input_kind="synthetic_signal"),
        lambda: replace(snapshot, state=None),
        lambda: replace(snapshot, profile="fryer"),
        lambda: ProfileSpec("fryer"),
        lambda: ProfileSpec(FRYER_REFERENCE, lambda: None),
        lambda: EquipmentFsmRegistry([binding], ()),
        lambda: EquipmentFsmRegistry((binding,), []),
    ):
        with pytest.raises(TypeError):
            factory()
