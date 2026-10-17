"""Registry의 Hood 선택·입력 격리·snapshot 및 출력 보존 검증."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.state.adapters import FryerReferenceAdapter, HoodRpmAdapter
from streaming.state.bindings import (
    FRYER_REFERENCE, HOOD_PROFILE, EquipmentBinding, InputKind,
)
from streaming.state.contracts import (
    HealthStatus, ModeSignal, OperationalState, ReportedMode,
    StateObservation, SyntheticSignal,
)
from streaming.state.fsm import TwinState
from streaming.state.hood import (
    HoodDecision, HoodPolicy, HoodRpmEvent, HoodSnapshot, HoodState,
    qualify_rpm,
)
from streaming.state.registry import (
    ProfileSnapshot, ProfileSpec, RegistryError, RegistryErrorCode,
    build_registry,
)


BASE = datetime(2026, 10, 17, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[3]
POLICY = HoodPolicy(
    600, 200, timedelta(seconds=3), timedelta(seconds=2), timedelta(seconds=2),
)
HOOD = EquipmentBinding(
    "store-001", "hood-001", HOOD_PROFILE, InputKind.NUMERIC_TELEMETRY,
)
FRYER = EquipmentBinding(
    "store-001", "fryer-001", FRYER_REFERENCE, InputKind.SYNTHETIC_SIGNAL,
)


def sample(second=0, rpm=600, **changes):
    return replace(HoodRpmEvent(
        f"rpm-{second}", BASE + timedelta(seconds=second), "store-001",
        "hood-001", "hood", "fan_rpm", rpm, "rpm", "1.0.0", "simulator",
    ), **changes)


def initial():
    return ProfileSnapshot(
        HOOD_PROFILE, HoodSnapshot("store-001", "hood-001", POLICY),
    )


def evaluate(registry, snapshot, event, **changes):
    arguments = dict(
        equipment_type="hood", reference_time=BASE + timedelta(days=1),
    )
    arguments.update(changes)
    return registry.evaluate(snapshot, event, **arguments)


def test_hood_fixture_matches_direct_qualifier_and_preserves_every_snapshot():
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase4/hood_rpm.json").read_text()
    )
    registry = build_registry((HOOD, FRYER))
    assert registry.resolve(
        store_id=HOOD.store_id, equipment_id=HOOD.equipment_id,
        equipment_type="hood", profile=HOOD_PROFILE,
    ) == HoodRpmAdapter()

    def run():
        previous = initial()
        results = []
        for step in fixture["steps"]:
            event = sample(step["second"], step["rpm"])
            direct = qualify_rpm(
                previous.state, event, reference_time=BASE + timedelta(days=1),
            )
            result = evaluate(registry, previous, event)
            assert result.fsm == direct
            assert result.snapshot.state is result.fsm.state
            assert result.snapshot.profile == HOOD_PROFILE
            assert result.snapshot.state.policy == POLICY
            assert result.fsm.decision.value == step["decision"]
            current = result.snapshot.state.current
            assert (current.value if current else None) == step["state"]
            assert evaluate(
                registry, previous, event,
                reference_time=BASE + timedelta(days=2),
            ) == result
            duplicate = evaluate(registry, result.snapshot, event)
            assert duplicate.fsm.decision is HoodDecision.DUPLICATE
            assert duplicate.snapshot.state is result.snapshot.state
            assert not duplicate.fsm.intents
            if step["decision"] != "TRANSITION":
                assert not result.fsm.intents
            results.append(result)
            previous = result.snapshot
        return results

    first = run()
    assert run() == first
    intents = [i for r in first for i in r.fsm.intents]
    assert [i.current for i in intents] == [
        HoodState.ROTATING, HoodState.STOPPED,
    ]
    assert len({i.state_change_id for i in intents}) == 2
    assert first[0].snapshot.state.candidate is HoodState.STOPPED
    assert first[0].snapshot.state.candidate_since == BASE
    assert initial().state.receipts == ()


@pytest.mark.parametrize("changes,code", [
    ({"metric_name": "temperature_celsius"}, "UNSUPPORTED_INPUT"),
    ({"unit": "hz"}, "UNSUPPORTED_INPUT"),
    ({"schema_version": "2.0.0"}, "UNSUPPORTED_INPUT"),
    ({"equipment_type": "fryer"}, "EQUIPMENT_TYPE_MISMATCH"),
    ({"equipment_id": "missing"}, "UNKNOWN_EQUIPMENT"),
    ({"store_id": "other"}, "STORE_MISMATCH"),
])
def test_invalid_contract_never_calls_engine(monkeypatch, changes, code):
    def forbidden(*args, **kwargs):
        pytest.fail("rejected input must not reach either engine")

    registry = build_registry((HOOD, FRYER))
    pending = evaluate(registry, initial(), sample()).snapshot
    monkeypatch.setattr(HoodRpmAdapter, "evaluate", forbidden)
    monkeypatch.setattr(FryerReferenceAdapter, "evaluate", forbidden)
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, pending, sample(1, **changes))
    assert raised.value.code.value == code
    assert pending.state.candidate_since == BASE
    assert len(pending.state.receipts) == 1


@pytest.mark.parametrize("key", [
    replace(HOOD_PROFILE, profile_id="other"),
    replace(HOOD_PROFILE, version="2"),
    FRYER_REFERENCE,
])
def test_hood_snapshot_profile_mismatch_is_rejected(key):
    registry = build_registry((HOOD, FRYER))
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, replace(initial(), profile=key), sample())
    assert raised.value.code is RegistryErrorCode.PROFILE_MISMATCH


def test_unknown_bound_version_does_not_fallback_to_hood_v1():
    key = replace(HOOD_PROFILE, version="2")
    registry = build_registry((replace(HOOD, profile=key),))
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, replace(initial(), profile=key), sample())
    assert raised.value.code is RegistryErrorCode.PROFILE_NOT_FOUND


@pytest.mark.parametrize("field", ["store_id", "equipment_id"])
def test_foreign_snapshot_identity_is_rejected(field):
    snapshot = replace(initial(), state=replace(
        initial().state, **{field: "other"},
    ))
    with pytest.raises(RegistryError) as raised:
        evaluate(build_registry((HOOD,)), snapshot, sample())
    assert raised.value.code is RegistryErrorCode.SNAPSHOT_IDENTITY_MISMATCH


@pytest.mark.parametrize("numeric_to_fryer", [True, False])
def test_cross_profile_input_cannot_reach_wrong_engine(
    monkeypatch, numeric_to_fryer,
):
    def forbidden(*args, **kwargs):
        pytest.fail("cross-profile input must not reach engine")

    monkeypatch.setattr(HoodRpmAdapter, "evaluate", forbidden)
    monkeypatch.setattr(FryerReferenceAdapter, "evaluate", forbidden)
    registry = build_registry((HOOD, FRYER))
    signal = SyntheticSignal(
        "mode", BASE, "store-001", "hood-001", "controller",
        ModeSignal(ReportedMode.PREHEATING), "synthetic_test_only",
    )
    if numeric_to_fryer:
        snapshot = ProfileSnapshot(FRYER_REFERENCE, TwinState(
            "store-001", "fryer-001",
            StateObservation(OperationalState.IDLE, HealthStatus.STALE),
        ))
        event = sample(equipment_id="fryer-001", equipment_type="fryer")
        equipment_type = "fryer"
    else:
        snapshot, event, equipment_type = initial(), signal, "hood"
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, snapshot, event, equipment_type=equipment_type)
    assert raised.value.code is RegistryErrorCode.UNSUPPORTED_INPUT


@pytest.mark.parametrize("hood_profile", [True, False])
def test_snapshot_runtime_type_must_match_selected_handler(hood_profile):
    fryer_state = TwinState(
        "store-001", "hood-001",
        StateObservation(OperationalState.IDLE, HealthStatus.STALE),
    )
    if hood_profile:
        snapshot = ProfileSnapshot(HOOD_PROFILE, fryer_state)
        event, equipment_type = sample(), "hood"
    else:
        snapshot = ProfileSnapshot(FRYER_REFERENCE, HoodSnapshot(
            "store-001", "fryer-001", POLICY,
        ))
        event = SyntheticSignal(
            "mode", BASE, "store-001", "fryer-001", "controller",
            ModeSignal(ReportedMode.PREHEATING), "synthetic_test_only",
        )
        equipment_type = "fryer"
    with pytest.raises(RegistryError) as raised:
        evaluate(
            build_registry((HOOD, FRYER)), snapshot, event,
            equipment_type=equipment_type,
        )
    assert raised.value.code is RegistryErrorCode.SNAPSHOT_TYPE_MISMATCH


def test_hood_input_kind_is_explicit_and_missing_binding_is_not_inferred():
    registry = build_registry((replace(
        HOOD, input_kind=InputKind.SYNTHETIC_SIGNAL,
    ),))
    with pytest.raises(RegistryError) as raised:
        evaluate(registry, initial(), sample())
    assert raised.value.code is RegistryErrorCode.INPUT_CONTRACT_UNAVAILABLE
    with pytest.raises(RegistryError) as raised:
        evaluate(build_registry(()), initial(), sample())
    assert raised.value.code is RegistryErrorCode.UNKNOWN_EQUIPMENT


@pytest.mark.parametrize("key", [
    FRYER_REFERENCE, replace(HOOD_PROFILE, version="2"),
])
def test_hood_handler_requires_exact_profile_key(key):
    with pytest.raises(RegistryError) as raised:
        ProfileSpec(key, HoodRpmAdapter())
    assert raised.value.code is RegistryErrorCode.HANDLER_PROFILE_MISMATCH


@pytest.mark.parametrize("event,decision", [
    (sample(0, event_id="same-time"), HoodDecision.SAME_TIME),
    (sample(-1), HoodDecision.OUT_OF_ORDER),
    (sample(0), HoodDecision.DUPLICATE),
    (sample(0, 601), HoodDecision.ID_CONFLICT),
    (sample(1, -1), HoodDecision.INVALID_SAMPLE),
])
def test_qualifier_rejections_and_invalid_rpm_keep_original_semantics(
    event, decision,
):
    registry = build_registry((HOOD,))
    pending = evaluate(registry, initial(), sample()).snapshot
    direct = qualify_rpm(
        pending.state, event, reference_time=BASE + timedelta(days=1),
    )
    result = evaluate(registry, pending, event)
    assert result.fsm == direct
    assert result.fsm.decision is decision
    assert not result.fsm.intents
    if decision is HoodDecision.INVALID_SAMPLE:
        assert result.snapshot.state.candidate is None
    else:
        assert result.snapshot.state is pending.state
