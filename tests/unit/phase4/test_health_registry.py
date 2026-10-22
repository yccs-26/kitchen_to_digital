"""별도 Health Registry의 선택·입력 권한·위임·Operational 격리 검증."""

from dataclasses import asdict, replace
from datetime import timedelta

import pytest

from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind, ProfileKey,
)
from streaming.state.contracts import (
    FaultSignal, HealthStatus, ModeSignal, OperationalState, ReportedMode,
    StateObservation, SyntheticSignal,
)
from streaming.state.fsm import TwinState, transition
from streaming.state.health import (
    HealthDecision, HealthObservation, HealthTick, initialize_health,
    reduce_health, timer_token,
)
from streaming.state.health_codec import (
    HealthCodecError, encode_health_snapshot,
)
from streaming.state.health_registry import (
    EquipmentHealthRegistry, HealthRegistryError,
)
from streaming.state.health_timer_coordinator import (
    TimerStatus, coordinate_health_timer, plan_health_timer,
    restore_health_timer,
)
from streaming.state.hood import (
    HoodPolicy, HoodRpmEvent, HoodSnapshot, qualify_rpm,
)
from streaming.state.registry import ProfileSnapshot, build_registry
from test_health import BASE, EPS, POLICY, TIMEOUT, boot, observation


FRYER_POLICY = replace(
    POLICY,
    binding=EquipmentBinding(
        "store-001", "fryer-001", FRYER_REFERENCE, InputKind.NUMERIC_TELEMETRY,
    ),
    profile=ProfileKey("fryer", "fixture-oil-freshness", "1"),
    metric_name="oil_temperature_celsius", unit="celsius",
)


def lookup(registry, policy=POLICY, **changes):
    arguments = dict(
        store_id=policy.binding.store_id,
        equipment_id=policy.binding.equipment_id,
        equipment_type=policy.profile.equipment_type, profile=policy.profile,
    )
    arguments.update(changes)
    return registry.resolve(**arguments)


def apply(registry, snapshot, event, at=BASE, profile=POLICY.profile):
    return registry.evaluate(
        snapshot, event, profile=profile, reference_time=at,
    )


def fryer_observation(at=BASE, event_id="oil-1"):
    return HealthObservation(
        event_id, at, "store-001", "fryer-001", "fryer",
        "oil_temperature_celsius", 180, "celsius", "1.0.0",
        "fixture-simulator",
    )


@pytest.mark.parametrize("policy", [POLICY, FRYER_POLICY])
def test_exact_profile_selects_injected_policy_and_matches_reducer(policy):
    registry = EquipmentHealthRegistry((POLICY, FRYER_POLICY))
    selected = lookup(registry, policy)
    assert selected is policy
    state = initialize_health(
        selected, monitoring_epoch="test-epoch", monitoring_started_at=BASE,
    )
    event = observation() if policy is POLICY else fryer_observation()
    result = apply(registry, state, event, profile=policy.profile)
    assert result == reduce_health(
        state, event, policy=policy, reference_time=BASE,
    )
    assert result.snapshot.last_seen == BASE
    assert result.snapshot.freshness_deadline == BASE + TIMEOUT
    assert result.snapshot.health_status is HealthStatus.HEALTHY


@pytest.mark.parametrize("changes,code", [
    ({"equipment_id": "unknown"}, "UNKNOWN_EQUIPMENT"),
    ({"store_id": "other"}, "STORE_MISMATCH"),
    ({"equipment_type": "fryer"}, "EQUIPMENT_TYPE_MISMATCH"),
    ({"metric_name": "temperature_celsius"}, "METRIC_MISMATCH"),
    ({"unit": "hz"}, "UNIT_MISMATCH"),
    ({"schema_version": "2.0.0"}, "SCHEMA_VERSION_MISMATCH"),
    ({"source": "not-approved"}, "SOURCE_NOT_ALLOWED"),
])
def test_registry_contract_errors_cannot_call_core_or_extend_timer(
    monkeypatch, changes, code,
):
    registry = EquipmentHealthRegistry((POLICY,))
    state = apply(registry, boot(), observation()).snapshot
    original = asdict(state)

    def forbidden(*args, **kwargs):
        pytest.fail("Registry admission error must not invoke Health Core")

    monkeypatch.setattr(
        "streaming.state.health_registry.reduce_health", forbidden,
    )
    with pytest.raises(HealthRegistryError) as raised:
        bad = observation(BASE + EPS, "bad", **changes)
        apply(registry, state, bad, BASE + EPS)
    assert raised.value.code == code
    assert asdict(state) == original
    assert registry.plan_timer(
        state, profile=POLICY.profile, evaluation_time=BASE + TIMEOUT,
    ).token.expected_deadline == BASE + TIMEOUT


@pytest.mark.parametrize("profile", [
    replace(POLICY.profile, version="2"),
    replace(POLICY.profile, profile_id="other"), HOOD_PROFILE,
])
def test_health_key_is_exact_and_distinct_from_operational_key(profile):
    registry = EquipmentHealthRegistry((POLICY,))
    with pytest.raises(HealthRegistryError) as raised:
        apply(registry, boot(), observation(), profile=profile)
    assert raised.value.code == "PROFILE_MISMATCH"


def test_foreign_snapshot_identity_is_not_rebound_to_event():
    registry = EquipmentHealthRegistry((POLICY, FRYER_POLICY))
    state = boot(FRYER_POLICY)
    with pytest.raises(HealthRegistryError) as raised:
        apply(registry, state, observation())
    assert raised.value.code == "SNAPSHOT_IDENTITY_MISMATCH"
    assert state.last_seen is None


@pytest.mark.parametrize("change", [
    {"freshness_timeout": 2 * TIMEOUT},
    {"binding_revision": "new"}, {"input_contract_revision": "new"},
    {"sources": ("other",)},
    {"profile": replace(POLICY.profile, version="2")},
])
def test_policy_change_rejects_old_snapshot_for_observation_and_timer(change):
    policy = replace(POLICY, **change)
    registry = EquipmentHealthRegistry((policy,))
    state = boot()
    with pytest.raises(HealthRegistryError) as raised:
        apply(registry, state, observation(), profile=policy.profile)
    assert raised.value.code == "INCOMPATIBLE_SNAPSHOT"
    with pytest.raises(HealthRegistryError):
        registry.plan_timer(
            state, profile=policy.profile, evaluation_time=BASE,
        )


@pytest.mark.parametrize("value", [-1, True, None, "800", float("nan")])
def test_numeric_suitability_is_delegated_to_reducer(value):
    registry = EquipmentHealthRegistry((POLICY,))
    state = boot()
    event = observation(metric_value=value)
    result = apply(registry, state, event)
    assert result == reduce_health(
        state, event, policy=POLICY, reference_time=BASE,
    )
    assert result.decision is HealthDecision.INVALID_OBSERVATION
    assert result.snapshot is state
    assert not result.changes


def test_registry_duplicate_order_conflict_expiry_and_recovery_match_core():
    registry = EquipmentHealthRegistry((POLICY,))
    state = boot()
    initial = state
    events = [
        (observation(), BASE, HealthDecision.INITIALIZED),
        (observation(BASE + EPS, "e2"), BASE + EPS, HealthDecision.NO_CHANGE),
        (observation(), BASE + EPS, HealthDecision.DUPLICATE),
        (observation(metric_value=801), BASE + EPS,
         HealthDecision.ID_CONFLICT),
        (observation(BASE, "old"), BASE + EPS, HealthDecision.OUT_OF_ORDER),
        (observation(BASE + EPS, "same-time"), BASE + EPS,
         HealthDecision.SAME_TIME),
        (observation(BASE + 2 * EPS, "expired"), BASE + TIMEOUT + 2 * EPS,
         HealthDecision.TOO_OLD_FOR_FRESHNESS),
    ]
    for event, at, decision in events:
        result = apply(registry, state, event, at)
        assert result == reduce_health(
            state, event, policy=POLICY, reference_time=at,
        )
        assert result.decision is decision
        state = result.snapshot
    at = BASE + TIMEOUT + 2 * EPS
    token = timer_token(state)
    tick = registry.coordinate_timer(
        state, token, profile=POLICY.profile, evaluation_time=at,
    )
    assert tick == coordinate_health_timer(
        state, token, policy=POLICY, evaluation_time=at,
    )
    assert tick.snapshot.health_status is HealthStatus.STALE
    recovered = observation(at + EPS, "recover")
    result = apply(registry, tick.snapshot, recovered, at + EPS)
    assert result.snapshot.health_status is HealthStatus.HEALTHY
    assert result == reduce_health(
        tick.snapshot, recovered, policy=POLICY, reference_time=at + EPS,
    )
    assert initial.receipts == ()


def test_codec_restore_then_timer_keeps_native_result_and_identity(tmp_path):
    registry = EquipmentHealthRegistry((POLICY,))
    state = apply(registry, boot(), observation()).snapshot
    at = BASE + TIMEOUT
    path = tmp_path / "health.json"
    path.write_bytes(encode_health_snapshot(state))
    restored = registry.restore(
        path.read_bytes(), store_id="store-001", equipment_id="hood-001",
        equipment_type="hood", profile=POLICY.profile, evaluation_time=at,
    )
    assert restored == restore_health_timer(
        path.read_bytes(), policy=POLICY, evaluation_time=at,
    )
    assert restored.request.token == timer_token(state)
    assert restored.request.status is TimerStatus.DUE
    result = registry.coordinate_timer(
        restored.snapshot, restored.request.token, profile=POLICY.profile,
        evaluation_time=at,
    )
    direct = coordinate_health_timer(
        state, timer_token(state), policy=POLICY, evaluation_time=at,
    )
    assert result == direct
    assert result.changes[0].health_change_id == (
        direct.changes[0].health_change_id
    )
    with pytest.raises(HealthCodecError):
        registry.restore(
            b"broken", store_id="store-001", equipment_id="hood-001",
            equipment_type="hood", profile=POLICY.profile, evaluation_time=at,
        )


def test_two_equipment_policies_keep_deadlines_snapshots_and_timers_isolated():
    registry = EquipmentHealthRegistry((POLICY, FRYER_POLICY))
    hood = apply(registry, boot(), observation()).snapshot
    fryer = apply(
        registry, boot(FRYER_POLICY), fryer_observation(),
        profile=FRYER_POLICY.profile,
    ).snapshot
    hood = apply(
        registry, hood, observation(BASE + EPS, "new"), BASE + EPS,
    ).snapshot
    assert fryer.last_seen == BASE
    assert hood.freshness_deadline == BASE + TIMEOUT + EPS
    cross = registry.coordinate_timer(
        fryer, timer_token(hood), profile=FRYER_POLICY.profile,
        evaluation_time=BASE + TIMEOUT,
    )
    assert cross.status is TimerStatus.OBSOLETE
    assert cross.snapshot is fryer
    assert not cross.changes


def test_hood_rpm_input_can_feed_both_paths_without_snapshot_cross_mutation():
    registry = EquipmentHealthRegistry((POLICY,))
    operational = build_registry((POLICY.binding,))
    hood_snapshot = ProfileSnapshot(HOOD_PROFILE, HoodSnapshot(
        "store-001", "hood-001", HoodPolicy(
            600, 200, timedelta(seconds=3), timedelta(seconds=2),
            timedelta(seconds=2),
        ),
    ))
    health = boot()
    saved_initial_operational = asdict(hood_snapshot)
    for index in (0, 1, 3):
        at = BASE + timedelta(seconds=index)
        event = HoodRpmEvent(
            f"rpm-{index}", at, "store-001", "hood-001", "hood", "fan_rpm",
            800, "rpm", "1.0.0", "fixture-simulator",
        )
        op_before = asdict(hood_snapshot)
        selected = apply(registry, health, event, at)
        assert selected == reduce_health(
            health, HealthObservation(**asdict(event)),
            policy=POLICY, reference_time=at,
        )
        assert asdict(hood_snapshot) == op_before
        health_before = asdict(selected.snapshot)
        result = operational.evaluate(
            hood_snapshot, event, equipment_type="hood", reference_time=at,
        )
        assert result.fsm == qualify_rpm(
            hood_snapshot.state, event, reference_time=at,
        )
        assert asdict(selected.snapshot) == health_before
        hood_snapshot, health = result.snapshot, selected.snapshot
    assert saved_initial_operational["state"]["receipts"] == ()
    assert health.health_status is HealthStatus.HEALTHY
    assert hood_snapshot.state.current.value == "ROTATING"


def test_fryer_numeric_health_does_not_grant_operational_synthetic_authority():
    registry = EquipmentHealthRegistry((FRYER_POLICY,))
    health = boot(FRYER_POLICY)
    op_state = TwinState(
        "store-001", "fryer-001",
        StateObservation(OperationalState.IDLE, HealthStatus.STALE),
    )
    signal = SyntheticSignal(
        "mode", BASE, "store-001", "fryer-001", "controller",
        ModeSignal(ReportedMode.PREHEATING), "synthetic_test_only",
    )
    numeric = apply(
        registry, health, fryer_observation(), profile=FRYER_POLICY.profile,
    )
    assert op_state.observation.health_status is HealthStatus.STALE
    with pytest.raises(HealthRegistryError) as raised:
        apply(registry, numeric.snapshot, signal, profile=FRYER_POLICY.profile)
    assert raised.value.code == "UNSUPPORTED_INPUT"
    operational = build_registry((replace(
        FRYER_POLICY.binding, input_kind=InputKind.SYNTHETIC_SIGNAL,
    ),))
    before_health = asdict(numeric.snapshot)
    selected = operational.evaluate(
        ProfileSnapshot(FRYER_REFERENCE, op_state), signal,
        equipment_type="fryer", reference_time=BASE,
    )
    assert selected.fsm == transition(op_state, signal, reference_time=BASE)
    assert selected.fsm.state.observation.operational_state is (
        OperationalState.PREHEATING
    )
    assert asdict(numeric.snapshot) == before_health
    assert numeric.snapshot.health_status is HealthStatus.HEALTHY


@pytest.mark.parametrize("item", [FaultSignal("E1", True), None, {}, "rpm"])
def test_fault_and_untyped_inputs_are_not_implicitly_converted(item):
    registry = EquipmentHealthRegistry((POLICY,))
    with pytest.raises(HealthRegistryError) as raised:
        apply(registry, boot(), item)
    assert raised.value.code == "UNSUPPORTED_INPUT"


@pytest.mark.parametrize("equipment_type,metric", [
    ("refrigerator", "temperature_celsius"),
    ("freezer", "temperature_celsius"),
])
def test_core_capability_does_not_automatically_grant_new_registry_support(
    equipment_type, metric,
):
    policy = replace(
        POLICY,
        binding=replace(POLICY.binding, profile=ProfileKey(
            equipment_type, "cooling-controller", "1",
        )),
        profile=ProfileKey(equipment_type, "test-freshness", "1"),
        metric_name=metric, unit="celsius",
    )
    with pytest.raises(HealthRegistryError) as raised:
        EquipmentHealthRegistry((policy,))
    assert raised.value.code == "UNSUPPORTED_EQUIPMENT"
    with pytest.raises(HealthRegistryError):
        lookup(EquipmentHealthRegistry(()), equipment_type="dishwasher")


def test_duplicate_equipment_ids_across_stores_cannot_shadow_policy():
    with pytest.raises(HealthRegistryError) as raised:
        EquipmentHealthRegistry((POLICY, replace(
            POLICY, binding=replace(POLICY.binding, store_id="other-store"),
        )))
    assert raised.value.code == "DUPLICATE_BINDING"


@pytest.mark.parametrize("at", [None, "bad", BASE.replace(tzinfo=None)])
def test_invalid_evaluation_time_does_not_change_snapshot(at):
    registry = EquipmentHealthRegistry((POLICY,))
    state = boot()
    with pytest.raises(ValueError):
        apply(registry, state, observation(), at)
    assert state.last_seen is None
    assert state.freshness_deadline == BASE + TIMEOUT


def test_wrong_wire_time_is_not_guessed_and_valid_aware_time_is_required():
    with pytest.raises(ValueError):
        observation(at="2026-10-21")
    with pytest.raises(ValueError):
        observation(at=BASE.replace(tzinfo=None))


def test_same_snapshot_sequence_and_registry_retry_preserve_health_ids():
    registry = EquipmentHealthRegistry((POLICY,))

    def run():
        state = boot()
        results = []
        for at, event_id in ((BASE, "e1"), (BASE + EPS, "e2")):
            result = apply(registry, state, observation(at, event_id), at)
            retry = apply(registry, state, observation(at, event_id), at)
            assert retry == result
            results.append(result)
            state = result.snapshot
        result = registry.coordinate_timer(
            state, timer_token(state), profile=POLICY.profile,
            evaluation_time=BASE + TIMEOUT + EPS,
        )
        results.append(result)
        return results

    assert run() == run()



def test_old_generation_timer_stays_obsolete_after_registry_observation():
    registry = EquipmentHealthRegistry((POLICY,))
    first = apply(registry, boot(), observation()).snapshot
    old = registry.plan_timer(
        first, profile=POLICY.profile, evaluation_time=BASE,
    )
    assert old == plan_health_timer(first, policy=POLICY, evaluation_time=BASE)
    current = apply(
        registry, first, observation(BASE + EPS, "new"), BASE + EPS,
    ).snapshot
    result = registry.coordinate_timer(
        current, old.token, profile=POLICY.profile,
        evaluation_time=BASE + TIMEOUT + EPS,
    )
    assert result.status is TimerStatus.OBSOLETE
    assert result.snapshot is current
    assert not result.changes
    due = registry.plan_timer(
        current, profile=POLICY.profile,
        evaluation_time=BASE + TIMEOUT + EPS,
    )
    assert due.status is TimerStatus.DUE
    assert due.token.expected_deadline == current.freshness_deadline


def test_unimplemented_operational_dependency_is_not_assumed_compatible():
    policy = replace(POLICY, binding=replace(
        POLICY.binding, profile=replace(HOOD_PROFILE, version="2"),
    ))
    with pytest.raises(HealthRegistryError) as raised:
        EquipmentHealthRegistry((policy,))
    assert raised.value.code == "UNSUPPORTED_OPERATIONAL_PROFILE"
