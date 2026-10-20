"""Health 계약의 관측·원장·profile·복원 결정성 검증."""

from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.state.bindings import (
    EquipmentBinding, HOOD_PROFILE, InputKind, ProfileKey,
)
from streaming.state.contracts import FaultSignal, HealthStatus
from streaming.state.health import (
    HealthDecision, HealthObservation, HealthPolicy, HealthSnapshot,
    HealthTick, initialize_health, reduce_health, timer_token,
)


BASE = datetime(2026, 10, 20, tzinfo=timezone.utc)
TIMEOUT = timedelta(seconds=10)  # 테스트 전용이며 운영 timeout이 아니다.
EPS = timedelta(microseconds=1)
POLICY = HealthPolicy(
    binding=EquipmentBinding(
        "store-001", "hood-001", HOOD_PROFILE, InputKind.NUMERIC_TELEMETRY,
    ),
    binding_revision="fixture-binding-1",
    input_contract_revision="sensor-metric-1.0.0",
    profile=ProfileKey("hood", "fixture-rpm-freshness", "1"),
    freshness_timeout=TIMEOUT, metric_name="fan_rpm", unit="rpm",
    schema_versions=("1.0.0",), sources=("fixture-simulator",),
)


def observation(at=BASE, event_id="e1", **changes):
    return replace(HealthObservation(
        event_id, at, "store-001", "hood-001", "hood", "fan_rpm", 800,
        "rpm", "1.0.0", "fixture-simulator",
    ), **changes)


def boot(policy=POLICY, start=BASE):
    return initialize_health(
        policy, monitoring_epoch="fixture-epoch-1",
        monitoring_started_at=start,
    )


def evaluate(snapshot, item, at=BASE, policy=POLICY):
    return reduce_health(snapshot, item, policy=policy, reference_time=at)


def healthy():
    return evaluate(boot(), observation()).snapshot


def stale():
    state = healthy()
    return evaluate(
        state, HealthTick(timer_token(state)), BASE + TIMEOUT,
    ).snapshot


def reload_snapshot(snapshot):
    """테스트용 JSON 재구성이며 운영 wire/storage codec이 아니다."""
    data = json.loads(json.dumps(asdict(snapshot), default=lambda value: (
        value.isoformat() if isinstance(value, datetime)
        else value.total_seconds() if isinstance(value, timedelta)
        else value.value
    )))
    policy = data.pop("policy")
    binding = policy.pop("binding")
    binding["profile"] = ProfileKey(**binding["profile"])
    binding["input_kind"] = InputKind(binding["input_kind"])
    policy["binding"] = EquipmentBinding(**binding)
    policy["profile"] = ProfileKey(**policy["profile"])
    policy["freshness_timeout"] = timedelta(
        seconds=policy["freshness_timeout"],
    )
    for name in ("sources", "schema_versions"):
        policy[name] = tuple(policy[name])
    data["policy"] = HealthPolicy(**policy)
    for name in (
        "monitoring_started_at", "last_seen", "freshness_deadline",
        "last_evaluated_at",
    ):
        if data[name] is not None:
            data[name] = datetime.fromisoformat(data[name])
    data["receipts"] = tuple(HealthObservation(
        **{**item, "event_time": datetime.fromisoformat(item["event_time"])},
    ) for item in data["receipts"])
    data["active_faults"] = tuple(data["active_faults"])
    if data["health_status"] is not None:
        data["health_status"] = HealthStatus(data["health_status"])
    return HealthSnapshot(**data)


def test_first_observation_initializes_without_touching_original():
    before = boot()
    result = evaluate(before, observation())
    assert before.health_status is None
    assert before.last_seen is None
    assert before.receipts == ()
    assert result.decision is HealthDecision.INITIALIZED
    assert result.snapshot.health_status is HealthStatus.HEALTHY
    assert result.snapshot.last_seen == BASE
    assert result.snapshot.freshness_deadline == BASE + TIMEOUT
    assert result.snapshot.health_revision == 1
    assert result.snapshot.snapshot_revision == 1
    assert result.changes[0].previous is None
    assert result.changes[0].record_kind == "INITIALIZED"
    assert len(result.changes[0].health_change_id) == 64


def test_new_observation_updates_deadline_without_change_event():
    before = healthy()
    at = BASE + EPS
    result = evaluate(before, observation(at, "e2"), at)
    assert result.decision is HealthDecision.NO_CHANGE
    assert result.snapshot.last_seen == at
    assert result.snapshot.freshness_deadline == at + TIMEOUT
    assert result.snapshot.health_revision == before.health_revision
    assert result.snapshot.snapshot_revision == before.snapshot_revision + 1
    assert result.snapshot.last_health_change_id == (
        before.last_health_change_id
    )
    assert not result.changes


@pytest.mark.parametrize("fresh", [True, False])
def test_delayed_observation_recovers_only_inside_freshness_window(fresh):
    before = stale()
    at = BASE + TIMEOUT + EPS
    event_time = BASE + TIMEOUT - EPS if fresh else BASE + EPS
    result = evaluate(before, observation(event_time, "e2"), at)
    if fresh:
        assert result.snapshot.health_status is HealthStatus.HEALTHY
        assert result.snapshot.last_seen == event_time
        assert result.snapshot.last_evaluated_at == at
        assert result.changes[0].previous is HealthStatus.STALE
        assert result.snapshot.health_revision == 3
    else:
        assert result.decision is HealthDecision.TOO_OLD_FOR_FRESHNESS
        assert result.snapshot is before
        assert not result.changes


def test_duplicate_precedes_time_guard_and_never_extends_timeout():
    before = healthy()
    duplicate = evaluate(before, observation(), BASE - EPS)
    assert duplicate.decision is HealthDecision.DUPLICATE
    assert duplicate.snapshot is before
    assert not duplicate.changes
    result = evaluate(before, HealthTick(timer_token(before)), BASE + TIMEOUT)
    assert result.snapshot.health_status is HealthStatus.STALE


@pytest.mark.parametrize("changes", [
    {"metric_value": 801}, {"source": "unapproved"}, {"unit": "hz"},
    {"event_time": BASE + EPS}, {"schema_version": "2"},
    {"metric_name": "temperature"}, {"metric_value": True},
    {"metric_value": "800"},
])
def test_conflict_preserves_canonical_before_time_and_semantic_checks(changes):
    before = healthy()
    result = evaluate(before, observation(**changes), BASE - EPS)
    assert result.decision is HealthDecision.ID_CONFLICT
    assert result.snapshot is before
    assert result.snapshot.receipts == (observation(),)
    assert not result.changes


@pytest.mark.parametrize("offset,decision", [
    (-1, HealthDecision.OUT_OF_ORDER), (0, HealthDecision.SAME_TIME),
])
def test_old_or_equal_event_time_cannot_rollback(offset, decision):
    state = evaluate(boot(start=BASE - TIMEOUT), observation()).snapshot
    event = observation(BASE + offset * EPS, "other")
    result = evaluate(state, event, BASE + EPS)
    assert result.decision is decision
    assert result.snapshot is state
    assert not result.changes


@pytest.mark.parametrize("changes", [
    {"metric_name": "temperature"}, {"unit": "hz"},
    {"schema_version": "2.0.0"}, {"source": "unknown"},
    {"metric_value": -1}, {"metric_value": True}, {"metric_value": "800"},
    {"metric_value": None}, {"metric_value": float("nan")},
    {"metric_value": float("inf")}, {"metric_value": float("-inf")},
])
def test_bad_sample_cannot_preempt_valid_sample_or_extend_deadline(changes):
    before = healthy()
    event_time = BASE + EPS
    invalid = observation(event_time, "bad", **changes)
    result = evaluate(before, invalid, BASE + 2 * EPS)
    assert result.decision is HealthDecision.INVALID_OBSERVATION
    assert result.snapshot is before
    assert not result.changes
    accepted = evaluate(
        result.snapshot, observation(event_time, "good"), BASE + 2 * EPS,
    )
    assert accepted.snapshot.last_seen == event_time
    assert accepted.snapshot.last_accepted_event_id == "good"


@pytest.mark.parametrize("changes", [
    {"store_id": "other"}, {"equipment_id": "other"},
    {"equipment_type": "fryer"},
])
def test_wrong_identity_cannot_pollute_ledger(changes):
    state = healthy()
    result = evaluate(state, observation(**changes))
    assert result.decision is HealthDecision.IDENTITY_MISMATCH
    assert result.snapshot is state


@pytest.mark.parametrize("item", [FaultSignal("E1", True), {}, None])
def test_unsupported_input_cannot_create_fault(item):
    state = healthy()
    result = evaluate(state, item, BASE + EPS)
    assert result.decision is HealthDecision.UNSUPPORTED_INPUT
    assert result.snapshot is state
    assert not result.changes


@pytest.mark.parametrize("rpm", [0, 400, 1800])
def test_valid_rpm_value_never_implies_fault(rpm):
    result = evaluate(boot(), observation(metric_value=rpm))
    assert result.snapshot.health_status is HealthStatus.HEALTHY


@pytest.mark.parametrize("change", [
    {"profile": replace(POLICY.profile, version="2")},
    {"freshness_timeout": 2 * TIMEOUT},
    {"binding_revision": "new-binding"},
    {"input_contract_revision": "new-input"},
    {"binding": replace(POLICY.binding, profile=replace(
        HOOD_PROFILE, version="2",
    ))},
    {"sources": ("different-source",)},
])
def test_policy_or_dependency_mismatch_rejects_observation_and_tick(change):
    state = healthy()
    new_policy = replace(POLICY, **change)
    items = (observation(BASE + EPS, "e2"), HealthTick(timer_token(state)))
    for item in items:
        result = evaluate(state, item, BASE + TIMEOUT, new_policy)
        assert result.decision is HealthDecision.INCOMPATIBLE_SNAPSHOT
        assert result.snapshot is state
        assert not result.changes


def test_future_retry_and_before_activation():
    state = boot()
    event = observation(BASE + EPS)
    first = evaluate(state, event)
    assert first.decision is HealthDecision.FUTURE_EVENT
    assert first.snapshot is state
    assert evaluate(state, event, BASE + EPS).snapshot.last_seen == BASE + EPS
    old = evaluate(state, observation(BASE - EPS))
    assert old.decision is HealthDecision.BEFORE_ACTIVATION
    assert old.snapshot is state


@pytest.mark.parametrize("make_state", [boot, healthy, stale])
def test_json_snapshot_restore_and_same_input_give_same_result(make_state):
    state = make_state()
    restored = reload_snapshot(state)
    assert restored == state
    assert restored is not state
    item = HealthTick(timer_token(state))
    assert evaluate(state, item, BASE + TIMEOUT + EPS) == evaluate(
        restored, item, BASE + TIMEOUT + EPS,
    )
    event = observation(BASE + TIMEOUT + 2 * EPS, "restored-recovery")
    assert evaluate(state, event, event.event_time) == evaluate(
        restored, event, event.event_time,
    )


def test_numeric_utc_equivalence_is_duplicate():
    state = healthy()
    sample = observation(
        BASE.astimezone(timezone(timedelta(hours=9))), metric_value=800.0,
    )
    assert evaluate(state, sample).decision is HealthDecision.DUPLICATE


def test_profile_lists_are_canonical_and_policy_has_no_hidden_timeout():
    policy = replace(POLICY, sources=("z", "a"))
    reordered = replace(policy, sources=("a", "z"))
    assert policy == reordered
    assert policy.fingerprint == reordered.fingerprint
    assert len(policy.fingerprint) == 64
    with pytest.raises(TypeError):
        HealthPolicy()


@pytest.mark.parametrize("change", [
    {"freshness_timeout": timedelta(0)}, {"freshness_timeout": 10},
    {"sources": ()}, {"sources": ["mutable"]}, {"sources": ("a", "a")},
    {"metric_name": "unconfigured"}, {"unit": "hz"},
    {"fault_supported": True},
])
def test_invalid_or_unsupported_policy_cannot_activate(change):
    with pytest.raises((TypeError, ValueError)):
        replace(POLICY, **change)


@pytest.mark.parametrize("change", [
    {"freshness_deadline": BASE}, {"last_evaluated_at": BASE - EPS},
    {"last_seen": BASE}, {"receipts": []},
    {"health_status": HealthStatus.HEALTHY},
    {"health_status": HealthStatus.FAULT}, {"active_faults": ("E1",)},
    {"health_revision": True},
])
def test_inconsistent_snapshot_is_not_silently_restored_as_healthy(change):
    with pytest.raises((TypeError, ValueError)):
        replace(boot(), **change)


@pytest.mark.parametrize("change", [
    {"event_time": BASE.replace(tzinfo=None)}, {"event_time": "now"},
    {"event_id": " "}, {"metric_value": []},
])
def test_invalid_observation_structure_is_an_api_error(change):
    with pytest.raises((TypeError, ValueError)):
        observation(**change)


def test_exact_ten_field_shape_and_current_metric_contracts():
    root = Path(__file__).resolve().parents[3]
    schema_path = root / "schemas/avro/sensor_metric_event.avsc"
    schema = json.loads(schema_path.read_text())
    assert {f.name for f in fields(HealthObservation)} == {
        f["name"] for f in schema["fields"]
    }
    rules = json.loads((root / "config/validation_rules.json").read_text())
    for equipment in rules["equipment_registry"]:
        kind = equipment["equipment_type"]
        metric, unit = next(iter(rules["metric_units"][kind].items()))
        binding = EquipmentBinding(
            equipment["store_id"], equipment["equipment_id"],
            ProfileKey(kind, "fixture-operational-dependency", "1"),
            InputKind.NUMERIC_TELEMETRY,
        )
        policy = replace(
            POLICY, binding=binding,
            profile=ProfileKey(kind, "freshness", "1"),
            metric_name=metric, unit=unit,
        )
        sample = observation(
            equipment_id=equipment["equipment_id"], equipment_type=kind,
            metric_name=metric, unit=unit, metric_value=-10 if kind == (
                "freezer"
            ) else 800,
        )
        result = evaluate(boot(policy), sample, policy=policy)
        assert result.snapshot.health_status is HealthStatus.HEALTHY


def test_new_event_after_stale_recovers_and_preserves_operational_objects():
    from streaming.state.hood import HoodPolicy, HoodSnapshot, HoodState

    operational = HoodSnapshot(
        "store-001", "hood-001", HoodPolicy(
            600, 200, timedelta(seconds=3), timedelta(seconds=2),
            timedelta(seconds=2),
        ), current=HoodState.ROTATING,
    )
    before = stale()
    at = BASE + TIMEOUT + EPS
    result = evaluate(before, observation(at, "new"), at)
    assert result.snapshot.health_status is HealthStatus.HEALTHY
    assert result.snapshot.health_revision == 3
    assert result.snapshot.last_seen == at
    assert len(result.changes) == 1
    assert operational.current is HoodState.ROTATING
    assert not hasattr(result.snapshot, "operational_state")


@pytest.mark.parametrize("at", [None, "now", BASE.replace(tzinfo=None)])
def test_reference_time_requires_explicit_aware_datetime(at):
    with pytest.raises(ValueError):
        evaluate(healthy(), observation(BASE + EPS, "e2"), at)


@pytest.mark.parametrize("field", [
    "snapshot_schema_version", "health_contract_version",
])
def test_unknown_snapshot_format_or_contract_never_silently_migrates(field):
    state = replace(healthy(), **{field: "unsupported"})
    result = evaluate(state, HealthTick(timer_token(state)), BASE + TIMEOUT)
    assert result.decision is HealthDecision.INCOMPATIBLE_SNAPSHOT
    assert result.snapshot is state


def test_fresh_observation_identity_does_not_include_processing_delay():
    initial = boot()
    direct = evaluate(initial, observation())
    delayed = evaluate(initial, observation(), BASE + TIMEOUT - EPS)
    assert direct.changes == delayed.changes
    assert direct.snapshot.last_evaluated_at != (
        delayed.snapshot.last_evaluated_at
    )


def test_typed_boot_generation_cannot_collide_with_event_id():
    initial = boot()
    first_token = timer_token(initial)
    accepted = evaluate(
        initial, observation(event_id=initial.monitoring_epoch),
    ).snapshot
    assert timer_token(accepted).observation_generation != (
        first_token.observation_generation
    )
