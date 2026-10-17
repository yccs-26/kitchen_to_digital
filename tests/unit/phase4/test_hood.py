"""Hood RPM qualifier의 사건 시간, 노이즈, identity 계약."""

from dataclasses import asdict, fields, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.state.hood import (
    HoodDecision,
    HoodPolicy,
    HoodRpmEvent,
    HoodSnapshot,
    HoodState,
    qualify_rpm,
)
from streaming.validation.domain_validator import (
    EquipmentSpec,
    validate_domain_event,
)


BASE = datetime(2026, 10, 17, tzinfo=timezone.utc)
POLICY = HoodPolicy(
    600, 200, timedelta(seconds=3), timedelta(seconds=2), timedelta(seconds=2),
)
ROOT = Path(__file__).resolve().parents[3]


def event(second, rpm, **changes):
    sample = HoodRpmEvent(
        f"rpm-{second}", BASE + timedelta(seconds=second),
        "store-001", "hood-001", "hood", "fan_rpm", rpm, "rpm",
        "1.0.0", "simulator",
    )
    return replace(sample, **changes)


def initial(current=None, policy=POLICY):
    return HoodSnapshot("store-001", "hood-001", policy, current=current)


def apply(state, sample, reference=None):
    return qualify_rpm(
        state, sample, reference_time=reference or BASE + timedelta(days=1)
    )


def test_fixture_cycle_noise_and_replay():
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase4/hood_rpm.json").read_text()
    )

    assert fixture["provenance"] == "synthetic_test_only"
    assert fixture["policy"] == {
        "enter_rpm": POLICY.enter_rpm, "exit_rpm": POLICY.exit_rpm,
        "enter_seconds": POLICY.enter_duration.total_seconds(),
        "exit_seconds": POLICY.exit_duration.total_seconds(),
        "max_gap_seconds": POLICY.max_sample_gap.total_seconds(),
    }

    def run():
        state = initial()
        intents = []
        for step in fixture["steps"]:
            sample = event(step["second"], step["rpm"])
            result = apply(state, sample)
            assert result.decision.value == step["decision"]
            actual = result.state.current
            assert (actual.value if actual else None) == step["state"]
            assert apply(state, sample, BASE + timedelta(days=2)) == result
            assert not result.intents or result.decision is (
                HoodDecision.TRANSITION
            )
            intents.extend(result.intents)
            state = result.state
        return state, tuple(intents)

    state, intents = run()
    assert run() == (state, intents)
    assert [i.current for i in intents] == [
        HoodState.ROTATING, HoodState.STOPPED
    ]
    assert intents[0].event_id == "rpm-10"
    assert intents[1].event_id == "rpm-17"
    assert len({i.state_change_id for i in intents}) == 2
    assert initial().receipts == ()


@pytest.mark.parametrize("current,value,target,duration", [
    (HoodState.STOPPED, 600, HoodState.ROTATING, 3),
    (HoodState.ROTATING, 200, HoodState.STOPPED, 2),
    (None, 600, HoodState.ROTATING, 3),
    (None, 0, HoodState.STOPPED, 2),
])
def test_inclusive_threshold_and_duration_boundary(
    current, value, target, duration
):
    state = initial(current)
    for second in (0, 1, duration - 0.000001):
        result = apply(state, event(second, value))
        assert result.decision is HoodDecision.PENDING
        assert result.state.current is current
        assert not result.intents
        state = result.state
    result = apply(state, event(duration, value))
    assert result.state.current is target
    assert result.state.candidate is None
    assert result.state.candidate_since is None
    assert result.decision is (
        HoodDecision.INITIALIZED if current is None
        else HoodDecision.TRANSITION
    )
    assert len(result.intents) == (0 if current is None else 1)


@pytest.mark.parametrize("current", [None, *HoodState])
@pytest.mark.parametrize("value", [201, 400, 599])
def test_deadband_retains_current_and_cancels_candidate(current, value):
    rpm = 0 if current is HoodState.ROTATING else 600
    state = apply(initial(current), event(0, rpm)).state
    result = apply(state, event(1, value))
    assert result.decision is HoodDecision.NO_CHANGE
    assert result.state.current is current
    assert result.state.candidate is None
    assert result.state.candidate_since is None
    assert not result.intents
    restart = apply(result.state, event(2, rpm))
    assert restart.state.candidate_since == BASE + timedelta(seconds=2)


@pytest.mark.parametrize("current,rpm", [
    (HoodState.STOPPED, 0), (HoodState.ROTATING, 1800)
])
def test_current_region_cancels_opposite_candidate(current, rpm):
    opposite = 600 if current is HoodState.STOPPED else 0
    state = apply(initial(current), event(0, opposite)).state
    result = apply(state, event(1, rpm))
    assert result.state.current is current
    assert result.state.candidate is None
    assert result.decision is HoodDecision.NO_CHANGE


@pytest.mark.parametrize("gap,restarted", [(2, False), (2.000001, True)])
def test_max_gap_boundary(gap, restarted):
    state = apply(initial(HoodState.STOPPED), event(0, 600)).state
    result = apply(state, event(gap, 600))
    assert result.state.current is HoodState.STOPPED
    assert result.state.candidate_since == (
        BASE + timedelta(seconds=gap) if restarted else BASE
    )
    assert not result.intents


def test_long_gap_and_wall_clock_cannot_confirm_old_candidate():
    state = apply(initial(HoodState.STOPPED), event(0, 600)).state
    result = apply(state, event(100, 600))
    assert result.decision is HoodDecision.PENDING
    assert result.state.candidate_since == BASE + timedelta(seconds=100)
    assert result.state.current is HoodState.STOPPED
    assert apply(state, event(0, 600)).state is state


@pytest.mark.parametrize("change", [
    {"metric_value": -1}, {"unit": "hz"}, {"metric_name": "temperature"},
    {"equipment_type": "fryer"}, {"schema_version": "2.0.0"},
])
def test_invalid_sample_breaks_candidate_and_preserves_current(change):
    state = apply(initial(HoodState.STOPPED), event(0, 600)).state
    sample = event(1, 600, **change)
    result = apply(state, sample)
    assert result.decision is HoodDecision.INVALID_SAMPLE
    assert result.state.current is HoodState.STOPPED
    assert result.state.candidate is None
    assert result.state.last_event_time == sample.event_time
    assert apply(result.state, sample).decision is HoodDecision.DUPLICATE
    restarted = apply(result.state, event(2, 600))
    assert restarted.state.candidate_since == BASE + timedelta(seconds=2)


@pytest.mark.parametrize("sample,reference,decision", [
    (event(-1, 0), BASE, HoodDecision.OUT_OF_ORDER),
    (event(0, 0, event_id="equal"), BASE, HoodDecision.SAME_TIME),
    (event(1, 0), BASE, HoodDecision.FUTURE_EVENT),
    (event(1, 0), BASE - timedelta(seconds=1),
     HoodDecision.REFERENCE_REVERSED),
    (event(1, 0, equipment_id="other"), BASE,
     HoodDecision.IDENTITY_MISMATCH),
    (event(1, 0, store_id="other"), BASE, HoodDecision.IDENTITY_MISMATCH),
])
def test_rejected_events_preserve_candidate(sample, reference, decision):
    state = apply(initial(HoodState.STOPPED), event(0, 600)).state
    result = apply(state, sample, reference)
    assert result.decision is decision
    assert result.state is state
    assert not result.intents


@pytest.mark.parametrize("field,value", [
    ("metric_value", 601), ("source", "other"), ("unit", "hz"),
    ("metric_name", "other"), ("equipment_type", "other"),
    ("schema_version", "other"), ("event_time", BASE + timedelta(seconds=1)),
])
def test_full_payload_conflict_precedes_time_and_never_mutates(field, value):
    sample = event(0, 600)
    state = apply(initial(HoodState.STOPPED), sample).state
    result = apply(
        state, replace(sample, **{field: value}), BASE - timedelta(1)
    )
    assert result.decision is HoodDecision.ID_CONFLICT
    assert result.state is state
    assert not result.intents


def test_duplicates_after_transition_and_utc_equivalent_numeric_payload():
    state = initial(HoodState.STOPPED)
    samples = [event(t, 600) for t in (0, 1, 3)]
    for sample in samples:
        result = apply(state, sample)
        state = result.state
    assert result.decision is HoodDecision.TRANSITION
    for sample in samples:
        equivalent = replace(
            sample, metric_value=600.0,
            event_time=sample.event_time.astimezone(
                timezone(timedelta(hours=9))
            ),
        )
        replay = apply(state, equivalent, BASE - timedelta(1))
        assert replay.decision is HoodDecision.DUPLICATE
        assert replay.state is state
        assert not replay.intents


def test_future_retries_and_opposite_initial_candidate_reset():
    state = apply(initial(), event(0, 600)).state
    sample = event(1, 0)
    assert apply(state, sample, BASE).decision is HoodDecision.FUTURE_EVENT
    result = apply(state, sample)
    assert result.state.candidate is HoodState.STOPPED
    assert result.state.candidate_since == sample.event_time
    assert result.state.current is None


def test_policy_is_bound_to_snapshot_and_affects_transition_identity():
    def run(policy):
        state = initial(HoodState.STOPPED, policy)
        for second in (0, 1, 3):
            result = apply(state, event(second, 600))
            state = result.state
        return result

    first = run(POLICY)
    other = run(replace(POLICY, exit_rpm=100))
    assert first.intents[0].state_change_id != other.intents[0].state_change_id
    assert first.state.policy == POLICY
    assert len(first.intents[0].state_change_id) == 64


@pytest.mark.parametrize("change", [
    {"enter_rpm": 200}, {"exit_rpm": -1}, {"enter_rpm": float("inf")},
    {"exit_rpm": True}, {"enter_duration": timedelta(0)},
    {"exit_duration": timedelta(seconds=-1)},
    {"max_sample_gap": timedelta(0)}, {"enter_duration": 3},
])
def test_invalid_policy(change):
    with pytest.raises((ValueError, TypeError)):
        replace(POLICY, **change)


@pytest.mark.parametrize("change", [
    {"metric_value": True}, {"metric_value": "600"},
    {"metric_value": float("nan")}, {"metric_value": float("inf")},
    {"event_time": BASE.replace(tzinfo=None)}, {"event_time": "bad"},
    {"event_id": " "}, {"source": ""},
])
def test_invalid_input_structure(change):
    with pytest.raises((ValueError, TypeError)):
        event(0, 600, **change)


def test_input_uses_exact_existing_ten_field_contract_and_fixture_domain():
    schema = json.loads(
        (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text()
    )
    assert {f.name for f in fields(HoodRpmEvent)} == {
        f["name"] for f in schema["fields"]
    }
    rules = json.loads((ROOT / "config/validation_rules.json").read_text())
    registry = {
        e["equipment_id"]: EquipmentSpec(e["store_id"], e["equipment_type"])
        for e in rules["equipment_registry"]
    }
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase4/hood_rpm.json").read_text()
    )
    for step in fixture["steps"]:
        sample = event(step["second"], step["rpm"])
        validated = validate_domain_event(
            asdict(sample), equipment_registry=registry,
            metric_units=rules["metric_units"],
            supported_schema_versions=rules["supported_schema_versions"],
            kafka_key=b"hood-001", reference_time=BASE + timedelta(days=1),
            max_future_skew=timedelta(0),
        )
        assert validated.is_valid, validated.errors


@pytest.mark.parametrize("change", [
    {"current": "STOPPED"}, {"policy": None},
    {"candidate": HoodState.ROTATING}, {"candidate_since": BASE},
    {"candidate": HoodState.STOPPED, "candidate_since": BASE,
     "last_event_time": BASE},
    {"receipts": [event(0, 600)]},
    {"receipts": (event(0, 600),)},
])
def test_invalid_snapshot(change):
    with pytest.raises((TypeError, ValueError)):
        replace(initial(HoodState.STOPPED), **change)


@pytest.mark.parametrize("current,rpm,duration", [
    (HoodState.STOPPED, 600, 3), (HoodState.ROTATING, 0, 2),
])
def test_replay_cannot_advance_or_cancel_dwell(current, rpm, duration):
    first = event(0, rpm)
    state = apply(initial(current), first).state
    state = apply(state, event(1, rpm)).state
    replay = apply(state, first)
    assert replay.decision is HoodDecision.DUPLICATE
    assert replay.state is state
    conflict = apply(state, replace(first, metric_value=400))
    assert conflict.decision is HoodDecision.ID_CONFLICT
    assert conflict.state is state
    result = apply(state, event(duration, rpm))
    assert result.decision is HoodDecision.TRANSITION
    assert result.state.candidate is None


@pytest.mark.parametrize("current", list(HoodState))
def test_gap_does_not_imply_stopped_or_health(current):
    rpm = 0 if current is HoodState.STOPPED else 800
    state = apply(initial(current), event(0, rpm)).state
    result = apply(state, event(100, 400))
    assert result.state.current is current
    assert result.decision is HoodDecision.NO_CHANGE
    assert not result.intents
    assert not hasattr(result.state, "health_status")


def test_numeric_policy_representation_has_same_identity():
    def run(policy):
        state = initial(HoodState.STOPPED, policy)
        for second in (0, 1, 3):
            result = apply(state, event(second, 600))
            state = result.state
        return result.intents[0].state_change_id

    equivalent = replace(POLICY, enter_rpm=600.0, exit_rpm=200.0)
    assert run(POLICY) == run(equivalent)
