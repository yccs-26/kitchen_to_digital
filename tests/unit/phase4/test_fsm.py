"""명시적 synthetic 신호의 전이·재실행·순서 정책을 검증한다."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.state.contracts import (
    CleaningCompletedSignal,
    CleaningSignal,
    FaultSignal,
    HealthStatus,
    ModeSignal,
    OperationalState,
    ReportedMode,
    StateObservation,
    SyntheticSignal,
)
from streaming.state.fsm import Decision, TwinState, transition


NOW = datetime(2026, 10, 17, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[3]


def state(operational=OperationalState.IDLE, health=HealthStatus.STALE):
    return TwinState(
        "store", "equipment", StateObservation(operational, health)
    )


def signal(payload, number=1, **changes):
    event = SyntheticSignal(
        signal_id=f"synthetic-{number}",
        event_time=NOW + timedelta(seconds=number),
        store_id="store",
        equipment_id="equipment",
        source="synthetic-controller",
        payload=payload,
        provenance="synthetic_test_only",
    )
    return replace(event, **changes)


def apply(previous, event, reference_time=NOW + timedelta(hours=1)):
    return transition(previous, event, reference_time=reference_time)


@pytest.mark.parametrize("health", list(HealthStatus))
def test_reference_fixture_is_deterministic_and_preserves_health(health):
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase4/reference_cycle.json").read_text()
    )
    initial = TwinState(
        fixture["store_id"], fixture["equipment_id"],
        StateObservation(
            OperationalState(fixture["initial_operational_state"]), health
        ),
    )

    def run():
        snapshot = initial
        results = []
        for step in fixture["steps"]:
            if step["kind"] == "mode":
                payload = ModeSignal(ReportedMode(step["mode"]))
            elif step["kind"] == "cleaning":
                payload = CleaningSignal(step["active"])
            else:
                assert step["kind"] == "cleaning_completed"
                payload = CleaningCompletedSignal()
            event = SyntheticSignal(
                step["signal_id"],
                datetime.fromisoformat(step["event_time"]),
                fixture["store_id"],
                fixture["equipment_id"],
                fixture["source"],
                payload, fixture["provenance"],
            )
            result = apply(snapshot, event)
            assert result.decision is Decision.TRANSITION
            assert result.state.observation.operational_state.value == (
                step["expected"]
            )
            assert result.state.observation.health_status is health
            assert len(result.intents) == 1
            intent = result.intents[0]
            assert intent.previous is snapshot.observation.operational_state
            assert intent.current is result.state.observation.operational_state
            assert intent.signal_id == event.signal_id
            assert intent.equipment_id == event.equipment_id
            snapshot = result.state
            results.append(result)
        return results

    first = run()
    assert first == run()
    assert len({r.intents[0].state_change_id for r in first}) == 4
    assert initial.receipts == ()
    assert initial.last_event_time is None


PAYLOADS = [
    *(ModeSignal(mode) for mode in ReportedMode),
    CleaningSignal(True), CleaningSignal(False), CleaningCompletedSignal(),
]


@pytest.mark.parametrize("operational", list(OperationalState))
@pytest.mark.parametrize("payload", PAYLOADS)
def test_complete_operational_transition_matrix(operational, payload):
    allowed = {
        (OperationalState.IDLE, ModeSignal(ReportedMode.PREHEATING)):
            OperationalState.PREHEATING,
        (OperationalState.PREHEATING, ModeSignal(ReportedMode.COOKING)):
            OperationalState.COOKING,
        (OperationalState.COOKING, CleaningSignal(True)):
            OperationalState.CLEANING,
        (OperationalState.CLEANING, CleaningCompletedSignal()):
            OperationalState.IDLE,
    }
    unchanged = (
        payload == CleaningSignal(False)
        or isinstance(payload, ModeSignal)
        and payload.mode.value == operational.value
        or payload == CleaningSignal(True)
        and operational is OperationalState.CLEANING
        or isinstance(payload, CleaningCompletedSignal)
        and operational is OperationalState.IDLE
    )
    before = state(operational)
    event = signal(payload)
    result = apply(before, event)
    target = allowed.get((operational, payload))
    expected = (
        Decision.TRANSITION if target else
        Decision.NO_CHANGE if unchanged else Decision.FORBIDDEN
    )
    assert result.decision is expected
    assert result.state.observation.operational_state is (
        target or operational
    )
    assert bool(result.intents) is (target is not None)
    assert result.state.receipts == (event,)
    assert before == state(operational)


def test_duplicates_and_conflicts_preserve_canonical_state():
    event = signal(ModeSignal(ReportedMode.PREHEATING))
    first = apply(state(), event)
    later = apply(first.state, signal(ModeSignal(ReportedMode.COOKING), 2))
    duplicate = apply(later.state, event)
    assert duplicate.decision is Decision.DUPLICATE
    assert duplicate.state is later.state
    assert not duplicate.intents
    for changes in [
        {"payload": ModeSignal(ReportedMode.COOKING)},
        {"source": "other-controller"},
        {"event_time": event.event_time + timedelta(seconds=4)},
    ]:
        conflict = apply(later.state, replace(event, **changes))
        assert conflict.decision is Decision.ID_CONFLICT
        assert conflict.state is later.state
        assert not conflict.intents


def test_forbidden_input_is_not_reinterpreted_after_state_advances():
    cook = signal(ModeSignal(ReportedMode.COOKING))
    rejected = apply(state(), cook)
    assert rejected.decision is Decision.FORBIDDEN
    preheat = apply(
        rejected.state, signal(ModeSignal(ReportedMode.PREHEATING), 2)
    )
    replay = apply(preheat.state, cook)
    assert replay.decision is Decision.DUPLICATE
    assert replay.state.observation.operational_state is (
        OperationalState.PREHEATING
    )


@pytest.mark.parametrize("field", ["store_id", "equipment_id"])
def test_foreign_identity_cannot_change_state(field):
    before = state()
    event = signal(ModeSignal(ReportedMode.PREHEATING), **{field: "other"})
    result = apply(before, event)
    assert result.decision is Decision.IDENTITY_MISMATCH
    assert result.state is before
    assert not result.intents


@pytest.mark.parametrize(
    "seconds, expected",
    [(-1, Decision.OUT_OF_ORDER), (0, Decision.SAME_TIME)],
)
def test_old_or_ambiguous_timestamp_cannot_roll_back(seconds, expected):
    first = apply(state(), signal(ModeSignal(ReportedMode.PREHEATING), 2))
    event = signal(
        ModeSignal(ReportedMode.COOKING), 3,
        event_time=first.state.last_event_time + timedelta(seconds=seconds),
    )
    result = apply(first.state, event)
    assert result.decision is expected
    assert result.state is first.state
    assert not result.intents


def test_future_retries_and_reversed_reference():
    event = signal(ModeSignal(ReportedMode.PREHEATING))
    before = state()
    future = apply(before, event, NOW)
    assert future.decision is Decision.FUTURE_EVENT
    assert future.state is before
    accepted = apply(future.state, event, event.event_time)
    assert accepted.decision is Decision.TRANSITION
    reversed_time = apply(
        accepted.state, signal(ModeSignal(ReportedMode.COOKING), 2), NOW
    )
    assert reversed_time.decision is Decision.REFERENCE_REVERSED
    assert reversed_time.state is accepted.state


def test_retry_from_same_snapshot_has_same_intent_despite_processing_delay():
    before = state()
    event = signal(ModeSignal(ReportedMode.PREHEATING))
    first = apply(before, event, event.event_time)
    assert first == apply(before, event, event.event_time + timedelta(days=2))
    local = replace(
        event, event_time=event.event_time.astimezone(
            timezone(timedelta(hours=9))
        )
    )
    assert first == apply(before, local)


@pytest.mark.parametrize("operational", list(OperationalState))
@pytest.mark.parametrize("health", list(HealthStatus))
@pytest.mark.parametrize("active", [True, False])
def test_fault_is_deferred_without_mixing_health_and_operational(
    operational, health, active
):
    result = apply(
        state(operational, health), signal(FaultSignal("E1", active))
    )
    assert result.decision is Decision.HEALTH_DEFERRED
    assert result.state.observation == StateObservation(operational, health)
    assert not result.intents


@pytest.mark.parametrize("bad_time", [None, NOW.replace(tzinfo=None), "now"])
def test_reference_requires_explicit_aware_time(bad_time):
    with pytest.raises(ValueError):
        apply(state(), signal(CleaningSignal(False)), bad_time)


@pytest.mark.parametrize("event", [None, {}, {"metric_value": 180.0}])
def test_numeric_telemetry_cannot_enter_explicit_fsm(event):
    with pytest.raises(TypeError):
        apply(state(), event)


def test_snapshot_rejects_mutable_or_inconsistent_receipts():
    event = signal(CleaningSignal(False))
    for changes in [
        {"receipts": [event]},
        {"receipts": (event,)},
        {"receipts": (event, event), "last_event_time": event.event_time},
        {"receipts": (event,), "last_event_time": NOW},
        {"receipts": (replace(event, equipment_id="other"),),
         "last_event_time": event.event_time},
    ]:
        with pytest.raises((TypeError, ValueError)):
            replace(state(), **changes)
