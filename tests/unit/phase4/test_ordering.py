"""P4-2의 명시적 보고 반복·사건 순서·재실행 경계를 검증한다."""

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


def make_event(payload, *, signal_id="new", event_time=NOW):
    return SyntheticSignal(
        signal_id, event_time, "store", "equipment", "synthetic-controller",
        payload, "synthetic_test_only",
    )


def make_state(operational=OperationalState.IDLE, *, last_time=None):
    return TwinState(
        "store", "equipment",
        StateObservation(operational, HealthStatus.STALE),
        last_event_time=last_time,
    )


def read_fixture():
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase4/rapid_reports.json").read_text()
    )
    events = []
    for step in fixture["steps"]:
        if step["kind"] == "mode":
            payload = ModeSignal(ReportedMode(step["mode"]))
        elif step["kind"] == "cleaning":
            payload = CleaningSignal(step["active"])
        else:
            assert step["kind"] == "cleaning_completed"
            payload = CleaningCompletedSignal()
        events.append(SyntheticSignal(
            step["signal_id"], datetime.fromisoformat(step["event_time"]),
            fixture["store_id"], fixture["equipment_id"], fixture["source"],
            payload, fixture["provenance"],
        ))
    return fixture, events


@pytest.mark.parametrize("health", list(HealthStatus))
def test_rapid_reports_follow_explicit_policy_without_dwell(health):
    fixture, events = read_fixture()
    initial = TwinState(
        fixture["store_id"], fixture["equipment_id"],
        StateObservation(OperationalState(fixture["initial_state"]), health),
    )
    reference = datetime.fromisoformat(fixture["reference_time"])

    def run():
        snapshot = initial
        intents = []
        for step, event in zip(fixture["steps"], events, strict=True):
            result = transition(snapshot, event, reference_time=reference)
            assert result.decision.value == step["decision"]
            assert result.state.observation.operational_state.value == (
                step["state"]
            )
            assert result.state.observation.health_status is health
            # 같은 snapshot 재계산에서 처리 지연은 출력 ID에 영향이 없다.
            retry = transition(
                replace(snapshot), event,
                reference_time=reference + timedelta(days=1),
            )
            assert retry == result
            if result.decision is Decision.TRANSITION:
                assert len(result.intents) == 1
                assert retry.intents[0].state_change_id == (
                    result.intents[0].state_change_id
                )
                intents.extend(result.intents)
            else:
                assert not result.intents
            if result.decision in {
                Decision.DUPLICATE, Decision.SAME_TIME, Decision.OUT_OF_ORDER
            }:
                assert result.state is snapshot
            snapshot = result.state
        return snapshot, tuple(intents)

    snapshot, intents = run()
    assert (snapshot, intents) == run()
    assert len(intents) == 6
    assert len({intent.state_change_id for intent in intents}) == 6
    assert intents[-1].previous is OperationalState.PREHEATING
    assert intents[-1].current is OperationalState.COOKING
    # 서로 다른 순환의 동일 전이도 사건 identity가 다르므로 ID가 다르다.
    assert intents[0].state_change_id != intents[4].state_change_id
    assert initial.receipts == ()


@pytest.mark.parametrize(
    "operational, payload",
    [
        (OperationalState.IDLE, ModeSignal(ReportedMode.PREHEATING)),
        (OperationalState.PREHEATING, ModeSignal(ReportedMode.COOKING)),
        (OperationalState.COOKING, CleaningSignal(True)),
        (OperationalState.CLEANING, CleaningCompletedSignal()),
    ],
)
@pytest.mark.parametrize("age_us", [0, 1, 1_000_000])
def test_stale_or_equal_valid_edge_cannot_change_current_state(
    operational, payload, age_us
):
    before = make_state(operational, last_time=NOW)
    event = make_event(
        payload, event_time=NOW - timedelta(microseconds=age_us)
    )
    result = transition(before, event, reference_time=NOW)
    expected = Decision.OUT_OF_ORDER if age_us else Decision.SAME_TIME
    assert result.decision is expected
    assert result.state is before
    assert not result.intents
    # 같은 입력도 current를 바꾸지 않으며 시간 거부는 원장에 추가하지 않는다.
    assert transition(result.state, event, reference_time=NOW) == result


@pytest.mark.parametrize(
    "payload, decision",
    [
        (ModeSignal(ReportedMode.IDLE), Decision.NO_CHANGE),
        (ModeSignal(ReportedMode.COOKING), Decision.FORBIDDEN),
        (FaultSignal("SYNTHETIC_E1", True), Decision.HEALTH_DEFERRED),
    ],
)
def test_consumed_non_transition_advances_cursor_without_reinterpreting_id(
    payload, decision
):
    event = make_event(payload)
    first = transition(make_state(), event, reference_time=NOW)
    assert first.decision is decision
    assert first.state.last_event_time == NOW
    assert first.state.receipts == (event,)
    second = transition(
        first.state,
        make_event(
            ModeSignal(ReportedMode.PREHEATING), signal_id="later",
            event_time=NOW + timedelta(seconds=1),
        ),
        reference_time=NOW + timedelta(seconds=1),
    )
    replay = transition(second.state, event, reference_time=NOW)
    assert replay.decision is Decision.DUPLICATE
    assert replay.state is second.state
    assert not replay.intents
    conflict = transition(
        second.state, replace(event, source="different-source"),
        reference_time=NOW,
    )
    assert conflict.decision is Decision.ID_CONFLICT
    assert conflict.state is second.state


@pytest.mark.parametrize("preheat_first", [True, False])
def test_distinct_ids_at_same_time_are_arrival_order_dependent(preheat_first):
    preheat = make_event(ModeSignal(ReportedMode.PREHEATING), signal_id="heat")
    idle = make_event(ModeSignal(ReportedMode.IDLE), signal_id="idle")
    first, second = (preheat, idle) if preheat_first else (idle, preheat)
    accepted = transition(make_state(), first, reference_time=NOW)
    refused = transition(accepted.state, second, reference_time=NOW)
    assert refused.decision is Decision.SAME_TIME
    assert refused.state is accepted.state
    expected = (
        OperationalState.PREHEATING if preheat_first else OperationalState.IDLE
    )
    assert refused.state.observation.operational_state is expected
    assert not refused.intents
