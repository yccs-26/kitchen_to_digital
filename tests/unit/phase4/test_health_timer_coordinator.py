"""순수 timer 조정·codec 재적재·명시 순서·장비 격리 검증."""

from dataclasses import asdict, replace
from datetime import timezone, timedelta

import pytest

from streaming.state.contracts import HealthStatus
from streaming.state.health import (
    HealthDecision, HealthTick, reduce_health, timer_token,
)
from streaming.state.health_codec import (
    HealthCodecError, encode_health_snapshot,
)
from streaming.state.health_timer_coordinator import (
    HealthTimerCoordinatorError, TimerStatus, coordinate_health_timer,
    plan_health_timer, restore_health_timer,
)
from test_health import (
    BASE, EPS, POLICY, TIMEOUT, boot, evaluate, healthy, observation, stale,
)


def coordinate(state, token=None, at=None, policy=POLICY, **options):
    return coordinate_health_timer(
        state, timer_token(state) if token is None else token,
        policy=policy, evaluation_time=at or BASE + TIMEOUT, **options,
    )


def restart(state, path, at, policy=POLICY):
    path.write_bytes(encode_health_snapshot(state))
    result = restore_health_timer(
        path.read_bytes(), policy=policy, evaluation_time=at,
    )
    assert result.snapshot is not state
    assert asdict(result.snapshot) == asdict(state)
    assert result.request.token == timer_token(state)
    return result


@pytest.mark.parametrize("factory", [boot, healthy])
def test_plan_reuses_current_deadline_and_typed_generation(factory):
    state = factory()
    request = plan_health_timer(state, policy=POLICY, evaluation_time=BASE)
    assert request.status is TimerStatus.WAITING
    assert request.token == timer_token(state)
    assert request.token.expected_deadline == BASE + TIMEOUT
    tag = "boot" if state.last_seen is None else "event"
    assert request.token.observation_generation[0] == tag


def test_before_deadline_never_invokes_reducer_or_moves_cursor():
    def forbidden(*args, **kwargs):
        pytest.fail("pending timer must not be evaluated")

    state = healthy()
    result = coordinate(state, at=BASE + TIMEOUT - EPS, reducer=forbidden)
    assert result.status is TimerStatus.WAITING
    assert result.snapshot is state
    assert result.reduction is None
    assert not result.changes


@pytest.mark.parametrize("offset", [timedelta(0), EPS, TIMEOUT])
@pytest.mark.parametrize("factory", [boot, healthy])
def test_due_timer_delegates_exactly_to_core(factory, offset):
    state = factory()
    at = BASE + TIMEOUT + offset
    request = plan_health_timer(state, policy=POLICY, evaluation_time=at)
    assert request.status is TimerStatus.DUE
    expected = evaluate(state, HealthTick(request.token), at)
    result = coordinate(state, request.token, at)
    assert result.status is TimerStatus.EVALUATED
    assert result.reduction == expected
    assert result.snapshot is result.reduction.snapshot
    assert result.changes == expected.changes
    assert result.snapshot.health_status is HealthStatus.STALE
    assert len(result.changes) == 1


def test_completed_request_and_repeated_delivery_preserve_core_semantics():
    state = healthy()
    first = coordinate(state)
    for at in (BASE + TIMEOUT, BASE + 2 * TIMEOUT):
        plan = plan_health_timer(
            first.snapshot, policy=POLICY, evaluation_time=at,
        )
        assert plan.status is TimerStatus.ALREADY_EVALUATED
        result = coordinate(first.snapshot, at=at)
        direct = evaluate(first.snapshot, HealthTick(plan.token), at)
        assert result.status is TimerStatus.ALREADY_EVALUATED
        assert result.reduction == direct
        assert result.snapshot.last_evaluated_at == at
        assert result.snapshot.health_revision == (
            first.snapshot.health_revision
        )
        assert not result.changes


def test_old_generation_is_rejected_without_evaluating_new_due_timer():
    original = healthy()
    old_token = timer_token(original)
    at = BASE + EPS
    current = evaluate(original, observation(at, "e2"), at).snapshot
    ignored = coordinate(current, old_token, BASE + TIMEOUT + EPS)
    assert ignored.status is TimerStatus.OBSOLETE
    assert ignored.reduction.decision is HealthDecision.OBSOLETE_TIMER
    assert ignored.snapshot is current
    assert not ignored.changes
    due = coordinate(current, at=BASE + TIMEOUT + EPS)
    assert due.status is TimerStatus.EVALUATED
    assert len(due.changes) == 1


@pytest.mark.parametrize("factory", [healthy, stale])
def test_current_timer_reference_reversal_uses_core_rejection(factory):
    state = factory()
    at = state.last_evaluated_at - EPS
    request = plan_health_timer(state, policy=POLICY, evaluation_time=at)
    assert request.status is TimerStatus.REFERENCE_REVERSED
    result = coordinate(state, at=at)
    assert result.reduction.decision is HealthDecision.REFERENCE_REVERSED
    assert result.snapshot is state
    assert not result.changes


def test_obsolete_timer_precedes_reversed_evaluation_time():
    state = healthy()
    token = replace(timer_token(state), monitoring_epoch="old")
    result = coordinate(state, token, BASE - EPS)
    assert result.status is TimerStatus.OBSOLETE
    assert result.snapshot is state


@pytest.mark.parametrize("offset,status", [
    (-EPS, TimerStatus.WAITING),
    (timedelta(0), TimerStatus.DUE),
    (EPS, TimerStatus.DUE),
])
def test_restore_before_or_after_deadline_rebuilds_same_timer(
    tmp_path, offset, status,
):
    state = healthy()
    at = BASE + TIMEOUT + offset
    restored = restart(state, tmp_path / "snapshot.json", at)
    assert restored.request.status is status
    result = coordinate(restored.snapshot, restored.request.token, at)
    uninterrupted = coordinate(state, at=at)
    assert result == uninterrupted


def test_stale_restart_rebuilds_completed_request_without_new_change(tmp_path):
    state = stale()
    at = BASE + 2 * TIMEOUT
    restored = restart(state, tmp_path / "stale.json", at)
    assert restored.request.status is TimerStatus.ALREADY_EVALUATED
    result = coordinate(restored.snapshot, restored.request.token, at)
    assert restored.snapshot.last_evaluated_at == state.last_evaluated_at
    assert not result.changes
    assert result.snapshot.last_evaluated_at == at


def test_restart_after_new_observation_does_not_revive_old_timer(tmp_path):
    state = healthy()
    old = timer_token(state)
    at = BASE + EPS
    current = evaluate(state, observation(at, "new"), at).snapshot
    restored = restart(current, tmp_path / "new.json", BASE + TIMEOUT)
    assert restored.request.status is TimerStatus.WAITING
    result = coordinate(restored.snapshot, old, BASE + TIMEOUT)
    assert result.status is TimerStatus.OBSOLETE
    assert result.snapshot is restored.snapshot
    assert not result.changes


def test_repeated_restoration_has_identical_due_result_and_change_id(tmp_path):
    state = healthy()
    at = BASE + TIMEOUT + EPS
    first = restart(state, tmp_path / "same.json", at)
    second = restore_health_timer(
        (tmp_path / "same.json").read_bytes(),
        policy=POLICY, evaluation_time=at,
    )
    assert first == second
    left = coordinate(first.snapshot, first.request.token, at)
    right = coordinate(second.snapshot, second.request.token, at)
    assert left == right
    assert left.changes[0].health_change_id == (
        coordinate(state, at=at).changes[0].health_change_id
    )


def test_restart_immediately_before_and_after_timer_evaluation(tmp_path):
    state = healthy()
    deadline = BASE + TIMEOUT
    before = restart(state, tmp_path / "boundary.json", deadline)
    evaluated = coordinate(before.snapshot, before.request.token, deadline)
    after = restart(evaluated.snapshot, tmp_path / "boundary.json", deadline)
    repeated = coordinate(after.snapshot, after.request.token, deadline)
    assert repeated.status is TimerStatus.ALREADY_EVALUATED
    assert not repeated.changes
    assert repeated.snapshot.last_health_change_id == (
        evaluated.changes[0].health_change_id
    )


@pytest.mark.parametrize("observation_first", [True, False])
def test_recorded_race_order_matches_core_across_restarts(
    tmp_path, observation_first,
):
    at = BASE + TIMEOUT
    initial = healthy()
    old = timer_token(initial)
    event = observation(at, "race")
    commands = (event, old) if observation_first else (old, event)

    def run():
        direct = initial
        coordinated = restart(initial, tmp_path / "race.json", at).snapshot
        changes = []
        for command in commands:
            if command is event:
                expected = evaluate(direct, event, at)
                result = evaluate(coordinated, event, at)
                next_snapshot, output = result.snapshot, result.changes
                assert result == expected
            else:
                expected = evaluate(direct, HealthTick(old), at)
                result = coordinate(coordinated, old, at)
                next_snapshot, output = result.snapshot, result.changes
                assert result.reduction == expected
            changes.extend(output)
            direct = expected.snapshot
            restored = restart(next_snapshot, tmp_path / "race.json", at)
            coordinated = restored.snapshot
        assert coordinated == direct
        return coordinated, tuple(changes)

    first = run()
    assert first == run()
    assert len(first[1]) == (0 if observation_first else 2)
    assert first[0].health_status is HealthStatus.HEALTHY


@pytest.mark.parametrize("other_store,same_equipment_id", [
    ("store-001", False), ("store-002", True),
])
def test_devices_stores_and_deadlines_are_isolated(
    other_store, same_equipment_id,
):
    first = healthy()
    binding = replace(
        POLICY.binding, store_id=other_store,
        equipment_id="hood-001" if same_equipment_id else "hood-002",
    )
    other_policy = replace(
        POLICY, binding=binding, freshness_timeout=2 * TIMEOUT,
    )
    second = evaluate(
        boot(other_policy), observation(
            store_id=binding.store_id, equipment_id=binding.equipment_id,
        ), policy=other_policy,
    ).snapshot
    plan = plan_health_timer(
        second, policy=other_policy, evaluation_time=BASE + TIMEOUT,
    )
    assert plan.status is TimerStatus.WAITING
    first_result = coordinate(first)
    assert first_result.snapshot.health_status is HealthStatus.STALE
    cross = coordinate(second, timer_token(first), policy=other_policy)
    assert cross.status is TimerStatus.OBSOLETE
    assert cross.snapshot is second
    assert not cross.changes
    assert second.health_status is HealthStatus.HEALTHY


@pytest.mark.parametrize("change", [
    {"profile": replace(POLICY.profile, version="2")},
    {"binding": replace(POLICY.binding, equipment_id="other")},
    {"binding": replace(POLICY.binding, store_id="other")},
    {"freshness_timeout": 2 * TIMEOUT},
])
def test_context_mismatch_is_rejected_before_any_timer_request(change):
    policy = replace(POLICY, **change)
    state = healthy()
    with pytest.raises(HealthTimerCoordinatorError) as raised:
        plan_health_timer(state, policy=policy, evaluation_time=BASE + TIMEOUT)
    assert raised.value.code is HealthDecision.INCOMPATIBLE_SNAPSHOT
    with pytest.raises(HealthTimerCoordinatorError):
        coordinate(state, policy=policy)
    with pytest.raises(HealthCodecError):
        restore_health_timer(
            encode_health_snapshot(state), policy=policy,
            evaluation_time=BASE + TIMEOUT,
        )


def test_corrupted_snapshot_is_not_used_to_construct_timer(tmp_path):
    path = tmp_path / "bad.json"
    payload = encode_health_snapshot(healthy())
    path.write_bytes(payload[:len(payload) // 2])
    with pytest.raises(HealthCodecError):
        restore_health_timer(
            path.read_bytes(), policy=POLICY, evaluation_time=BASE + TIMEOUT,
        )


@pytest.mark.parametrize("after_computation", [False, True])
def test_injected_failure_before_snapshot_acceptance_retries_same_logical_id(
    tmp_path, after_computation,
):
    state = healthy()
    at = BASE + TIMEOUT
    captured = []
    path = tmp_path / "old-state.json"
    persisted = encode_health_snapshot(state)
    path.write_bytes(persisted)

    def failing(previous, item, *, policy, reference_time):
        if after_computation:
            captured.append(reduce_health(
                previous, item, policy=policy, reference_time=reference_time,
            ))
        raise RuntimeError("injected before caller accepts next snapshot")

    with pytest.raises(RuntimeError, match="injected"):
        coordinate(state, at=at, reducer=failing)
    assert path.read_bytes() == persisted
    restored = restore_health_timer(
        path.read_bytes(), policy=POLICY, evaluation_time=at,
    )
    assert restored.snapshot == state
    retry = coordinate(restored.snapshot, restored.request.token, at)
    direct = coordinate(state, at=at)
    assert retry == direct
    if captured:
        assert retry.reduction == captured[0]
    assert state.health_status is HealthStatus.HEALTHY
    assert len(retry.changes) == 1


@pytest.mark.parametrize("at", [None, "now", BASE.replace(tzinfo=None)])
def test_explicit_aware_time_is_required(at):
    with pytest.raises(ValueError):
        plan_health_timer(healthy(), policy=POLICY, evaluation_time=at)


def test_offset_time_normalization_has_same_result():
    state = healthy()
    at = BASE + TIMEOUT
    shifted = at.astimezone(timezone(timedelta(hours=9)))
    assert coordinate(state, at=shifted) == coordinate(state, at=at)
