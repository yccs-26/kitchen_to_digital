"""Health tick 경계·경합·기록 순서 replay와 변화 identity 검증."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.health import (
    HealthDecision, HealthTick, reduce_health, timer_token,
)
from streaming.state.contracts import HealthStatus
from test_health import (
    BASE, EPS, POLICY, TIMEOUT, boot, evaluate, healthy, observation,
    reload_snapshot, stale,
)


@pytest.mark.parametrize("offset,status,count", [
    (-EPS, HealthStatus.HEALTHY, 0),
    (timedelta(0), HealthStatus.STALE, 1),
    (EPS, HealthStatus.STALE, 1),
    (TIMEOUT, HealthStatus.STALE, 1),
])
def test_deadline_boundary_and_sensor_silence(offset, status, count):
    before = healthy()
    result = evaluate(
        before, HealthTick(timer_token(before)), BASE + TIMEOUT + offset,
    )
    assert result.snapshot.health_status is status
    assert result.snapshot.last_seen == before.last_seen
    assert result.snapshot.freshness_deadline == before.freshness_deadline
    assert len(result.changes) == count


def test_boot_silence_does_not_invent_prior_healthy_state():
    state = boot()
    early = evaluate(
        state, HealthTick(timer_token(state)), BASE + TIMEOUT - EPS,
    )
    assert early.snapshot.health_status is None
    assert not early.changes
    expired = evaluate(
        early.snapshot, HealthTick(timer_token(state)), BASE + TIMEOUT,
    )
    assert expired.snapshot.health_status is HealthStatus.STALE
    assert expired.snapshot.last_seen is None
    assert expired.changes[0].previous is None
    assert expired.changes[0].record_kind == "INITIALIZED"


def test_delayed_tick_identity_and_repeated_tick_have_no_duplicate_change():
    state = healthy()
    tick = HealthTick(timer_token(state))
    exact = evaluate(state, tick, BASE + TIMEOUT)
    delayed = evaluate(state, tick, BASE + 2 * TIMEOUT)
    assert exact.changes == delayed.changes
    assert exact.snapshot.last_evaluated_at != (
        delayed.snapshot.last_evaluated_at
    )
    for at in (BASE + TIMEOUT, BASE + TIMEOUT + EPS):
        result = evaluate(exact.snapshot, tick, at)
        assert result.snapshot.health_status is HealthStatus.STALE
        assert not result.changes
        assert result.snapshot.health_revision == (
            exact.snapshot.health_revision
        )


@pytest.mark.parametrize("change", [
    {"store_id": "other"}, {"equipment_id": "other"},
    {"profile": replace(POLICY.profile, version="2")},
    {"policy_fingerprint": "0" * 64}, {"monitoring_epoch": "old"},
    {"observation_generation": ("event", "other")},
    {"expected_deadline": BASE + EPS},
])
def test_obsolete_token_cannot_advance_evaluation_cursor(change):
    state = healthy()
    token = replace(timer_token(state), **change)
    result = evaluate(state, HealthTick(token), BASE - EPS)
    assert result.decision is HealthDecision.OBSOLETE_TIMER
    assert result.snapshot is state
    assert not result.changes


def test_old_generation_then_current_deadline_tick():
    state = healthy()
    old = timer_token(state)
    state = evaluate(state, observation(BASE + EPS, "e2"), BASE + EPS).snapshot
    at = BASE + TIMEOUT + EPS
    ignored = evaluate(state, HealthTick(old), at)
    assert ignored.snapshot is state
    assert ignored.decision is HealthDecision.OBSOLETE_TIMER
    current = evaluate(state, HealthTick(timer_token(state)), at)
    assert current.snapshot.health_status is HealthStatus.STALE
    assert len(current.changes) == 1


def test_reference_reversal_for_observation_and_current_tick():
    state = healthy()
    state = evaluate(
        state, HealthTick(timer_token(state)), BASE + 2 * EPS,
    ).snapshot
    for item in (
        observation(BASE + EPS, "e2"), HealthTick(timer_token(state)),
    ):
        result = evaluate(state, item, BASE + EPS)
        assert result.decision is HealthDecision.REFERENCE_REVERSED
        assert result.snapshot is state
        assert not result.changes


@pytest.mark.parametrize("observation_first,expected", [
    (True, []), (False, [HealthStatus.STALE, HealthStatus.HEALTHY]),
])
def test_same_time_race_preserves_explicit_order(observation_first, expected):
    state = healthy()
    tick = HealthTick(timer_token(state))
    at = BASE + TIMEOUT
    sample = observation(at, "e2")
    items = (sample, tick) if observation_first else (tick, sample)
    changes = []
    for item in items:
        result = evaluate(state, item, at)
        changes.extend(result.changes)
        state = result.snapshot
    assert [change.current for change in changes] == expected
    assert state.health_status is HealthStatus.HEALTHY
    assert state.last_seen == at
    assert state.freshness_deadline == at + TIMEOUT


def test_recorded_sequence_replays_identical_snapshots_and_change_chain():
    initial = boot()
    initial_tick = HealthTick(timer_token(initial))
    first = evaluate(initial, observation()).snapshot
    first_tick = HealthTick(timer_token(first))
    sequence = [
        (observation(), BASE),
        (initial_tick, BASE + EPS),
        (observation(BASE + EPS, "bad", unit="hz"), BASE + EPS),
        (first_tick, BASE + TIMEOUT),
        (observation(BASE + TIMEOUT - EPS, "late"), BASE + TIMEOUT + EPS),
        (observation(), BASE + TIMEOUT + EPS),
    ]

    def run():
        state = reload_snapshot(initial)
        results = []
        for item, at in sequence:
            restored = reload_snapshot(state)
            result = reduce_health(
                restored, item, policy=POLICY, reference_time=at,
            )
            assert result == reduce_health(
                state, item, policy=POLICY, reference_time=at,
            )
            results.append(result)
            state = result.snapshot
        return results

    first_run = run()
    assert first_run == run()
    changes = [change for result in first_run for change in result.changes]
    assert [c.current for c in changes] == [
        HealthStatus.HEALTHY, HealthStatus.STALE, HealthStatus.HEALTHY,
    ]
    assert len({c.health_change_id for c in changes}) == 3
    assert [c.health_revision for c in changes] == [1, 2, 3]
    assert changes[1].previous_health_change_id == changes[0].health_change_id
    assert changes[2].previous_health_change_id == changes[1].health_change_id


@pytest.mark.parametrize("make_state", [boot, healthy, stale])
def test_recovery_rebuilds_current_token_without_resetting_epoch(make_state):
    state = reload_snapshot(make_state())
    result = evaluate(
        state, HealthTick(timer_token(state)), BASE + TIMEOUT + EPS,
    )
    assert result.snapshot.monitoring_epoch == state.monitoring_epoch
    assert result.snapshot.monitoring_started_at == BASE
    assert result.snapshot.health_status is HealthStatus.STALE
    assert len(result.changes) == (0 if state.health_status is (
        HealthStatus.STALE
    ) else 1)


def test_health_id_changes_with_policy_and_epoch():
    base = evaluate(boot(), observation()).changes[0].health_change_id
    changed_policy = replace(POLICY, freshness_timeout=2 * TIMEOUT)
    other = evaluate(
        boot(changed_policy), observation(), policy=changed_policy,
    ).changes[0].health_change_id
    epoch = evaluate(
        replace(boot(), monitoring_epoch="epoch-2"), observation(),
    ).changes[0].health_change_id
    assert len({base, other, epoch}) == 3
