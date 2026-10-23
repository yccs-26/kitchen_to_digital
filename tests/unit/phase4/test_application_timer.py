"""Clock-injected planning/ticks, native ordering and publication linkage."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.application_timer import LocalHealthTimers
from streaming.state.health_timer_coordinator import TimerStatus
from tests.fixtures.phase4.application_fixture import (
    BASE, TIMEOUT, bundle, numeric,
)


@pytest.fixture
def local(tmp_path):
    resource = bundle(tmp_path, initialize=True)
    yield resource
    resource.close()


def observe(local, second=0, command="obs"):
    event = numeric(second)
    return local.app.process_validated(event, command_ref=command,
                                       reference_time=event["event_time"])


def test_deadline_waiting_exact_stale_recovery_and_native_ids(local):
    observe(local)
    timer = LocalHealthTimers(local.app)
    request = timer.plan("hood-001", evaluation_time=BASE)
    assert request.status is TimerStatus.WAITING
    before = local.app.load_operational("hood-001")
    early = timer.evaluate("hood-001", request.token, command_ref="early",
                           reference_time=BASE + TIMEOUT - timedelta(
                               microseconds=1,
                           ))
    assert early.decision == "WAITING" and early.output_id is None
    stale = timer.evaluate("hood-001", request.token, command_ref="due",
                           reference_time=BASE + TIMEOUT)
    output = local.store.output(stale.output_id)
    assert output.change.data["body"]["current"] == "STALE"
    assert output.change.data["source_ref"]["input_kind"] == "tick"
    assert local.app.load_operational("hood-001") == before
    repeat = timer.evaluate("hood-001", request.token, command_ref="repeat",
                            reference_time=BASE + TIMEOUT)
    assert repeat.output_id is None
    recovery = observe(local, 10, "recovery")
    assert recovery[1].decision == "TRANSITION"
    local.app.reconcile(local.coordinator)
    assert local.current.get(("store-001", "hood-001", "HEALTH")).data[
        "current"
    ]["state"] == "HEALTHY"
    assert len([row for row in local.history.rows()
                if row.identity[0] == "HEALTH"]) == 3


@pytest.mark.parametrize("wrong", ["old", "store", "equipment", "profile"])
def test_obsolete_token_cannot_extend_or_stale_fresh_generation(local, wrong):
    observe(local)
    timer = LocalHealthTimers(local.app)
    token = timer.plan("hood-001", evaluation_time=BASE).token
    observe(local, 1, "new")
    if wrong == "store":
        token = replace(token, store_id="other")
    elif wrong == "equipment":
        token = replace(token, equipment_id="fryer-001")
    elif wrong == "profile":
        token = replace(token, profile=replace(token.profile, version="2"))
    before = local.app.load_health("hood-001")
    result = timer.evaluate("hood-001", token, command_ref="old",
                            reference_time=BASE + TIMEOUT)
    assert result.decision == "OBSOLETE_TIMER"
    assert result.output_id is None
    assert local.app.load_health("hood-001") == before


@pytest.mark.parametrize("observation_first", [False, True])
def test_same_time_race_preserves_command_order(local, observation_first):
    observe(local)
    timer = LocalHealthTimers(local.app)
    token = timer.plan("hood-001", evaluation_time=BASE).token
    if observation_first:
        observe(local, 10, "obs-first")
    tick = timer.evaluate("hood-001", token, command_ref="tick",
                          reference_time=BASE + TIMEOUT)
    if not observation_first:
        observe(local, 10, "obs-after")
    assert tick.decision == ("OBSOLETE_TIMER" if observation_first
                             else "TRANSITION")
    local.app.reconcile(local.coordinator)
    states = [row.data["body"]["current"] for row in local.history.rows()
              if row.identity[0] == "HEALTH"]
    assert states == (["HEALTHY"] if observation_first
                       else ["HEALTHY", "STALE", "HEALTHY"])


def test_restart_reconstructs_same_timer_and_cached_tick_id(tmp_path):
    local = bundle(tmp_path, initialize=True)
    observe(local)
    timer = LocalHealthTimers(local.app)
    plan = timer.plan("hood-001", evaluation_time=BASE)
    result = timer.evaluate("hood-001", plan.token, command_ref="tick",
                            reference_time=BASE + TIMEOUT)
    local.close()
    local = bundle(tmp_path)
    restored = LocalHealthTimers(local.app)
    assert restored.plan("hood-001", evaluation_time=BASE + TIMEOUT).token == (
        plan.token
    )
    plan = restored.plan("hood-001", evaluation_time=BASE + TIMEOUT)
    assert plan.status == (
        TimerStatus.ALREADY_EVALUATED
    )
    replay = restored.evaluate("hood-001", plan.token, command_ref="tick",
                               reference_time=BASE + TIMEOUT)
    assert replay.output_id == result.output_id and replay.replayed
    local.app.reconcile(local.coordinator)
    assert len(local.history.rows()) == 2
    local.close()


def test_reversed_reference_time_does_not_change_snapshot(local):
    observe(local, 1)
    timer = LocalHealthTimers(local.app)
    token = timer.plan("hood-001", evaluation_time=BASE + TIMEOUT).token
    before = local.app.load_health("hood-001")
    result = timer.evaluate("hood-001", token, command_ref="reverse",
                            reference_time=BASE)
    assert result.decision == "REFERENCE_REVERSED"
    assert result.output_id is None
    assert local.app.load_health("hood-001") == before


def test_unobserved_boot_timeout_does_not_invent_previous_healthy(local):
    timer = LocalHealthTimers(local.app)
    plan = timer.plan("fryer-001", evaluation_time=BASE + TIMEOUT)
    result = timer.evaluate("fryer-001", plan.token, command_ref="boot-due",
                            reference_time=BASE + TIMEOUT)
    change = local.store.output(result.output_id).change.data
    assert change["record_kind"] == "INITIALIZED"
    assert change["body"]["previous"] is None
    assert change["body"]["current"] == "STALE"
    assert change["body"]["last_seen"] is None
