"""Validated event → existing registries/core → atomic local publication."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.application import ApplicationContractError
from streaming.state.bindings import HOOD_PROFILE
from streaming.state.change_envelope import OutputConflict, snapshot_bytes
from streaming.state.contracts import (
    CleaningCompletedSignal, CleaningSignal, FaultSignal, ModeSignal,
    ReportedMode,
)
from streaming.state.operational_codec import OperationalCodecError
from tests.fixtures.phase4.application_fixture import (
    BASE, FRYER, HOOD, bundle, health_policy, numeric, signal,
)


@pytest.fixture
def local(tmp_path):
    result = bundle(tmp_path, initialize=True)
    yield result
    result.close()


def process(local, event=None, command="event", **options):
    event = numeric() if event is None else event
    return local.app.process_validated(
        event, command_ref=command, reference_time=event["event_time"],
        **options,
    )


def test_hood_axes_are_independent_and_candidate_is_current_only(local):
    result = process(local)
    assert [item.decision for item in result] == ["PENDING", "INITIALIZED"]
    op = local.app.load_operational("hood-001")
    health = local.app.load_health("hood-001")
    assert op.state.current.value == "STOPPED"
    assert op.state.candidate.value == "ROTATING"
    assert health.health_status.value == "HEALTHY"
    outputs = [local.store.output(item.output_id) for item in result]
    assert outputs[0].change is None
    assert outputs[1].change.identity[0] == "HEALTH"
    local.app.reconcile(local.coordinator)
    assert len(local.history.rows()) == 1
    assert local.current.get(("store-001", "hood-001", "OPERATIONAL"))


def test_fryer_numeric_event_is_health_only(local):
    before = local.app.load_operational("fryer-001")
    result = process(local, numeric(0, 180, "fryer-001"))
    assert len(result) == 1 and result[0].axis_key[2] == "HEALTH"
    assert local.app.load_operational("fryer-001") == before
    root = local.store.state(("store-001", "fryer-001", "OPERATIONAL"))
    assert root.revision == 0


def test_fryer_signals_follow_native_cycle_and_preserve_health(local):
    health = local.app.load_health("fryer-001")
    for second, payload, expected in (
        (0, ModeSignal(ReportedMode.PREHEATING), "PREHEATING"),
        (1, ModeSignal(ReportedMode.COOKING), "COOKING"),
        (2, CleaningSignal(True), "CLEANING"),
        (3, CleaningCompletedSignal(), "IDLE"),
    ):
        event = signal(second, payload)
        result = local.app.process_signal(
            event, equipment_type="fryer", command_ref=f"s{second}",
            reference_time=event.event_time,
        )
        assert result.decision == "TRANSITION"
        snapshot = local.app.load_operational("fryer-001")
        assert snapshot.state.observation.operational_state.value == expected
    assert local.app.load_health("fryer-001") == health
    local.app.reconcile(local.coordinator)
    assert len(local.history.rows()) == 4


@pytest.mark.parametrize("changes", [
    {"metric_name": "temperature"}, {"unit": "hz"}, {"source": "other"},
    {"schema_version": "2"}, {"store_id": "other"},
    {"equipment_id": "unknown"}, {"equipment_type": "fryer"},
    {"metric_value": True}, {"metric_value": float("nan")},
    {"metric_value": "800"}, {"event_time": "2026-10-23T00:00:00"},
    {"extra": "unexpected"},
])
def test_bad_input_preserves_snapshots_and_freshness(local, changes):
    roots = {axis: local.store.state(("store-001", "hood-001", axis))
             for axis in ("OPERATIONAL", "HEALTH")}
    with pytest.raises((ValueError, TypeError)):
        local.app.process_validated(
            {**numeric(), **changes}, command_ref="bad", reference_time=BASE,
        )
    assert local.store.pending() == ()
    for axis, before in roots.items():
        assert local.store.state(before.axis_key) == before


@pytest.mark.parametrize("kind", ["operational_profile", "health_profile"])
def test_profile_version_mismatch_is_rejected_before_any_commit(local, kind):
    profile = HOOD_PROFILE if kind == "operational_profile" else (
        health_policy(HOOD).profile
    )
    with pytest.raises(ValueError):
        process(local, **{kind: replace(profile, version="2")})
    assert local.store.pending() == ()


@pytest.mark.parametrize("case,expected", [
    ("duplicate", "DUPLICATE"), ("conflict", "ID_CONFLICT"),
    ("same_time", "SAME_TIME"), ("older", "OUT_OF_ORDER"),
])
def test_existing_core_ordering_decisions_are_not_reimplemented(local, case,
                                                              expected):
    first = numeric(1)
    process(local, first, "first")
    before = (local.app.load_operational("hood-001"),
              local.app.load_health("hood-001"))
    event = dict(first)
    if case == "conflict":
        event["metric_value"] = 801
    elif case == "same_time":
        event["event_id"] = "same-time-other"
    elif case == "older":
        event = numeric(0)
    result = local.app.process_validated(
        event, command_ref="retry", reference_time=BASE + timedelta(seconds=2),
    )
    assert {item.decision for item in result} == {expected}
    assert all(item.output_id is None for item in result)
    assert before == (local.app.load_operational("hood-001"),
                      local.app.load_health("hood-001"))


def test_hood_transition_and_current_only_updates_use_registry(local):
    for second in (0, 1, 3):
        result = process(local, numeric(second), f"e{second}")
    assert result[0].decision == "TRANSITION"
    native_id = local.store.output(result[0].output_id).change.identity[1]
    assert local.app.load_operational("hood-001").state.current.value == (
        "ROTATING"
    )
    assert len(native_id) == 64
    assert result[1].decision == "NO_CHANGE"
    assert local.store.output(result[1].output_id).change is None
    local.app.reconcile(local.coordinator)
    assert len(local.history.rows()) == 2


def test_recorded_command_replay_returns_original_id_without_rollback(local):
    first = process(local, numeric(), "same")
    process(local, numeric(1), "next")
    roots = {axis: local.store.state(("store-001", "hood-001", axis))
             for axis in ("OPERATIONAL", "HEALTH")}
    replay = process(local, numeric(), "same")
    assert [item.output_id for item in replay] == [
        item.output_id for item in first
    ]
    assert all(item.replayed for item in replay)
    for before in roots.values():
        assert local.store.state(before.axis_key) == before
    with pytest.raises(OutputConflict):
        process(local, numeric(value=801), "same")


def test_unsupported_fault_signal_is_not_health_authority(local):
    with pytest.raises(ApplicationContractError):
        local.app.process_signal(signal(payload=FaultSignal("fault", True)),
                                 equipment_type="fryer", command_ref="fault",
                                 reference_time=BASE)
    assert local.store.pending() == ()


def test_legacy_opaque_operational_snapshot_is_not_silently_migrated(tmp_path):
    local = bundle(tmp_path)
    from streaming.state.hood import HoodSnapshot
    from tests.fixtures.phase4.application_fixture import HOOD_POLICY
    state = HoodSnapshot("store-001", "hood-001", HOOD_POLICY)
    local.store.initialize(("store-001", "hood-001", "OPERATIONAL"), 1,
                           snapshot_bytes(state))
    with pytest.raises(OperationalCodecError):
        local.app.load_operational("hood-001")
    assert local.store.pending() == ()
    local.close()


@pytest.mark.parametrize("representation", ["offset_string", "double"])
def test_validated_wire_normalization_keeps_recorded_command_identity(
    local, representation,
):
    first = process(local, command="canonical")
    event = numeric()
    if representation == "offset_string":
        event["event_time"] = "2026-10-23T09:00:00+09:00"
    else:
        event["metric_value"] = 800.0
    replay = local.app.process_validated(event, command_ref="canonical",
                                         reference_time=BASE)
    assert [item.output_id for item in replay] == [
        item.output_id for item in first
    ]
    assert all(item.replayed for item in replay)


def test_changed_health_policy_refuses_existing_snapshot_without_migration(
    tmp_path,
):
    local = bundle(tmp_path, initialize=True)
    process(local)
    roots = {axis: local.store.state(("store-001", "hood-001", axis))
             for axis in ("OPERATIONAL", "HEALTH")}
    local.close()
    changed = replace(health_policy(HOOD),
                      freshness_timeout=timedelta(seconds=20))
    local = bundle(tmp_path, policies=(health_policy(FRYER), changed))
    with pytest.raises(ValueError):
        process(local, numeric(1), "incompatible")
    for root in roots.values():
        assert local.store.state(root.axis_key) == root
    local.close()
