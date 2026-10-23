"""Native Core results → deterministic immutable output; no external sinks."""

from dataclasses import replace
from datetime import timedelta, timezone

import pytest

from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind,
)
from streaming.state.change_envelope import (
    ChangeEnvelope, CurrentProjection, OutputConflict, OutputContext,
    StateOutput, canonical, digest, health_output, operational_output,
    read_json, snapshot_bytes, stamp,
)
from streaming.state.contracts import (
    HealthStatus, ModeSignal, OperationalState, ReportedMode, StateObservation,
    SyntheticSignal,
)
from streaming.state.fsm import TwinState, transition
from streaming.state.health import HealthTick, timer_token
from streaming.state.health_codec import decode_health_snapshot
from test_health import BASE, EPS, POLICY, TIMEOUT, boot, evaluate, observation
from test_hood import POLICY as HOOD_POLICY
from streaming.state.hood import (
    HoodRpmEvent, HoodSnapshot, HoodState, qualify_rpm,
)


BINDING = EquipmentBinding(
    "store-001", "fryer-001", FRYER_REFERENCE, InputKind.SYNTHETIC_SIGNAL,
)


def fryer_pair(command="op-1", signal_id="s1", change_revision=1):
    before = TwinState(
        BINDING.store_id, BINDING.equipment_id,
        StateObservation(OperationalState.IDLE, HealthStatus.HEALTHY),
    )
    sample = SyntheticSignal(
        signal_id, BASE, BINDING.store_id, BINDING.equipment_id,
        "fixture-controller", ModeSignal(ReportedMode.PREHEATING),
        "synthetic_test_only",
    )
    result = transition(before, sample, reference_time=BASE)
    context = OutputContext(command, 1, change_revision, change_revision)
    output = operational_output(
        before, result, sample, binding=BINDING, context=context,
        reference_time=BASE,
    )
    return before, result, sample, context, output


def health_pair(before=None, sample=None, at=BASE, command="health-1"):
    before = boot() if before is None else before
    sample = observation() if sample is None else sample
    result = evaluate(before, sample, at)
    context = OutputContext(
        command, 1, result.snapshot.health_revision,
        result.snapshot.snapshot_revision, before.last_health_change_id,
    )
    output = health_output(
        before, result, sample, context=context, reference_time=at,
    )
    return before, result, sample, context, output


def test_fryer_preserves_identity_lineage_and_deterministic_bytes():
    before, result, event, context, out = fryer_pair()
    again = operational_output(
        before, result, event, binding=BINDING, context=context,
        reference_time=BASE.astimezone(timezone(timedelta(hours=9))),
    )
    assert out == again
    assert out.change.identity == (
        "OPERATIONAL", result.intents[0].state_change_id,
    )
    data = out.change.data
    assert data["source_ref"]["signal_id"] == event.signal_id
    assert data["source_ref"]["event_id"] is None
    assert data["source_ref"]["provenance"] == "synthetic_test_only"
    assert data["body"] == {"previous": "IDLE", "current": "PREHEATING"}
    assert ChangeEnvelope(out.change.payload) == out.change
    assert read_json(out.change.payload) == data
    data["body"]["current"] = "COOKING"
    assert out.change.data["body"]["current"] == "PREHEATING"


def test_health_replay_via_existing_codec_has_same_id_payload_and_projection():
    before, result, event, context, out = health_pair()
    restored = decode_health_snapshot(snapshot_bytes(before), policy=POLICY)
    again = health_output(
        restored, evaluate(restored, event), event,
        context=context, reference_time=BASE,
    )
    assert out == again
    assert out.change.identity == (
        "HEALTH", result.changes[0].health_change_id,
    )
    assert out.change.data["record_kind"] == "INITIALIZED"
    assert out.change.data["body"]["previous"] is None
    assert out.change.data["policy"]["fingerprint"] == POLICY.fingerprint
    assert CurrentProjection(out.projection.payload) == out.projection


def test_timeout_lineage_and_first_committed_evaluation_must_be_retained():
    state = health_pair()[1].snapshot
    item = HealthTick(timer_token(state))
    exact = health_pair(state, item, BASE + TIMEOUT, "tick")[4]
    late = health_pair(state, item, BASE + 2 * TIMEOUT, "tick")[4]
    assert exact.change.identity == late.change.identity
    assert exact.change.payload != late.change.payload
    assert exact.change.data["effective_at"] == stamp(BASE + TIMEOUT)
    assert exact.change.data["source_ref"]["input_kind"] == "tick"
    assert exact.change.data["body"]["timer_token"] == (
        late.change.data["body"]["timer_token"]
    )


def test_noop_freshness_is_current_only_and_duplicate_has_no_output():
    before = health_pair()[1].snapshot
    pair = health_pair(before, observation(BASE + EPS, "e2"), BASE + EPS)
    assert pair[4].change is None
    assert pair[4].projection.data["current"]["last_seen"] == stamp(BASE + EPS)
    assert pair[4].projection.data["change_revision"] == before.health_revision
    assert pair[4].projection.data["change_id"] == before.last_health_change_id
    result = evaluate(before, observation())
    assert health_output(
        before, result, observation(), context=OutputContext("dup", 1, 1, 1),
        reference_time=BASE,
    ) is None


def test_operational_duplicate_has_no_envelope_or_projection():
    _, result, event, context, _ = fryer_pair()
    duplicate = transition(result.state, event, reference_time=BASE)
    assert operational_output(
        result.state, duplicate, event, binding=BINDING, context=context,
        reference_time=BASE,
    ) is None


def test_hood_initialization_has_no_transition_and_later_id_is_native():
    binding = EquipmentBinding(
        "store-001", "hood-001", HOOD_PROFILE, InputKind.NUMERIC_TELEMETRY,
    )
    for initial in (None, HoodState.STOPPED):
        state = HoodSnapshot("store-001", "hood-001", HOOD_POLICY,
                             current=initial)
        for index, second in enumerate((0, 1, 3), 1):
            event = HoodRpmEvent(
                f"e{index}", BASE + timedelta(seconds=second),
                "store-001", "hood-001", "hood", "fan_rpm", 600,
                "rpm", "1.0.0", "fixture",
            )
            result = qualify_rpm(state, event, reference_time=event.event_time)
            out = operational_output(
                state, result, event, binding=binding,
                context=OutputContext(f"c{index}", 1, 1, index),
                reference_time=event.event_time,
            )
            assert not out.change if index < 3 or initial is None else (
                out.change.identity[1] == result.intents[0].state_change_id
            )
            state = result.state
        image = read_json(snapshot_bytes(state))
        assert len(image["state"]["receipts"]) == 3
        assert image["state"]["current"] == "ROTATING"


@pytest.mark.parametrize("bad", [True, -1, 1.0, "1"])
def test_revision_types_are_not_coerced(bad):
    with pytest.raises(ValueError):
        OutputContext("command", bad, 1, 1)


@pytest.mark.parametrize("payload", [b'{}', b'{"a":1,"a":1}', b'NaN'])
def test_corrupt_or_wrong_schema_is_rejected(payload):
    with pytest.raises((ValueError, TypeError)):
        ChangeEnvelope(payload)


def test_digest_corruption_and_mixed_output_are_rejected():
    op = fryer_pair()[4]
    health = health_pair()[4]
    with pytest.raises(OutputConflict):
        StateOutput(op.change, health.projection)
    data = op.change.data
    data["body"]["current"] = "COOKING"
    with pytest.raises(ValueError, match="digest"):
        ChangeEnvelope(canonical(data))


def test_bad_time_context_and_health_revision_are_rejected():
    before, result, event, context, _ = health_pair()
    with pytest.raises(ValueError):
        stamp(BASE.replace(tzinfo=None))
    with pytest.raises(OutputConflict):
        health_output(
            before, result, event, context=replace(context, change_revision=2),
            reference_time=BASE,
        )


def test_nonfinite_unsupported_mutable_values_are_rejected():
    for item in (float("nan"), float("inf"), {1: "bad"}, object()):
        with pytest.raises((ValueError, TypeError)):
            canonical(item)
    assert canonical(True) != canonical(1)
    assert digest(b"x") != digest(b"y")


def test_adapter_rejects_foreign_source_or_wrong_native_trigger():
    pair = health_pair()
    with pytest.raises(OutputConflict, match="source"):
        health_output(
            pair[0], pair[1], replace(pair[2], equipment_id="other"),
            context=pair[3], reference_time=BASE,
        )
    with pytest.raises(OutputConflict, match="cause"):
        health_output(
            pair[0], pair[1], replace(pair[2], event_id="other"),
            context=pair[3], reference_time=BASE,
        )
    before, result, event, context, _ = fryer_pair()
    with pytest.raises(OutputConflict, match="lineage"):
        operational_output(
            before, result, replace(event, signal_id="other"),
            binding=BINDING, context=context, reference_time=BASE,
        )


def test_envelope_rejects_even_resealed_unsupported_state_and_version():
    from streaming.state.change_envelope import _sealed

    original = health_pair()[4].change.data
    for name in ("current", "version", "equipment"):
        data = read_json(canonical(original))
        data.pop("content_sha256")
        if name == "current":
            data["body"]["current"] = "FAULT"
        elif name == "version":
            data["version"] = "state-output-v2"
        else:
            data["equipment"]["equipment_type"] = "freezer"
            data["profile"]["equipment_type"] = "freezer"
        with pytest.raises(ValueError):
            ChangeEnvelope(_sealed(data))
