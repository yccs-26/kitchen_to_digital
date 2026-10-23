"""Operational codec schema, type/policy checks and whole snapshot equality."""

from dataclasses import replace

import pytest

from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind,
)
from streaming.state.change_envelope import (
    _sealed, canonical, read_json, snapshot_bytes,
)
from streaming.state.operational_codec import (
    OperationalCodecError, decode_operational_snapshot,
    encode_operational_snapshot,
)
from streaming.state.registry import ProfileSnapshot
from test_change_envelope import BINDING, fryer_pair
from test_health import boot
from test_hood import POLICY, apply, event, initial


HOOD_BINDING = EquipmentBinding(
    "store-001", "hood-001", HOOD_PROFILE, InputKind.NUMERIC_TELEMETRY,
)


def hood_candidate():
    state = apply(initial(), event(0, 800)).state
    return ProfileSnapshot(HOOD_PROFILE, state)


def encode(snapshot, binding=HOOD_BINDING):
    return encode_operational_snapshot(snapshot, binding=binding)


def decode(payload, binding=HOOD_BINDING, policy=POLICY):
    return decode_operational_snapshot(payload, binding=binding,
                                       hood_policy=policy)


@pytest.mark.parametrize("pending", [False, True])
def test_hood_round_trip_preserves_all_fields_and_candidate(pending):
    snapshot = hood_candidate() if pending else ProfileSnapshot(
        HOOD_PROFILE, initial(),
    )
    payload = encode(snapshot)
    restored = decode(payload)
    assert restored == snapshot
    assert restored is not snapshot and restored.state is not snapshot.state
    assert encode(restored) == payload
    assert restored.state.receipts == snapshot.state.receipts
    assert restored.state.candidate_since == snapshot.state.candidate_since


def test_fryer_round_trip_keeps_independent_legacy_health_and_receipts():
    state = fryer_pair()[1].state
    snapshot = ProfileSnapshot(FRYER_REFERENCE, state)
    payload = encode(snapshot, BINDING)
    restored = decode(payload, BINDING, None)
    assert restored == snapshot
    assert restored.state.observation.health_status == (
        state.observation.health_status
    )
    assert restored.state.receipts[0].payload == state.receipts[0].payload
    assert encode(restored, BINDING) == payload


@pytest.mark.parametrize("field", [
    "codec_version", "snapshot_type", "binding", "engine_version",
    "policy_fingerprint", "state", "content_sha256",
])
def test_missing_fields_fail_closed(field):
    data = read_json(encode(hood_candidate()))
    data.pop(field)
    with pytest.raises(OperationalCodecError):
        decode(canonical(data))


@pytest.mark.parametrize("mutation", [
    "version", "engine", "type", "identity", "enum", "time", "candidate",
    "ledger-type", "ledger-id", "receipt-time", "policy-bool",
])
def test_resealed_but_inconsistent_data_is_rejected(mutation):
    data = read_json(encode(hood_candidate()))
    data.pop("content_sha256")
    state = data["state"]
    if mutation == "version":
        data["codec_version"] = "v2"
    elif mutation == "engine":
        data["engine_version"] = "different"
    elif mutation == "type":
        data["snapshot_type"] = "TwinState"
    elif mutation == "identity":
        state["equipment_id"] = "other"
    elif mutation == "enum":
        state["current"] = "COOKING"
    elif mutation == "time":
        state["last_event_time"] = "2026-10-17T00:00:00"
    elif mutation == "candidate":
        state["candidate_since"] = None
    elif mutation == "ledger-type":
        state["receipts"] = {}
    elif mutation == "ledger-id":
        state["receipts"].append(state["receipts"][0])
    elif mutation == "receipt-time":
        state["receipts"][0]["event_time"] = None
    else:
        state["policy"]["enter_duration"] = True
    with pytest.raises(OperationalCodecError):
        decode(_sealed(data))


@pytest.mark.parametrize("payload", [b'{}', b'bad', b'NaN', b'{"a":1,"a":2}'])
def test_bad_json_or_digest_does_not_initialize(payload):
    with pytest.raises(OperationalCodecError):
        decode(payload)


def test_different_policy_binding_profile_and_health_snapshot_are_rejected():
    payload = encode(hood_candidate())
    for binding in (replace(HOOD_BINDING, equipment_id="other"), replace(
        HOOD_BINDING, profile=replace(HOOD_PROFILE, version="2"),
    )):
        with pytest.raises(OperationalCodecError):
            decode(payload, binding)
    with pytest.raises(OperationalCodecError):
        decode(payload, policy=replace(POLICY, exit_rpm=100))
    with pytest.raises(OperationalCodecError):
        decode(snapshot_bytes(boot()))
    with pytest.raises(OperationalCodecError):
        decode(snapshot_bytes(hood_candidate().state))


def test_native_digest_damage_and_unknown_field_are_rejected():
    data = read_json(encode(hood_candidate()))
    data["state"]["current"] = "STOPPED"
    from streaming.state.change_envelope import canonical
    with pytest.raises(OperationalCodecError, match="DIGEST"):
        decode(canonical(data))
    data["unknown"] = None
    with pytest.raises(OperationalCodecError):
        decode(_sealed(data))


def test_equal_policy_numeric_representations_have_identical_bytes():
    snapshot = hood_candidate()
    same = replace(POLICY, enter_rpm=float(POLICY.enter_rpm),
                   exit_rpm=float(POLICY.exit_rpm))
    equivalent = ProfileSnapshot(HOOD_PROFILE, replace(snapshot.state,
                                                       policy=same))
    assert snapshot == equivalent
    assert encode(snapshot) == encode(equivalent)
    assert decode(encode(snapshot), policy=same) == equivalent
