"""Explicit versioned Operational snapshot codec; no migration or I/O."""

from dataclasses import fields
from datetime import datetime

from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind,
)
from streaming.state.change_envelope import (
    _sealed, canonical, digest, read_json, stamp, wire,
)
from streaming.state.contracts import (
    CleaningCompletedSignal, CleaningSignal, FaultSignal, HealthStatus,
    ModeSignal, OperationalState, ReportedMode, StateObservation,
    SyntheticSignal,
)
from streaming.state.fsm import POLICY_VERSION as FRYER_VERSION, TwinState
from streaming.state.hood import (
    POLICY_VERSION as HOOD_VERSION, HoodPolicy, HoodRpmEvent,
    HoodSnapshot, HoodState,
)
from streaming.state.registry import ProfileSnapshot


CODEC_VERSION = "operational-snapshot-v1"


class OperationalCodecError(ValueError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


def _shape(value, names):
    if type(value) is not dict or set(value) != set(names):
        raise OperationalCodecError("INVALID_SCHEMA")
    return value


def _time(value, nullable=False):
    if value is None and nullable:
        return None
    if type(value) is not str:
        raise OperationalCodecError("INVALID_TIMESTAMP")
    try:
        parsed = datetime.fromisoformat(value)
        if stamp(parsed) != value:
            raise ValueError("noncanonical UTC")
        return parsed
    except (ValueError, OverflowError) as exc:
        raise OperationalCodecError("INVALID_TIMESTAMP") from exc


def _bool(value):
    if type(value) is not bool:
        raise OperationalCodecError("INVALID_BOOLEAN")
    return value


def _signal(event):
    data = wire(event)
    payload = event.payload
    kind = {
        ModeSignal: "MODE", CleaningSignal: "CLEANING",
        CleaningCompletedSignal: "CLEANING_COMPLETED", FaultSignal: "FAULT",
    }[type(payload)]
    data["payload"] = {"kind": kind, **wire(payload)}
    return data


def _read_signal(data):
    _shape(data, (field.name for field in fields(SyntheticSignal)))
    body = data["payload"]
    if type(body) is not dict:
        raise OperationalCodecError("INVALID_SIGNAL")
    kind = body.get("kind")
    if kind == "MODE":
        _shape(body, ("kind", "mode"))
        payload = ModeSignal(ReportedMode(body["mode"]))
    elif kind == "CLEANING":
        _shape(body, ("kind", "active"))
        payload = CleaningSignal(_bool(body["active"]))
    elif kind == "CLEANING_COMPLETED":
        _shape(body, ("kind",))
        payload = CleaningCompletedSignal()
    elif kind == "FAULT":
        _shape(body, ("kind", "code", "active"))
        payload = FaultSignal(body["code"], _bool(body["active"]))
    else:
        raise OperationalCodecError("UNSUPPORTED_SIGNAL")
    return SyntheticSignal(**{
        **data, "event_time": _time(data["event_time"]), "payload": payload,
    })


def _policy_data(policy):
    return {**wire(policy), "enter_rpm": float(policy.enter_rpm),
            "exit_rpm": float(policy.exit_rpm)}


def _context(binding, hood_policy):
    if not isinstance(binding, EquipmentBinding):
        raise OperationalCodecError("INVALID_BINDING")
    if binding.profile == FRYER_REFERENCE and binding.input_kind is (
        InputKind.SYNTHETIC_SIGNAL
    ):
        if hood_policy is not None:
            raise OperationalCodecError("POLICY_MISMATCH")
        return TwinState, FRYER_VERSION, None
    if binding.profile == HOOD_PROFILE and binding.input_kind is (
        InputKind.NUMERIC_TELEMETRY
    ) and isinstance(hood_policy, HoodPolicy):
        return HoodSnapshot, HOOD_VERSION, _policy_data(hood_policy)
    raise OperationalCodecError("UNSUPPORTED_PROFILE_OR_POLICY")


def encode_operational_snapshot(snapshot, *, binding):
    """Preserve profile, binding, engine, ledger and candidate fields."""
    if not isinstance(snapshot, ProfileSnapshot):
        raise OperationalCodecError("INVALID_SNAPSHOT_TYPE")
    state = snapshot.state
    policy = state.policy if isinstance(state, HoodSnapshot) else None
    expected, engine, policy_data = _context(binding, policy)
    if snapshot.profile != binding.profile or type(state) is not expected:
        raise OperationalCodecError("PROFILE_OR_TYPE_MISMATCH")
    if (state.store_id, state.equipment_id) != (
        binding.store_id, binding.equipment_id,
    ):
        raise OperationalCodecError("IDENTITY_MISMATCH")
    data = wire(state)
    if isinstance(state, TwinState):
        data["receipts"] = [_signal(event) for event in state.receipts]
    else:
        data["policy"] = policy_data
    return _sealed({
        "codec_version": CODEC_VERSION, "snapshot_type": expected.__name__,
        "binding": wire(binding), "engine_version": engine,
        "policy_fingerprint": digest(canonical(policy_data)), "state": data,
    })


def decode_operational_snapshot(payload, *, binding, hood_policy=None):
    """Expected context is supplied by caller, never selected from payload."""
    try:
        data = read_json(payload)
        _shape(data, (
            "codec_version", "snapshot_type", "binding", "engine_version",
            "policy_fingerprint", "state", "content_sha256",
        ))
        if data["codec_version"] != CODEC_VERSION:
            raise OperationalCodecError("CODEC_VERSION_MISMATCH")
        body = {key: value for key, value in data.items()
                if key != "content_sha256"}
        if digest(canonical(body)) != data["content_sha256"]:
            raise OperationalCodecError("CONTENT_DIGEST_MISMATCH")
        expected, engine, policy_data = _context(binding, hood_policy)
        if data["binding"] != wire(binding):
            raise OperationalCodecError("BINDING_PROFILE_MISMATCH")
        if data["snapshot_type"] != expected.__name__ or (
            data["engine_version"] != engine
        ):
            raise OperationalCodecError("ENGINE_TYPE_MISMATCH")
        if data["policy_fingerprint"] != digest(canonical(policy_data)):
            raise OperationalCodecError("POLICY_MISMATCH")
        state = data["state"]
        _shape(state, (field.name for field in fields(expected)))
        if type(state["receipts"]) is not list:
            raise OperationalCodecError("INVALID_LEDGER")
        if (state["store_id"], state["equipment_id"]) != (
            binding.store_id, binding.equipment_id,
        ):
            raise OperationalCodecError("IDENTITY_MISMATCH")
        values = {**state, "last_event_time": _time(
            state["last_event_time"], nullable=True,
        )}
        if expected is TwinState:
            observation = _shape(state["observation"], (
                "operational_state", "health_status",
            ))
            values["observation"] = StateObservation(
                OperationalState(observation["operational_state"]),
                HealthStatus(observation["health_status"]),
            )
            values["receipts"] = tuple(
                _read_signal(item) for item in state["receipts"]
            )
        else:
            if state["policy"] != policy_data:
                raise OperationalCodecError("POLICY_MISMATCH")
            # Compare policy primitives strictly; bool must not equal 1.
            if canonical(state["policy"]) != canonical(policy_data):
                raise OperationalCodecError("POLICY_MISMATCH")
            values.update(
                policy=hood_policy,
                current=(HoodState(state["current"]) if state["current"]
                         is not None else None),
                candidate=(HoodState(state["candidate"]) if state["candidate"]
                           is not None else None),
                candidate_since=_time(state["candidate_since"], nullable=True),
            )
            receipts = []
            for item in state["receipts"]:
                _shape(item, (field.name for field in fields(HoodRpmEvent)))
                receipts.append(HoodRpmEvent(**{
                    **item, "event_time": _time(item["event_time"]),
                }))
            values["receipts"] = tuple(receipts)
        ledger = values["receipts"]
        if any(left.event_time >= right.event_time
               for left, right in zip(ledger, ledger[1:])):
            raise OperationalCodecError("INVALID_LEDGER_ORDER")
        return ProfileSnapshot(binding.profile, expected(**values))
    except OperationalCodecError:
        raise
    except (ValueError, TypeError, KeyError, OverflowError) as exc:
        raise OperationalCodecError("INVALID_SNAPSHOT") from exc
