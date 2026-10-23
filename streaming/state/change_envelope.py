"""Pure local output envelopes without I/O or transition logic."""

from dataclasses import dataclass, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import hashlib
import json

from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind,
)
from streaming.state.fsm import FsmResult, TwinState
from streaming.state.health import (
    HealthObservation, HealthResult, HealthSnapshot, HealthTick, timer_token,
)
from streaming.state.health_codec import encode_health_snapshot
from streaming.state.hood import HoodResult, HoodRpmEvent, HoodSnapshot
from streaming.state.contracts import SyntheticSignal


class OutputConflict(ValueError):
    """An identity, revision or payload conflicts with canonical data."""


def text(value, name="text"):
    if type(value) is not str or not value.strip():
        raise ValueError(f"{name} must be nonblank text")
    return value


def revision(value, minimum=0):
    if type(value) is not int or value < minimum:
        raise ValueError("invalid integer revision")
    return value


def stamp(value):
    if not isinstance(value, datetime) or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(
        timespec="microseconds",
    ).replace("+00:00", "Z")


def wire(value):
    """JSON primitives, also used for opaque local snapshot images."""
    if isinstance(value, datetime):
        return stamp(value)
    if isinstance(value, timedelta):
        return value // timedelta(microseconds=1)
    if isinstance(value, Enum):
        return value.value
    if is_dataclass(value) and not isinstance(value, type):
        return {field.name: wire(getattr(value, field.name))
                for field in fields(value)}
    if type(value) is dict:
        if any(type(key) is not str for key in value):
            raise ValueError("JSON object keys must be strings")
        return {key: wire(item) for key, item in value.items()}
    if type(value) in (tuple, list):
        return [wire(item) for item in value]
    if value is None or type(value) in (str, bool, int, float):
        return value
    raise ValueError("unsupported JSON value")


def canonical(value):
    return json.dumps(
        wire(value), sort_keys=True, ensure_ascii=False,
        separators=(",", ":"), allow_nan=False,
    ).encode("utf-8")


def digest(payload):
    if type(payload) is not bytes:
        raise TypeError("payload must be bytes")
    return hashlib.sha256(payload).hexdigest()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def read_json(payload):
    if type(payload) is not bytes:
        raise TypeError("payload must be bytes")
    value = json.loads(payload.decode("utf-8"), object_pairs_hook=_pairs)
    if canonical(value) != payload:
        raise ValueError("noncanonical or nonfinite JSON")
    return value


def snapshot_bytes(snapshot):
    """Health reuses its Codec; Operational images remain opaque JSON bytes.

    Operational typed restoration/migration is intentionally not a new codec.
    These bytes preserve every dataclass field, including candidates/receipts.
    """
    if isinstance(snapshot, HealthSnapshot):
        return encode_health_snapshot(snapshot)
    if not isinstance(snapshot, (TwinState, HoodSnapshot)):
        raise TypeError("unsupported snapshot")
    return canonical({"snapshot_type": type(snapshot).__name__,
                      "state": snapshot})


def _sealed(data):
    body = wire(data)
    return canonical({**body, "content_sha256": digest(canonical(body))})


_COMMON = {
    "version", "axis", "equipment", "profile", "policy", "generation",
    "change_revision", "command_ref", "previous_change_id", "content_sha256",
}


def _validated(payload, version, extra):
    data = read_json(payload)
    if type(data) is not dict or set(data) != _COMMON | extra:
        raise ValueError("invalid output schema")
    if data["version"] != version or data["axis"] not in (
        "OPERATIONAL", "HEALTH",
    ):
        raise ValueError("unsupported output version/axis")
    body = {key: value for key, value in data.items()
            if key != "content_sha256"}
    if digest(canonical(body)) != data["content_sha256"]:
        raise ValueError("corrupt output digest")
    equipment = data["equipment"]
    if type(equipment) is not dict or set(equipment) != {
        "store_id", "equipment_id", "equipment_type",
    }:
        raise ValueError("invalid equipment identity")
    for value in equipment.values():
        text(value)
    profile = data["profile"]
    if type(profile) is not dict or set(profile) != {
        "equipment_type", "profile_id", "version",
    }:
        raise ValueError("invalid profile")
    for value in profile.values():
        text(value)
    if equipment["equipment_type"] not in ("fryer", "hood"):
        raise ValueError("unsupported equipment output")
    if profile["equipment_type"] != equipment["equipment_type"]:
        raise ValueError("profile/equipment mismatch")
    revision(data["generation"], 1)
    revision(data["change_revision"])
    text(data["command_ref"])
    if data["previous_change_id"] is not None:
        text(data["previous_change_id"])
    if type(data["policy"]) is not dict or not data["policy"]:
        raise ValueError("policy context is required")
    return data


@dataclass(frozen=True)
class ChangeEnvelope:
    payload: bytes

    def __post_init__(self):
        raw = read_json(self.payload)
        field = ("state_change_id" if type(raw) is dict and raw.get(
            "axis",
        ) == "OPERATIONAL" else "health_change_id")
        data = _validated(self.payload, "state-output-v1", {
            field, "record_kind", "effective_at", "evaluation_time",
            "source_ref", "origin_ref", "body",
        })
        text(data[field])
        if len(data[field]) != 64 or any(
            char not in "0123456789abcdef" for char in data[field]
        ):
            raise ValueError("native ID must be an existing SHA-256 hex ID")
        revision(data["change_revision"], 1)
        for key in ("effective_at", "evaluation_time"):
            if stamp(datetime.fromisoformat(data[key])) != data[key]:
                raise ValueError("noncanonical timestamp")
        if data["record_kind"] not in ("TRANSITION", "INITIALIZED"):
            raise ValueError("invalid record kind")
        if data["axis"] == "OPERATIONAL" and (
            data["record_kind"] != "TRANSITION"
        ):
            raise ValueError("no synthetic Operational initialization")
        if type(data["body"]) is not dict or (
            type(data["source_ref"]) is not dict
        ):
            raise ValueError("invalid output body/lineage")
        allowed = ("HEALTHY", "STALE") if data["axis"] == "HEALTH" else (
            ("IDLE", "PREHEATING", "COOKING", "CLEANING")
            if data["equipment"]["equipment_type"] == "fryer"
            else ("STOPPED", "ROTATING")
        )
        previous = data["body"].get("previous")
        current = data["body"].get("current")
        if current not in allowed or (
            previous is not None and previous not in allowed
        ) or previous == current or (
            data["record_kind"] == "INITIALIZED" and previous is not None
        ) or (data["record_kind"] == "TRANSITION" and previous is None):
            raise ValueError("invalid previous/current state")

    @property
    def data(self):
        return read_json(self.payload)

    @property
    def identity(self):
        data = self.data
        field = ("state_change_id" if data["axis"] == "OPERATIONAL"
                 else "health_change_id")
        return data["axis"], data[field]

    @property
    def sha256(self):
        return digest(self.payload)

    @property
    def axis_key(self):
        data = self.data
        return (*[data["equipment"][key]
                  for key in ("store_id", "equipment_id")], data["axis"])


@dataclass(frozen=True)
class CurrentProjection:
    payload: bytes

    def __post_init__(self):
        data = _validated(self.payload, "current-projection-v1", {
            "projection_revision", "change_id", "current",
        })
        revision(data["projection_revision"], 1)
        if type(data["current"]) is not dict:
            raise ValueError("invalid current projection")
        if data["change_id"] is not None:
            text(data["change_id"])

    @property
    def data(self):
        return read_json(self.payload)

    @property
    def identity(self):
        return self.data["content_sha256"]

    @property
    def sha256(self):
        return digest(self.payload)

    @property
    def axis_key(self):
        data = self.data
        return (data["equipment"]["store_id"],
                data["equipment"]["equipment_id"], data["axis"])


@dataclass(frozen=True)
class OutputContext:
    command_ref: str
    generation: int
    change_revision: int
    projection_revision: int
    previous_change_id: str | None = None

    def __post_init__(self):
        text(self.command_ref)
        revision(self.generation, 1)
        revision(self.change_revision)
        revision(self.projection_revision, 1)
        if self.previous_change_id is not None:
            text(self.previous_change_id)


@dataclass(frozen=True)
class StateOutput:
    change: ChangeEnvelope | None
    projection: CurrentProjection

    def __post_init__(self):
        if not isinstance(self.projection, CurrentProjection):
            raise TypeError("projection required")
        if self.change is not None:
            if not isinstance(self.change, ChangeEnvelope):
                raise TypeError("invalid change")
            change, projection = self.change.data, self.projection.data
            common = _COMMON - {"content_sha256", "version"}
            if any(change[key] != projection[key] for key in common) or (
                self.change.identity[1] != projection["change_id"]
                or change["body"]["current"] != (
                    projection["current"]["state"]
                )
            ):
                raise OutputConflict("change/projection context mismatch")

    @property
    def identity(self):
        if self.change is not None:
            return ":".join(self.change.identity)
        return "CURRENT:" + self.projection.identity


def _base(binding, profile, policy, context, axis):
    return {
        "axis": axis, "equipment": {
            "store_id": binding.store_id,
            "equipment_id": binding.equipment_id,
            "equipment_type": binding.profile.equipment_type,
        }, "profile": profile, "policy": policy,
        "generation": context.generation,
        "change_revision": context.change_revision,
        "command_ref": context.command_ref,
        "previous_change_id": context.previous_change_id,
    }


def _source(event):
    if isinstance(event, HealthTick):
        return {"input_kind": "tick", "token": wire(event.token)}
    return {
        "input_kind": ("synthetic_signal" if isinstance(
            event, SyntheticSignal,
        ) else "numeric_telemetry"),
        "event_id": getattr(event, "event_id", None),
        "signal_id": getattr(event, "signal_id", None),
        "event_time": stamp(event.event_time), "source": event.source,
        "provenance": getattr(event, "provenance", None),
        "schema_version": getattr(event, "schema_version", None),
        "input_contract_revision": None,
    }


def _assemble(base, context, current, change, origin):
    projection = CurrentProjection(_sealed({
        **base, "version": "current-projection-v1",
        "projection_revision": context.projection_revision,
        "change_id": (change.get("state_change_id") or change.get(
            "health_change_id",
        )) if change else (
            context.previous_change_id
        ), "current": current,
    }))
    envelope = None if change is None else ChangeEnvelope(_sealed({
        **base, **change, "version": "state-output-v1", "origin_ref": origin,
    }))
    return StateOutput(envelope, projection)


def operational_output(
    previous, result, event, *, binding, context, reference_time, origin=None,
):
    """Adapt native results with explicit counters and activation context."""
    stamp(reference_time)
    if not isinstance(binding, EquipmentBinding):
        raise TypeError("binding required")
    if binding.profile == FRYER_REFERENCE:
        valid = isinstance(result, FsmResult) and isinstance(
            previous, TwinState,
        ) and isinstance(event, SyntheticSignal) and (
            binding.input_kind is InputKind.SYNTHETIC_SIGNAL
        )
        current = (result.state.observation.operational_state.value
                   if isinstance(result, FsmResult) else None)
    elif binding.profile == HOOD_PROFILE:
        valid = isinstance(result, HoodResult) and isinstance(
            previous, HoodSnapshot,
        ) and isinstance(event, HoodRpmEvent) and (
            binding.input_kind is InputKind.NUMERIC_TELEMETRY
        )
        current = (result.state.current.value if isinstance(result, HoodResult)
                   and result.state.current else None)
    else:
        raise ValueError("unsupported Operational profile")
    if not valid:
        raise ValueError("incompatible Operational result/input")
    for obj in (previous, result.state, event):
        if (obj.store_id, obj.equipment_id) != (
            binding.store_id, binding.equipment_id,
        ):
            raise OutputConflict("Operational identity mismatch")
    if result.state == previous:
        if result.intents:
            raise OutputConflict("intent without snapshot mutation")
        return None
    if len(result.intents) > 1:
        raise ValueError("single-input adapter expects at most one intent")
    policy = {"profile": wire(binding.profile)}
    if isinstance(result, HoodResult):
        policy["hood_policy"] = {
            **wire(result.state.policy),
            "enter_rpm": float(result.state.policy.enter_rpm),
            "exit_rpm": float(result.state.policy.exit_rpm),
        }
    change = None
    if result.intents:
        intent = result.intents[0]
        trigger_id = getattr(event, "event_id", None) or getattr(
            event, "signal_id", None,
        )
        native_trigger = getattr(intent, "event_id", None) or getattr(
            intent, "signal_id", None,
        )
        if trigger_id != native_trigger or intent.event_time != (
            event.event_time
        ) or intent.current.value != current:
            raise OutputConflict("Operational intent/input lineage mismatch")
        policy["core_policy_version"] = intent.policy_version
        change = {
            "state_change_id": intent.state_change_id,
            "record_kind": "TRANSITION",
            "effective_at": stamp(intent.event_time),
            "evaluation_time": stamp(reference_time),
            "source_ref": _source(event), "body": {
                "previous": intent.previous.value,
                "current": intent.current.value,
            },
        }
    else:
        from streaming.state.fsm import POLICY_VERSION as FRYER_VERSION
        from streaming.state.hood import POLICY_VERSION as HOOD_VERSION
        policy["core_policy_version"] = (
            FRYER_VERSION if isinstance(result, FsmResult) else HOOD_VERSION
        )
    base = _base(binding, binding.profile, policy, context, "OPERATIONAL")
    return _assemble(base, context, {
        "state": current, "snapshot_sha256": digest(snapshot_bytes(
            result.state,
        )), "last_event_time": wire(result.state.last_event_time),
    }, change, origin)


def health_output(
    previous, result, item, *, context, reference_time, origin=None,
):
    """Reuse Health IDs; retain evaluation time from this recorded command."""
    now = stamp(reference_time)
    if not isinstance(previous, HealthSnapshot) or not isinstance(
        result, HealthResult,
    ) or not isinstance(item, (HealthObservation, HealthTick)):
        raise TypeError("typed Health context required")
    state = result.snapshot
    if isinstance(item, HealthObservation) and (
        item.store_id, item.equipment_id, item.equipment_type,
    ) != (
        state.policy.binding.store_id, state.policy.binding.equipment_id,
        state.policy.profile.equipment_type,
    ):
        raise OutputConflict("Health source identity mismatch")
    if state.policy != previous.policy:
        raise OutputConflict("Health policy mismatch")
    if state == previous:
        if result.changes:
            raise OutputConflict("change without snapshot mutation")
        return None
    if len(result.changes) > 1 or (
        context.change_revision != state.health_revision
        or context.projection_revision != state.snapshot_revision
        or context.previous_change_id != previous.last_health_change_id
        or now != stamp(state.last_evaluated_at)
    ):
        raise OutputConflict("Health revision/evaluation context mismatch")
    policy = state.policy
    base = _base(policy.binding, policy.profile, {
        "fingerprint": policy.fingerprint,
        "health_contract_version": state.health_contract_version,
        "monitoring_epoch": state.monitoring_epoch,
        "binding_revision": policy.binding_revision,
        "input_contract_revision": policy.input_contract_revision,
    }, context, "HEALTH")
    details = {
        "state": state.health_status.value if state.health_status else None,
        "last_seen": wire(state.last_seen),
        "freshness_deadline": stamp(state.freshness_deadline),
        "last_evaluated_at": now, "timer_token": wire(timer_token(state)),
        "snapshot_sha256": digest(snapshot_bytes(state)),
    }
    change = None
    if result.changes:
        native = result.changes[0]
        cause = native.cause
        if native.health_revision != state.health_revision or (
            native.health_change_id != state.last_health_change_id
            or native.policy_fingerprint != policy.fingerprint
            or native.profile != policy.profile
            or native.monitoring_epoch != state.monitoring_epoch
            or native.previous != previous.health_status
            or native.current != state.health_status
        ):
            raise OutputConflict("Health change/snapshot mismatch")
        if isinstance(item, HealthObservation) and cause != (
            "observation", item.event_id,
            item.event_time.isoformat(timespec="microseconds"),
        ):
            raise OutputConflict("Health cause/input mismatch")
        source_ref = _source(item)
        source_ref["input_contract_revision"] = policy.input_contract_revision
        change = {
            "health_change_id": native.health_change_id,
            "record_kind": native.record_kind,
            "effective_at": stamp(
                state.freshness_deadline if cause[0] == "timeout"
                else item.event_time
            ), "evaluation_time": now, "source_ref": source_ref,
            "body": {
                "previous": native.previous.value if native.previous else None,
                "current": native.current.value, "reason": wire(cause),
                "last_seen": details["last_seen"],
                "freshness_deadline": details["freshness_deadline"],
                "snapshot_revision_at_change": state.snapshot_revision,
                "timer_token": details["timer_token"],
            },
        }
    return _assemble(base, context, details, change, origin)
