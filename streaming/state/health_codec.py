"""Health snapshot 전용 canonical UTF-8 JSON codec. 파일·시계를 읽지 않는다.

정책은 caller가 제공하며 payload로 정책을 교체하지 않는다. digest는 우발적
손상 검출용으로, 인증 서명이나 원장 완전성/외부 전달 보장이 아니다.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
from math import isfinite

from streaming.state.bindings import ProfileKey
from streaming.state.contracts import HealthStatus
from streaming.state.health import (
    HEALTH_CONTRACT_VERSION, SNAPSHOT_SCHEMA_VERSION,
    HealthObservation, HealthPolicy, HealthSnapshot,
)


CODEC_VERSION = "1"
_ROOT_FIELDS = (
    "codec_version", "snapshot_schema_version", "health_contract_version",
    "equipment", "health_profile", "policy_fingerprint", "policy",
    "snapshot", "content_sha256",
)
_STATE_FIELDS = (
    "monitoring_epoch", "monitoring_started_at", "freshness_deadline",
    "last_evaluated_at", "last_seen", "last_accepted_event_id",
    "health_status",
    "receipts", "health_revision", "snapshot_revision",
    "last_health_change_id",
    "active_faults",
)
_RECEIPT_FIELDS = (
    "event_id", "event_time", "store_id", "equipment_id", "equipment_type",
    "metric_name", "metric_value", "unit", "schema_version", "source",
)
_PROFILE_FIELDS = ("equipment_type", "profile_id", "version")
_POLICY_FIELDS = (
    "binding_revision", "input_contract_revision", "operational_profile",
    "input_kind", "freshness_timeout_us", "metric_name", "unit",
    "schema_versions", "sources", "fault_supported",
)


class HealthCodecError(ValueError):
    """호환성/손상을 코드와 필드 경로로 설명하며 기본 상태로 복구하지 않는다."""

    def __init__(self, code: str, field: str) -> None:
        self.code = code
        self.field = field
        super().__init__(f"{code}: {field}")


def _canonical(value: object) -> bytes:
    try:
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True,
            separators=(",", ":"), allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as exc:
        raise HealthCodecError("INVALID_SCHEMA", "JSON values") from exc


def _profile(key: ProfileKey) -> dict:
    return {
        "equipment_type": key.equipment_type,
        "profile_id": key.profile_id,
        "version": key.version,
    }


def _equipment(policy: HealthPolicy) -> dict:
    return {
        "store_id": policy.binding.store_id,
        "equipment_id": policy.binding.equipment_id,
        "equipment_type": policy.profile.equipment_type,
    }


def _policy(policy: HealthPolicy) -> dict:
    return {
        "binding_revision": policy.binding_revision,
        "input_contract_revision": policy.input_contract_revision,
        "operational_profile": _profile(policy.binding.profile),
        "input_kind": policy.binding.input_kind.value,
        "freshness_timeout_us": (
            policy.freshness_timeout // timedelta(microseconds=1)
        ),
        "metric_name": policy.metric_name,
        "unit": policy.unit,
        "schema_versions": list(policy.schema_versions),
        "sources": list(policy.sources),
        "fault_supported": policy.fault_supported,
    }


def _stamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _observation(event: HealthObservation) -> dict:
    return {
        "event_id": event.event_id,
        "event_time": _stamp(event.event_time),
        "store_id": event.store_id,
        "equipment_id": event.equipment_id,
        "equipment_type": event.equipment_type,
        "metric_name": event.metric_name,
        "metric_value": event.metric_value,
        "unit": event.unit,
        "schema_version": event.schema_version,
        "source": event.source,
    }


def _versions(codec, snapshot, contract) -> None:
    for actual, expected, code in (
        (codec, CODEC_VERSION, "CODEC_VERSION_MISMATCH"),
        (snapshot, SNAPSHOT_SCHEMA_VERSION, "SNAPSHOT_VERSION_MISMATCH"),
        (contract, HEALTH_CONTRACT_VERSION, "CONTRACT_VERSION_MISMATCH"),
    ):
        if type(actual) is not str or actual != expected:
            raise HealthCodecError(code, "version")


def encode_health_snapshot(snapshot: HealthSnapshot) -> bytes:
    """명시적 schema를 canonical JSON bytes로 만든다. 원장 순서는 보존한다."""
    if not isinstance(snapshot, HealthSnapshot):
        raise TypeError("snapshot must be HealthSnapshot")
    _versions(
        CODEC_VERSION, snapshot.snapshot_schema_version,
        snapshot.health_contract_version,
    )
    _revision_bounds(
        snapshot.snapshot_revision, snapshot.health_revision,
        len(snapshot.receipts),
    )
    body = {
        "codec_version": CODEC_VERSION,
        "snapshot_schema_version": snapshot.snapshot_schema_version,
        "health_contract_version": snapshot.health_contract_version,
        "equipment": _equipment(snapshot.policy),
        "health_profile": _profile(snapshot.policy.profile),
        "policy_fingerprint": snapshot.policy.fingerprint,
        "policy": _policy(snapshot.policy),
        "snapshot": {
            "monitoring_epoch": snapshot.monitoring_epoch,
            "monitoring_started_at": _stamp(snapshot.monitoring_started_at),
            "freshness_deadline": _stamp(snapshot.freshness_deadline),
            "last_evaluated_at": _stamp(snapshot.last_evaluated_at),
            "last_seen": _stamp(snapshot.last_seen),
            "last_accepted_event_id": snapshot.last_accepted_event_id,
            "health_status": (
                snapshot.health_status.value
                if snapshot.health_status else None
            ),
            "receipts": [_observation(event) for event in snapshot.receipts],
            "health_revision": snapshot.health_revision,
            "snapshot_revision": snapshot.snapshot_revision,
            "last_health_change_id": snapshot.last_health_change_id,
            "active_faults": list(snapshot.active_faults),
        },
    }
    body["content_sha256"] = hashlib.sha256(_canonical(body)).hexdigest()
    return _canonical(body)


def _shape(value: object, keys: tuple[str, ...], path: str) -> dict:
    if type(value) is not dict:
        raise HealthCodecError("INVALID_SCHEMA", path)
    missing = set(keys) - value.keys()
    if missing:
        raise HealthCodecError("MISSING_FIELD", f"{path}.{sorted(missing)[0]}")
    extra = value.keys() - set(keys)
    if extra:
        raise HealthCodecError("UNKNOWN_FIELD", f"{path}.{sorted(extra)[0]}")
    return value


def _text(value: object, path: str, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not value.strip():
        raise HealthCodecError("INVALID_SCHEMA", path)
    try:
        value.encode("utf-8")
    except UnicodeError as exc:
        raise HealthCodecError("INVALID_SCHEMA", path) from exc
    return value


def _time(value: object, path: str, nullable: bool = False) -> datetime | None:
    if value is None and nullable:
        return None
    try:
        if type(value) is not str:
            raise ValueError("timestamp must be canonical UTC text")
        parsed = datetime.fromisoformat(value)
        if parsed.utcoffset() != timedelta(0) or (
            parsed.isoformat(timespec="microseconds") != value
        ):
            raise ValueError("timestamp must be canonical UTC with six digits")
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError) as exc:
        raise HealthCodecError("INVALID_TIMESTAMP", path) from exc


def _revision_bounds(
    snapshot_revision: int, health_revision: int, size: int,
) -> None:
    # 수용 관측마다 snapshot revision이 한 번 이상 진행한다. 표시 변화는
    # 초기 boot 만료와 각 관측/그 뒤 만료 이외에 생길 수 없다(fault 미지원).
    if snapshot_revision < size or health_revision > 2 * size + 1:
        raise HealthCodecError("INCONSISTENT_SNAPSHOT", "revision/receipts")


def _revision(value: object, path: str) -> int:
    if type(value) is not int or value < 0:
        raise HealthCodecError("INVALID_REVISION", path)
    return value


def _unique_object(pairs: list) -> dict:
    result = {}
    for key, value in pairs:
        if key in result:
            raise HealthCodecError("INVALID_JSON", "duplicate object key")
        result[key] = value
    return result


def _nonfinite(token: str):
    raise HealthCodecError("INVALID_JSON", "nonfinite numeric token")


def _read_observation(value: object, index: int) -> HealthObservation:
    path = f"snapshot.receipts[{index}]"
    data = _shape(value, _RECEIPT_FIELDS, path)
    number = data["metric_value"]
    try:
        valid = type(number) in (int, float) and isfinite(number)
    except OverflowError:
        valid = False
    if not valid:
        raise HealthCodecError("INVALID_LEDGER", f"{path}.metric_value")
    return HealthObservation(
        event_id=_text(data["event_id"], f"{path}.event_id"),
        event_time=_time(data["event_time"], f"{path}.event_time"),
        store_id=_text(data["store_id"], f"{path}.store_id"),
        equipment_id=_text(data["equipment_id"], f"{path}.equipment_id"),
        equipment_type=_text(data["equipment_type"], f"{path}.equipment_type"),
        metric_name=_text(data["metric_name"], f"{path}.metric_name"),
        metric_value=number,
        unit=_text(data["unit"], f"{path}.unit"),
        schema_version=_text(data["schema_version"], f"{path}.schema_version"),
        source=_text(data["source"], f"{path}.source"),
    )


def decode_health_snapshot(
    payload: bytes, *, policy: HealthPolicy,
) -> HealthSnapshot:
    """동일 정책만 허용하고 schema·digest·Core 불변식을 검사해 새 객체를 만든다."""
    if type(payload) is not bytes:
        raise TypeError("payload must be UTF-8 JSON bytes")
    if not isinstance(policy, HealthPolicy):
        raise TypeError("policy must be HealthPolicy")
    try:
        data = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_unique_object,
            parse_constant=_nonfinite,
        )
    except (ValueError, UnicodeError, RecursionError) as exc:
        if isinstance(exc, HealthCodecError):
            raise
        raise HealthCodecError("INVALID_JSON", "payload") from exc
    root = _shape(data, _ROOT_FIELDS, "root")
    _versions(
        root["codec_version"], root["snapshot_schema_version"],
        root["health_contract_version"],
    )
    equipment = _shape(
        root["equipment"], ("store_id", "equipment_id", "equipment_type"),
        "equipment",
    )
    if _canonical(equipment) != _canonical(_equipment(policy)):
        raise HealthCodecError("EQUIPMENT_IDENTITY_MISMATCH", "equipment")
    profile = _shape(root["health_profile"], _PROFILE_FIELDS, "health_profile")
    if _canonical(profile) != _canonical(_profile(policy.profile)):
        raise HealthCodecError("PROFILE_MISMATCH", "health_profile")
    if root["policy_fingerprint"] != policy.fingerprint:
        raise HealthCodecError(
            "POLICY_FINGERPRINT_MISMATCH", "policy_fingerprint",
        )
    policy_data = _shape(root["policy"], _POLICY_FIELDS, "policy")
    _shape(
        policy_data["operational_profile"], _PROFILE_FIELDS, "policy.profile",
    )
    # canonical 비교는 True == 1, 1.0 == 1 같은 Python coercion을 허용하지 않는다.
    if _canonical(policy_data) != _canonical(_policy(policy)):
        raise HealthCodecError("POLICY_MISMATCH", "policy")
    state = _shape(root["snapshot"], _STATE_FIELDS, "snapshot")
    status = state["health_status"]
    if status == "FAULT":
        raise HealthCodecError("UNSUPPORTED_FAULT", "snapshot.health_status")
    if status is not None and (
        type(status) is not str or status not in ("HEALTHY", "STALE")
    ):
        raise HealthCodecError("INVALID_HEALTH", "snapshot.health_status")
    if type(state["active_faults"]) is not list:
        raise HealthCodecError("INVALID_SCHEMA", "snapshot.active_faults")
    if state["active_faults"]:
        raise HealthCodecError("UNSUPPORTED_FAULT", "snapshot.active_faults")
    if type(state["receipts"]) is not list:
        raise HealthCodecError("INVALID_LEDGER", "snapshot.receipts")
    receipts = tuple(
        _read_observation(item, index)
        for index, item in enumerate(state["receipts"])
    )
    arguments = {
        "policy": policy,
        "monitoring_epoch": _text(state["monitoring_epoch"], "snapshot.epoch"),
        "monitoring_started_at": _time(
            state["monitoring_started_at"], "snapshot.monitoring_started_at",
        ),
        "freshness_deadline": _time(
            state["freshness_deadline"], "snapshot.freshness_deadline",
        ),
        "last_evaluated_at": _time(
            state["last_evaluated_at"], "snapshot.last_evaluated_at",
        ),
        "last_seen": _time(state["last_seen"], "snapshot.last_seen", True),
        "last_accepted_event_id": _text(
            state["last_accepted_event_id"], "snapshot.last_accepted_event_id",
            True,
        ),
        "health_status": HealthStatus(status) if status is not None else None,
        "receipts": receipts,
        "health_revision": _revision(
            state["health_revision"], "snapshot.health_revision",
        ),
        "snapshot_revision": _revision(
            state["snapshot_revision"], "snapshot.snapshot_revision",
        ),
        "last_health_change_id": _text(
            state["last_health_change_id"], "snapshot.last_health_change_id",
            True,
        ),
        "active_faults": (),
        "snapshot_schema_version": root["snapshot_schema_version"],
        "health_contract_version": root["health_contract_version"],
    }
    _revision_bounds(
        arguments["snapshot_revision"], arguments["health_revision"],
        len(receipts),
    )
    body = {
        key: value for key, value in root.items() if key != "content_sha256"
    }
    if root["content_sha256"] != hashlib.sha256(_canonical(body)).hexdigest():
        raise HealthCodecError("INTEGRITY_MISMATCH", "content_sha256")
    try:
        return HealthSnapshot(**arguments)
    except (ValueError, TypeError, OverflowError) as exc:
        raise HealthCodecError("INCONSISTENT_SNAPSHOT", "snapshot") from exc
