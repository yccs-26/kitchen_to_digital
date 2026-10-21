"""명시적 Health snapshot JSON schema와 엄격한 복원 검증."""

from dataclasses import asdict, fields, replace
from datetime import timedelta, timezone
import hashlib
import json

import pytest

from streaming.state.health import HealthTick, timer_token
from streaming.state.health_codec import (
    HealthCodecError, decode_health_snapshot, encode_health_snapshot,
)
from test_health import (
    BASE, EPS, POLICY, TIMEOUT, boot, evaluate, healthy, observation, stale,
)


def signed_payload(data):
    """유효한 digest로 semantic 검사를 별도로 시험한다. 인증 서명이 아니다."""
    body = {
        key: value for key, value in data.items() if key != "content_sha256"
    }
    encoded = json.dumps(
        body, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    data["content_sha256"] = hashlib.sha256(encoded).hexdigest()
    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def rich_snapshot():
    state = healthy()
    state = evaluate(state, observation(BASE + EPS, "e2"), BASE + EPS).snapshot
    return evaluate(
        state, HealthTick(timer_token(state)), BASE + TIMEOUT + EPS,
    ).snapshot


@pytest.mark.parametrize("factory", [boot, healthy, stale, rich_snapshot])
def test_all_snapshot_fields_roundtrip_and_deterministic_bytes(factory):
    before = factory()
    encoded = encode_health_snapshot(before)
    assert type(encoded) is bytes
    assert encoded == encode_health_snapshot(before)
    decoded = decode_health_snapshot(encoded, policy=POLICY)
    assert decoded == before
    assert decoded is not before
    assert asdict(decoded) == asdict(before)
    assert {f.name for f in fields(decoded)} == set(asdict(before))
    assert timer_token(decoded) == timer_token(before)
    assert encode_health_snapshot(decoded) == encoded


def test_canonical_keys_utc_microseconds_and_ledger_order():
    before = rich_snapshot()
    data = json.loads(encode_health_snapshot(before))
    assert data["codec_version"] == "1"
    assert data["snapshot"]["last_seen"].endswith(".000001+00:00")
    receipts = data["snapshot"]["receipts"]
    assert [e["event_id"] for e in receipts] == ["e1", "e2"]
    equivalent = replace(
        before, last_seen=before.last_seen.astimezone(
            timezone(timedelta(hours=9)),
        ),
    )
    assert encode_health_snapshot(equivalent) == encode_health_snapshot(before)
    reordered = json.dumps(data, indent=3).encode("utf-8")
    assert decode_health_snapshot(reordered, policy=POLICY) == before


def test_integer_microseconds_and_large_revision_are_not_float_seconds():
    policy = replace(
        POLICY, freshness_timeout=timedelta(days=120000, microseconds=1),
    )
    state = replace(boot(policy), snapshot_revision=2 ** 53 + 1)
    payload = encode_health_snapshot(state)
    data = json.loads(payload)
    assert type(data["policy"]["freshness_timeout_us"]) is int
    assert data["policy"]["freshness_timeout_us"] == (
        policy.freshness_timeout // EPS
    )
    decoded = decode_health_snapshot(payload, policy=policy)
    assert asdict(decoded) == asdict(state)
    assert type(decoded.snapshot_revision) is int


@pytest.mark.parametrize("value", [800, 800.0, -0.0])
def test_metric_numeric_representation_is_lossless(value):
    before = evaluate(boot(), observation(metric_value=value)).snapshot
    decoded = decode_health_snapshot(
        encode_health_snapshot(before), policy=POLICY,
    )
    assert type(decoded.receipts[0].metric_value) is type(value)
    assert json.dumps(decoded.receipts[0].metric_value) == json.dumps(value)


@pytest.mark.parametrize(
    "section", [None, "policy", "health_profile", "snapshot"],
)
def test_unknown_fields_are_not_silently_discarded(section):
    data = json.loads(encode_health_snapshot(healthy()))
    target = data if section is None else data[section]
    target["future_field"] = "unsupported"
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "UNKNOWN_FIELD"


@pytest.mark.parametrize("section", [None, "policy", "equipment", "snapshot"])
def test_every_required_field_is_required(section):
    original = json.loads(encode_health_snapshot(healthy()))
    target = original if section is None else original[section]
    for key in target:
        data = json.loads(json.dumps(original))
        container = data if section is None else data[section]
        del container[key]
        # digest도 필수이므로 제거한 경우 재생성하지 않는다.
        payload = json.dumps(data).encode() if key == "content_sha256" else (
            signed_payload(data)
        )
        with pytest.raises(HealthCodecError) as raised:
            decode_health_snapshot(payload, policy=POLICY)
        assert raised.value.code == "MISSING_FIELD", key


@pytest.mark.parametrize("section,key,value,code", [
    (None, "codec_version", "2", "CODEC_VERSION_MISMATCH"),
    (None, "snapshot_schema_version", "2", "SNAPSHOT_VERSION_MISMATCH"),
    (None, "health_contract_version", "2", "CONTRACT_VERSION_MISMATCH"),
    ("equipment", "equipment_id", "other", "EQUIPMENT_IDENTITY_MISMATCH"),
    ("equipment", "store_id", "other", "EQUIPMENT_IDENTITY_MISMATCH"),
    ("equipment", "equipment_type", "fryer", "EQUIPMENT_IDENTITY_MISMATCH"),
    ("health_profile", "version", "2", "PROFILE_MISMATCH"),
    (None, "policy_fingerprint", "0" * 64, "POLICY_FINGERPRINT_MISMATCH"),
    ("policy", "freshness_timeout_us", 11_000_000, "POLICY_MISMATCH"),
    ("policy", "binding_revision", "changed", "POLICY_MISMATCH"),
    ("policy", "input_contract_revision", "changed", "POLICY_MISMATCH"),
    ("policy", "sources", ["other"], "POLICY_MISMATCH"),
    ("snapshot", "health_status", "invalid", "INVALID_HEALTH"),
    ("snapshot", "health_status", "FAULT", "UNSUPPORTED_FAULT"),
    ("snapshot", "active_faults", ["E1"], "UNSUPPORTED_FAULT"),
])
def test_explicit_incompatibility_reasons(section, key, value, code):
    data = json.loads(encode_health_snapshot(healthy()))
    target = data if section is None else data[section]
    target[key] = value
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == code


@pytest.mark.parametrize("key", ["health_revision", "snapshot_revision"])
@pytest.mark.parametrize("value", [-1, True, 1.0, "1", None])
def test_revision_types_are_exact_integers(key, value):
    data = json.loads(encode_health_snapshot(healthy()))
    data["snapshot"][key] = value
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "INVALID_REVISION"


@pytest.mark.parametrize("value", [
    None, 123, "bad", "2026-10-20T00:00:00",
    "2026-10-20T00:00:00.0000001+00:00",
    "2026-10-20T09:00:00.000000+09:00",
    "2026-13-20T00:00:00.000000+00:00",
])
def test_timestamp_errors_cannot_truncate_or_guess_timezone(value):
    data = json.loads(encode_health_snapshot(healthy()))
    data["snapshot"]["last_evaluated_at"] = value
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "INVALID_TIMESTAMP"


@pytest.mark.parametrize("changes", [
    {"freshness_deadline": "2026-10-20T00:00:01.000000+00:00"},
    {"last_seen": None}, {"last_accepted_event_id": "missing"},
    {"health_status": "STALE"}, {"health_revision": 0},
    {"snapshot_revision": 0}, {"last_health_change_id": "broken"},
])
def test_contradictory_snapshot_is_rejected(changes):
    data = json.loads(encode_health_snapshot(healthy()))
    data["snapshot"].update(changes)
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "INCONSISTENT_SNAPSHOT"


@pytest.mark.parametrize(
    "damage", ["reverse", "duplicate", "drop", "bad-value"],
)
def test_ledger_corruption_is_not_repaired_or_sorted(damage):
    data = json.loads(encode_health_snapshot(rich_snapshot()))
    receipts = data["snapshot"]["receipts"]
    if damage == "reverse":
        receipts.reverse()
    elif damage == "duplicate":
        receipts.append(dict(receipts[-1]))
    elif damage == "drop":
        receipts.pop()
    else:
        receipts[0]["metric_value"] = True
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code in {"INVALID_LEDGER", "INCONSISTENT_SNAPSHOT"}


@pytest.mark.parametrize("payload", [
    b"{", b"[]", b"null", b"\xff",
    b'{"codec_version":"1","codec_version":"1"}',
    b'{"value":NaN}', b'{"value":Infinity}', b'{} trailing',
])
def test_malformed_json_duplicate_keys_and_nonfinite_tokens(payload):
    with pytest.raises(HealthCodecError):
        decode_health_snapshot(payload, policy=POLICY)


def test_integrity_digest_detects_otherwise_valid_ledger_value_corruption():
    data = json.loads(encode_health_snapshot(healthy()))
    data["snapshot"]["receipts"][0]["metric_value"] = 801
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(json.dumps(data).encode(), policy=POLICY)
    assert raised.value.code == "INTEGRITY_MISMATCH"


def test_external_policy_is_required_and_exact_match():
    payload = encode_health_snapshot(healthy())
    for policy in (
        replace(POLICY, freshness_timeout=2 * TIMEOUT),
        replace(POLICY, profile=replace(POLICY.profile, version="2")),
    ):
        with pytest.raises(HealthCodecError):
            decode_health_snapshot(payload, policy=policy)


def test_unsupported_snapshot_header_is_not_encoded():
    state = replace(healthy(), snapshot_schema_version="2")
    with pytest.raises(HealthCodecError) as raised:
        encode_health_snapshot(state)
    assert raised.value.code == "SNAPSHOT_VERSION_MISMATCH"


def test_unicode_identity_and_source_survive_utf8_roundtrip():
    source = "관측-🌀"
    policy = replace(POLICY, sources=(source,))
    initial = replace(boot(policy), monitoring_epoch="활성화-🐧")
    state = evaluate(
        initial, observation(event_id="사건-1", source=source), policy=policy,
    ).snapshot
    encoded = encode_health_snapshot(state)
    assert "활성화-🐧" in encoded.decode("utf-8")
    decoded = decode_health_snapshot(encoded, policy=policy)
    assert asdict(decoded) == asdict(state)


@pytest.mark.parametrize("key", ["event_id", "event_time", "metric_value"])
def test_missing_receipt_fields_are_rejected(key):
    data = json.loads(encode_health_snapshot(healthy()))
    del data["snapshot"]["receipts"][0][key]
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "MISSING_FIELD"


@pytest.mark.parametrize("key,value", [
    ("freshness_timeout_us", 10_000_000.0),
    ("fault_supported", 0), ("fault_supported", "false"),
])
def test_policy_comparison_does_not_coerce_json_types(key, value):
    data = json.loads(encode_health_snapshot(healthy()))
    data["policy"][key] = value
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "POLICY_MISMATCH"


@pytest.mark.parametrize(
    "damage", ["missing-update", "too-many-health-changes"],
)
def test_revision_ledger_counts_must_be_reachable(damage):
    data = json.loads(encode_health_snapshot(rich_snapshot()))
    if damage == "missing-update":
        data["snapshot"]["health_revision"] = 1
        data["snapshot"]["snapshot_revision"] = 1
    else:
        data["snapshot"]["health_revision"] = 10
        data["snapshot"]["snapshot_revision"] = 10
    with pytest.raises(HealthCodecError) as raised:
        decode_health_snapshot(signed_payload(data), policy=POLICY)
    assert raised.value.code == "INCONSISTENT_SNAPSHOT"
