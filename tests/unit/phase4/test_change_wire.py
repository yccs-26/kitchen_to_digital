"""Local Avro draft, framing and installed offline compatibility checks."""

from copy import deepcopy
from io import BytesIO
import json

from avro.compatibility import (
    ReaderWriterCompatibilityChecker, SchemaCompatibilityType,
)
from avro.schema import parse
from fastavro import parse_schema, schemaless_reader, schemaless_writer
import pytest

from streaming.state.change_envelope import operational_output, OutputConflict
from streaming.state.change_wire import (
    ChangeWireError, decode_change, encode_change, load_change_schema,
)
from streaming.state.health import HealthTick, timer_token
from test_change_envelope import BINDING, fryer_pair, health_pair
from test_health import BASE, EPS, TIMEOUT


@pytest.mark.parametrize("health", [False, True])
@pytest.mark.parametrize("schema_id", [None, 123])
def test_round_trip_ids_key_nullable_fields_and_bytes_are_deterministic(
    health, schema_id,
):
    change = (health_pair() if health else fryer_pair())[4].change
    one = encode_change(change, schema_id=schema_id)
    assert encode_change(change, schema_id=schema_id) == one
    assert decode_change(one.key, one.value, schema_id=schema_id) == change
    raw = one.value if schema_id is None else one.value[5:]
    data = schemaless_reader(BytesIO(raw), parse_schema(load_change_schema()))
    assert data["state_change_id"] == (None if health else change.identity[1])
    assert data["health_change_id"] == (change.identity[1] if health else None)
    assert data["canonical_payload"] == change.payload
    assert data["last_seen"] == (change.data["body"]["last_seen"]
                                 if health else None)


def test_timeout_has_exact_deadline_and_timer_provenance():
    state = health_pair()[1].snapshot
    change = health_pair(state, HealthTick(timer_token(state)),
                         BASE + TIMEOUT, "tick")[4].change
    encoded = encode_change(change)
    data = schemaless_reader(BytesIO(encoded.value),
                             parse_schema(load_change_schema()))
    assert data["timer_provenance"] is not None
    assert data["effective_at"] == data["freshness_deadline"]
    assert decode_change(encoded.key, encoded.value) == change


def test_same_native_id_different_payload_requires_canonical_conflict():
    before, result, event, context, output = fryer_pair()
    changed = operational_output(
        before, result, event, binding=BINDING, context=context,
        reference_time=BASE + EPS,
    ).change
    assert changed.identity == output.change.identity
    with pytest.raises(OutputConflict):
        encode_change(changed, canonical_change=output.change)
    assert encode_change(output.change, canonical_change=output.change)


@pytest.mark.parametrize("field,value", [
    ("wire_version", "v2"), ("equipment_id", "other"),
    ("health_change_id", "wrong"), ("payload_sha256", "0" * 64),
    ("current_state", "FAULT"), ("evaluation_time", "2026-10-23"),
])
def test_mutated_outer_fields_cannot_disagree_with_canonical(field, value):
    wire = encode_change(fryer_pair()[4].change)
    schema = parse_schema(load_change_schema())
    data = schemaless_reader(BytesIO(wire.value), schema)
    data[field] = value
    stream = BytesIO()
    schemaless_writer(stream, schema, data)
    with pytest.raises(ChangeWireError):
        decode_change(wire.key, stream.getvalue())


@pytest.mark.parametrize("schema_id", [True, -1, 0, 2 ** 32])
def test_invalid_schema_id_is_not_assumed_registered(schema_id):
    with pytest.raises(ValueError):
        encode_change(health_pair()[4].change, schema_id=schema_id)


def test_wrong_frame_key_and_trailing_bytes_are_rejected():
    wire = encode_change(health_pair()[4].change, schema_id=1)
    for key, payload, schema_id in (
        (b"other", wire.value, 1), (wire.key, wire.value, 2),
        (wire.key, b"bad", 1), (wire.key, wire.value + b"x", 1),
    ):
        with pytest.raises(ChangeWireError):
            decode_change(key, payload, schema_id=schema_id)


def compatible(reader, writer):
    return ReaderWriterCompatibilityChecker().get_compatibility(
        parse(json.dumps(reader)), parse(json.dumps(writer)),
    ).compatibility == SchemaCompatibilityType.compatible


def test_offline_backward_compatibility_optional_default_and_resolution():
    original = load_change_schema()
    reader = deepcopy(original)
    reader["fields"].append({"name": "trace_label", "type": ["null", "string"],
                             "default": None})
    assert compatible(reader, original)
    wire = encode_change(health_pair()[4].change)
    resolved = schemaless_reader(BytesIO(wire.value), parse_schema(original),
                                 parse_schema(reader))
    assert resolved["trace_label"] is None
    assert compatible(original, reader)


@pytest.mark.parametrize("change", ["required", "field_type", "axis_enum"])
def test_offline_incompatible_reader_is_detected(change):
    writer = load_change_schema()
    reader = deepcopy(writer)
    if change == "required":
        reader["fields"].append({"name": "new_required", "type": "string"})
    elif change == "field_type":
        next(field for field in reader["fields"] if field["name"] == (
            "change_revision"
        ))["type"] = "string"
    else:
        reader["fields"][1]["type"]["symbols"] = ["OPERATIONAL"]
    assert not compatible(reader, writer)
