"""Offline contracts for Quarantine identity, provenance, and failure data."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256

import pytest

from streaming.quarantine.quarantine_record import (
    QuarantineRecord,
    build_quarantine_record,
)
from streaming.validation.domain_validator import (
    EquipmentSpec,
    ValidationError,
    ValidationResult,
    validate_domain_event,
)


UNIT_ERROR = ValidationError("UNIT_MISMATCH", "unit", "Expected celsius.")
VERSION_ERROR = ValidationError(
    "UNSUPPORTED_SCHEMA_VERSION", "schema_version", "Unsupported version."
)


@pytest.fixture
def source():
    return {
        "source_topic": "kitchen.sensor.raw",
        "source_partition": 2,
        "source_offset": 18452,
        "raw_key": b"fridge-001",
        "raw_value": b"\x00\x00\x00\x00\x01\xff",
        "event_id": "event-1",
        "schema_id": 1,
    }


def build(source, errors=(UNIT_ERROR,)):
    return build_quarantine_record(ValidationResult(errors), **source)


def test_one_domain_error_preserves_all_record_fields(source):
    record = build(source)

    assert isinstance(record, QuarantineRecord)
    for name in (
        "source_topic", "source_partition", "source_offset", "raw_key",
        "event_id", "schema_id",
    ):
        assert getattr(record, name) == source[name]
    assert record.errors == (UNIT_ERROR,)
    assert record.raw_value_sha256 == sha256(source["raw_value"]).hexdigest()
    assert len(record.quarantine_id) == 64
    assert not hasattr(record, "raw_value")


def test_real_domain_result_produces_one_record_with_multiple_errors(source):
    now = datetime(2026, 10, 7, tzinfo=timezone.utc)
    event = {
        "event_id": "event-1", "event_time": now, "store_id": "store-1",
        "equipment_id": "fridge-001", "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius", "metric_value": 4.2,
        "unit": "fahrenheit", "schema_version": "unknown",
        "source": "simulator",
    }
    original = event.copy()
    result = validate_domain_event(
        event, source["raw_key"], supported_schema_versions={"1.0.0"},
        equipment_registry={
            "fridge-001": EquipmentSpec("store-1", "refrigerator")
        },
        metric_units={"refrigerator": {"temperature_celsius": "celsius"}},
        reference_time=now, max_future_skew=timedelta(minutes=5),
    )
    record = build_quarantine_record(result, **source)

    assert isinstance(record, QuarantineRecord)
    assert record.errors == result.errors
    assert {error.code for error in record.errors} == {
        "UNIT_MISMATCH", "UNSUPPORTED_SCHEMA_VERSION",
    }
    assert record.event_id == event["event_id"]
    assert event == original


@pytest.mark.parametrize(("code", "field", "schema_id"), [
    ("AVRO_DECODE_FAILED", "raw_value", 1),
    ("UNKNOWN_SCHEMA_ID", "schema_id", 999),
    ("INVALID_CONFLUENT_FRAME", "raw_value", None),
])
def test_explicit_decode_failure_has_lineage_without_domain_identity(
    source, code, field, schema_id
):
    source.update(event_id=None, schema_id=schema_id)
    errors = (ValidationError(code, field, "Source cannot be decoded."),)
    record = build(source, errors)

    assert record.event_id is None
    assert record.schema_id == schema_id
    assert record.source_topic == "kitchen.sensor.raw"
    assert record.source_partition == 2
    assert record.source_offset == 18452
    assert record.errors == errors
    assert record.quarantine_id == build(source, errors).quarantine_id
    assert not hasattr(record, "equipment_id")
    assert not hasattr(record, "event_time")


@pytest.mark.parametrize("event_id", [None, "event-1", ""])
def test_identity_is_deterministic_and_error_order_independent(
    source, event_id
):
    source["event_id"] = event_id
    errors = (UNIT_ERROR, VERSION_ERROR)
    first = build(source, errors)
    reordered = build(source, tuple(reversed(errors)))

    assert first == build(source, errors)
    assert first.quarantine_id == reordered.quarantine_id
    assert first.errors == errors
    assert reordered.errors == tuple(reversed(errors))


def test_duplicate_errors_and_diagnostic_wording_do_not_change_id(source):
    errors = (UNIT_ERROR, replace(UNIT_ERROR, details="New diagnostic."))
    record = build(source, errors)
    assert record.quarantine_id == build(source).quarantine_id
    assert record.errors == errors


@pytest.mark.parametrize(("field", "value"), [
    ("source_topic", "another.raw"), ("source_partition", 3),
    ("source_offset", 18453), ("event_id", "event-2"),
])
def test_identity_changes_with_source_or_event(source, field, value):
    assert build(source).quarantine_id != build(
        {**source, field: value}
    ).quarantine_id


@pytest.mark.parametrize("error", [
    replace(UNIT_ERROR, code="UNSUPPORTED_METRIC"),
    replace(UNIT_ERROR, field="other_field"),
])
def test_different_failure_code_or_field_changes_identity(source, error):
    assert build(source).quarantine_id != build(source, (error,)).quarantine_id


def test_id_without_event_id_still_distinguishes_offsets(source):
    source["event_id"] = None
    assert build(source).quarantine_id != build(
        {**source, "source_offset": 18453}
    ).quarantine_id


def test_value_hash_depends_on_bytes_not_lineage(source):
    record = build(source)
    assert record.raw_value_sha256 == build(
        {**source, "source_offset": 18453}
    ).raw_value_sha256
    changed = build({**source, "raw_value": b"different"})
    assert record.raw_value_sha256 != changed.raw_value_sha256
    assert record.quarantine_id == changed.quarantine_id


def test_empty_and_null_values_are_distinct(source):
    empty = build({**source, "raw_key": b"", "raw_value": b""})
    null = build({**source, "raw_key": None, "raw_value": None})
    assert empty.raw_key == b""
    assert empty.raw_value_sha256 == (
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    )
    assert null.raw_key is None
    assert null.raw_value_sha256 is None


def test_inputs_unchanged_and_output_frozen(source):
    original = source.copy()
    errors = (VERSION_ERROR, UNIT_ERROR)
    result = ValidationResult(errors)
    record = build_quarantine_record(result, **source)

    assert source == original
    assert result.errors == (VERSION_ERROR, UNIT_ERROR)
    assert record.raw_key == original["raw_key"]
    with pytest.raises(FrozenInstanceError):
        record.event_id = "changed"
    with pytest.raises(FrozenInstanceError):
        record.errors[0].code = "changed"


@pytest.mark.parametrize(("field", "value"), [
    ("source_topic", ""), ("source_topic", " \t"), ("source_topic", None),
    ("source_partition", -1), ("source_offset", -1),
    ("source_partition", True), ("source_offset", False),
    ("source_partition", 1.0), ("source_offset", "1"),
    ("schema_id", -1), ("schema_id", True), ("schema_id", 1.0),
    ("schema_id", 2 ** 32), ("event_id", 123),
    ("raw_key", "key"), ("raw_value", "payload"),
    ("raw_key", bytearray(b"key")), ("raw_value", bytearray(b"value")),
])
def test_invalid_builder_arguments_fail_fast(source, field, value):
    with pytest.raises(ValueError, match=field):
        build({**source, field: value})


@pytest.mark.parametrize("schema_id", [None, 0, 1, 2 ** 32 - 1])
def test_wire_schema_id_boundaries_and_zero_lineage(source, schema_id):
    record = build({
        **source, "schema_id": schema_id,
        "source_partition": 0, "source_offset": 0,
    })
    assert record.schema_id == schema_id
    assert record.source_partition == record.source_offset == 0


@pytest.mark.parametrize("result", [
    None, ValidationResult(()), ValidationResult([UNIT_ERROR]),
    ValidationResult(("bad error",)),
    ValidationResult((replace(UNIT_ERROR, code=""),)),
    ValidationResult((replace(UNIT_ERROR, field=None),)),
    ValidationResult((replace(UNIT_ERROR, details=" "),)),
])
def test_invalid_or_successful_results_cannot_be_quarantined(source, result):
    with pytest.raises(ValueError):
        build_quarantine_record(result, **source)
