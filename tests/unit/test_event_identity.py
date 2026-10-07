"""Offline event identity contracts; no persistent state or sink writes."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType

import pytest

from streaming.validation.event_identity import (
    PAYLOAD_FIELDS,
    EventIdentityResult,
    EventIdentityStatus,
    classify_event_identity,
)
from streaming.validation.event_time_normalizer import normalize_event_time


@pytest.fixture
def event() -> dict[str, object]:
    return {
        "event_id": "identity-test-1",
        "event_time": datetime(2026, 10, 7, 12, tzinfo=timezone.utc),
        "store_id": "store-001",
        "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius",
        "metric_value": 4.2,
        "unit": "celsius",
        "schema_version": "1.0.0",
        "source": "simulator",
    }


def test_projection_covers_current_avro_payload() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "schemas/avro/sensor_metric_event.avsc").read_text()
    )
    assert PAYLOAD_FIELDS == tuple(
        field["name"] for field in schema["fields"]
        if field["name"] != "event_id"
    )


def test_missing_canonical_is_new(event) -> None:
    assert classify_event_identity(event, None) == EventIdentityResult(
        EventIdentityStatus.NEW
    )


def test_identical_payload_is_duplicate(event) -> None:
    assert classify_event_identity(event, dict(event)) == EventIdentityResult(
        EventIdentityStatus.DUPLICATE
    )


@pytest.mark.parametrize(("field", "value"), [
    ("event_time", datetime(2026, 10, 7, 12, 1, tzinfo=timezone.utc)),
    ("store_id", "store-002"),
    ("equipment_id", "fridge-002"),
    ("equipment_type", "oven"),
    ("metric_name", "humidity"),
    ("metric_value", 4.2000001),
    ("unit", "fahrenheit"),
    ("schema_version", "1.0.1"),
    ("source", "sensor"),
])
def test_each_payload_difference_is_conflict(event, field, value) -> None:
    incoming = {**event, field: value}
    assert classify_event_identity(incoming, event) == EventIdentityResult(
        EventIdentityStatus.CONFLICT, (field,)
    )


def test_different_event_id_is_invalid_comparison(event) -> None:
    incoming = {**event, "event_id": "identity-test-2"}
    with pytest.raises(ValueError, match="same event_id"):
        classify_event_identity(incoming, event)


def test_insertion_order_does_not_change_identity(event) -> None:
    reordered = dict(reversed(list(event.items())))
    assert classify_event_identity(reordered, event).status is (
        EventIdentityStatus.DUPLICATE
    )


@pytest.mark.parametrize(("field", "old", "new"), [
    ("topic", "kitchen.sensor.raw", "kitchen.sensor.reprocess"),
    ("partition", 0, 1),
    ("offset", 10, 20),
    ("ingested_at", "2026-10-07T12:00:00Z", "2026-10-07T13:00:00Z"),
    ("kafka_timestamp", 1000, 2000),
    ("headers", [("attempt", b"1")], [("attempt", b"2")]),
])
def test_transport_metadata_is_excluded(event, field, old, new) -> None:
    assert classify_event_identity(
        {**event, field: new}, {**event, field: old}
    ) == EventIdentityResult(EventIdentityStatus.DUPLICATE)
    assert classify_event_identity(
        {**event, field: new}, event
    ).status is EventIdentityStatus.DUPLICATE


def test_equivalent_timestamp_encodings_after_normalization(event) -> None:
    canonical = {
        **event, "event_time": normalize_event_time("2026-10-07T12:00:00Z")
    }
    incoming = {
        **event,
        "event_time": normalize_event_time("2026-10-07T21:00:00.000+09:00"),
    }
    assert classify_event_identity(incoming, canonical).status is (
        EventIdentityStatus.DUPLICATE
    )


@pytest.mark.parametrize(("old", "new"), [(4, 4.0), (-0.0, 0.0)])
def test_equal_numeric_values_are_duplicate(event, old, new) -> None:
    assert classify_event_identity(
        {**event, "metric_value": new}, {**event, "metric_value": old}
    ).status is EventIdentityStatus.DUPLICATE


def test_multiple_differences_have_stable_order(event) -> None:
    incoming = dict(reversed(list(event.items())))
    incoming.update(source="sensor", metric_value=8, unit="fahrenheit")
    expected = EventIdentityResult(
        EventIdentityStatus.CONFLICT, ("metric_value", "unit", "source")
    )
    assert classify_event_identity(incoming, event) == expected
    assert classify_event_identity(incoming, event) == expected
    assert classify_event_identity(event, incoming) == expected


@pytest.mark.parametrize("status", list(EventIdentityStatus))
def test_classification_does_not_mutate_either_input(event, status) -> None:
    event["headers"] = [("attempt", b"1")]
    canonical = (
        deepcopy(event) if status is not EventIdentityStatus.NEW else None
    )
    if status is EventIdentityStatus.CONFLICT:
        event["metric_value"] = 9.5
        event["event_time"] += timedelta(seconds=1)
    before_event, before_canonical = deepcopy(event), deepcopy(canonical)

    result = classify_event_identity(event, canonical)

    assert result.status is status
    assert event == before_event
    assert canonical == before_canonical


def test_read_only_mappings_are_supported(event) -> None:
    assert classify_event_identity(
        MappingProxyType(event), MappingProxyType(dict(event))
    ).status is EventIdentityStatus.DUPLICATE


@pytest.mark.parametrize("side", ["incoming", "canonical"])
def test_missing_payload_field_does_not_become_duplicate(event, side) -> None:
    incoming, canonical = dict(event), dict(event)
    del (incoming if side == "incoming" else canonical)["source"]
    with pytest.raises(KeyError, match="source"):
        classify_event_identity(incoming, canonical)
