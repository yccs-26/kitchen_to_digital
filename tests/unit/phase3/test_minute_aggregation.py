"""중복 전달과 충돌을 포함한 유한 입력의 1분 집계 계약을 검증한다."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from streaming.aggregations.minute_aggregation import (
    EventConflictError,
    aggregate_minutes,
)


@pytest.fixture
def rules():
    root = Path(__file__).resolve().parents[3]
    return json.loads(
        (root / "config/validation_rules.json").read_text()
    )["metric_units"]


@pytest.fixture
def event():
    return dict(
        event_id="a", event_time="2026-10-16T09:00:00+09:00",
        store_id="store-001", equipment_id="fridge-001",
        equipment_type="refrigerator", metric_name="temperature_celsius",
        metric_value=2.0, unit="celsius", schema_version="1.0.0",
        source="phase3-fixture",
    )


def test_duplicate_statistics_and_half_open_windows(event, rules):
    events = [
        event,
        {**event, "event_time": "2026-10-16T00:00:00Z", "offset": 99},
        {**event, "event_id": "b", "metric_value": 6.0,
         "event_time": "2026-10-16T00:00:59.999999Z"},
        {**event, "event_id": "c", "metric_value": -3.0,
         "event_time": "2026-10-16T00:01:00Z"},
        event,
    ]
    original = deepcopy(events)
    original_rules = deepcopy(rules)
    result = aggregate_minutes(iter(events), metric_units=rules)
    assert result == aggregate_minutes(reversed(events), metric_units=rules)
    first, second = result
    assert tuple(first.key) == (
        datetime(2026, 10, 16, tzinfo=timezone.utc), "1m_tumbling",
        "store-001", "fridge-001", "temperature_celsius",
    )
    assert (first.count, first.sum, first.avg, first.min, first.max) == (
        2, 8.0, 4.0, 2.0, 6.0,
    )
    assert first.window_end == second.key.window_start
    assert second.window_end == datetime(2026, 10, 16, 0, 2,
                                         tzinfo=timezone.utc)
    assert (second.count, second.sum, second.avg, second.min, second.max) == (
        1, -3.0, -3.0, -3.0, -3.0,
    )
    assert first.unit == second.unit == "celsius"
    assert events == original
    assert rules == original_rules


@pytest.mark.parametrize("field,value", [
    ("event_time", "2026-10-16T00:01:00Z"),
    ("store_id", "store-002"), ("equipment_id", "fridge-002"),
    ("equipment_type", "freezer"), ("metric_name", "other"),
    ("metric_value", 10.0), ("unit", "fahrenheit"),
    ("schema_version", "2.0.0"), ("source", "other"),
])
def test_conflict_in_any_payload_field_fails_whole_call(
    event, rules, field, value,
):
    changed = {**event, field: value}
    with pytest.raises(EventConflictError) as caught:
        aggregate_minutes([event, changed], metric_units=rules)
    assert caught.value.event_id == "a"
    assert caught.value.differing_fields == (field,)


def test_distinct_ids_same_payload_count_separately(event, rules):
    result = aggregate_minutes(
        [event, {**event, "event_id": "b"}], metric_units=rules,
    )
    assert result[0].count == 2
    assert result[0].sum == 4.0


def test_dimensions_and_units_stay_separate(event, rules):
    events = [event, {**event, "event_id": "b", "store_id": "store-002"},
              {**event, "event_id": "c", "equipment_id": "fridge-002"},
              {**event, "event_id": "d", "equipment_type": "hood",
               "metric_name": "fan_rpm", "unit": "rpm"}]
    result = aggregate_minutes(events, metric_units=rules)
    assert len(result) == 4
    assert {row.unit for row in result} == {"celsius", "rpm"}
    assert all(row.count == 1 for row in result)


@pytest.mark.parametrize("value", [True, None, "2", float("nan"),
                                       float("inf"), -float("inf")])
def test_invalid_numeric_values_rejected(event, rules, value):
    with pytest.raises(ValueError, match="metric_value"):
        aggregate_minutes([{**event, "metric_value": value}],
                          metric_units=rules)


def test_invalid_unit_and_conflicting_rules_reuse_existing_contract(
    event, rules,
):
    with pytest.raises(ValueError, match="requires unit"):
        aggregate_minutes([{**event, "unit": "fahrenheit"}],
                          metric_units=rules)
    rules["freezer"]["temperature_celsius"] = "fahrenheit"
    with pytest.raises(ValueError, match="Conflicting units"):
        aggregate_minutes([event], metric_units=rules)


@pytest.mark.parametrize("value", [None, "", " ", 1])
def test_event_id_required(event, rules, value):
    with pytest.raises(ValueError, match="event_id"):
        aggregate_minutes([{**event, "event_id": value}], metric_units=rules)


def test_empty_input_and_independent_calls(event, rules):
    assert aggregate_minutes([], metric_units=rules) == ()
    assert aggregate_minutes([event], metric_units=rules) == aggregate_minutes(
        [event], metric_units=rules,
    )


def test_equivalent_aware_datetime_is_duplicate(event, rules):
    copy = {**event, "event_time": datetime(2026, 10, 16,
                                           tzinfo=timezone.utc)}
    assert aggregate_minutes([event, copy], metric_units=rules)[0].count == 1
