"""외부 서비스 없이 UTC 경계·집계 키·단위 계약을 검증한다."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.aggregations.windows import (
    assign_metric_window,
    minute_window,
)
from streaming.validation.event_time_normalizer import normalize_event_time


ROOT = Path(__file__).resolve().parents[3]
FIXTURES = ROOT / "tests/fixtures/phase3"
BOUNDARIES = json.loads((FIXTURES / "minute_boundaries.json").read_text())
METRICS = json.loads((FIXTURES / "metric_units.json").read_text())


@pytest.fixture
def rules():
    """실제 validation 설정을 읽되 파일이나 전역 상태를 변경하지 않는다."""
    return json.loads(
        (ROOT / "config/validation_rules.json").read_text()
    )["metric_units"]


@pytest.fixture
def event():
    """기존 Avro 10필드 형태의 wire 이벤트를 제공한다."""
    return {
        "event_id": "phase3-boundary-001",
        "event_time": "2026-10-16T09:00:30+09:00",
        "store_id": "store-001",
        "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius",
        "metric_value": 4.5,
        "unit": "celsius",
        "schema_version": "1.0.0",
        "source": "phase3-fixture",
    }


@pytest.mark.parametrize("case", BOUNDARIES)
@pytest.mark.parametrize("internal", [False, True])
def test_utc_half_open_boundaries(case, internal):
    value = case["time"]
    if internal:
        value = normalize_event_time(value)
    window = minute_window(value)
    assert window.start.isoformat() == case["start"]
    assert window.end.isoformat() == case["end"]
    assert window.start.tzinfo is timezone.utc
    assert window.end.tzinfo is timezone.utc
    assert window.end - window.start == timedelta(minutes=1)
    assert window.start <= normalize_event_time(case["time"]) < window.end
    assert minute_window(window.end).start == window.end


@pytest.mark.parametrize("case", METRICS)
def test_actual_metric_unit_rules(case, event, rules):
    event.update(
        equipment_type=case["type"], metric_name=case["metric"],
        unit=case["unit"],
    )
    if not case["valid"]:
        with pytest.raises(ValueError):
            assign_metric_window(event, metric_units=rules)
        return
    assignment = assign_metric_window(event, metric_units=rules)
    assert assignment.unit == case["unit"]
    assert assignment.key.metric_name == case["metric"]


def test_key_order_and_equivalent_times_without_mutation(event, rules):
    original = deepcopy(event)
    original_rules = deepcopy(rules)
    assignment = assign_metric_window(event, metric_units=rules)
    assert tuple(assignment.key) == (
        datetime(2026, 10, 16, tzinfo=timezone.utc),
        "1m_tumbling", "store-001", "fridge-001", "temperature_celsius",
    )
    for value in (
        "2026-10-16T00:00:30Z",
        datetime.fromisoformat("2026-10-16T09:00:30+09:00"),
        normalize_event_time(event["event_time"]),
    ):
        assert assign_metric_window(
            {**event, "event_time": value}, metric_units=rules,
        ) == assignment
    assert event == original
    assert rules == original_rules


@pytest.mark.parametrize("change", [
    {"equipment_id": "freezer-001", "equipment_type": "freezer"},
    {"store_id": "store-002"},
    {"metric_name": "oil_temperature_celsius", "equipment_type": "fryer"},
    {"event_time": "2026-10-16T00:01:00Z"},
])
def test_key_separates_dimensions(event, rules, change):
    before = assign_metric_window(event, metric_units=rules)
    after = assign_metric_window({**event, **change}, metric_units=rules)
    assert before.key != after.key


def test_unit_mixing_is_rejected_for_same_key(event, rules):
    assign_metric_window(event, metric_units=rules)
    with pytest.raises(ValueError, match="requires unit"):
        assign_metric_window(
            {**event, "unit": "fahrenheit"}, metric_units=rules,
        )
    rules["freezer"]["temperature_celsius"] = "fahrenheit"
    with pytest.raises(ValueError, match="Conflicting units"):
        assign_metric_window(event, metric_units=rules)


@pytest.mark.parametrize("value", [
    "2026-10-16T00:00:00", "invalid", datetime(2026, 10, 16), None, 123,
])
def test_invalid_times_fail(value, event, rules):
    with pytest.raises(ValueError):
        minute_window(value)
    with pytest.raises(ValueError):
        assign_metric_window(
            {**event, "event_time": value}, metric_units=rules,
        )


@pytest.mark.parametrize("field", [
    "store_id", "equipment_id", "equipment_type", "metric_name", "unit",
])
@pytest.mark.parametrize("value", [None, "", " ", 1])
def test_invalid_key_and_unit_fields_fail(field, value, event, rules):
    with pytest.raises(ValueError, match=field):
        assign_metric_window({**event, field: value}, metric_units=rules)
    event.pop(field)
    with pytest.raises(ValueError, match=field):
        assign_metric_window(event, metric_units=rules)
