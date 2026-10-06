"""Offline domain contracts, independent of Kafka, Spark, and Registry."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone, tzinfo

import pytest

from streaming.validation.domain_validator import (
    EquipmentSpec,
    ValidationError,
    ValidationResult,
    validate_domain_event,
)


FIELDS = (
    "event_id", "event_time", "store_id", "equipment_id",
    "equipment_type", "metric_name", "metric_value", "unit",
    "schema_version", "source",
)
STRING_FIELDS = tuple(
    field for field in FIELDS if field not in {"event_time", "metric_value"}
)
NOW = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
KEY = b"fridge-001"


@pytest.fixture
def event():
    return {
        "event_id": "domain-test-1",
        "event_time": NOW,
        "store_id": "store-001",
        "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius",
        "metric_value": 4.2,
        "unit": "celsius",
        "schema_version": "1.0.0",
        "source": "simulator",
    }


@pytest.fixture
def rules():
    return {
        "supported_schema_versions": {"1.0.0"},
        "equipment_registry": {
            "fridge-001": EquipmentSpec("store-001", "refrigerator"),
            "hood-001": EquipmentSpec("store-001", "hood"),
        },
        "metric_units": {
            "refrigerator": {"temperature_celsius": "celsius"},
            "hood": {"fan_rpm": "rpm"},
        },
        "reference_time": NOW,
        "max_future_skew": timedelta(minutes=5),
    }


def assert_errors(result, *expected):
    assert not result.is_valid
    assert [(error.code, error.field) for error in result.errors] == list(
        expected
    )
    assert all(error.details for error in result.errors)


def test_normal_decoded_event_and_matching_key_are_valid(event, rules):
    result = validate_domain_event(event, KEY, **rules)

    assert isinstance(result, ValidationResult)
    assert result.is_valid
    assert result.errors == ()


def test_high_fridge_temperature_is_valid_contract_data(event, rules):
    event["metric_value"] = 15.0
    assert validate_domain_event(event, KEY, **rules).is_valid


@pytest.mark.parametrize("field", FIELDS)
def test_each_required_field_must_be_present(event, rules, field):
    del event[field]
    assert_errors(
        validate_domain_event(event, KEY, **rules), ("MISSING_FIELD", field)
    )


@pytest.mark.parametrize("field", FIELDS)
def test_each_required_field_rejects_none(event, rules, field):
    event[field] = None
    assert_errors(
        validate_domain_event(event, KEY, **rules), ("NULL_FIELD", field)
    )


@pytest.mark.parametrize("field", STRING_FIELDS)
@pytest.mark.parametrize("value", ["", " \t\n"])
def test_required_strings_reject_blank_values(event, rules, field, value):
    event[field] = value
    assert_errors(
        validate_domain_event(event, KEY, **rules), ("EMPTY_FIELD", field)
    )


@pytest.mark.parametrize("field", STRING_FIELDS)
def test_string_fields_reject_non_strings_without_cascades(
    event, rules, field
):
    event[field] = []
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("INVALID_FIELD_TYPE", field),
    )


def test_schema_support_is_injected(event, rules):
    event["schema_version"] = "2.0.0"
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("UNSUPPORTED_SCHEMA_VERSION", "schema_version"),
    )
    rules["supported_schema_versions"] = {"2.0.0"}
    assert validate_domain_event(event, KEY, **rules).is_valid


class NoOffsetTimezone(tzinfo):
    def utcoffset(self, dt):
        return None


@pytest.mark.parametrize("value", [
    "2026-10-07T12:00:00Z", 123, NOW.replace(tzinfo=None),
    NOW.replace(tzinfo=NoOffsetTimezone()),
    datetime.min.replace(tzinfo=timezone(timedelta(hours=9))),
])
def test_event_time_rejects_wrong_naive_or_unrepresentable_values(
    event, rules, value
):
    event["event_time"] = value
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("INVALID_EVENT_TIME", "event_time"),
    )


@pytest.mark.parametrize("value", [
    NOW,
    NOW.astimezone(timezone(timedelta(hours=9))),
    NOW - timedelta(days=3650),
    NOW + timedelta(minutes=5),
])
def test_aware_old_and_exact_future_boundary_times_are_valid(
    event, rules, value
):
    event["event_time"] = value
    assert validate_domain_event(event, KEY, **rules).is_valid


def test_future_time_just_past_tolerance_is_rejected(event, rules):
    event["event_time"] = NOW + timedelta(minutes=5, microseconds=1)
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("FUTURE_EVENT_TIME", "event_time"),
    )


def test_reference_time_and_future_tolerance_are_injected(event, rules):
    event["event_time"] = NOW + timedelta(minutes=6)
    rules["max_future_skew"] = timedelta(minutes=6)
    assert validate_domain_event(event, KEY, **rules).is_valid
    rules["max_future_skew"] = timedelta(0)
    rules["reference_time"] = event["event_time"].astimezone(
        timezone(timedelta(hours=9))
    )
    assert validate_domain_event(event, KEY, **rules).is_valid


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_non_finite_metric_values_are_rejected(event, rules, value):
    event["metric_value"] = value
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("NON_FINITE_METRIC_VALUE", "metric_value"),
    )


@pytest.mark.parametrize("value", [True, False, "4.2", [], 1 + 2j])
def test_non_numeric_and_boolean_metric_values_are_rejected(
    event, rules, value
):
    event["metric_value"] = value
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("INVALID_METRIC_VALUE", "metric_value"),
    )


@pytest.mark.parametrize("value", [4, 4.2, 15.0, -4.2, 0, 10 ** 400])
def test_finite_ints_and_floats_are_valid(event, rules, value):
    event["metric_value"] = value
    assert validate_domain_event(event, KEY, **rules).is_valid


def test_unknown_equipment_is_rejected(event, rules):
    event["equipment_id"] = "fridge-999"
    assert_errors(
        validate_domain_event(event, b"fridge-999", **rules),
        ("UNKNOWN_EQUIPMENT", "equipment_id"),
    )


def test_store_must_match_equipment_registry(event, rules):
    event["store_id"] = "store-002"
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("STORE_MISMATCH", "store_id"),
    )


def test_equipment_type_must_match_registry(event, rules):
    event.update(
        equipment_type="hood", metric_name="fan_rpm", unit="rpm"
    )
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("EQUIPMENT_TYPE_MISMATCH", "equipment_type"),
    )


@pytest.mark.parametrize("metric", ["humidity", "fan_rpm"])
def test_metric_must_be_allowed_for_equipment_type(event, rules, metric):
    event["metric_name"] = metric
    event["unit"] = "rpm"
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("UNSUPPORTED_METRIC", "metric_name"),
    )


def test_unit_must_match_metric(event, rules):
    event["unit"] = "fahrenheit"
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("UNIT_MISMATCH", "unit"),
    )


def test_registry_and_metric_rules_are_injected(event, rules):
    event.update(
        equipment_id="hood-001", equipment_type="hood",
        metric_name="fan_rpm", unit="rpm",
    )
    assert validate_domain_event(event, b"hood-001", **rules).is_valid


def test_unconfigured_type_does_not_allow_arbitrary_metrics(event, rules):
    rules["metric_units"].clear()
    assert_errors(
        validate_domain_event(event, KEY, **rules),
        ("UNSUPPORTED_METRIC", "metric_name"),
    )


@pytest.mark.parametrize(("key", "code"), [
    (None, "MISSING_KAFKA_KEY"),
    (b"\xff", "INVALID_KAFKA_KEY"),
    ("fridge-001", "INVALID_KAFKA_KEY"),
    (b"fridge-002", "KEY_MISMATCH"),
    (b"", "KEY_MISMATCH"),
    (b"fridge-001 ", "KEY_MISMATCH"),
])
def test_kafka_key_failures_are_structured(event, rules, key, code):
    assert_errors(
        validate_domain_event(event, key, **rules), (code, "kafka_key")
    )


def test_invalid_key_is_reported_even_when_equipment_id_is_missing(
    event, rules
):
    del event["equipment_id"]
    assert_errors(
        validate_domain_event(event, b"\xff", **rules),
        ("MISSING_FIELD", "equipment_id"),
        ("INVALID_KAFKA_KEY", "kafka_key"),
    )


def test_independent_errors_are_collected_in_stable_order(event, rules):
    event.update(schema_version="unknown", metric_value=float("nan"))
    result = validate_domain_event(event, None, **rules)
    assert_errors(
        result,
        ("UNSUPPORTED_SCHEMA_VERSION", "schema_version"),
        ("NON_FINITE_METRIC_VALUE", "metric_value"),
        ("MISSING_KAFKA_KEY", "kafka_key"),
    )
    assert all(isinstance(error, ValidationError) for error in result.errors)
    assert result == validate_domain_event(event, None, **rules)


def test_empty_event_has_only_missing_fields_and_missing_key(rules):
    assert_errors(
        validate_domain_event({}, None, **rules),
        *(("MISSING_FIELD", field) for field in FIELDS),
        ("MISSING_KAFKA_KEY", "kafka_key"),
    )


@pytest.mark.parametrize("invalid", [False, True])
def test_validation_does_not_mutate_event_or_rules(event, rules, invalid):
    event["event_time"] = NOW.astimezone(timezone(timedelta(hours=9)))
    event["extra_metadata"] = {"tags": ["original"]}
    if invalid:
        event.update(metric_value="4.2", schema_version="unsupported")
    original_event, original_rules = deepcopy(event), deepcopy(rules)
    result = validate_domain_event(event, KEY, **rules)
    assert result.is_valid is not invalid
    assert event == original_event
    assert event["event_time"].tzinfo == original_event["event_time"].tzinfo
    assert rules == original_rules


@pytest.mark.parametrize(("name", "value"), [
    ("reference_time", NOW.replace(tzinfo=None)),
    ("reference_time", datetime.min.replace(
        tzinfo=timezone(timedelta(hours=9))
    )),
    ("max_future_skew", timedelta(microseconds=-1)),
])
def test_invalid_clock_configuration_raises_instead_of_quarantining(
    event, rules, name, value
):
    rules[name] = value
    with pytest.raises(ValueError):
        validate_domain_event(event, KEY, **rules)
