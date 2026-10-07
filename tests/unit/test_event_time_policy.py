"""Spark watermark 실행과 독립적인 순수 시간 정책 계약 테스트."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from streaming.validation.domain_validator import (
    EquipmentSpec,
    validate_domain_event,
    validate_event_time,
)
from streaming.validation.event_time_policy import (
    EventTimeClassification,
    EventTimeStatus,
    classify_event_time,
)


REFERENCE = datetime(2026, 10, 7, 12, tzinfo=timezone.utc)
LATENESS = timedelta(minutes=10)
SKEW = timedelta(minutes=5)
MICROSECOND = timedelta(microseconds=1)


def classify(value: datetime) -> EventTimeClassification:
    return classify_event_time(
        value, reference_time=REFERENCE,
        allowed_lateness=LATENESS, max_future_skew=SKEW,
    )


@pytest.mark.parametrize(("age", "status"), [
    (timedelta(minutes=5), EventTimeStatus.ON_TIME),
    (timedelta(minutes=20), EventTimeStatus.LATE),
    (LATENESS, EventTimeStatus.ON_TIME),
    (LATENESS + MICROSECOND, EventTimeStatus.LATE),
    (LATENESS - MICROSECOND, EventTimeStatus.ON_TIME),
    (timedelta(0), EventTimeStatus.ON_TIME),
    (-timedelta(seconds=1), EventTimeStatus.ON_TIME),
    (-SKEW, EventTimeStatus.ON_TIME),
    (-SKEW - MICROSECOND, EventTimeStatus.FUTURE_INVALID),
])
def test_time_boundaries(age, status) -> None:
    assert classify(REFERENCE - age) == EventTimeClassification(
        status, status is not EventTimeStatus.FUTURE_INVALID
    )


@pytest.mark.parametrize(("delta", "status"), [
    (-MICROSECOND, EventTimeStatus.LATE),
    (timedelta(0), EventTimeStatus.ON_TIME),
    (MICROSECOND, EventTimeStatus.ON_TIME),
])
def test_zero_allowed_lateness(delta, status) -> None:
    assert classify_event_time(
        REFERENCE + delta, reference_time=REFERENCE,
        allowed_lateness=timedelta(0), max_future_skew=SKEW,
    ) == EventTimeClassification(status, True)


@pytest.mark.parametrize("field", ["event_time", "reference_time"])
@pytest.mark.parametrize("bad", [REFERENCE.replace(tzinfo=None), "invalid"])
def test_invalid_timestamps_fail_fast(field, bad) -> None:
    arguments = {
        "event_time": REFERENCE, "reference_time": REFERENCE,
        "allowed_lateness": LATENESS, "max_future_skew": SKEW,
    }
    arguments[field] = bad
    with pytest.raises(ValueError):
        classify_event_time(**arguments)


@pytest.mark.parametrize("field", ["allowed_lateness", "max_future_skew"])
def test_negative_durations_fail_fast(field) -> None:
    arguments = {
        "event_time": REFERENCE, "reference_time": REFERENCE,
        "allowed_lateness": LATENESS, "max_future_skew": SKEW,
    }
    arguments[field] = -MICROSECOND
    with pytest.raises(ValueError, match=field):
        classify_event_time(**arguments)


def test_zero_future_skew_excludes_any_future_time() -> None:
    result = classify_event_time(
        REFERENCE + MICROSECOND, reference_time=REFERENCE,
        allowed_lateness=LATENESS, max_future_skew=timedelta(0),
    )
    assert result == EventTimeClassification(
        EventTimeStatus.FUTURE_INVALID, False
    )


def test_equivalent_aware_offsets_have_same_classification() -> None:
    kst = timezone(timedelta(hours=9))
    event_time = REFERENCE - LATENESS
    assert classify_event_time(
        event_time.astimezone(kst),
        reference_time=REFERENCE.astimezone(kst),
        allowed_lateness=LATENESS, max_future_skew=SKEW,
    ) == classify(event_time)


def test_2099_poison_cannot_advance_candidate_maximum() -> None:
    poison = datetime(2099, 1, 1, tzinfo=timezone.utc)
    normal = REFERENCE - timedelta(minutes=5)
    assert classify(poison) == EventTimeClassification(
        EventTimeStatus.FUTURE_INVALID, False
    )
    # 테스트용 관측 모의 계산이며, Spark watermark나 상태 검증 증빙은 아니다.
    candidates = [
        value for value in (REFERENCE, poison, normal)
        if classify(value).is_watermark_candidate
    ]
    assert max(candidates) == REFERENCE
    assert classify(normal) == EventTimeClassification(
        EventTimeStatus.ON_TIME, True
    )


@pytest.mark.parametrize("year", [2000, 2026, 2099])
def test_only_injected_reference_controls_classification(year) -> None:
    reference = REFERENCE.replace(year=year)
    assert classify_event_time(
        reference - timedelta(minutes=5), reference_time=reference,
        allowed_lateness=LATENESS, max_future_skew=SKEW,
    ) == EventTimeClassification(EventTimeStatus.ON_TIME, True)


@pytest.mark.parametrize("age", [timedelta(0), LATENESS * 2, -SKEW * 2])
def test_deterministic_and_no_input_mutation(age) -> None:
    arguments = {
        "event_time": REFERENCE - age, "reference_time": REFERENCE,
        "allowed_lateness": LATENESS, "max_future_skew": SKEW,
    }
    original = deepcopy(arguments)
    first = classify_event_time(**arguments)
    assert classify_event_time(**arguments) == first
    assert arguments == original


@pytest.mark.parametrize("delta", [
    -LATENESS * 2, SKEW, SKEW + MICROSECOND,
    datetime(2099, 1, 1, tzinfo=timezone.utc) - REFERENCE,
])
def test_domain_and_policy_share_future_validity(delta) -> None:
    event_time = REFERENCE + delta
    event = {
        "event_id": "time-policy-test-1", "event_time": event_time,
        "store_id": "store-001", "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius", "metric_value": 4.2,
        "unit": "celsius", "schema_version": "1.0.0",
        "source": "simulator",
    }
    domain = validate_domain_event(
        event, b"fridge-001", supported_schema_versions={"1.0.0"},
        equipment_registry={
            "fridge-001": EquipmentSpec("store-001", "refrigerator")
        },
        metric_units={"refrigerator": {"temperature_celsius": "celsius"}},
        reference_time=REFERENCE, max_future_skew=SKEW,
    )
    shared = validate_event_time(
        event_time, reference_time=REFERENCE, max_future_skew=SKEW
    )
    assert domain == shared
    assert classify(event_time).is_watermark_candidate is domain.is_valid
    if not domain.is_valid:
        assert [error.code for error in domain.errors] == [
            "FUTURE_EVENT_TIME"
        ]
