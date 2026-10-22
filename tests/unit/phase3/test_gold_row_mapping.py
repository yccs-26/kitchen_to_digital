"""Gold 물리 매핑·비교 정책의 명세 테스트이며 생산 Mapper가 아니다."""

from copy import deepcopy
from dataclasses import fields
from datetime import datetime, timedelta, timezone
import json
import math
import struct
import sys

import pytest

from streaming.aggregations.minute_aggregation import MinuteAggregate
from streaming.validation.domain_validator import (
    EquipmentSpec,
    validate_domain_event,
)
from test_gold_sink_contract import (
    KEY_FIELDS,
    MemoryGoldReference,
    ROOT,
    minute_row,
    sliding_row,
)


# 별칭과 SQL 타입은 이번 작업의 저장 명세이며 실제 DDL은 아니다.
FIELD_MAPPING = {
    "window_start": ("window_start", "TIMESTAMP_LTZ"),
    "window_type": ("window_type", "STRING"),
    "store_id": ("store_id", "STRING"),
    "equipment_id": ("equipment_id", "STRING"),
    "metric_name": ("metric_name", "STRING"),
    "window_end": ("window_end", "TIMESTAMP_LTZ"),
    "unit": ("unit", "STRING"),
    "count": ("sample_count", "BIGINT"),
    "sum": ("sum_value", "DOUBLE"),
    "min": ("min_value", "DOUBLE"),
    "max": ("max_value", "DOUBLE"),
    "avg": ("avg_value", "DOUBLE"),
}
WINDOW_SPECS = {
    "1m_tumbling": (timedelta(minutes=1), timedelta(minutes=1)),
    "5m_sliding": (timedelta(minutes=5), timedelta(minutes=1)),
}
NUMERIC_FIELDS = ("sum", "min", "max", "avg")


def physical_row_spec(logical):
    """검증된 논리 행의 매핑/정규화만 모델링하며 저장하지 않는다."""
    if set(logical) != set(FIELD_MAPPING):
        raise ValueError("Unexpected logical fields")
    result = {}
    for source, (target, sql_type) in FIELD_MAPPING.items():
        value = logical[source]
        if value is None:
            raise ValueError(f"NULL is forbidden: {source}")
        if sql_type == "STRING":
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Expected nonblank string: {source}")
        elif sql_type == "TIMESTAMP_LTZ":
            if not isinstance(value, datetime) or value.utcoffset() is None:
                raise ValueError(f"Expected aware datetime: {source}")
            value = value.astimezone(timezone.utc)
        elif sql_type == "BIGINT":
            if type(value) is not int or not 0 < value <= 2**63 - 1:
                raise ValueError("Expected positive signed BIGINT count")
        else:
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise ValueError(f"Expected finite number: {source}")
            original = value
            value = float(value)
            if isinstance(original, int) and value != original:
                raise ValueError(
                    "DOUBLE conversion would lose integer precision"
                )
            if not math.isfinite(value):
                raise ValueError(f"Expected finite number: {source}")
            if value == 0.0:
                value = 0.0
        result[target] = value
    derived_avg = result["sum_value"] / result["sample_count"]
    if result["avg_value"] != derived_avg:
        raise ValueError("avg must equal sum / count")
    return result


def normalized_logical_spec(logical):
    physical = physical_row_spec(logical)
    return {source: physical[target]
            for source, (target, _) in FIELD_MAPPING.items()}


def classify_spec(canonical, candidate):
    """기존 exact 비교 참조 모델을 정규화된 행에 그대로 적용한다."""
    store = MemoryGoldReference()
    store.apply_batch([normalized_logical_spec(canonical)])
    return store.classify(normalized_logical_spec(candidate))[0]


def test_mapping_covers_existing_fields_and_preserves_key(minute_row):
    model_fields = {f.name for f in fields(MinuteAggregate)} - {"key"}
    assert set(FIELD_MAPPING) == model_fields | set(KEY_FIELDS)
    physical = physical_row_spec(minute_row)
    assert len(set(physical)) == len(FIELD_MAPPING) == 12
    assert FIELD_MAPPING["count"] == ("sample_count", "BIGINT")
    assert physical["sample_count"] == minute_row["count"] == 3
    assert tuple(physical[field] for field in KEY_FIELDS) == tuple(
        minute_row[field] for field in KEY_FIELDS
    )
    assert physical["avg_value"] == physical["sum_value"] / 3
    assert normalized_logical_spec(minute_row) == minute_row


def test_window_namespace_fixes_duration_slide_and_end(
    minute_row, sliding_row,
):
    assert WINDOW_SPECS == {
        "1m_tumbling": (timedelta(minutes=1), timedelta(minutes=1)),
        "5m_sliding": (timedelta(minutes=5), timedelta(minutes=1)),
    }
    for row in (minute_row, sliding_row):
        duration, slide = WINDOW_SPECS[row["window_type"]]
        assert row["window_end"] == row["window_start"] + duration
        assert row["window_start"].second == 0
        assert row["window_start"].microsecond == 0
        assert slide.total_seconds() == 60
    # 동일 label/end/start로 slide만 바꾸면 행만으로 설정 변경을 식별할 수 없다.
    alternate = (timedelta(minutes=5), timedelta(seconds=30))
    assert alternate != WINDOW_SPECS["5m_sliding"]


def test_identical_row_and_json_roundtrip_remain_noop(minute_row):
    before = deepcopy(minute_row)
    physical = physical_row_spec(minute_row)
    wire = {
        name: value.isoformat() if isinstance(value, datetime) else value
        for name, value in physical.items()
    }
    decoded = json.loads(json.dumps(wire, allow_nan=False))
    for name in ("window_start", "window_end"):
        decoded[name] = datetime.fromisoformat(decoded[name])
    logical = {source: decoded[target]
               for source, (target, _) in FIELD_MAPPING.items()}
    assert classify_spec(minute_row, logical) == "NO-OP"
    for field in NUMERIC_FIELDS:
        column = FIELD_MAPPING[field][0]
        assert struct.pack("!d", decoded[column]) == struct.pack(
            "!d", physical[column],
        )
    assert minute_row == before
    assert json.loads("0.3") == json.loads("3e-1")


@pytest.mark.parametrize("changes", [
    {"count": 4, "sum": 16.0},
    {"sum": 15.0, "avg": 5.0, "max": 7.0},
    {"unit": "fahrenheit"},
    {"window_end": datetime(2026, 10, 20, 12, 2, tzinfo=timezone.utc)},
])
def test_changed_invariants_conflict(minute_row, changes):
    assert classify_spec(minute_row, {**minute_row, **changes}) == "CONFLICT"


def test_signed_zero_normalizes_to_positive_zero(minute_row):
    positive = {**minute_row, **dict.fromkeys(NUMERIC_FIELDS, 0.0)}
    negative = {**minute_row, **dict.fromkeys(NUMERIC_FIELDS, -0.0)}
    assert classify_spec(positive, negative) == "NO-OP"
    physical = physical_row_spec(negative)
    for field in NUMERIC_FIELDS:
        value = physical[FIELD_MAPPING[field][0]]
        assert struct.pack("!d", value) == struct.pack("!d", 0.0)


def test_one_ulp_difference_is_not_hidden_by_tolerance(minute_row):
    changed_sum = math.nextafter(minute_row["sum"], math.inf)
    changed = {**minute_row, "sum": changed_sum,
               "avg": changed_sum / minute_row["count"]}
    # 기존 수치 검산용 tolerance는 가까운 다른 확정값도 같게 볼 수 있다.
    assert math.isclose(minute_row["sum"], changed_sum,
                        rel_tol=1e-12, abs_tol=1e-12)
    assert classify_spec(minute_row, changed) == "CONFLICT"


def test_aggregation_order_difference_is_a_fail_closed_conflict(minute_row):
    ordinary = sum([0.1, 0.2, 0.3])
    stable = math.fsum([0.1, 0.2, 0.3])
    assert ordinary != stable
    canonical = {**minute_row, "sum": stable, "avg": stable / 3,
                 "min": 0.1, "max": 0.3}
    recomputed = {**canonical, "sum": ordinary, "avg": ordinary / 3}
    assert classify_spec(canonical, recomputed) == "CONFLICT"
    assert sys.float_info.mant_dig == 53
    assert sys.float_info.max_exp == 1024


def test_avg_is_validated_as_derived_value(minute_row):
    with pytest.raises(ValueError, match="sum / count"):
        physical_row_spec({**minute_row, "avg": 999.0})


@pytest.mark.parametrize("count", [None, True, 0, -1, 3.0, 2**63])
def test_invalid_count_is_rejected(minute_row, count):
    with pytest.raises(ValueError):
        physical_row_spec({**minute_row, "count": count})


def test_bigint_count_never_uses_double_equality(minute_row):
    row = {**minute_row, **dict.fromkeys(NUMERIC_FIELDS, 0.0)}
    row["count"] = 2**53 + 1
    physical = physical_row_spec(row)
    encoded = json.dumps({"sample_count": physical["sample_count"]})
    assert json.loads(encoded)["sample_count"] == 2**53 + 1
    assert physical["sample_count"] != int(float(row["count"]))
    other = {**row, "count": 2**53}
    assert classify_spec(row, other) == "CONFLICT"


def test_integer_statistics_require_lossless_double_conversion(minute_row):
    exact = {**minute_row, "count": 1,
             **dict.fromkeys(NUMERIC_FIELDS, 2**53)}
    assert physical_row_spec(exact)["sum_value"] == 2**53
    lossy = {**exact, **dict.fromkeys(NUMERIC_FIELDS, 2**53 + 1)}
    with pytest.raises(ValueError, match="lose integer precision"):
        physical_row_spec(lossy)


@pytest.mark.parametrize("field", tuple(FIELD_MAPPING))
def test_all_proposed_columns_reject_null(minute_row, field):
    with pytest.raises(ValueError, match="NULL"):
        physical_row_spec({**minute_row, field: None})


@pytest.mark.parametrize("field", NUMERIC_FIELDS)
@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_aggregate_rejected_before_comparison(
    minute_row, field, value,
):
    with pytest.raises(ValueError, match="finite"):
        physical_row_spec({**minute_row, field: value})


@pytest.mark.parametrize("value,code", [
    (None, "NULL_FIELD"), (True, "INVALID_METRIC_VALUE"),
    (float("nan"), "NON_FINITE_METRIC_VALUE"),
    (float("inf"), "NON_FINITE_METRIC_VALUE"),
    (-float("inf"), "NON_FINITE_METRIC_VALUE"),
])
def test_existing_phase2_validator_rejects_invalid_metric(value, code):
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase3/multi_batch_events.json").read_text()
    )
    event = deepcopy(fixture["scenarios"][0]["steps"][0]["events"][0])
    event["event_time"] = datetime.fromisoformat(event["event_time"])
    event["metric_value"] = value
    rules = json.loads((ROOT / "config/validation_rules.json").read_text())
    registry = {
        entry["equipment_id"]: EquipmentSpec(
            entry["store_id"], entry["equipment_type"],
        ) for entry in rules["equipment_registry"]
    }
    result = validate_domain_event(
        event, event["equipment_id"].encode(),
        supported_schema_versions=rules["supported_schema_versions"],
        equipment_registry=registry, metric_units=rules["metric_units"],
        reference_time=event["event_time"],
        max_future_skew=timedelta(minutes=5),
    )
    assert not result.is_valid
    assert any(error.field == "metric_value" and error.code == code
               for error in result.errors)
