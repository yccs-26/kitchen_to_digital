"""Gold 저장 명세의 테스트 전용 참조 모델이며 실제 sink 검증이 아니다.

생산용 Gold helper가 없어 INSERT/NO-OP/CONFLICT와 메모리 부분 실패를 모델링한다.
전제는 upstream에서 확정·검증한 집계다. closed 여부나 Delta 원자성은 검증하지 않는다.
근접 부동소수점 허용오차·정책 버전·물리 DDL은 선택하지 않는다.
"""

from copy import deepcopy
from dataclasses import asdict, fields
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.aggregations.minute_aggregation import (
    MinuteAggregate,
    aggregate_minutes,
)
from streaming.aggregations.windows import AggregationKey


ROOT = Path(__file__).resolve().parents[3]
KEY_FIELDS = AggregationKey._fields
VALUE_FIELDS = tuple(
    field.name for field in fields(MinuteAggregate) if field.name != "key"
)


def natural_key(row):
    """기존 생산 모델의 5개 키 필드를 순서 그대로 투영한다."""
    key = tuple(row[field] for field in KEY_FIELDS)
    if any(value is None or value == "" for value in key):
        raise ValueError("Gold natural key must not be null or empty")
    return key


class ContractConflict(ValueError):
    """같은 자연키의 상이한 확정 속성을 나타내는 테스트 전용 오류다."""

    def __init__(self, key, differing_fields):
        self.key = key
        self.differing_fields = differing_fields
        super().__init__(f"Gold conflict: {key}, {differing_fields}")


class DuplicateBatchKey(ValueError):
    """쓰기 전 source batch의 중복 자연키를 거부한다."""


class InjectedFailure(RuntimeError):
    """메모리 쓰기 prefix 뒤에 주입하는 일시적 테스트 오류다."""


class MemoryGoldReference:
    """테스트 전용 명세 모델. durable sink나 원자적 batch를 구현하지 않는다."""

    def __init__(self):
        self._rows = {}
        self.insert_count = 0

    @property
    def snapshot(self):
        return deepcopy(self._rows)

    def classify(self, row):
        canonical = self._rows.get(natural_key(row))
        if canonical is None:
            return "INSERT", ()
        differing = tuple(
            field for field in VALUE_FIELDS
            if canonical[field] != row[field]
        )
        return ("CONFLICT", differing) if differing else ("NO-OP", ())

    def apply_batch(self, rows, *, fail_after=None):
        batch = deepcopy(list(rows))
        keys = [natural_key(row) for row in batch]
        if len(keys) != len(set(keys)):
            raise DuplicateBatchKey("Source natural key appears twice")
        decisions = []
        for index, row in enumerate(batch):
            if index == fail_after:
                raise InjectedFailure(f"after {index} completed rows")
            decision, differing = self.classify(row)
            if decision == "CONFLICT":
                raise ContractConflict(natural_key(row), differing)
            if decision == "INSERT":
                self._rows[natural_key(row)] = deepcopy(row)
                self.insert_count += 1
            decisions.append(decision)
        if fail_after == len(batch):
            raise InjectedFailure("after all rows, before acknowledgement")
        return decisions


@pytest.fixture
def minute_row():
    """기존 fixture로 실제 MinuteAggregate를 만들고 필드를 펼친다."""
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase3/multi_batch_events.json").read_text()
    )
    normal = next(s for s in fixture["scenarios"] if s["name"] == "normal")
    events = []
    for step in normal["steps"]:
        for event in step["events"]:
            if event["equipment_id"] != fixture["target_equipment_id"]:
                continue
            instant = datetime.fromisoformat(event["event_time"]).replace(
                year=2026, month=10, day=20, hour=12, minute=0,
            )
            events.append({**event, "event_time": instant})
    rules = json.loads(
        (ROOT / "config/validation_rules.json").read_text()
    )["metric_units"]
    result, = aggregate_minutes(events, metric_units=rules)
    attributes = asdict(result)
    attributes.pop("key")
    return {**result.key._asdict(), **attributes}


@pytest.fixture
def sliding_row():
    """기존 sliding 수동 기대값의 유효한 행을 재사용한다."""
    fixture = json.loads(
        (ROOT / "tests/fixtures/phase3/sliding_window_events.json").read_text()
    )
    row = next(
        row for row in fixture["expected_rows"]
        if row["equipment_id"] == "fridge-001"
        and row["window_start"] == "2026-10-20T12:00:00+00:00"
    )
    fields_to_copy = (*KEY_FIELDS, *VALUE_FIELDS)
    result = {field: row[field] for field in fields_to_copy}
    for name in ("window_start", "window_end"):
        result[name] = datetime.fromisoformat(result[name])
    return result


def test_existing_key_and_aggregate_fields_are_reused(minute_row):
    assert KEY_FIELDS == (
        "window_start", "window_type", "store_id", "equipment_id",
        "metric_name",
    )
    assert set(minute_row) == set(KEY_FIELDS) | set(VALUE_FIELDS)
    assert {"unit", "window_end"} <= set(VALUE_FIELDS)
    assert "unit" not in KEY_FIELDS
    assert "window_end" not in KEY_FIELDS
    assert minute_row["avg"] == minute_row["sum"] / minute_row["count"]
    assert (minute_row["count"], minute_row["sum"], minute_row["avg"]) == (
        3, 12.0, 4.0,
    )


def test_new_final_inserts_absolute_values_and_repeat_is_noop(minute_row):
    store = MemoryGoldReference()
    assert store.apply_batch([minute_row]) == ["INSERT"]
    initial = store.snapshot
    assert initial == {natural_key(minute_row): minute_row}
    assert store.apply_batch([deepcopy(minute_row)]) == ["NO-OP"]
    assert store.snapshot == initial
    assert store.insert_count == 1


@pytest.mark.parametrize("changes,differing", [
    ({"count": 4, "sum": 16.0}, ("count", "sum")),
    ({"sum": 15.0, "avg": 5.0, "max": 7.0}, ("sum", "avg", "max")),
    ({"min": 1.0}, ("min",)),
    ({"max": 7.0}, ("max",)),
    ({"unit": "fahrenheit"}, ("unit",)),
    ({"window_end": datetime(2026, 10, 20, 12, 2,
                             tzinfo=timezone.utc)}, ("window_end",)),
])
def test_changed_final_conflicts_and_preserves_canonical(
    minute_row, changes, differing,
):
    store = MemoryGoldReference()
    store.apply_batch([minute_row])
    before = store.snapshot
    changed = {**minute_row, **changes}
    assert natural_key(changed) == natural_key(minute_row)
    assert store.classify(changed) == ("CONFLICT", differing)
    with pytest.raises(ContractConflict) as caught:
        store.apply_batch([changed])
    assert caught.value.differing_fields == differing
    assert store.snapshot == before
    assert store.insert_count == 1


@pytest.mark.parametrize("field,value", [
    ("store_id", "store-002"),
    ("equipment_id", "fridge-002"),
    ("metric_name", "oil_temperature_celsius"),
    ("window_start", datetime(2026, 10, 20, 12, 1,
                              tzinfo=timezone.utc)),
])
def test_distinct_key_dimensions_insert_separately(minute_row, field, value):
    other = {**minute_row, field: value}
    if field == "window_start":
        other["window_end"] = value + timedelta(minutes=1)
    store = MemoryGoldReference()
    assert store.apply_batch([minute_row, other]) == ["INSERT", "INSERT"]
    assert len(store.snapshot) == 2


def test_different_metric_and_unit_do_not_mix(minute_row):
    other = {**minute_row, "metric_name": "fan_rpm", "unit": "rpm"}
    store = MemoryGoldReference()
    store.apply_batch([minute_row, other])
    assert len(store.snapshot) == 2
    assert store.snapshot[natural_key(minute_row)]["unit"] == "celsius"
    assert store.snapshot[natural_key(other)]["unit"] == "rpm"
    assert all(row["count"] == 3 for row in store.snapshot.values())


def test_valid_window_resolutions_share_start_without_collision(
    minute_row, sliding_row,
):
    assert minute_row["window_start"] == sliding_row["window_start"]
    assert minute_row["window_end"] != sliding_row["window_end"]
    assert natural_key(minute_row) != natural_key(sliding_row)
    store = MemoryGoldReference()
    assert store.apply_batch([minute_row, sliding_row]) == ["INSERT", "INSERT"]
    assert len(store.snapshot) == 2
    assert store.apply_batch([sliding_row, minute_row]) == ["NO-OP", "NO-OP"]


def test_type_discriminator_even_for_identical_boundary_projection(minute_row):
    # 키 투영만 확인한다. 1분 길이 행을 유효한 5분 집계로 저장하지 않는다.
    projected = {**minute_row, "window_type": "5m_sliding"}
    assert projected["window_end"] == minute_row["window_end"]
    assert natural_key(projected) != natural_key(minute_row)


def test_same_batch_replay_preserves_all_absolute_results(
    minute_row, sliding_row,
):
    batch = [minute_row, sliding_row,
             {**minute_row, "equipment_id": "fridge-002"}]
    store = MemoryGoldReference()
    assert store.apply_batch(batch) == ["INSERT"] * 3
    before = store.snapshot
    assert store.apply_batch(batch) == ["NO-OP"] * 3
    assert store.apply_batch(list(reversed(batch))) == ["NO-OP"] * 3
    assert store.snapshot == before
    assert store.insert_count == 3
    for row in store.snapshot.values():
        assert row["avg"] == row["sum"] / row["count"]


@pytest.mark.parametrize("fail_after", [0, 1, 2, 3])
def test_partial_failure_then_retry_converges_without_double_apply(
    minute_row, sliding_row, fail_after,
):
    batch = [minute_row, sliding_row,
             {**minute_row, "equipment_id": "fridge-002"}]
    before_input = deepcopy(batch)
    expected = {natural_key(row): deepcopy(row) for row in batch}
    store = MemoryGoldReference()
    with pytest.raises(InjectedFailure):
        store.apply_batch(batch, fail_after=fail_after)
    assert len(store.snapshot) == fail_after
    assert store.insert_count == fail_after
    prefix = store.snapshot
    assert store.apply_batch(batch) == (
        ["NO-OP"] * fail_after + ["INSERT"] * (3 - fail_after)
    )
    assert all(store.snapshot[key] == row for key, row in prefix.items())
    assert store.snapshot == expected
    assert store.insert_count == 3
    assert batch == before_input
    assert store.apply_batch(batch) == ["NO-OP"] * 3
    assert store.snapshot == expected


@pytest.mark.parametrize("different", [False, True])
def test_duplicate_source_key_rejected_before_any_write(minute_row, different):
    existing = {**minute_row, "equipment_id": "fridge-002"}
    duplicate = deepcopy(minute_row)
    if different:
        duplicate.update(count=4, sum=16.0)
    store = MemoryGoldReference()
    store.apply_batch([existing])
    before = store.snapshot
    with pytest.raises(DuplicateBatchKey):
        store.apply_batch([minute_row, duplicate])
    assert store.snapshot == before
    assert store.insert_count == 1


def test_conflict_after_new_prefix_does_not_claim_batch_atomicity(minute_row):
    store = MemoryGoldReference()
    store.apply_batch([minute_row])
    first = {**minute_row, "equipment_id": "fridge-002"}
    conflict = {**minute_row, "count": 4, "sum": 16.0}
    with pytest.raises(ContractConflict):
        store.apply_batch([first, conflict])
    assert store.snapshot == {
        natural_key(minute_row): minute_row, natural_key(first): first,
    }
    with pytest.raises(ContractConflict):
        store.apply_batch([first, conflict])
    assert store.insert_count == 2
    assert store.snapshot[natural_key(minute_row)] == minute_row


def test_input_and_snapshot_mutation_cannot_change_canonical(minute_row):
    store = MemoryGoldReference()
    canonical = deepcopy(minute_row)
    store.apply_batch([minute_row])
    minute_row["sum"] = 999.0
    snapshot = store.snapshot
    snapshot[natural_key(canonical)]["sum"] = 888.0
    assert store.snapshot[natural_key(canonical)] == canonical


def test_empty_batch_is_noop():
    store = MemoryGoldReference()
    assert store.apply_batch([]) == []
    assert store.snapshot == {}
    assert store.insert_count == 0
