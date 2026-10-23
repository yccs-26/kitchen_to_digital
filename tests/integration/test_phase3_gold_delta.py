"""실제 로컬 Delta 트랜잭션에서 Gold 확정값 불변성을 검증한다."""

from datetime import datetime, timezone
import json

import pytest

from phase3_local_runtime import delta_spark, saved_rows
from streaming.aggregations.gold_row_comparison import compare_gold_rows
from streaming.aggregations.gold_row_mapper import map_gold_row
from streaming.sinks.gold_delta import DuplicateGoldBatchKey, GoldDeltaSink


@pytest.fixture
def final_row():
    return map_gold_row({
        "window_start": datetime(2026, 10, 20, 12, tzinfo=timezone.utc),
        "window_end": datetime(2026, 10, 20, 12, 1, tzinfo=timezone.utc),
        "window_type": "1m_tumbling", "store_id": "store-001",
        "equipment_id": "fridge-001", "metric_name": "temperature_celsius",
        "unit": "celsius", "count": 3, "sum": 12.0, "min": 2.0,
        "max": 6.0, "avg": 4.0,
    })


@pytest.fixture
def sink(delta_spark, tmp_path):
    target = GoldDeltaSink(delta_spark, tmp_path / "gold")
    target.create_table()
    return target


def test_delta_insert_noop_replay_schema(sink, final_row, tmp_path):
    assert compare_gold_rows(None, final_row).decision == "INSERT"
    assert sink.merge_rows([final_row]) == 1
    initial = saved_rows(sink)
    assert compare_gold_rows(final_row, final_row).decision == "NO-OP"
    assert sink.merge_rows([final_row]) == 1
    assert saved_rows(sink) == initial
    assert initial[0]["sample_count"] == 3
    assert initial[0]["sum_value"] == 12.0
    assert len(initial[0]) == 12
    logs = list((sink.path / "_delta_log").glob("*.json"))
    metadata = [json.loads(line)["metaData"] for log in logs
                for line in log.read_text().splitlines()
                if "metaData" in json.loads(line)]
    fields = json.loads(metadata[0]["schemaString"])["fields"]
    assert len(fields) == 12
    assert all(not field["nullable"] for field in fields)
    sink.create_table()
    assert saved_rows(sink) == initial
    (tmp_path / "delta-replay.json").write_text(json.dumps(initial, indent=2))


@pytest.mark.parametrize("attribute,value", [
    ("unit", "fahrenheit"), ("min_value", 1.0), ("max_value", 7.0),
])
def test_conflict_aborts_new_rows_in_same_merge(
    sink, final_row, attribute, value, tmp_path,
):
    sink.merge_rows([final_row])
    before = saved_rows(sink)
    version_before = len(list((sink.path / "_delta_log").glob("*.json")))
    changed = {**final_row, attribute: value}
    assert compare_gold_rows(final_row, changed).decision == "CONFLICT"
    new = {**final_row, "equipment_id": "fridge-002"}
    with pytest.raises(Exception, match="GOLD_FINAL_CONFLICT"):
        sink.merge_rows([new, changed])
    assert saved_rows(sink) == before
    versions_after = len(list((sink.path / "_delta_log").glob("*.json")))
    assert versions_after == version_before
    (tmp_path / "conflict-atomicity.json").write_text(json.dumps({
        "before": before, "after": saved_rows(sink),
        "committed_versions": version_before,
    }, indent=2))
    sink.merge_rows([new, final_row])
    assert len(saved_rows(sink)) == 2


def test_duplicate_source_key_rejected_before_commit(sink, final_row):
    with pytest.raises(DuplicateGoldBatchKey):
        sink.merge_rows([final_row, final_row])
    assert saved_rows(sink) == []
    assert sink.merge_rows([]) == 0


def test_not_null_is_enforced_by_delta(sink, final_row):
    from streaming.sinks.gold_delta import gold_schema

    # Mapper와 createDataFrame의 방어를 우회해 실제 Delta 제약을 검증한다.
    schema = gold_schema()
    for field in schema:
        field.nullable = True
    frame = sink.spark.createDataFrame(
        [{**final_row, "unit": None}], schema=schema,
    )
    with pytest.raises(Exception, match="(?i)(NOT NULL|NOT_NULL)"):
        frame.write.format("delta").mode("append").save(str(sink.path))
    assert saved_rows(sink) == []


def test_wrong_physical_columns_rejected(sink, final_row):
    wrong = {**final_row, "count": final_row["sample_count"]}
    wrong.pop("sample_count")
    with pytest.raises(ValueError, match="physical columns"):
        sink.merge_rows([wrong])
    assert saved_rows(sink) == []
