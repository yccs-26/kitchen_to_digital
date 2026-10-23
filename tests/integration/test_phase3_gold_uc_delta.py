"""로컬 spark_catalog의 이름 기반 Delta 테스트. 실제 UC 검증은 미실행."""

from datetime import datetime
import json
from uuid import uuid4

import pytest

from phase3_local_runtime import delta_spark
from test_phase3_gold_delta import final_row
from test_phase3_metric_aggregation import (
    FIXTURE, add_events, expected_logical_rows, local_plan,
)
from streaming.aggregations.gold_row_mapper import map_gold_row
from streaming.jobs.metric_aggregation import gold_batch_callback
from streaming.sinks.gold_delta import DuplicateGoldBatchKey, gold_schema
from streaming.sinks.gold_uc_delta import GoldUCDeltaSink


@pytest.fixture
def named_sink(delta_spark):
    # 테스트 전용 로컬 warehouse. Writer 자체에는 DDL 기능이 없다.
    assert delta_spark.sparkContext.master == "local[2]"
    table = f"spark_catalog.default.uc_candidate_{uuid4().hex}"
    sink = GoldUCDeltaSink(delta_spark, table, single_writer=True)
    ddl = ", ".join(f"`{f.name}` {f.dataType.simpleString()} NOT NULL"
                    for f in gold_schema())
    delta_spark.sql(f"CREATE TABLE {sink.identifier} ({ddl}) USING DELTA")
    return sink


def named_rows(sink):
    from pyspark.sql import functions as functions

    frame = sink.spark.table(sink.identifier)
    for name in ("window_start", "window_end"):
        frame = frame.withColumn(name, functions.date_format(
            name, "yyyy-MM-dd'T'HH:mm:ssXXX",
        ))
    return sorted([r.asDict() for r in frame.collect()], key=str)


def test_named_insert_noop_replay(named_sink, final_row, tmp_path):
    assert named_sink.merge_rows([final_row]) == 1
    before = named_rows(named_sink)
    assert named_sink.merge_rows([final_row]) == 1
    assert named_rows(named_sink) == before
    assert len(before) == 1 and len(before[0]) == 12
    assert before[0]["sample_count"] == 3
    assert before[0]["sum_value"] == 12.0
    (tmp_path / "named-replay.json").write_text(json.dumps(before, indent=2))


@pytest.mark.parametrize("field,value", [
    ("unit", "other"), ("min_value", 1.0), ("max_value", 7.0),
])
def test_named_conflict_aborts_entire_merge(
    named_sink, final_row, field, value, tmp_path,
):
    named_sink.merge_rows([final_row])
    before = named_rows(named_sink)
    new = {**final_row, "equipment_id": "other"}
    with pytest.raises(Exception, match="GOLD_FINAL_CONFLICT"):
        named_sink.merge_rows([new, {**final_row, field: value}])
    assert named_rows(named_sink) == before
    assert named_sink.merge_rows([new, final_row]) == 2
    assert len(named_rows(named_sink)) == 2
    (tmp_path / "named-conflict.json").write_text(json.dumps({
        "existing_preserved": True, "retry_rows": named_rows(named_sink),
    }, indent=2))


def test_named_empty_duplicate_and_not_null(named_sink, final_row):
    assert named_sink.merge_rows([]) == 0
    with pytest.raises(DuplicateGoldBatchKey):
        named_sink.merge_rows([final_row, final_row])
    assert named_rows(named_sink) == []
    schema = gold_schema()
    for field in schema:
        field.nullable = True
    invalid = named_sink.spark.createDataFrame(
        [{**final_row, "unit": None}], schema=schema,
    )
    with pytest.raises(Exception, match="(?i)(NOT NULL|NOT_NULL)"):
        invalid.write.format("delta").mode("append").saveAsTable(
            named_sink.identifier,
        )
    assert named_rows(named_sink) == []


def test_missing_table_is_not_created(delta_spark, final_row):
    table = f"spark_catalog.default.missing_{uuid4().hex}"
    sink = GoldUCDeltaSink(delta_spark, table, single_writer=True)
    with pytest.raises(Exception,
                       match="(?i)(TABLE_OR_VIEW_NOT_FOUND|not found)"):
        sink.merge_rows([final_row])
    assert not delta_spark.catalog.tableExists(sink.identifier)


def test_named_wrong_schema_rejected(delta_spark, final_row):
    table = f"spark_catalog.default.wrong_{uuid4().hex}"
    sink = GoldUCDeltaSink(delta_spark, table, single_writer=True)
    delta_spark.sql(
        f"CREATE TABLE {sink.identifier} (wrong STRING) USING DELTA"
    )
    with pytest.raises(ValueError, match="schema differs"):
        sink.merge_rows([final_row])
    assert delta_spark.table(sink.identifier).count() == 0


def test_named_callback_serialization(named_sink):
    from pyspark import cloudpickle

    callback = gold_batch_callback(named_sink)
    assert callback.__code__.co_freevars == ("table",)
    restored = cloudpickle.loads(cloudpickle.dumps(callback))
    assert callable(restored)


def test_named_streaming_two_windows_24_rows(named_sink, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    events = [e for step in FIXTURE["steps"][:3] for e in step["events"]]
    events.extend(FIXTURE["steps"][5]["events"])
    add_events(source, events, 0)
    query = (local_plan(named_sink.spark, source).writeStream
             .outputMode("append")
             .foreachBatch(gold_batch_callback(named_sink))
             .option("checkpointLocation", str(tmp_path / "checkpoint"))
             .trigger(availableNow=True).start())
    try:
        assert query.awaitTermination(120)
        progress = [json.loads(p.json) for p in query.recentProgress]
    finally:
        query.stop()
    expected = []
    for row in expected_logical_rows():
        logical = dict(row)
        for name in ("window_start", "window_end"):
            logical[name] = datetime.fromisoformat(logical[name])
        physical = map_gold_row(logical)
        for name in ("window_start", "window_end"):
            physical[name] = (
                physical[name].isoformat().replace("+00:00", "Z")
            )
        expected.append(physical)
    actual = [row for row in named_rows(named_sink)
              if row["equipment_id"] in FIXTURE["target_equipment_ids"]]
    assert len(actual) == 24
    assert sorted(actual, key=str) == sorted(expected, key=str)
    assert sum(r["window_type"] == "1m_tumbling" for r in actual) == 6
    assert sum(r["window_type"] == "5m_sliding" for r in actual) == 18
    (tmp_path / "named-streaming.json").write_text(json.dumps({
        "expected": sorted(expected, key=str),
        "actual": sorted(actual, key=str),
        "progress": progress,
    }, indent=2))
