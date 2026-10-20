"""생산 구현과 분리하여 5분/1분 sliding의 native Spark 동작을 검증한다."""

from collections import Counter
from datetime import datetime
import json
import math
from pathlib import Path

import pytest

from streaming.aggregations.minute_aggregation import aggregate_minutes
from test_phase3_streaming import local_spark, run_available


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = json.loads(
    (ROOT / "tests/fixtures/phase3/sliding_window_events.json").read_text()
)
STATS = ("count", "sum", "min", "max", "avg")


def row_key(row):
    return tuple(row[name] for name in FIXTURE["natural_key"])


def assert_rows(actual, expected):
    """수동 표와 모든 차원·경계·수치를 비교한다."""
    actual = sorted(actual, key=row_key)
    expected = sorted(expected, key=row_key)
    assert len(actual) == len(expected)
    for observed, manual in zip(actual, expected, strict=True):
        for field, value in manual.items():
            if field == "member_event_ids":
                continue
            if field in STATS and field != "count":
                assert observed[field] == pytest.approx(
                    value, rel=1e-12, abs=1e-12,
                )
            else:
                assert observed[field] == value


def test_manual_sliding_rows_against_existing_python_contract():
    """Spark를 사용하지 않고 수동 표를 기존 유한 입력 수치 계약과 대조한다."""
    events = [
        event for step in FIXTURE["steps"] for event in step["events"]
        if event["equipment_id"] in FIXTURE["target_equipment_ids"]
    ]
    wire_fields = {
        field["name"] for field in json.loads(
            (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text()
        )["fields"]
    }
    assert all(set(event) == wire_fields for event in events)
    rules = json.loads(
        (ROOT / "config/validation_rules.json").read_text()
    )["metric_units"]
    expected = FIXTURE["expected_rows"]
    assert len(expected) == 18
    assert len({row_key(row) for row in expected}) == len(expected)
    for row in expected:
        start = datetime.fromisoformat(row["window_start"])
        end = datetime.fromisoformat(row["window_end"])
        assert (end - start).total_seconds() == 300
        assert start.second == start.microsecond == 0
        selected = [
            event for event in events
            if event["equipment_id"] == row["equipment_id"]
            and event["metric_name"] == row["metric_name"]
            and start <= datetime.fromisoformat(event["event_time"]) < end
        ]
        assert sorted({e["event_id"] for e in selected}) == sorted(
            row["member_event_ids"]
        )
        minutes = aggregate_minutes(selected, metric_units=rules)
        count = sum(part.count for part in minutes)
        total = math.fsum(part.sum for part in minutes)
        assert count == row["count"]
        assert total == row["sum"]
        assert total / count == pytest.approx(row["avg"])
        assert min(part.min for part in minutes) == row["min"]
        assert max(part.max for part in minutes) == row["max"]
        assert {part.unit for part in minutes} == {row["unit"]}
    memberships = Counter(
        identity for row in expected for identity in row["member_event_ids"]
    )
    assert set(memberships.values()) == {5}
    at_noon = next(row for row in expected
                   if row["equipment_id"] == "fridge-001"
                   and row["window_start"] == "2026-10-20T12:00:00+00:00")
    assert at_noon["member_event_ids"] == ["a", "b", "c", "d"]
    assert at_noon["avg"] == 7.5
    assert at_noon["avg"] != (3.0 + 8.0 + 16.0) / 3


def build_sliding_test_plan(spark, source):
    """fixture 전용 native 5분/1분 plan이며 운영 Job을 구현하지 않는다."""
    from pyspark.sql import functions as functions
    from pyspark.sql.types import DoubleType, StringType, StructField
    from pyspark.sql.types import StructType

    fields = [
        "event_id", "event_time", "store_id", "equipment_id",
        "equipment_type", "metric_name", "metric_value", "unit",
        "schema_version", "source",
    ]
    schema = StructType([
        StructField(name, DoubleType() if name == "metric_value"
                    else StringType(), nullable=False)
        for name in fields
    ])
    stream = (
        spark.readStream.schema(schema).option("maxFilesPerTrigger", 1)
        .json(str(source))
        .withColumn("event_time", functions.to_timestamp("event_time"))
        .withWatermark("event_time", FIXTURE["watermark_delay"])
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    aggregated = stream.groupBy(
        functions.window(
            "event_time", FIXTURE["window_duration"],
            FIXTURE["slide_duration"],
        ),
        "store_id", "equipment_id", "metric_name", "unit",
    ).agg(
        functions.count("event_id").alias("count"),
        functions.sum("metric_value").alias("sum"),
        functions.min("metric_value").alias("min"),
        functions.max("metric_value").alias("max"),
    ).withColumn("avg", functions.col("sum") / functions.col("count"))
    return aggregated.select(
        functions.date_format(
            "window.start", "yyyy-MM-dd'T'HH:mm:ssXXX",
        ).alias("window_start"),
        functions.date_format(
            "window.end", "yyyy-MM-dd'T'HH:mm:ssXXX",
        ).alias("window_end"),
        functions.lit("5m_sliding").alias("window_type"),
        "store_id", "equipment_id", "metric_name", "unit", *STATS,
    )


def max_watermark(progress):
    return max(
        datetime.fromisoformat(p["eventTime"]["watermark"])
        for p in progress if p.get("eventTime", {}).get("watermark")
    )


def run_steps(spark, tmp_path, steps):
    """동일 checkpoint의 결정적 유한 micro-batch 패턴을 재사용한다."""
    source = tmp_path / "source"
    source.mkdir()
    plan = build_sliding_test_plan(spark, source)
    progress = []
    attempts = []
    stages = []
    for index, step in enumerate(steps):
        before_idle = (
            max_watermark(progress) if step["name"] == "idle" else None
        )
        if step["events"]:
            staged = tmp_path / f"staged-{index}.json"
            staged.write_text("\n".join(
                json.dumps(event) for event in step["events"]
            ) + "\n")
            staged.replace(source / f"batch-{index:02}.json")

        def observe(batch, batch_id):
            all_rows = [row.asDict() for row in batch.collect()]
            rows = [
                row for row in all_rows
                if row["equipment_id"] in FIXTURE["target_equipment_ids"]
            ]
            for row in rows:
                for field in ("window_start", "window_end"):
                    row[field] = datetime.fromisoformat(row[field]).isoformat()
            attempts.append({
                "batch_id": batch_id, "stage": step["name"],
                "rows": rows, "all_row_count": len(all_rows),
            })

        recent = run_available(plan, tmp_path / "checkpoint", observe)
        progress.extend(json.loads(p.json) for p in recent)
        actual = [row for attempt in attempts for row in attempt["rows"]]
        after = max_watermark(progress)
        stages.append({
            "stage": step["name"], "watermark": after.isoformat(),
            "watermark_before_idle": (
                before_idle.isoformat() if before_idle is not None else None
            ),
            "target_final_rows": len(actual),
        })
        for name, data in (
            ("progress", progress), ("attempts", attempts), ("stages", stages),
        ):
            (tmp_path / f"{name}.json").write_text(json.dumps(data, indent=2))
        assert len(actual) == step["final_row_count"]
        expected = []
        if step.get("closed_before"):
            cutoff = datetime.fromisoformat(step["closed_before"])
            expected = [
                row for row in FIXTURE["expected_rows"]
                if datetime.fromisoformat(row["window_end"]) < cutoff
            ]
        assert_rows(actual, expected)
        if before_idle is not None:
            assert after == before_idle
    return progress, attempts


def test_sliding_multibatch_overlap_dedup_and_append(local_spark, tmp_path):
    """수동 표 18행과 배치 간 중복, 실제 W에 따른 확정을 검증한다."""
    progress, attempts = run_steps(local_spark, tmp_path, FIXTURE["steps"])
    duplicate_rows = sum(
        state["customMetrics"].get("numDroppedDuplicateRows", 0)
        for p in progress for state in p.get("stateOperators", [])
        if state["operatorName"] == "dedupeWithinWatermark"
    )
    assert duplicate_rows == 1
    for attempt in attempts:
        if not attempt["rows"]:
            continue
        matching = [
            p for p in progress if p["batchId"] == attempt["batch_id"]
            and p.get("eventTime", {}).get("watermark")
        ]
        assert matching
        applied = datetime.fromisoformat(matching[0]["eventTime"]["watermark"])
        assert all(
            applied > datetime.fromisoformat(row["window_end"])
            for row in attempt["rows"]
        )
    final = [row for attempt in attempts for row in attempt["rows"]]
    assert sum(row["count"] for row in final) == 35
    assert {(row["metric_name"], row["unit"]) for row in final} == {
        ("temperature_celsius", "celsius"), ("fan_rpm", "rpm"),
    }
    assert len({row_key(row) for row in final}) == 18


def test_sliding_idle_does_not_finalize_tail(local_spark, tmp_path):
    """미래 진행 행 없이 window가 열린 채로 유지되는지 확인한다."""
    progress, attempts = run_steps(local_spark, tmp_path, FIXTURE["steps"][:4])
    assert not any(attempt["rows"] for attempt in attempts)
    assert max_watermark(progress).isoformat() == "2026-10-20T11:55:00+00:00"
    assert sum(
        state["numRowsRemoved"]
        for p in progress for state in p.get("stateOperators", [])
    ) == 0
