"""생산 dedup→두 window→Append plan을 수동 fixture와 실제 Spark로 검증한다."""

from dataclasses import asdict
from datetime import datetime, timezone
import json
from pathlib import Path

from streaming.aggregations.minute_aggregation import aggregate_minutes
from streaming.jobs.metric_aggregation import (
    build_metric_aggregation,
    gold_batch_callback,
    validated_schema,
)
from test_phase3_streaming import local_spark, run_available


ROOT = Path(__file__).resolve().parents[2]
FIXTURE = json.loads(
    (ROOT / "tests/fixtures/phase3/sliding_window_events.json").read_text()
)


def add_events(source, events, index):
    if events:
        staged = source.parent / f"staged-{index}.json"
        staged.write_text("\n".join(json.dumps(e) for e in events) + "\n")
        staged.replace(source / f"batch-{index:02}.json")


def local_plan(spark, source):
    frame = spark.readStream.schema(validated_schema()).option(
        "maxFilesPerTrigger", 1,
    ).json(str(source))
    return build_metric_aggregation(frame)


def json_row(row):
    return {
        name: value.isoformat() if isinstance(value, datetime) else value
        for name, value in row.items()
    }


def expected_logical_rows():
    sliding = [{name: value for name, value in row.items()
                if name != "member_event_ids"}
               for row in FIXTURE["expected_rows"]]
    events = [e for step in FIXTURE["steps"] for e in step["events"]
              if e["equipment_id"] in FIXTURE["target_equipment_ids"]]
    units = json.loads((ROOT / "config/validation_rules.json").read_text())[
        "metric_units"
    ]
    minute = []
    for aggregate in aggregate_minutes(events, metric_units=units):
        row = asdict(aggregate)
        row.pop("key")
        minute.append(json_row({**aggregate.key._asdict(), **row}))
    return minute + sliding


class CaptureSink:
    """Mapper 출력을 관측할 뿐 영속 저장을 모사하지 않는다."""

    def __init__(self):
        self.rows = []

    def merge_rows(self, rows):
        self.rows.extend(json_row(row) for row in rows)


def test_production_two_windows_single_query(local_spark, tmp_path):
    from streaming.aggregations.gold_row_mapper import map_gold_row

    source = tmp_path / "source"
    source.mkdir()
    plan = local_plan(local_spark, source)
    capture = CaptureSink()
    progress = []
    stages = []
    for index, step in enumerate(FIXTURE["steps"]):
        before = max((p.get("eventTime", {}).get("watermark", "")
                      for p in progress), default="")
        add_events(source, step["events"], index)
        recent = run_available(
            plan, tmp_path / "checkpoint", gold_batch_callback(capture),
        )
        progress.extend(json.loads(p.json) for p in recent)
        current = max(p.get("eventTime", {}).get("watermark", "")
                      for p in progress)
        target = [r for r in capture.rows
                  if r["equipment_id"] in FIXTURE["target_equipment_ids"]]
        sliding = [r for r in target if r["window_type"] == "5m_sliding"]
        assert len(sliding) == step["final_row_count"]
        if step["name"] == "idle":
            assert current == before
        stages.append({"stage": step["name"], "watermark": current,
                       "target_rows": len(target), "sliding": len(sliding)})
    expected = []
    for row in expected_logical_rows():
        logical = dict(row)
        for name in ("window_start", "window_end"):
            logical[name] = datetime.fromisoformat(logical[name])
        expected.append(json_row(map_gold_row(logical)))
    assert sorted(target, key=str) == sorted(expected, key=str)
    states = [p["stateOperators"] for p in progress if p.get("stateOperators")]
    assert states and max(len(operators) for operators in states) == 4
    assert sum(s.get("customMetrics", {}).get("numDroppedDuplicateRows", 0)
               for operators in states for s in operators) == 2
    assert any(s["numRowsRemoved"] > 0
               for operators in states for s in operators)
    for name, value in (("progress", progress), ("stages", stages),
                        ("actual", target), ("expected", expected)):
        (tmp_path / f"{name}.json").write_text(json.dumps(value, indent=2))


def test_production_late_rows_after_actual_state_eviction(
    local_spark, tmp_path,
):
    contract = json.loads(
        (ROOT / "tests/fixtures/phase3/multi_batch_events.json").read_text()
    )
    scenario = next(s for s in contract["scenarios"]
                    if s["name"] == "watermark_late")
    source = tmp_path / "source"
    source.mkdir()
    plan = local_plan(local_spark, source)
    capture = CaptureSink()
    progress = []
    before_late = None
    for index, step in enumerate(scenario["steps"]):
        if step.get("require_dedup_eviction"):
            watermark = max(p.get("eventTime", {}).get("watermark", "")
                            for p in progress)
            assert datetime.fromisoformat(watermark) > datetime.fromisoformat(
                step["require_watermark_after"],
            )
            dedup_states = [s for p in progress for s in p["stateOperators"]
                            if s["operatorName"] == "dedupeWithinWatermark"]
            assert sum(s["numRowsRemoved"] for s in dedup_states) > 0
            before_late = list(capture.rows)
        add_events(source, step["events"], index)
        progress.extend(json.loads(p.json) for p in run_available(
            plan, tmp_path / "checkpoint", gold_batch_callback(capture),
        ))
        if step.get("require_dedup_eviction"):
            assert capture.rows == before_late
    target = [row for row in capture.rows
              if row["equipment_id"] == contract["target_equipment_id"]]
    assert len(target) == 6
    assert {row["sample_count"] for row in target} == {2}
    assert {row["sum_value"] for row in target} == {6.0}
    assert {row["avg_value"] for row in target} == {3.0}
    assert sum(s["numRowsDroppedByWatermark"] for p in progress
               for s in p["stateOperators"]) >= 4
    (tmp_path / "progress.json").write_text(json.dumps(progress, indent=2))
    (tmp_path / "late-actual.json").write_text(json.dumps(target, indent=2))
