"""생산 Streaming→Mapper→실제 Delta의 확정 출력 연결을 검증한다."""

from datetime import datetime
import json

from phase3_local_runtime import delta_spark, saved_rows
from streaming.aggregations.gold_row_mapper import map_gold_row
from streaming.jobs.metric_aggregation import start_metric_query
from streaming.sinks.gold_delta import GoldDeltaSink
from test_phase3_metric_aggregation import (
    FIXTURE, add_events, expected_logical_rows, json_row, local_plan,
)


def await_query(query):
    try:
        assert query.awaitTermination(90), "Local production query timed out"
        return [json.loads(p.json) for p in query.recentProgress]
    finally:
        if query.isActive:
            query.stop()


def test_streaming_final_rows_reach_delta(delta_spark, tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    sink = GoldDeltaSink(delta_spark, tmp_path / "gold")
    sink.create_table()
    plan = local_plan(delta_spark, source)
    progress = []
    stages = []
    for index, step in enumerate(FIXTURE["steps"]):
        add_events(source, step["events"], index)
        progress.extend(await_query(start_metric_query(
            plan, sink, str(tmp_path / "checkpoint"), available_now=True,
        )))
        actual = [r for r in saved_rows(sink)
                  if r["equipment_id"] in FIXTURE["target_equipment_ids"]]
        sliding = [r for r in actual if r["window_type"] == "5m_sliding"]
        assert len(sliding) == step["final_row_count"]
        watermark = max(p.get("eventTime", {}).get("watermark", "")
                        for p in progress)
        assert all(datetime.fromisoformat(row["window_end"]) <
                   datetime.fromisoformat(watermark) for row in actual)
        if step["name"] == "idle":
            assert stages[-1]["rows"] == actual
            assert stages[-1]["watermark"] == watermark
        stages.append({"stage": step["name"], "watermark": watermark,
                       "rows": actual})
    expected = []
    for row in expected_logical_rows():
        logical = dict(row)
        for name in ("window_start", "window_end"):
            logical[name] = datetime.fromisoformat(logical[name])
        physical = json_row(map_gold_row(logical))
        for name in ("window_start", "window_end"):
            physical[name] = physical[name].replace("+00:00", "Z")
        expected.append(physical)
    assert sorted(actual, key=str) == sorted(expected, key=str)
    before = saved_rows(sink)
    assert sink.merge_rows([
        map_gold_row({
            **row,
            "window_start": datetime.fromisoformat(row["window_start"]),
            "window_end": datetime.fromisoformat(row["window_end"]),
        }) for row in expected_logical_rows()
    ]) == len(expected)
    assert saved_rows(sink) == before
    for name, value in (("progress", progress), ("stages", stages),
                        ("expected", expected), ("actual", actual)):
        (tmp_path / f"{name}.json").write_text(json.dumps(value, indent=2))
