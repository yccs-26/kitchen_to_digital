"""동일 checkpoint 재시작과 MERGE 전후 실패를 실제 Delta로 검증한다."""

import json

import pytest

from phase3_local_runtime import delta_spark, saved_rows
from streaming.jobs.metric_aggregation import start_metric_query
from streaming.sinks.gold_delta import GoldDeltaSink
from test_phase3_metric_aggregation import FIXTURE, add_events, local_plan
from test_phase3_streaming_gold import await_query


class FailOnceSink:
    """실제 MERGE의 직전 또는 직후에 한 번만 callback을 실패시킨다."""

    def __init__(self, sink, boundary):
        self.sink = sink
        self.boundary = boundary
        self.failed = False
        self.committed_at_failure = None

    def merge_rows(self, rows):
        rows = list(rows)
        if not rows or self.failed:
            return self.sink.merge_rows(rows)
        self.failed = True
        if self.boundary == "before_merge":
            self.committed_at_failure = saved_rows(self.sink)
            raise RuntimeError("GOLD_INJECT_BEFORE_MERGE")
        self.sink.merge_rows(rows)
        self.committed_at_failure = saved_rows(self.sink)
        raise RuntimeError("GOLD_INJECT_AFTER_MERGE_BEFORE_CHECKPOINT")


@pytest.mark.parametrize("boundary", ["before_merge", "after_merge"])
def test_same_checkpoint_recovers_sink_failure(
    delta_spark, tmp_path, boundary,
):
    source = tmp_path / "source"
    source.mkdir()
    checkpoint = tmp_path / "checkpoint"
    sink = GoldDeltaSink(delta_spark, tmp_path / "gold")
    sink.create_table()
    plan = local_plan(delta_spark, source)
    progress = []
    # 실제 초기 state와 checkpoint를 먼저 만든 후 확정 출력을 유도한다.
    for index, step in enumerate(FIXTURE["steps"][:3]):
        add_events(source, step["events"], index)
        progress.extend(await_query(start_metric_query(
            plan, sink, str(checkpoint), available_now=True,
        )))
    before = saved_rows(sink)
    failing = FailOnceSink(sink, boundary)
    closing = FIXTURE["steps"][4]
    add_events(source, closing["events"], 4)
    with pytest.raises(Exception, match="GOLD_INJECT_"):
        await_query(start_metric_query(
            plan, failing, str(checkpoint), available_now=True,
        ))
    assert failing.failed
    commits_before_retry = sorted(
        int(p.name) for p in (checkpoint / "commits").iterdir()
        if p.name.isdigit()
    )
    offsets_before_retry = sorted(
        int(p.name) for p in (checkpoint / "offsets").iterdir()
        if p.name.isdigit()
    )
    assert max(offsets_before_retry) > max(commits_before_retry)
    if boundary == "before_merge":
        assert saved_rows(sink) == before
    else:
        assert saved_rows(sink) == failing.committed_at_failure
        assert len(saved_rows(sink)) > len(before)
    progress.extend(await_query(start_metric_query(
        plan, sink, str(checkpoint), available_now=True,
    )))
    after_retry = saved_rows(sink)
    assert len(after_retry) > len(before)
    if boundary == "after_merge":
        assert after_retry == failing.committed_at_failure
    # 동일 checkpoint의 idle 재시작으로 이미 완료한 입력이 재적용되지 않는다.
    progress.extend(await_query(start_metric_query(
        plan, sink, str(checkpoint), available_now=True,
    )))
    assert saved_rows(sink) == after_retry
    # 다른 source 파일로 같은 이벤트를 재전달해 downstream dedup도 확인한다.
    add_events(source, FIXTURE["steps"][0]["events"], 6)
    progress.extend(await_query(start_metric_query(
        plan, sink, str(checkpoint), available_now=True,
    )))
    assert saved_rows(sink) == after_retry
    (tmp_path / "recovery.json").write_text(json.dumps({
        "boundary": boundary, "before": before,
        "at_failure": failing.committed_at_failure, "after": after_retry,
        "commits_before_retry": commits_before_retry,
        "offsets_before_retry": offsets_before_retry,
    }, indent=2))
    (tmp_path / "progress.json").write_text(json.dumps(progress, indent=2))


def test_separate_process_reuses_checkpoint(tmp_path):
    """별도 Python/JVM 프로세스가 저장된 state/offset을 복구하는지 확인한다."""
    from datetime import datetime
    import os
    import subprocess
    import sys
    import textwrap

    from test_phase3_metric_aggregation import ROOT, expected_logical_rows
    from streaming.aggregations.gold_row_mapper import map_gold_row

    source = tmp_path / "source"
    source.mkdir()
    initial = [event for step in FIXTURE["steps"][:3]
               for event in step["events"]]
    add_events(source, initial, 0)
    script = textwrap.dedent('''
        import json
        import os
        from pathlib import Path
        import sys
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession
        sys.path.insert(0, str(Path.cwd() / "tests/integration"))
        from phase3_local_runtime import saved_rows
        from test_phase3_metric_aggregation import local_plan
        from streaming.jobs.metric_aggregation import start_metric_query
        from streaming.sinks.gold_delta import GoldDeltaSink
        base = Path(sys.argv[1])
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        builder = (SparkSession.builder.master("local[2]")
            .appName("phase3-process-restart")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.jars.ivy", os.environ["KTD_LOCAL_IVY"])
            .config("spark.sql.warehouse.dir", str(base / "warehouse"))
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.snapshotPartitions", "2"))
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        try:
            sink = GoldDeltaSink(spark, base / "gold")
            sink.create_table()
            query = start_metric_query(local_plan(spark, base / "source"),
                sink, str(base / "checkpoint"), available_now=True)
            assert query.awaitTermination(90)
            (base / (sys.argv[2] + "-rows.json")).write_text(
                json.dumps(saved_rows(sink), indent=2))
            (base / (sys.argv[2] + "-progress.json")).write_text(
                json.dumps([json.loads(p.json) for p in query.recentProgress]))
        finally:
            spark.stop()
    ''')
    for phase in ("first", "second"):
        if phase == "second":
            assert (tmp_path / "checkpoint" / "commits").exists()
            add_events(source, FIXTURE["steps"][5]["events"], 1)
        result = subprocess.run(
            [sys.executable, "-c", script, str(tmp_path), phase],
            cwd=ROOT, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True, text=True, timeout=120, check=False,
        )
        (tmp_path / f"{phase}-process.log").write_text(
            result.stdout + result.stderr,
        )
        assert result.returncode == 0, result.stderr[-8000:]
    assert json.loads((tmp_path / "first-rows.json").read_text()) == []
    actual = json.loads((tmp_path / "second-rows.json").read_text())
    actual = [r for r in actual
              if r["equipment_id"] in FIXTURE["target_equipment_ids"]]
    expected = []
    for row in expected_logical_rows():
        logical = dict(row)
        for name in ("window_start", "window_end"):
            logical[name] = datetime.fromisoformat(logical[name])
        physical = map_gold_row(logical)
        for name in ("window_start", "window_end"):
            physical[name] = physical[name].isoformat().replace("+00:00", "Z")
        expected.append(physical)
    assert sorted(actual, key=str) == sorted(expected, key=str)
