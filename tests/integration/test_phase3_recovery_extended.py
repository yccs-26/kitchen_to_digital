"""전용 프로세스 그룹만 강제 종료하고 기존 checkpoint 복구를 검증한다."""

from datetime import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import textwrap
import time

import pytest

from phase3_local_runtime import delta_spark
from streaming.jobs.metric_aggregation import gold_batch_callback
from streaming.sinks.gold_delta import GoldDeltaSink
from test_phase3_metric_aggregation import (
    FIXTURE, ROOT, add_events, expected_logical_rows,
)
from streaming.aggregations.gold_row_mapper import map_gold_row


SCRIPT = textwrap.dedent('''
    import json
    import os
    from pathlib import Path
    import sys
    import threading
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession
    sys.path.insert(0, str(Path.cwd() / "tests/integration"))
    from phase3_local_runtime import saved_rows
    from test_phase3_metric_aggregation import local_plan
    from streaming.jobs.metric_aggregation import gold_batch_callback
    from streaming.sinks.gold_delta import GoldDeltaSink
    base, mode = Path(sys.argv[1]), sys.argv[2]
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    builder = (SparkSession.builder.master("local[2]")
        .appName("phase3-isolated-kill-recovery")
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
        consume = gold_batch_callback(sink)
        attempts = []
        def observe(batch, batch_id):
            consume(batch, batch_id)
            rows = saved_rows(sink)
            attempts.append(batch_id)
            if mode == "kill" and rows:
                marker = base / "merge-committed.json"
                staged = base / "commit-marker.staged"
                with staged.open("w") as output:
                    json.dump({"batch_id": batch_id, "rows": rows}, output)
                    output.flush()
                    os.fsync(output.fileno())
                staged.replace(marker)
                threading.Event().wait(180)
                raise RuntimeError("Parent did not terminate isolated process")
        query = (local_plan(spark, base / "source").writeStream
            .outputMode("append").foreachBatch(observe)
            .option("checkpointLocation", str(base / "checkpoint"))
            .trigger(availableNow=True).start())
        assert query.awaitTermination(120)
        (base / "recovered.json").write_text(json.dumps({
            "rows": saved_rows(sink), "attempted_batches": attempts,
            "progress": [json.loads(p.json) for p in query.recentProgress],
        }, indent=2))
    finally:
        spark.stop()
''')


def test_production_delta_callback_is_serializable(delta_spark, tmp_path):
    from pyspark import cloudpickle

    sink = GoldDeltaSink(delta_spark, tmp_path / "gold")
    # SparkSession을 캡처하는 종전 경로가 실패하는 정확한 경계도 관측한다.
    with pytest.raises(Exception, match="CONTEXT_ONLY_VALID_ON_DRIVER"):
        cloudpickle.dumps(sink)
    callback = gold_batch_callback(sink)
    restored = cloudpickle.loads(cloudpickle.dumps(callback))
    assert callable(restored)
    assert callback.__code__.co_freevars == ("path",)


def test_sigkill_after_merge_recovers_same_checkpoint(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    events = [e for step in FIXTURE["steps"][:3] for e in step["events"]]
    events.extend(FIXTURE["steps"][5]["events"])
    add_events(source, events, 0)
    marker = tmp_path / "merge-committed.json"
    log_path = tmp_path / "killed-process.log"
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    with log_path.open("w") as output:
        process = subprocess.Popen(
            [sys.executable, "-c", SCRIPT, str(tmp_path), "kill"],
            cwd=ROOT, env=environment, stdout=output,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            assert os.getpgid(process.pid) == process.pid
            deadline = time.monotonic() + 120
            while not marker.exists() and process.poll() is None:
                assert time.monotonic() < deadline, "Commit marker timeout"
                time.sleep(0.05)
            assert marker.exists(), log_path.read_text()[-8000:]
            # fsync된 commit 관측 후 전용 프로세스 그룹만 종료한다.
            before = json.loads(marker.read_text())
            killed_batch = before["batch_id"]
            checkpoint = tmp_path / "checkpoint"
            assert (checkpoint / "offsets" / str(killed_batch)).exists()
            assert not (checkpoint / "commits" / str(killed_batch)).exists()
            os.killpg(process.pid, signal.SIGKILL)
            assert process.wait(timeout=20) == -signal.SIGKILL
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=20)
    result = subprocess.run(
        [sys.executable, "-c", SCRIPT, str(tmp_path), "recover"],
        cwd=ROOT, env=environment, capture_output=True, text=True,
        timeout=150, check=False,
    )
    (tmp_path / "recovered-process.log").write_text(
        result.stdout + result.stderr,
    )
    assert result.returncode == 0, result.stderr[-8000:]
    recovered = json.loads((tmp_path / "recovered.json").read_text())
    assert killed_batch in recovered["attempted_batches"]
    assert recovered["rows"] == before["rows"]
    assert (checkpoint / "commits" / str(killed_batch)).exists()
    expected = []
    for row in expected_logical_rows():
        logical = dict(row)
        for name in ("window_start", "window_end"):
            logical[name] = datetime.fromisoformat(logical[name])
        physical = map_gold_row(logical)
        for name in ("window_start", "window_end"):
            physical[name] = physical[name].isoformat().replace("+00:00", "Z")
        expected.append(physical)
    actual = [row for row in recovered["rows"]
              if row["equipment_id"] in FIXTURE["target_equipment_ids"]]
    assert len(actual) == 24
    assert sorted(actual, key=str) == sorted(expected, key=str)
