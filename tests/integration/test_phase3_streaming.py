"""로컬 Spark 전용 dedup→window 검증. Kafka/Gold/DBR에 연결하지 않는다."""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
CONTRACT = json.loads(
    (ROOT / "tests/fixtures/phase3/multi_batch_events.json").read_text()
)


@pytest.fixture
def local_spark(tmp_path, monkeypatch):
    """승인된 설치 환경에서만 local classic 세션을 연다."""
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        pytest.skip("Local-only test; Databricks execution is forbidden")
    pyspark = pytest.importorskip(
        "pyspark", reason="PySpark 4.0.0 is absent; no dependency installed",
    )
    if pyspark.__version__ != "4.0.0":
        pytest.skip("This compatibility test targets PySpark 4.0.0 exactly")
    java_home = os.getenv("JAVA_HOME")
    java = str(Path(java_home) / "bin/java") if java_home else (
        shutil.which("java")
    )
    if not java:
        pytest.skip("A supported local JDK 17 or 21 is required")
    result = subprocess.run(
        [java, "-version"], capture_output=True, text=True, timeout=10,
        check=False,
    )
    match = re.search(r'version "(\d+)', result.stdout + result.stderr)
    if not match or int(match.group(1)) not in {17, 21}:
        pytest.skip("Spark 4.0.0 overview supports JDK 17/21; select one")
    for name in ("SPARK_REMOTE", "SPARK_CONNECT_MODE_ENABLED"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("SPARK_LOCAL_IP", "127.0.0.1")
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("phase3-local-contract")
        .config("spark.api.mode", "classic")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(tmp_path / "warehouse"))
        .config(
            "spark.sql.streaming.statefulOperator.checkCorrectness.enabled",
            "true",
        )
        .getOrCreate()
    )
    assert spark.sparkContext.master == "local[2]"
    assert spark.version == "4.0.0"
    try:
        yield spark
    finally:
        spark.stop()


def build_test_plan(spark, source):
    """운영 Job과 분리된 작은 fixture용 native API 조합을 구성한다."""
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
        .withWatermark("event_time", CONTRACT["watermark_delay"])
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    aggregate = stream.groupBy(
        functions.window("event_time", CONTRACT["window_duration"]),
        "store_id", "equipment_id", "metric_name", "unit",
    ).agg(
        functions.count("event_id").alias("count"),
        functions.sum("metric_value").alias("sum"),
        functions.avg("metric_value").alias("avg"),
        functions.min("metric_value").alias("min"),
        functions.max("metric_value").alias("max"),
    )
    return aggregate.select(
        functions.date_format(
            "window.start", "yyyy-MM-dd'T'HH:mm:ssXXX",
        ).alias("window_start"),
        functions.date_format(
            "window.end", "yyyy-MM-dd'T'HH:mm:ssXXX",
        ).alias("window_end"),
        "store_id", "equipment_id", "metric_name", "unit",
        "count", "sum", "avg", "min", "max",
    )


def observed_rows(rows):
    """작은 결과 전체를 호스트 시간대와 무관한 UTC 표현으로 바꾼다."""
    result = []
    for row in rows:
        item = row.asDict(recursive=True)
        if item["equipment_id"] != CONTRACT["target_equipment_id"]:
            continue
        for field in ("window_start", "window_end"):
            item[field] = datetime.fromisoformat(item[field]).astimezone(
                timezone.utc,
            ).isoformat()
        result.append({"window_type": "1m_tumbling", **item})
    return sorted(result, key=lambda r: r["window_start"])


def run_available(plan, checkpoint, observe):
    """유한 입력을 제한 시간 안에 처리하고 실제 progress를 반환한다."""
    query = (
        plan.writeStream.outputMode("append")
        .foreachBatch(observe)
        .option("checkpointLocation", str(checkpoint))
        .trigger(availableNow=True).start()
    )
    try:
        assert query.awaitTermination(45), "Local streaming query timed out"
        return query.recentProgress
    finally:
        if query.isActive:
            query.stop()


def attempts_at(path):
    return [json.loads(p.read_text()) for p in sorted(path.glob("*.json"))]


@pytest.mark.parametrize(
    "scenario", CONTRACT["scenarios"], ids=lambda scenario: scenario["name"],
)
def test_native_multibatch_contract(local_spark, tmp_path, scenario):
    """실제 state·late·idle·동일 batch retry의 출력 계약을 관측한다."""
    source = tmp_path / "source"
    source.mkdir()
    attempts = tmp_path / "attempts"
    attempts.mkdir()
    checkpoint = tmp_path / "checkpoint"
    failed_once = tmp_path / "failed-once"
    plan = build_test_plan(local_spark, source)
    progress = []
    watermark_before_idle = None

    for index, step in enumerate(scenario["steps"]):
        watermarks = [
            datetime.fromisoformat(p["eventTime"]["watermark"])
            for p in progress
            if p.get("eventTime", {}).get("watermark")
        ]
        if step.get("require_watermark_after"):
            required = datetime.fromisoformat(step["require_watermark_after"])
            assert watermarks and max(watermarks) > required
        if step.get("require_dedup_eviction"):
            removed = sum(
                state.get("numRowsRemoved", 0)
                for p in progress for state in p.get("stateOperators", [])
                if "dedup" in state.get("operatorName", "").lower()
            )
            assert removed > 0, "Observe real dedup eviction before late input"
        if step.get("idle"):
            watermark_before_idle = max(watermarks)
        if step["events"]:
            staged = tmp_path / f"staged-{index}.json"
            staged.write_text("\n".join(
                json.dumps(event) for event in step["events"]
            ) + "\n")
            staged.replace(source / f"batch-{index:02}.json")

        def observe(batch, batch_id):
            # 전량 관측 후 실패를 기록하며 Gold sink는 구현하지 않는다.
            records = observed_rows(batch.collect())
            attempt = {"batch_id": batch_id, "rows": records}
            number = len(list(attempts.glob("*.json")))
            (attempts / f"attempt-{number:04}.json").write_text(
                json.dumps(attempt, sort_keys=True)
            )
            if (step.get("fail_after_output") and records
                    and not failed_once.exists()):
                failed_once.write_text(str(batch_id))
                raise RuntimeError("PHASE3_LOCAL_OUTPUT_BEFORE_CHECKPOINT")

        if step.get("fail_after_output"):
            with pytest.raises(
                Exception, match="PHASE3_LOCAL_OUTPUT_BEFORE_CHECKPOINT",
            ):
                run_available(plan, checkpoint, observe)
            assert failed_once.exists()
        progress.extend(run_available(plan, checkpoint, observe))

    attempts_list = attempts_at(attempts)
    final_by_batch = {}
    for attempt in attempts_list:
        batch_id = attempt["batch_id"]
        if batch_id in final_by_batch:
            assert final_by_batch[batch_id] == attempt["rows"]
        final_by_batch[batch_id] = attempt["rows"]
    final = sorted(
        [row for rows in final_by_batch.values() for row in rows],
        key=lambda row: row["window_start"],
    )
    assert final == scenario["expected_stream_final"]
    if failed_once.exists():
        failed_id = int(failed_once.read_text())
        assert sum(a["batch_id"] == failed_id for a in attempts_list) >= 2
    if scenario["name"] == "watermark_late":
        assert sum(
            state.get("numRowsDroppedByWatermark", 0)
            for p in progress for state in p.get("stateOperators", [])
        ) >= 2
    if watermark_before_idle is not None:
        last = [
            datetime.fromisoformat(p["eventTime"]["watermark"])
            for p in progress if p.get("eventTime", {}).get("watermark")
        ]
        assert max(last) == watermark_before_idle
    (tmp_path / "progress.json").write_text(json.dumps(progress, indent=2))
