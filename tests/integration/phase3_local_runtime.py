"""명시적인 로컬 Spark 4/Delta 4 환경에서만 실행하는 공통 테스트 도구."""

import os

import pytest


@pytest.fixture(scope="module")
def delta_spark(tmp_path_factory):
    """의존성이 없으면 skip으로 성공을 가장하지 않고 실패한다."""
    if os.getenv("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("Local-only verification forbids Databricks")
    import delta
    import pyspark
    from pyspark.sql import SparkSession

    assert pyspark.__version__ == "4.0.0"
    from importlib.metadata import version
    assert version("delta-spark") == "4.0.0"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    temp = tmp_path_factory.mktemp("delta-session")
    builder = (
        SparkSession.builder.master("local[2]").appName("ktd-phase3-delta")
        .config("spark.ui.enabled", "false")
        .config("spark.api.mode", "classic")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", str(temp / "warehouse"))
        .config("spark.jars.ivy", os.environ["KTD_LOCAL_IVY"])
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.snapshotPartitions", "2")
        .config("spark.sql.streaming.statefulOperator."
                "checkCorrectness.enabled",
                "true")
    )
    spark = delta.configure_spark_with_delta_pip(builder).getOrCreate()
    assert spark.sparkContext.master == "local[2]"
    try:
        yield spark
    finally:
        spark.stop()


def saved_rows(sink):
    """timestamp 수집을 호스트 timezone과 무관하게 UTC 문자열로 표현한다."""
    from pyspark.sql import functions as functions

    frame = sink.spark.read.format("delta").load(str(sink.path))
    for field in ("window_start", "window_end"):
        frame = frame.withColumn(field, functions.date_format(
            field, "yyyy-MM-dd'T'HH:mm:ssXXX",
        ))
    return sorted(
        [row.asDict() for row in frame.collect()],
        key=lambda row: tuple(str(row[key]) for key in sorted(row)),
    )
