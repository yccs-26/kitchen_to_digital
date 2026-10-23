"""validated 입력의 bounded dedup과 두 해상도 Gold 확정 출력을 구성한다.

실행 환경 생성이나 자동 Kafka 연결은 하지 않는다. 단일 query/checkpoint의
두 window branch를 union하며 동일 ID의 payload 충돌 탐지는 upstream 책임이다.
"""

from collections.abc import Callable, Mapping
from datetime import datetime, timezone

from streaming.aggregations.gold_row_mapper import map_gold_row
from streaming.aggregations.windows import spark_window_spec


VALIDATED_FIELDS = (
    "event_id", "event_time", "store_id", "equipment_id", "equipment_type",
    "metric_name", "metric_value", "unit", "schema_version", "source",
)


def validated_schema():
    """Phase 2의 기존 Avro 10필드를 JSON 로컬 검증에서도 그대로 사용한다."""
    from pyspark.sql.types import (
        DoubleType, StringType, StructField, StructType,
    )

    return StructType([
        StructField(name, DoubleType() if name == "metric_value"
                    else StringType(), nullable=False)
        for name in VALIDATED_FIELDS
    ])


def read_validated_kafka(
    spark, options: Mapping[str, str], registry_factory: Callable,
):
    """호출자가 명시한 Kafka 설정과 executor Registry factory만 사용한다.

    Confluent frame/schema ID 처리는 기존 Phase 2 decoder에 위임한다.
    인증값을 환경에서 추측하거나 로그에 출력하지 않는다.
    """
    if options.get("subscribe") != "kitchen.sensor.validated":
        raise ValueError("Expected explicit kitchen.sensor.validated topic")
    if not options.get("kafka.bootstrap.servers"):
        raise ValueError("Kafka bootstrap servers must be supplied explicitly")
    if "assign" in options or "subscribePattern" in options:
        raise ValueError("Only the validated topic subscription is supported")
    from streaming.sources.validated_kafka import transform_kafka_records

    source = spark.readStream.format("kafka").options(**dict(options)).load()
    return transform_kafka_records(source, registry_factory)


def decode_validated_record(value, key, registry) -> dict[str, object]:
    """기존 Confluent/Avro decoder를 재사용하며 wire 계약을 바꾸지 않는다."""
    from streaming.sources.validated_kafka import decode_record

    return decode_record(value, key, registry)


def build_metric_aggregation(
    validated, *, window_types: tuple[str, ...] = (
        "1m_tumbling", "5m_sliding",
    ),
):
    """10분 watermark와 native dedup/window를 하나의 Append plan으로 만든다."""
    from functools import reduce
    from pyspark.sql import functions as functions

    if not validated.isStreaming:
        raise ValueError("Expected a streaming validated DataFrame")
    if validated.sparkSession.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("Metric aggregation requires UTC session timezone")
    if not window_types or len(set(window_types)) != len(window_types):
        raise ValueError("Window types must be nonempty and unique")
    specs = [(kind, spark_window_spec(kind)) for kind in window_types]
    if not set(VALIDATED_FIELDS) <= set(validated.columns):
        raise ValueError("Validated stream is missing required fields")
    invalid = reduce(
        lambda left, right: left | right,
        (functions.col(name).isNull() for name in VALIDATED_FIELDS),
    )
    for name in set(VALIDATED_FIELDS) - {"event_time", "metric_value"}:
        invalid = invalid | (functions.length(
            functions.trim(functions.col(name))
        ) == 0)
    raw_time = functions.col("event_time")
    if dict(validated.dtypes)["event_time"] == "string":
        invalid = invalid | ~raw_time.rlike(r"(Z|[+-]\d{2}:\d{2})$")
    value = functions.col("metric_value")
    invalid = invalid | functions.isnan(value) | (functions.abs(value) == (
        functions.lit(float("inf"))
    ))
    stream = validated.withColumn(
        "event_time", functions.when(
            invalid, functions.raise_error("INVALID_VALIDATED_EVENT"),
        ).otherwise(functions.to_timestamp(raw_time)),
    ).withWatermark("event_time", "10 minutes").dropDuplicatesWithinWatermark(
        ["event_id"]
    )
    plans = []
    for kind, (duration, slide) in specs:
        grouped = stream.groupBy(
            functions.window("event_time", duration, slide),
            "store_id", "equipment_id", "metric_name", "unit",
        ).agg(
            functions.count("event_id").alias("count"),
            functions.sum("metric_value").alias("sum"),
            functions.min("metric_value").alias("min"),
            functions.max("metric_value").alias("max"),
        ).withColumn("avg", functions.col("sum") / functions.col("count"))
        plans.append(grouped.select(
            functions.date_format(
                "window.start", "yyyy-MM-dd'T'HH:mm:ssXXX",
            ).alias("window_start"),
            functions.date_format(
                "window.end", "yyyy-MM-dd'T'HH:mm:ssXXX",
            ).alias("window_end"),
            functions.lit(kind).alias("window_type"),
            "store_id", "equipment_id", "metric_name", "unit",
            "count", "sum", "min", "max", "avg",
        ))
    return reduce(lambda left, right: left.unionByName(right), plans)


def _mapped_gold_rows(batch):
    """생산 plan의 UTC 경계를 확인하고 batch를 전량 순회한다."""
    for record in batch.toLocalIterator():
        logical = record.asDict()
        for field in ("window_start", "window_end"):
            value = logical[field]
            if not isinstance(value, str):
                raise ValueError("Expected UTC string window boundary")
            instant = datetime.fromisoformat(value)
            if instant.utcoffset() is None:
                raise ValueError("Window boundary must include timezone")
            logical[field] = instant.astimezone(timezone.utc)
        yield map_gold_row(logical)


def gold_batch_callback(sink):
    """경로 또는 테이블명만 캡처하고 batch 세션으로 sink를 다시 구성한다."""
    from streaming.sinks.gold_delta import GoldDeltaSink
    from streaming.sinks.gold_uc_delta import GoldUCDeltaSink

    if isinstance(sink, GoldUCDeltaSink):
        table = sink.table

        def consume_uc_delta(batch, batch_id):
            from streaming.sinks.gold_uc_delta import GoldUCDeltaSink

            target = GoldUCDeltaSink(
                batch.sparkSession, table, single_writer=True,
            )
            target.merge_rows(_mapped_gold_rows(batch))

        return consume_uc_delta

    if isinstance(sink, GoldDeltaSink):
        path = str(sink.path)

        def consume_delta(batch, batch_id):
            from streaming.sinks.gold_delta import GoldDeltaSink

            target = GoldDeltaSink(batch.sparkSession, path)
            target.merge_rows(_mapped_gold_rows(batch))

        return consume_delta

    # 로컬 장애 주입/관측 adapter 전용이다. 운영 직렬화 보장을 뜻하지 않는다.
    def consume_injected(batch, batch_id):
        sink.merge_rows(_mapped_gold_rows(batch))

    return consume_injected


def start_metric_query(
    plan, sink, checkpoint: str, *, available_now=False, callback=None,
):
    """기존 checkpoint를 삭제하지 않고 단일 Append-final query를 시작한다."""
    writer = (
        plan.writeStream.outputMode("append")
        .foreachBatch(callback or gold_batch_callback(sink))
        .option("checkpointLocation", checkpoint)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    return writer.start()


def start_metric_aggregation(
    spark, sink, checkpoint: str, *, kafka_options: Mapping[str, str],
    registry_factory: Callable, available_now: bool = False,
):
    """명시적 의존성을 연결하며 세션·테이블·클라우드 리소스는 생성하지 않는다."""
    validated = read_validated_kafka(spark, kafka_options, registry_factory)
    plan = build_metric_aggregation(validated)
    return start_metric_query(
        plan, sink, checkpoint, available_now=available_now,
    )


def prepare_metric_deployment(config_path, *, adapter: str):
    """기존 Preflight를 재사용한 오프라인 후보 선택이며 query를 시작하지 않는다."""
    from streaming.config.phase3_runtime import (
        Phase3DeploymentBlocked, load_phase3_runtime,
    )
    from streaming.tools.phase3_preflight import run_preflight

    report = run_preflight(config_path)
    if report["static_validation"] != "PASS":
        raise Phase3DeploymentBlocked("Static preflight failed")
    settings = load_phase3_runtime(config_path)
    settings.candidate_writer_target(adapter=adapter)
    return settings, report


def start_configured_metric_aggregation(
    spark, config_path, *, adapter: str,
    registry_factory: Callable | None = None,
    available_now: bool = False, approval=None, registry_credential_env=None,
    failure_probe=None,
):
    """명시적 범위 승인과 설정이 일치할 때만 단일 query를 시작한다.

    승인 flag만으로 gate를 해제하지 않는다. 별도 승인 기록·UC 수용과
    명시적 target/checkpoint를 확인한 후 기존 단일 query를 연결한다.
    """
    from streaming.sinks.gold_uc_delta import GoldUCDeltaSink

    settings, _ = prepare_metric_deployment(config_path, adapter=adapter)
    settings.require_deployment_ready(approval)
    if registry_factory is None:
        from streaming.sources.validated_kafka import ExecutorRegistryFactory

        if (not registry_credential_env
                or registry_credential_env
                != approval.registry_credential_env):
            raise ValueError("Approved executor credential ref required")
        registry_factory = ExecutorRegistryFactory(
            settings.registry_url, registry_credential_env,
        )
    sink = GoldUCDeltaSink(
        spark, ".".join((settings.target.catalog, settings.target.schema,
                         settings.target.table)), single_writer=True,
    )
    callback = gold_batch_callback(sink)
    if failure_probe is not None:
        callback = failure_probe.wrap(callback, settings, approval)
    validated = read_validated_kafka(
        spark, settings.kafka_options(), registry_factory,
    )
    plan = build_metric_aggregation(validated)
    return start_metric_query(
        plan, sink, settings.checkpoint, available_now=available_now,
        callback=callback,
    )


def main(argv=None, *, spark=None):
    """승인된 기존 DBR 세션에서만 실행하는 bounded Job3 entrypoint."""
    import argparse
    import os
    from streaming.config.phase3_authorization import load_execution_approval
    from streaming.tools.phase3_failure_probe import (
        FailureProbe, await_bounded_query,
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--approval", required=True)
    parser.add_argument("--adapter", required=True,
                        choices=("uc_managed_delta",))
    parser.add_argument("--registry-credential-env", required=True)
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--probe-stage",
                        choices=("before_merge", "after_merge"))
    parser.add_argument("--probe-batch-id", type=int)
    args = parser.parse_args(argv)
    settings, _ = prepare_metric_deployment(args.config, adapter=args.adapter)
    approval = load_execution_approval(args.approval)
    settings.require_deployment_ready(approval)
    probe = None
    if args.probe_stage is not None or args.probe_batch_id is not None:
        probe = FailureProbe(approval.job_tag, settings.checkpoint,
                             args.probe_stage, args.probe_batch_id)
        probe.wrap(lambda batch, n: None, settings, approval)
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        raise RuntimeError("Live entrypoint requires an approved existing DBR")
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Existing approved Spark session required")
    query = start_configured_metric_aggregation(
        spark, args.config, adapter=args.adapter, approval=approval,
        registry_credential_env=args.registry_credential_env,
        available_now=args.available_now, failure_probe=probe,
    )
    completed = await_bounded_query(query, settings.max_runtime_minutes * 60)
    return 0 if completed else 2


if __name__ == "__main__":
    raise SystemExit(main())
