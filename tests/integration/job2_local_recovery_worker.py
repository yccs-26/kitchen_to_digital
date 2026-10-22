"""격리된 로컬 Spark 자식 프로세스. Kafka/Registry만 테스트 대역이다."""

import argparse
from dataclasses import replace
from io import BytesIO
import json
import os
from pathlib import Path
import sys
from unittest.mock import Mock

from confluent_kafka.schema_registry import Schema
from fastavro import schemaless_writer
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests/unit"))

from test_sensor_validation_job import (  # noqa: E402
    NOW, SCHEMA, SCHEMA_TEXT, config, rules,
)
from streaming.jobs import sensor_validation_job as job  # noqa: E402
from streaming.sinks.delta_silver import DeltaSilverStorage  # noqa: E402
from streaming.sinks.silver import SilverSink  # noqa: E402


class JournalProducer:
    """영속 ACK 관측 journal이며 Kafka broker 구현이나 outbox가 아니다."""

    def __init__(self, root, mode):
        self.root = root
        self.mode = mode
        self.pending = None

    def produce(self, **kwargs):
        self.pending = kwargs

    def flush(self, timeout):
        pending = self.pending
        if self.mode == "before_ack":
            raise TimeoutError("injected: Silver committed, ACK unavailable")
        entry = {
            "topic": pending["topic"],
            "key": pending["key"].decode(),
            "value_hex": pending["value"].hex(),
        }
        with (self.root / "acks.jsonl").open("a") as output:
            output.write(json.dumps(entry) + "\n")
            output.flush()
            os.fsync(output.fileno())
        message = Mock()
        message.offset.return_value = 0
        pending["on_delivery"](None, message)
        return 0


def serialize(event, context):
    """실제 Avro payload를 만들되 Registry HTTP 호출은 하지 않는다."""
    body = BytesIO()
    schemaless_writer(body, SCHEMA, event)
    return b"\x00\x00\x00\x00\x01" + body.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("mode", choices=("before_ack", "after_ack", "success"))
    parser.add_argument("--checkpoint", default="checkpoint")
    args = parser.parse_args()
    root = args.root.resolve()
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ktd-p2-8h-local-recovery")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .config("spark.jars", os.environ["KTD_LOCAL_DELTA_JARS"])
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        table = "spark_catalog.p2_8h.canonical"
        spark.sql("CREATE DATABASE IF NOT EXISTS p2_8h")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "event_id STRING, event_time TIMESTAMP, store_id STRING, "
            "equipment_id STRING, equipment_type STRING, metric_name STRING, "
            "metric_value DOUBLE, unit STRING, schema_version STRING, "
            f"source STRING) USING DELTA LOCATION '{root / 'delta'}'"
        )
        registry = Mock()
        registry.get_schema.return_value = Schema(SCHEMA_TEXT, "AVRO")
        producer = JournalProducer(root, args.mode)
        dependencies = dict(
            registry_client=registry,
            silver=SilverSink(DeltaSilverStorage(
                spark=spark, table_name=table,
            )),
            quarantine_producer=producer, validated_producer=producer,
            serializer=serialize,
            config=replace(config.__wrapped__(), silver_table=table),
            rules=rules.__wrapped__(), reference_time=NOW,
        )

        def handle(batch, batch_id):
            counts = job.process_batch(batch, batch_id, **dependencies)
            with (root / "batches.jsonl").open("a") as output:
                output.write(json.dumps({
                    "batch_id": batch_id, **counts,
                }) + "\n")
            if args.mode == "after_ack":
                # foreachBatch가 반환하지 않았으므로 Spark commit은 없어야 한다.
                os._exit(23)

        source = spark.readStream.schema(
            "topic STRING, partition INT, offset LONG, "
            "key BINARY, value BINARY"
        ).json(str(root / "input"))
        query = (
            source.writeStream.foreachBatch(handle)
            .option("checkpointLocation", str(root / args.checkpoint))
            .queryName("ktd_p2_8h_local_only")
            .trigger(availableNow=True).start()
        )
        with (root / "queries.jsonl").open("a") as output:
            output.write(json.dumps({
                "id": str(query.id), "run_id": str(query.runId),
                "name": query.name, "checkpoint": args.checkpoint,
            }) + "\n")
        query.awaitTermination()
        rows = [
            row.asDict()
            for row in spark.table(table).orderBy("event_id").collect()
        ]
        # canonical 비교는 production adapter의 UTC 왕복 경계를 사용한다.
        storage = DeltaSilverStorage(spark=spark, table_name=table)
        canonical = [dict(storage.lookup(row["event_id"])) for row in rows]
        (root / "canonical.json").write_text(
            json.dumps(canonical, default=str)
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
