"""실제 로컬 Kafka/Registry/Delta를 사용하는 격리된 Spark 자식 프로세스."""

import argparse
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from confluent_kafka import Producer  # noqa: E402
from confluent_kafka.schema_registry import SchemaRegistryClient  # noqa: E402
from confluent_kafka.schema_registry.avro import AvroSerializer  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from streaming.jobs import sensor_validation_job as job  # noqa: E402
from streaming.jobs.raw_ingestion import IngestionConfig  # noqa: E402
from streaming.sinks.delta_silver import DeltaSilverStorage  # noqa: E402
from streaming.sinks.silver import SilverSink  # noqa: E402


def journal(path, value):
    with path.open("a") as output:
        output.write(json.dumps(value) + "\n")
        output.flush()
        os.fsync(output.fileno())


class ObservedProducer:
    """실제 Kafka ACK를 기록하며 지정된 경계에서만 결함을 주입한다."""

    def __init__(self, root, role, mode, bootstrap):
        self.root, self.role, self.mode = root, role, mode
        self.producer = Producer({
            "bootstrap.servers": bootstrap, "acks": "all",
            "allow.auto.create.topics": False,
            "delivery.report.only.error": False,
            "request.timeout.ms": 1000, "message.timeout.ms": 2500,
            "log_level": 0,
        })

    def produce(self, **kwargs):
        if (self.role == "validated" and self.mode == "before_publish"
                or self.role == "quarantine" and self.mode == "quarantine"):
            raise TimeoutError("controlled: before Kafka publish")
        callback = kwargs.pop("on_delivery")

        def observed(error, message):
            if error is None:
                journal(self.root / "deliveries.jsonl", dict(
                    role=self.role, topic=message.topic(),
                    partition=message.partition(), offset=message.offset(),
                    callback_forwarded=self.mode != "uncertain_ack",
                ))
            if self.mode != "uncertain_ack":
                callback(error, message)

        self.producer.produce(**kwargs, on_delivery=observed)

    def flush(self, timeout):
        return self.producer.flush(timeout)


class BeforeMergeStorage(DeltaSilverStorage):
    def insert_if_absent(self, event):
        # 실제 Spark/Delta 분석 오류를 주입한다. canonical target은 보존한다.
        self._spark.sql(
            f"MERGE INTO {self._table} t "
            "USING (SELECT :event_id AS event_id) s "
            "ON t.event_id = s.event_id "
            "WHEN NOT MATCHED THEN INSERT (missing_test_column) "
            "VALUES (s.event_id)", args={"event_id": event["event_id"]},
        ).collect()
        raise AssertionError("invalid controlled MERGE unexpectedly succeeded")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("root", type=Path)
    parser.add_argument("mode")
    parser.add_argument("--checkpoint", default="checkpoint")
    args = parser.parse_args()
    root = args.root.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    topics = manifest["contract"]["topics"]
    bootstrap = manifest["contract"]["bootstrap"]
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("ktd-local-kafka-mission")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .config("spark.sql.warehouse.dir", str(root / "warehouse"))
        .config("spark.jars", os.environ["KTD_LOCAL_KAFKA_JARS"])
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    query = None
    try:
        table = "spark_catalog.ktd_mission.canonical"
        spark.sql("CREATE DATABASE IF NOT EXISTS ktd_mission")
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "event_id STRING,event_time TIMESTAMP,store_id STRING,"
            "equipment_id STRING,equipment_type STRING,metric_name STRING,"
            "metric_value DOUBLE,unit STRING,schema_version STRING,"
            f"source STRING) USING DELTA LOCATION '{root / 'delta'}'"
        )
        config = job.ValidationJobConfig(
            source=IngestionConfig(
                bootstrap, "/Volumes/local/p2/checkpoints/job2/test",
                topic=topics["raw"],
            ), silver_table=table, processing_version="local-mission",
            validated_topic=topics["validated"],
            quarantine_topic=topics["quarantine"], delivery_timeout=4,
        )
        rules = job.load_rules(str(ROOT / "config/validation_rules.json"))
        registry = SchemaRegistryClient({
            "url": manifest["local_registry_url"], "timeout": 1,
            "max.retries": 0,
        })
        serializer = AvroSerializer(
            registry,
            (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text(),
            conf={"auto.register.schemas": False, "use.latest.version": False},
        )
        storage_class = (BeforeMergeStorage if args.mode == "before_merge"
                         else DeltaSilverStorage)
        storage = storage_class(spark=spark, table_name=table)
        dependencies = dict(
            registry_client=registry, silver=SilverSink(storage),
            quarantine_producer=ObservedProducer(
                root, "quarantine", args.mode, bootstrap,
            ),
            validated_producer=ObservedProducer(
                root, "validated", args.mode, bootstrap,
            ),
            serializer=serializer, config=config, rules=rules,
            # Fixture 시각 경계는 제어된 로컬 clock으로 고정한다.
            reference_time=(datetime.fromisoformat(manifest["reference_time"])
                            + timedelta(minutes=(
                                20 if args.mode == "late_duplicate" else 0
                            ))),
        )

        def handle(batch, batch_id):
            if args.mode == "broker":
                # 장애 경계를 sink로 고정한다. Kafka source의 lazy fetch가
                # 중단된 broker를 기다리는 상황과 Producer timeout을 구분한다.
                batch.persist()
                batch.count()
            if args.mode in ("broker", "registry"):
                (root / "fault-ready").write_text(args.mode)
                deadline = time.monotonic() + 30
                while not (root / "fault-released").exists():
                    if time.monotonic() > deadline:
                        raise TimeoutError("controller did not release fault")
                    time.sleep(0.1)
            counts = job.process_batch(batch, batch_id, **dependencies)
            journal(root / "batches.jsonl", dict(batch_id=batch_id, **counts))
            if args.mode == "kill_after_ack":
                (root / "kill-ready").write_text("all sink ACKs before commit")
                while True:
                    time.sleep(1)

        source = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("assign", json.dumps({topics["raw"]: [0, 1, 2]}))
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "true")
            .option("kafka.allow.auto.create.topics", "false")
            .load()
        )
        query = (
            source.writeStream.foreachBatch(handle)
            .option("checkpointLocation", str(root / args.checkpoint))
            .queryName("ktd_local_kafka_mission")
            .trigger(availableNow=True).start()
        )
        journal(root / "queries.jsonl", dict(
            id=str(query.id), run_id=str(query.runId),
            checkpoint=args.checkpoint, mode=args.mode,
        ))
        if not query.awaitTermination(90):
            raise TimeoutError("local Kafka query exceeded 90s")
        canonical = []
        for row in spark.table(table).select("event_id").collect():
            event = dict(storage.lookup(row.event_id))
            event["event_time"] = event["event_time"].isoformat()
            canonical.append(event)
        (root / "canonical.json").write_text(json.dumps(canonical))
        progress = [json.loads(x.json) if hasattr(x, "json") else x
                    for x in query.recentProgress]
        journal(root / "progress.jsonl", dict(
            mode=args.mode, checkpoint=args.checkpoint, progress=progress,
        ))
    finally:
        if query is not None:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
