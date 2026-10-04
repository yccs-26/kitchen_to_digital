"""Opt-in real Kafka -> Databricks tests; retain tables and checkpoints.

Requires KTD_RUN_BRONZE_INTEGRATION=1, Connect authentication,
KTD_KAFKA_BOOTSTRAP_SERVERS (reachable FROM compute), and
KTD_BRONZE_TEST_CHECKPOINT_ROOT=/Volumes/catalog/schema/volume/job1-tests.
A fresh managed table/checkpoint pair is created for each run, never deleted.
"""

import os
import time
from dataclasses import replace
from datetime import UTC
from uuid import uuid4

import pytest

from streaming.jobs.raw_ingestion import IngestionConfig, start_ingestion

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("KTD_RUN_BRONZE_INTEGRATION") != "1",
        reason="Requires opt-in Kafka/Databricks connectivity and UC write access",
    ),
]


def wait_for_offsets(spark, table, expected, query, timeout=180):
    from pyspark.sql import functions as F

    keys = list({record["key"] for record in expected.values()})
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if query.exception():
            raise AssertionError(str(query.exception()))
        rows = spark.table(table).where(F.col("key").isin(keys)).collect()
        actual = {(r.topic, r.partition, r.offset): r for r in rows}
        assert len(rows) == len(actual), "same Kafka offset stored twice"
        if actual.keys() == expected.keys():
            for position, record in expected.items():
                row = actual[position]
                assert bytes(row.key) == record["key"]
                assert (None if row.value is None else bytes(row.value)) == record[
                    "value"
                ]
                assert [
                    (h.key, None if h.value is None else bytes(h.value))
                    for h in row.headers
                ] == record["headers"]
                assert row.kafka_timestamp is not None
                assert row.ingested_at is not None
            return actual
        time.sleep(2)
    raise AssertionError(f"Bronze timed out; missing={expected.keys() - actual.keys()}")


def test_raw_bytes_republication_and_checkpoint_restart():
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.serialization import MessageField, SerializationContext
    from databricks.connect import DatabricksSession

    from simulator.equipment import EQUIPMENTS
    from simulator.main import generate_event
    from simulator.producer import KafkaEventProducer

    run_id = uuid4().hex
    root = os.environ["KTD_BRONZE_TEST_CHECKPOINT_ROOT"]
    namespace = os.getenv("KTD_BRONZE_TEST_SCHEMA", "ktd.bronze")
    config = IngestionConfig(
        bootstrap_servers=os.environ["KTD_KAFKA_BOOTSTRAP_SERVERS"],
        table=f"{namespace}.sensor_raw_test_{run_id}",
        checkpoint=f"{root}/{run_id}",
    )
    spark = DatabricksSession.builder.getOrCreate()
    producer = KafkaEventProducer()
    assert producer.topic == config.topic
    expected = {}
    source = Consumer(
        {
            "bootstrap.servers": producer.bootstrap_servers,
            "group.id": f"ktd-bronze-verify-{run_id}",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    def publish(label, payload=None, raw=None, headers=None):
        key = f"p1-{run_id}-{label}".encode()
        if payload is not None:
            key = payload["equipment_id"].encode()
            raw = producer.value_serializer(
                payload, SerializationContext(config.topic, MessageField.VALUE)
            )
        headers = headers or []
        delivered = []
        errors = []

        def delivered_callback(error, message):
            if error:
                errors.append(str(error))
            else:
                delivered.append(
                    (message.topic(), message.partition(), message.offset())
                )

        producer.producer.produce(
            config.topic,
            key=key,
            value=raw,
            headers=headers,
            on_delivery=delivered_callback,
        )
        assert producer.producer.flush(15) == 0
        assert not errors and len(delivered) == 1, errors
        position = delivered[0]
        # Check the broker bytes too, not just our intended producer input.
        source.assign([TopicPartition(*position)])
        message = source.poll(15)
        assert message is not None and message.error() is None
        assert (message.topic(), message.partition(), message.offset()) == position
        assert message.key() == key and message.value() == raw
        assert (message.headers() or []) == headers
        expected[position] = {"key": key, "value": raw, "headers": headers}
        print(f"[SOURCE] case={label} lineage={position} bytes={len(raw or b'')}")
        return position

    query = None
    try:
        print(f"[RESOURCES] table={config.table} checkpoint={config.checkpoint}")
        query = start_ingestion(spark, config)
        for index in range(3):
            payload = generate_event(EQUIPMENTS[0]).to_dict()
            payload["equipment_id"] = f"p1-{run_id}-normal-{index}"
            publish("normal", payload=payload)
        duplicate = generate_event(EQUIPMENTS[0]).to_dict()
        duplicate["equipment_id"] = f"p1-{run_id}-duplicate"
        first = publish("duplicate-1", payload=duplicate)
        second = publish("duplicate-2", payload=duplicate)
        assert first != second
        publish(
            "corrupt",
            raw=b"\xff\x00not-avro\x80",
            headers=[
                ("repeated", b"\x00\xff"),
                ("repeated", b"second"),
                ("null", None),
            ],
        )
        publish("tombstone", raw=None)
        before = wait_for_offsets(spark, config.table, expected, query)
        assert first in before and second in before
        print(f"[A/B/C PASS] event_id={duplicate['event_id']} offsets={first},{second}")
        query_id, run_before = query.id, query.runId
        query.stop()
        publish("while-stopped", raw=b"while-stopped")
        # A different startingOffsets value MUST be ignored with this checkpoint.
        query = start_ingestion(spark, replace(config, starting_offsets="latest"))
        assert query.id == query_id and query.runId != run_before
        publish("after-restart", raw=b"after-restart")
        after = wait_for_offsets(spark, config.table, expected, query)
        for position in before:
            assert before[position] == after[position], "existing record changed"
        query.stop()
        # Drain from exactly the same checkpoint, then perform a final duplicate audit.
        query = start_ingestion(spark, config, available_now=True)
        assert query.awaitTermination(180), "AvailableNow did not terminate"
        wait_for_offsets(spark, config.table, expected, query)
        print(f"[D PASS] {len(expected)} offsets, no missing or repeated lineage")
    finally:
        if query is not None and query.isActive:
            query.stop()
        source.close()


def test_native_iceberg_sink_restart_with_file_fixtures():
    """Isolate sink/checkpoint behavior; this is NOT a Kafka E2E test."""
    from datetime import datetime

    from databricks.connect import DatabricksSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType

    from streaming.jobs.raw_ingestion import (
        bronze_records,
        start_bronze_sink,
    )

    spark = DatabricksSession.builder.getOrCreate()
    run_id = uuid4().hex
    root = os.environ["KTD_BRONZE_TEST_CHECKPOINT_ROOT"]
    namespace = os.getenv("KTD_BRONZE_TEST_SCHEMA", "ktd.bronze")
    config = IngestionConfig(
        bootstrap_servers="unused-file-fixture:9092",
        table=f"{namespace}.sensor_raw_sink_test_{run_id}",
        checkpoint=f"{root}/{run_id}/checkpoint",
    )
    input_path = f"{root}/{run_id}/input"
    schema = StructType.fromDDL("""
        topic STRING, partition INT, offset BIGINT, timestamp TIMESTAMP,
        key BINARY, value BINARY, headers ARRAY<STRUCT<key: STRING, value: BINARY>>
    """)
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    timestamp = datetime(2026, 9, 30, tzinfo=UTC)
    fixtures = [
        ("fixture", 0, 10, timestamp, b"same", b"same-event", []),
        ("fixture", 0, 20, timestamp, b"same", b"same-event", []),
        (
            "fixture",
            1,
            0,
            timestamp,
            b"bad",
            b"\x00\xff\x80",
            [
                ("x", b"\xff"),
                ("x", b"second"),
                ("null", None),
            ],
        ),
        ("fixture", 1, 1, timestamp, None, None, []),
    ]
    spark.createDataFrame(fixtures, schema).coalesce(1).write.mode("append").json(
        input_path
    )

    def run_batch():
        source = spark.readStream.schema(schema).json(input_path)
        query = start_bronze_sink(
            spark,
            bronze_records(source),
            config,
            available_now=True,
        )
        try:
            assert query.awaitTermination(180), "sink query did not finish"
            return query.id, query.runId
        finally:
            if query.isActive:
                query.stop()

    print(f"[SINK RESOURCES] table={config.table} checkpoint={config.checkpoint}")
    first_id, first_run = run_batch()
    before = (
        spark.table(config.table)
        .withColumn("_kafka_epoch", F.unix_micros("kafka_timestamp"))
        .orderBy("partition", "offset")
        .collect()
    )
    assert len(before) == 4
    for row, fixture in zip(before, fixtures):
        assert (row.topic, row.partition, row.offset) == fixture[:3]
        assert (None if row.key is None else bytes(row.key)) == fixture[4]
        assert (None if row.value is None else bytes(row.value)) == fixture[5]
        assert [
            (h.key, None if h.value is None else bytes(h.value)) for h in row.headers
        ] == fixture[6]
        assert row._kafka_epoch == int(timestamp.timestamp() * 1_000_000)
        assert row.ingested_at is not None
    # Add a new file between runs, keeping the exact checkpoint and target.
    spark.createDataFrame(
        [("fixture", 1, 2, timestamp, b"new", b"new", [])],
        schema,
    ).coalesce(1).write.mode("append").json(input_path)
    second_id, second_run = run_batch()
    assert first_id == second_id and first_run != second_run
    after = (
        spark.table(config.table)
        .withColumn("_kafka_epoch", F.unix_micros("kafka_timestamp"))
        .orderBy("partition", "offset")
        .collect()
    )
    assert len(after) == 5 and after[:4] == before
    run_batch()  # No new input must not append old rows.
    assert spark.table(config.table).count() == 5
    assert not (
        spark.table(config.table)
        .groupBy("topic", "partition", "offset")
        .count()
        .where(F.col("count") != 1)
        .take(1)
    )
    print(
        "[SINK PASS] bytes, nulls, headers, 5 lineages, same checkpoint; Kafka NOT tested"
    )
