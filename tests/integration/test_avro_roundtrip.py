"""Opt-in, finite roundtrip against the real raw topic; never commits offsets."""

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import (
    Schema,
    SchemaRegistryClient,
    topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from simulator.producer import KafkaEventProducer

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("KTD_RUN_INTEGRATION") != "1",
        reason="Set KTD_RUN_INTEGRATION=1 to publish 4 records to the real raw topic",
    ),
]


def test_avro_roundtrip():
    producer = KafkaEventProducer()
    assert producer.topic == "kitchen.sensor.raw", "P0-6 acceptance requires the raw topic"
    run_id = uuid4().hex
    event_time = datetime.now(timezone.utc).isoformat()
    expected = {}
    for index, value in enumerate([2.5, -18.75, 0.0, 175.125]):
        event_id = str(uuid4())
        expected[event_id] = {
            "event_id": event_id,
            "event_time": event_time,
            "store_id": "p0-6-test-store",
            "equipment_id": f"p0-6-{run_id}-{index}",
            "equipment_type": "refrigerator",
            "metric_name": "temperature_celsius",
            "metric_value": value,
            "unit": "celsius",
            "schema_version": "1.0.0",
            "source": "simulator-왕복검증",
        }
    keys = {payload["equipment_id"].encode("utf-8") for payload in expected.values()}
    schema_path = Path(__file__).resolve().parents[2] / "schemas/avro/sensor_metric_event.avsc"
    registered = producer.registry.lookup_schema(
        f"{producer.topic}-value", Schema(schema_path.read_text(), "AVRO")
    )
    # A separate client and no reader schema: recover using the record's writer schema ID.
    registry = SchemaRegistryClient({
        "url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081"),
        "timeout": 10,
    })
    deserialize = AvroDeserializer(registry, conf={
        "use.latest.version": False,
        "subject.name.strategy": topic_subject_name_strategy,
    })
    consumer = Consumer({
        "bootstrap.servers": producer.bootstrap_servers,
        "group.id": f"ktd-p0-6-{run_id}",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    })
    try:
        metadata = consumer.list_topics(timeout=10)
        assert producer.topic in metadata.topics, "raw topic is missing"
        topic = metadata.topics[producer.topic]
        assert topic.error is None, topic.error
        assert topic.partitions, "raw topic has no partitions"
        starts = {}
        for partition in sorted(topic.partitions):
            _, high = consumer.get_watermark_offsets(
                TopicPartition(producer.topic, partition), timeout=10, cached=False
            )
            starts[partition] = high
        consumer.assign([
            TopicPartition(producer.topic, partition, offset)
            for partition, offset in starts.items()
        ])
        print(f"[ROUNDTRIP START] offsets={starts}")
        for payload in expected.values():
            producer.send(payload["equipment_id"], payload)
        producer.flush()
        assert producer.delivered == len(expected)
        received = set()
        deadline = time.monotonic() + 15
        while len(received) < len(expected) and time.monotonic() < deadline:
            message = consumer.poll(min(1.0, max(0.0, deadline - time.monotonic())))
            if message is None:
                continue
            assert message.error() is None, message.error()
            # Isolate this run from concurrent producers before decoding their payloads.
            if message.key() not in keys:
                continue
            raw = message.value()
            assert raw is not None and len(raw) > 5 and raw[0] == 0
            schema_id = int.from_bytes(raw[1:5], "big")
            assert schema_id == registered.schema_id
            decoded = deserialize(
                raw, SerializationContext(message.topic(), MessageField.VALUE)
            )
            event_id = decoded["event_id"]
            assert event_id in expected
            assert event_id not in received, "duplicate fixture event received"
            assert decoded == expected[event_id], "Avro fields differ from the original payload"
            assert type(decoded["metric_value"]) is float
            assert message.key().decode("utf-8") == decoded["equipment_id"]
            received.add(event_id)
            print(
                f"[ROUNDTRIP OK] event_id={event_id} key={decoded['equipment_id']} "
                f"partition={message.partition()} offset={message.offset()} "
                f"schema_id={schema_id} metric_value={decoded['metric_value']} "
                "fields_equal=True value_type=float"
            )
        assert received == set(expected), f"Timed out; missing event_ids={set(expected) - received}"
    finally:
        consumer.close()
