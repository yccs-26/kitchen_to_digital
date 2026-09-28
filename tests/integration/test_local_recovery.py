import os
import signal
import subprocess
import sys
import time

from pathlib import Path
from uuid import uuid4
from unittest.mock import Mock

import httpx
import pytest

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient, topic_subject_name_strategy
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from simulator.equipment import EQUIPMENTS
from simulator.main import generate_event
from simulator.producer import KafkaEventProducer

ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ["docker", "compose", "-f", "infra/docker/compose.yml"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("KTD_RUN_INTEGRATION") != "1" or os.getenv("KTD_RUN_RECOVERY") != "1",
        reason="Requires KTD_RUN_INTEGRATION=1 and KTD_RUN_RECOVERY=1; interrupts local services",
    ),
]


def compose(*args):
    result = subprocess.run(COMPOSE + list(args), cwd=ROOT, capture_output=True, text=True, timeout=45)
    assert result.returncode == 0, result.stderr


def registry_client():
    return SchemaRegistryClient({"url": "http://localhost:8081", "timeout": 3, "max.retries": 0})


def local_settings(monkeypatch):
    # Recovery is intentionally confined to this repository's local Compose services.
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC_SENSOR_RAW", "kitchen.sensor.raw")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")


def snapshot():
    registry = registry_client()
    subject = "kitchen.sensor.raw-value"
    versions = registry.get_versions(subject)
    schemas = {}
    for version in versions:
        registered = registry.get_version(subject, version)
        schemas[version] = (registered.schema_id, registered.schema.schema_str)
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092", "group.id": f"p0-7-snapshot-{uuid4()}",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "allow.auto.create.topics": False,
    })
    try:
        metadata = consumer.list_topics(timeout=5)
        topic = metadata.topics["kitchen.sensor.raw"]
        assert topic.error is None
        offsets = {p: consumer.get_watermark_offsets(
            TopicPartition(topic.topic, p), timeout=5, cached=False
        ) for p in topic.partitions}
        return schemas, registry.get_compatibility(subject), offsets
    finally:
        consumer.close()


def wait_ready():
    deadline = time.monotonic() + 60
    last_error = None

    while time.monotonic() < deadline:
        try:
            return snapshot()
        
        except Exception as error:
            last_error = error
            time.sleep(1)

    raise AssertionError(f"Services did not recover: {last_error}")


def read_at(partition, offset):
    consumer = Consumer({
        "bootstrap.servers": "localhost:9092", "group.id": f"p0-7-read-{uuid4()}",
        "enable.auto.commit": False, "enable.auto.offset.store": False,
    })

    try:
        consumer.assign([TopicPartition("kitchen.sensor.raw", partition, offset)])
        message = consumer.poll(10)
        assert message is not None and message.error() is None
        assert message.offset() == offset
        return message.key(), message.value()
    
    finally:
        consumer.close()


def publish_with_location(producer, payload):
    locations = []
    original = producer._delivery_callback

    def delivery(error, message):
        original(error, message)

        if error is None:
            locations.append((message.partition(), message.offset()))

    producer._delivery_callback = delivery
    producer.send(payload["equipment_id"], payload)
    producer.flush()

    assert len(locations) == 1
    return locations[0]


def test_sigint_drains_pending_events(monkeypatch, tmp_path):
    local_settings(monkeypatch)
    output_path = tmp_path / "simulator.log"

    with output_path.open("w") as output:
        process = subprocess.Popen(
            [sys.executable, "-u", "-m", "simulator.main"], cwd=ROOT,
            env=os.environ.copy(), stdout=output, stderr=subprocess.STDOUT,
        )

        try:
            deadline = time.monotonic() + 15
            while '"event_id"' not in output_path.read_text() and time.monotonic() < deadline:
                assert process.poll() is None, output_path.read_text()
                time.sleep(0.1)
            assert '"event_id"' in output_path.read_text(), "Simulator did not enqueue an event"
            process.send_signal(signal.SIGINT)
            process.wait(timeout=20)
            
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=5)

    log = output_path.read_text()
    flush = [line for line in log.splitlines() if line.startswith("[KAFKA FLUSH]")]
    assert len(flush) == 1, log
    assert "failed=0 remaining=0" in flush[0], log
    assert "[KAFKA DELIVERED]" in log, log
    assert process.returncode in (0, -signal.SIGINT, 130), log

    print(f"[SIGINT OK] {flush[0]} exit={process.returncode}")


def test_broker_registry_failure_and_restart(monkeypatch):
    local_settings(monkeypatch)
    snapshot()
    ids_before = subprocess.check_output(COMPOSE + ["ps", "-q", "kafka", "schema-registry"], cwd=ROOT, text=True).split()
    assert len(ids_before) == 2

    producer = KafkaEventProducer()
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    partition, offset = publish_with_location(producer, payload)
    record_before = read_at(partition, offset)
    before = snapshot()

    print(f"[BEFORE RESTART] offsets={before[2]} schema_ids={[v[0] for v in before[0].values()]} compatibility={before[1]}")

    outage_locations = []
    original = producer._delivery_callback

    def outage_delivery(error, message):
        original(error, message)

        if error is None:
            outage_locations.append((message.partition(), message.offset()))

    producer._delivery_callback = outage_delivery

    try:
        compose("stop", "-t", "10", "schema-registry", "kafka")
        # Warm serializer uses its cached schema: exercise broker failure, not Registry failure.
        failed_payload = generate_event(EQUIPMENTS[0]).to_dict()
        producer.send(failed_payload["equipment_id"], failed_payload)

        with pytest.raises(RuntimeError, match="delivery incomplete"):
            producer.flush()
        pending = 1 - producer.failed - len(outage_locations)
        assert producer.failed in (0, 1) and pending in (0, 1)
        assert not outage_locations

        print(f"[BROKER FAILURE OK] failed={producer.failed} unresolved_callbacks={pending}; incomplete delivery surfaced")

        cold = KafkaEventProducer()

        # len(Producer) also includes protocol requests; observe produce() directly instead.
        cold.producer = Mock(wraps=cold.producer)

        with pytest.raises(httpx.TransportError):
            cold.send(payload["equipment_id"], payload)
        cold.producer.produce.assert_not_called()

        print("[REGISTRY FAILURE OK] cold serializer failed before enqueue; produce_calls=0")

    finally:
        compose("start", "kafka", "schema-registry")
        wait_ready()
    # A flush timeout does not discard queued records. Account for their final outcome.
    try:
        producer.flush()

    except RuntimeError:
        assert producer.failed == 1

    assert producer.failed + len(outage_locations) == 1

    if outage_locations:
        _, recovered_bytes = read_at(*outage_locations[0])

        recovered = AvroDeserializer(registry_client())(
            recovered_bytes, SerializationContext("kitchen.sensor.raw", MessageField.VALUE)
        )
        assert recovered == failed_payload

    print(f"[OUTAGE OUTCOME] failed={producer.failed} delivered_after_restart={len(outage_locations)} unresolved_callbacks=0")
    after = snapshot()
    assert before[:2] == after[:2], "Schemas or compatibility changed across restart"

    expected_offsets = dict(before[2])

    for added_partition, added_offset in outage_locations:

        low, high = expected_offsets[added_partition]
        assert added_offset == high
        expected_offsets[added_partition] = (low, high + 1)

    assert after[2] == expected_offsets, "Offset ranges differ beyond the accounted queued record"
    ids_after = subprocess.check_output(COMPOSE + ["ps", "-q", "kafka", "schema-registry"], cwd=ROOT, text=True).split()
    assert sorted(ids_before) == sorted(ids_after), "Containers were recreated"
    assert read_at(partition, offset) == record_before

    decoded = AvroDeserializer(registry_client(), conf={
        "use.latest.version": False, "subject.name.strategy": topic_subject_name_strategy,
    })(record_before[1], SerializationContext("kitchen.sensor.raw", MessageField.VALUE))

    assert decoded == payload
    print(f"[RESTORE OK] event_id={payload['event_id']} partition={partition} offset={offset}; bytes/schema/config/container IDs unchanged; offsets={after[2]} accounted")

    fresh = KafkaEventProducer()
    recovered_payload = generate_event(EQUIPMENTS[0]).to_dict()
    position = publish_with_location(fresh, recovered_payload)
    restored = AvroDeserializer(registry_client())(
        read_at(*position)[1], SerializationContext("kitchen.sensor.raw", MessageField.VALUE)
    )
    
    assert restored == recovered_payload
    print(f"[RECONNECT OK] fresh Producer delivered and decoded event_id={restored['event_id']} at {position}")
