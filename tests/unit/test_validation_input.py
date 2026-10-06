"""Input decoding and UTC normalization contracts for Phase 2."""

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka.schema_registry import RegisteredSchema, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError

from simulator import producer as producer_module
from streaming.validation.decoder import decode_sensor_event, parse_confluent_frame
from streaming.validation.timestamps import normalize_event_time


@pytest.fixture
def registry(monkeypatch):
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "schemas/avro/sensor_metric_event.avsc"
    )
    schema = Schema(schema_path.read_text(encoding="utf-8"), "AVRO")
    client = Mock()
    client.lookup_schema.return_value = RegisteredSchema(
        schema_id=1,
        guid=None,
        schema=schema,
        subject="kitchen.sensor.raw-value",
        version=1,
    )
    client.get_schema.return_value = schema
    monkeypatch.setattr(
        producer_module, "SchemaRegistryClient", Mock(return_value=client)
    )
    monkeypatch.setenv("KAFKA_TOPIC_SENSOR_RAW", "kitchen.sensor.raw")
    return client


@pytest.fixture
def serialized_event(registry):
    payload = {
        "event_id": "validation-test-1",
        "event_time": "2026-10-06T09:30:00+09:00",
        "store_id": "test-store",
        "equipment_id": "test-fridge",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius",
        "metric_value": -2.5,
        "unit": "celsius",
        "schema_version": "1.0.0",
        "source": "simulator",
    }
    producer = producer_module.KafkaEventProducer(serialization_only=True)
    key, value = producer.serialize(payload["equipment_id"], payload)
    assert key == payload["equipment_id"].encode("utf-8")
    assert value is not None
    assert value[:5] == b"\x00\x00\x00\x00\x01"
    return payload, value


def test_parse_confluent_frame_separates_schema_id_and_body():
    frame = b"\x00\x01\x02\x03\x04\x02\xff\x00"

    schema_id, body = parse_confluent_frame(frame)

    assert schema_id == 0x01020304
    assert body == b"\x02\xff\x00"


@pytest.mark.parametrize("size", range(5))
def test_parse_confluent_frame_rejects_short_input(size):
    frame = b"\x00\x00\x00\x00\x01"[:size]

    with pytest.raises(ValueError):
        parse_confluent_frame(frame)


@pytest.mark.parametrize("magic_byte", [b"\x01", b"\xff"])
def test_parse_confluent_frame_rejects_invalid_magic_byte(magic_byte):
    frame = magic_byte + b"\x00\x00\x00\x01\x02"

    with pytest.raises(ValueError):
        parse_confluent_frame(frame)


def test_parse_confluent_frame_accepts_header_only():
    frame = b"\x00\x00\x00\x00\x01"

    schema_id, body = parse_confluent_frame(frame)

    assert schema_id == 1
    assert body == b""


def test_decode_sensor_event_decodes_real_avro(serialized_event, registry):
    payload, value = serialized_event

    event = decode_sensor_event(value, registry)

    expected = {
        **payload,
        "event_time": datetime(2026, 10, 6, 0, 30, tzinfo=timezone.utc),
    }
    assert event == expected
    assert event["event_time"].tzinfo == timezone.utc
    registry.get_schema.assert_called_once_with(1)


def test_decode_sensor_event_rejects_unknown_schema_id(serialized_event, registry):
    _, value = serialized_event
    unknown_frame = b"\x00\x00\x00\x03\xe7" + value[5:]
    registry.get_schema.side_effect = SchemaRegistryError(
        404, 40403, "Schema not found"
    )

    with pytest.raises(ValueError):
        decode_sensor_event(unknown_frame, registry)

    registry.get_schema.assert_called_once_with(999)


@pytest.mark.parametrize("status", [500, 502, 503])
def test_decode_sensor_event_propagates_registry_failure(
    serialized_event, registry, status
):
    _, value = serialized_event
    error = SchemaRegistryError(status, 50001, "Registry unavailable")
    registry.get_schema.side_effect = error

    with pytest.raises(SchemaRegistryError) as caught:
        decode_sensor_event(value, registry)

    assert caught.value is error
    registry.get_schema.assert_called_once_with(1)


def test_decode_sensor_event_rejects_corrupt_body(serialized_event, registry):
    _, value = serialized_event
    # Keep the header but truncate the body within the first string field.
    corrupt_frame = value[:6]

    with pytest.raises(ValueError):
        decode_sensor_event(corrupt_frame, registry)


def test_normalize_event_time_rejects_naive_datetime():
    with pytest.raises(ValueError):
        normalize_event_time("2026-10-06T09:30:00")


def test_normalize_event_time_converts_offset_to_utc():
    result = normalize_event_time("2026-10-06T09:30:00+09:00")

    assert result == datetime(2026, 10, 6, 0, 30, tzinfo=timezone.utc)
    assert result.tzinfo == timezone.utc
