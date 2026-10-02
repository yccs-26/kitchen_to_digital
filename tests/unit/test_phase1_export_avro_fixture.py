import base64
import io
import json
from pathlib import Path
from unittest.mock import Mock

import pytest
from confluent_kafka.schema_registry import RegisteredSchema, Schema
from fastavro import schemaless_reader

from scripts import phase1_export_avro_fixture as exporter
from simulator import producer as producer_module


@pytest.fixture
def registry(monkeypatch):
    schema = Path("schemas/avro/sensor_metric_event.avsc").read_text()
    registry = Mock()
    registry.lookup_schema.return_value = RegisteredSchema(
        schema_id=1,
        guid=None,
        schema=Schema(schema, "AVRO"),
        subject="kitchen.sensor.raw-value",
        version=1,
    )
    monkeypatch.setattr(
        producer_module, "SchemaRegistryClient", Mock(return_value=registry)
    )
    monkeypatch.setenv("KAFKA_TOPIC_SENSOR_RAW", "kitchen.sensor.raw")
    return registry


def test_export_real_avro_without_kafka(tmp_path, monkeypatch, registry):
    kafka_factory = Mock()
    monkeypatch.setattr(producer_module, "Producer", kafka_factory)
    send = Mock(side_effect=AssertionError("Exporter must not send"))
    monkeypatch.setattr(producer_module.KafkaEventProducer, "send", send)
    original = producer_module.KafkaEventProducer.serialize
    serialized = []

    def capture(self, key, payload):
        result = original(self, key, payload)
        serialized.append((payload, result))
        return result

    monkeypatch.setattr(producer_module.KafkaEventProducer, "serialize", capture)
    monkeypatch.setattr(exporter, "REPOSITORY_ROOT", tmp_path)
    monkeypatch.chdir(tmp_path.parent)
    output = exporter.export_fixture()
    assert output == tmp_path / exporter.DEFAULT_OUTPUT
    fixture = json.loads(output.read_text())
    assert set(fixture) == {
        "topic",
        "equipment_id",
        "key_base64",
        "value_base64",
        "event_id",
        "schema_version",
    }
    payload, (key, value) = serialized[0]
    assert base64.b64decode(fixture["key_base64"], validate=True) == key
    assert base64.b64decode(fixture["value_base64"], validate=True) == value
    assert key == fixture["equipment_id"].encode()
    assert value[:5] == b"\x00\x00\x00\x00\x01"
    schema = json.loads(registry.lookup_schema.return_value.schema.schema_str)
    assert schemaless_reader(io.BytesIO(value[5:]), schema) == payload
    assert fixture["event_id"] == payload["event_id"]
    assert fixture["schema_version"] == payload["schema_version"] == "1.0.0"
    assert fixture["topic"] == "kitchen.sensor.raw"
    kafka_factory.assert_not_called()
    kafka_factory.return_value.produce.assert_not_called()
    send.assert_not_called()
    registry.lookup_schema.assert_called_once()
    registry.get_latest_version.assert_not_called()
    registry.register_schema_full_response.assert_not_called()


def test_existing_fixture_is_preserved(tmp_path):
    output = tmp_path / "existing.json"
    output.write_text("keep me")
    with pytest.raises(FileExistsError):
        exporter.export_fixture(output)
    assert output.read_text() == "keep me"


def test_registry_failure_does_not_write_fixture(tmp_path, registry):
    registry.lookup_schema.side_effect = RuntimeError("Registry unavailable")
    output = tmp_path / "fixture.json"
    with pytest.raises(RuntimeError, match="Registry unavailable"):
        exporter.export_fixture(output)
    assert not output.exists()
