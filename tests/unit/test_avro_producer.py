import asyncio
from pathlib import Path
from unittest.mock import Mock, call

import pytest
from confluent_kafka.schema_registry import RegisteredSchema, Schema
from confluent_kafka.serialization import MessageField

from simulator import producer as producer_module
from simulator.equipment import EQUIPMENTS
from simulator.main import generate_event, main


@pytest.fixture
def producer(monkeypatch):
    schema = Path("schemas/avro/sensor_metric_event.avsc").read_text()
    registry = Mock()
    registry.lookup_schema.return_value = RegisteredSchema(
        schema_id=1,
        guid=None,
        schema=Schema(schema, "AVRO"),
        subject="kitchen.sensor.raw-value",
        version=1,
    )
    kafka = Mock()
    kafka.flush.return_value = 0
    monkeypatch.setattr(producer_module, "Producer", Mock(return_value=kafka))
    monkeypatch.setattr(
        producer_module, "SchemaRegistryClient", Mock(return_value=registry)
    )
    return producer_module.KafkaEventProducer()


def test_uses_registered_base_schema_and_equipment_key(producer):
    event = generate_event(EQUIPMENTS[0])
    producer.send(event.equipment_id, event.to_dict())
    sent = producer.producer.produce.call_args.kwargs
    assert sent["key"] == event.equipment_id.encode()
    assert sent["value"][:5] == b"\x00\x00\x00\x00\x01"
    producer.registry.lookup_schema.assert_called_once()
    producer.registry.get_latest_version.assert_not_called()
    producer.registry.register_schema_full_response.assert_not_called()


@pytest.mark.parametrize("value", [True, "3.5"])
def test_rejects_non_numeric_values_before_enqueue(producer, value):
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    payload["metric_value"] = value
    with pytest.raises(ValueError, match="metric_value"):
        producer.send(payload["equipment_id"], payload)
    producer.producer.produce.assert_not_called()


def test_rejects_wrong_key(producer):
    with pytest.raises(ValueError, match="Kafka key"):
        producer.send("wrong-key", generate_event(EQUIPMENTS[0]).to_dict())
    producer.producer.produce.assert_not_called()


def test_buffer_retry_reuses_serialized_record(producer):
    producer.producer.produce.side_effect = [BufferError(), None]
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    producer.send(payload["equipment_id"], payload)
    calls = producer.producer.produce.call_args_list
    assert len(calls) == 2
    assert calls[0] == calls[1]
    producer.producer.poll.assert_any_call(1.0)


@pytest.mark.parametrize("key", ["", "wrong-key", None])
def test_serialize_rejects_invalid_key(producer, key):
    producer.value_serializer = Mock()
    producer.key_serializer = Mock()
    with pytest.raises(ValueError, match="Kafka key"):
        producer.serialize(key, generate_event(EQUIPMENTS[0]).to_dict())
    producer.value_serializer.assert_not_called()
    producer.key_serializer.assert_not_called()
    producer.producer.produce.assert_not_called()


@pytest.mark.parametrize("value", [True, False, "3.5", None])
def test_serialize_rejects_invalid_metric(producer, value):
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    payload["metric_value"] = value
    producer.value_serializer = Mock()
    with pytest.raises(ValueError, match="metric_value"):
        producer.serialize(payload["equipment_id"], payload)
    producer.value_serializer.assert_not_called()
    producer.producer.produce.assert_not_called()


@pytest.mark.parametrize("value", [3, 3.5])
def test_serialize_calls_existing_serializers(producer, value):
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    payload["metric_value"] = value
    producer.value_serializer = Mock(return_value=b"\x00\xffvalue")
    producer.key_serializer = Mock(return_value=b"key")
    assert producer.serialize(payload["equipment_id"], payload) == (
        b"key",
        b"\x00\xffvalue",
    )
    producer.value_serializer.assert_called_once()
    actual_payload, context = producer.value_serializer.call_args.args
    assert actual_payload is payload
    assert context.topic == producer.topic
    assert context.field == MessageField.VALUE
    producer.key_serializer.assert_called_once_with(payload["equipment_id"])
    producer.producer.produce.assert_not_called()


def test_send_delegates_once_even_on_buffer_retry(producer):
    producer.serialize = Mock(return_value=(b"key", b"\x00\xffvalue"))
    producer.producer.produce.side_effect = [BufferError(), None]
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    producer.send(payload["equipment_id"], payload)
    producer.serialize.assert_called_once_with(payload["equipment_id"], payload)
    expected = call(
        topic=producer.topic,
        key=b"key",
        value=b"\x00\xffvalue",
        on_delivery=producer._delivery_callback,
    )
    assert producer.producer.produce.call_args_list == [expected, expected]
    assert producer.producer.poll.call_args_list == [call(1.0), call(0)]


@pytest.mark.parametrize("remaining,failed", [(1, 0), (0, 1)])
def test_flush_reports_unsent_or_failed_messages(producer, remaining, failed):
    producer.producer.flush.return_value = remaining
    if failed:
        producer._delivery_callback("delivery failed", Mock())
    with pytest.raises(RuntimeError, match="delivery incomplete"):
        producer.flush()


def test_finite_main_sends_only_numeric_events_without_faults(monkeypatch):
    kafka = Mock()
    monkeypatch.setattr("simulator.main.KafkaEventProducer", Mock(return_value=kafka))
    asyncio.run(main(count=1))
    calls = kafka.send.call_args_list
    assert len(calls) == 4
    for sent_call in calls:
        payload = sent_call.kwargs["payload"]
        assert sent_call.kwargs["key"] == payload["equipment_id"]
        assert isinstance(payload["metric_value"], float)
        assert payload["unit"]
        assert payload["schema_version"] == "1.0.0"
    kafka.flush.assert_called_once()
