"""실제 broker 없이 Avro 계약·ACK 경계·부분 실패 재처리를 검증한다."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from io import BytesIO
import json
from pathlib import Path
from unittest.mock import Mock

from confluent_kafka import KafkaError, KafkaException, Message
from confluent_kafka.schema_registry import (
    RegisteredSchema, Schema, topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from fastavro import schemaless_reader
import pytest

from streaming.sinks.silver import (
    SilverSink, SilverWriteResult, SilverWriteStatus,
)
from streaming.sinks.validated_publisher import (
    deliver_validated_event,
    publish_validated_event,
    serialize_validated_event,
)


@pytest.fixture
def event():
    return {
        "event_id": "evt-100",
        "event_time": datetime(2026, 10, 7, 12, tzinfo=timezone.utc),
        "store_id": "store-001", "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius", "metric_value": 4.2,
        "unit": "celsius", "schema_version": "1.0.0", "source": "simulator",
    }


@pytest.fixture
def serializer():
    schema = Path("schemas/avro/sensor_metric_event.avsc").read_text()
    registry = Mock()
    registry.lookup_schema.return_value = RegisteredSchema(
        schema_id=1, guid=None, schema=Schema(schema, "AVRO"),
        subject="kitchen.sensor.validated-value", version=1,
    )
    return AvroSerializer(
        registry, schema,
        conf={
            "auto.register.schemas": False, "use.latest.version": False,
            "subject.name.strategy": topic_subject_name_strategy,
        },
    )


class FakeProducer:
    """큐에 담은 뒤 flush 시점에 개별 전달 결과를 알린다."""

    def __init__(self, *, error=None, remaining=0, callback=True, offset=12):
        self.error = error
        self.remaining = remaining
        self.callback = callback
        self.message = Mock(spec=Message)
        self.message.offset.return_value = offset
        self.calls = []
        self.timeouts = []

    def produce(self, **kwargs):
        self.calls.append(kwargs)

    def flush(self, timeout):
        self.timeouts.append(timeout)
        if self.callback:
            self.calls[-1]["on_delivery"](self.error, self.message)
        return self.remaining


class FakeStorage:
    """재처리 계약용 저장소이며 영속성 또는 Iceberg 증빙은 아니다."""

    def __init__(self):
        self.rows = {}

    def lookup(self, event_id):
        return self.rows.get(event_id)

    def insert_if_absent(self, event):
        if event["event_id"] in self.rows:
            return False
        self.rows[event["event_id"]] = dict(event)
        return True


def test_existing_avro_contract_identity_and_no_mutation(event, serializer):
    original = deepcopy(event)
    incoming = {**event, "offset": 99}
    key, value = serialize_validated_event(incoming, serializer)
    assert key == event["equipment_id"].encode("utf-8")
    assert value[:5] == b"\x00\x00\x00\x00\x01"
    schema = json.loads(
        Path("schemas/avro/sensor_metric_event.avsc").read_text()
    )
    payload = schemaless_reader(BytesIO(value[5:]), schema)
    assert payload == {
        **event, "event_time": "2026-10-07T12:00:00.000000+00:00",
    }
    assert incoming == {**original, "offset": 99}
    assert event == original
    assert serialize_validated_event(dict(reversed(list(event.items()))),
                                     serializer) == (key, value)


def test_equivalent_timezones_serialize_identically(event, serializer):
    other = {**event, "event_time": event["event_time"].astimezone(
        timezone(timedelta(hours=9))
    )}
    assert serialize_validated_event(event, serializer) == (
        serialize_validated_event(other, serializer)
    )


@pytest.mark.parametrize(
    "topic", ["kitchen.sensor.validated", "custom.validated"],
)
def test_topic_and_ack_result(event, topic):
    serializer = Mock(return_value=b"avro")
    producer = FakeProducer(offset=0)
    kwargs = {} if topic == "kitchen.sensor.validated" else {"topic": topic}
    result = publish_validated_event(event, producer, serializer,
                                     timeout=2.5, **kwargs)
    assert result is producer.message
    assert producer.calls[0]["topic"] == topic
    assert serializer.call_args.args[1].topic == topic
    assert producer.timeouts == [2.5]


@pytest.mark.parametrize("status", [
    SilverWriteStatus.INSERTED, SilverWriteStatus.DUPLICATE_NOOP,
])
def test_write_precedes_publish_for_insert_and_duplicate(event, status):
    producer = FakeProducer()
    result = SilverWriteResult(status)

    def write(incoming):
        assert incoming == event
        assert producer.calls == []
        return result

    sink = Mock(spec=SilverSink)
    sink.write.side_effect = write
    delivery = deliver_validated_event(
        event, sink, producer, Mock(return_value=b"avro"),
    )
    assert delivery.silver is result
    assert delivery.message is producer.message
    assert len(producer.calls) == 1


def test_conflict_preserves_fields_without_serialization_or_publish(event):
    sink = SilverSink(FakeStorage())
    sink.write(event)
    producer, serializer = FakeProducer(), Mock()
    delivery = deliver_validated_event(
        {**event, "metric_value": 9, "unit": "fahrenheit"},
        sink, producer, serializer,
    )
    assert delivery.silver == SilverWriteResult(
        SilverWriteStatus.CONFLICT, ("metric_value", "unit"),
    )
    assert delivery.message is None
    assert producer.calls == []
    serializer.assert_not_called()


def test_silver_failure_prevents_publish(event):
    sink, producer, serializer = Mock(spec=SilverSink), FakeProducer(), Mock()
    failure = OSError("storage failed")
    sink.write.side_effect = failure
    with pytest.raises(OSError) as caught:
        deliver_validated_event(event, sink, producer, serializer)
    assert caught.value is failure
    assert producer.calls == []
    serializer.assert_not_called()


@pytest.mark.parametrize("operation", ["serialize", "produce", "flush"])
def test_failure_propagates_after_write_and_replay_recovers(event, operation):
    storage, producer = FakeStorage(), FakeProducer()
    serializer = Mock(return_value=b"avro")
    failure = RuntimeError("injected failure")
    failing = Mock(side_effect=failure)
    if operation == "serialize":
        serializer = failing
    else:
        setattr(producer, operation, failing)
    with pytest.raises(RuntimeError) as caught:
        deliver_validated_event(
            event, SilverSink(storage), producer, serializer,
        )
    assert caught.value is failure
    assert failing.call_count == 1
    assert storage.rows[event["event_id"]] == event
    retry_producer = FakeProducer()
    delivery = deliver_validated_event(
        event, SilverSink(storage), retry_producer, Mock(return_value=b"avro"),
    )
    assert delivery.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert delivery.message is retry_producer.message
    assert len(retry_producer.calls) == 1


def test_ack_error_propagates_and_retry_publishes_duplicate(event, serializer):
    storage = FakeStorage()
    producer = FakeProducer(error=KafkaError(KafkaError._MSG_TIMED_OUT))
    with pytest.raises(KafkaException):
        deliver_validated_event(
            event, SilverSink(storage), producer, serializer,
        )
    producer.error = None
    result = deliver_validated_event(
        event, SilverSink(storage), producer, serializer,
    )
    assert result.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert len(producer.calls) == 2


def test_caller_failure_after_ack_allows_duplicate_publish(event, serializer):
    storage, producer = FakeStorage(), FakeProducer()
    original = deepcopy(event)
    with pytest.raises(RuntimeError, match="checkpoint"):
        first = deliver_validated_event(
            event, SilverSink(storage), producer, serializer,
        )
        assert first.silver.status is SilverWriteStatus.INSERTED
        raise RuntimeError("checkpoint failed")
    second = deliver_validated_event(
        event, SilverSink(storage), producer, serializer,
    )
    assert second.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert len(storage.rows) == 1
    assert len(producer.calls) == 2
    for field in ("key", "value"):
        assert producer.calls[0][field] == producer.calls[1][field]
    assert event == original


@pytest.mark.parametrize(
    "callback,remaining", [(False, 0), (False, 1), (True, 1)],
)
def test_timeout_is_not_success_and_can_be_retried(event, callback, remaining):
    storage = FakeStorage()
    producer = FakeProducer(callback=callback, remaining=remaining)
    serializer = Mock(return_value=b"avro")
    with pytest.raises(TimeoutError):
        deliver_validated_event(
            event, SilverSink(storage), producer, serializer,
        )
    producer.callback, producer.remaining = True, 0
    result = deliver_validated_event(
        event, SilverSink(storage), producer, serializer,
    )
    assert result.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert len(producer.calls) == 2


@pytest.mark.parametrize("offset", [-1, None, True, "12", 1.5])
def test_invalid_callback_offset_rejected(event, offset):
    with pytest.raises(RuntimeError, match="acknowledged offset"):
        publish_validated_event(event, FakeProducer(offset=offset),
                                Mock(return_value=b"avro"))


def test_null_callback_message_rejected(event):
    producer = FakeProducer()
    producer.message = None
    with pytest.raises(TimeoutError):
        publish_validated_event(event, producer, Mock(return_value=b"avro"))


@pytest.mark.parametrize("value", [None, b"", "text"])
def test_invalid_serializer_output_rejected_before_produce(event, value):
    producer = FakeProducer()
    with pytest.raises(ValueError, match="serializer"):
        publish_validated_event(event, producer, Mock(return_value=value))
    assert producer.calls == []


@pytest.mark.parametrize("topic", [None, "", " ", 1])
def test_invalid_topic_rejected_before_io(event, topic):
    producer = FakeProducer()
    with pytest.raises(ValueError, match="topic"):
        publish_validated_event(event, producer, Mock(), topic=topic)
    assert producer.calls == []


@pytest.mark.parametrize(
    "timeout", [0, -1, True, None, float("inf"), float("nan")],
)
def test_invalid_timeout_rejected_before_io(event, timeout):
    producer = FakeProducer()
    with pytest.raises(ValueError, match="timeout"):
        publish_validated_event(event, producer, Mock(), timeout=timeout)
    assert producer.calls == []


@pytest.mark.parametrize("value", [None, "2026-10-07", datetime(2026, 10, 7)])
def test_noncanonical_time_rejected(event, value):
    with pytest.raises(ValueError, match="event_time"):
        serialize_validated_event({**event, "event_time": value}, Mock())


@pytest.mark.parametrize("value", [None, "", " ", 1])
def test_invalid_equipment_key_rejected(event, value):
    with pytest.raises(ValueError, match="equipment_id"):
        serialize_validated_event({**event, "equipment_id": value}, Mock())
