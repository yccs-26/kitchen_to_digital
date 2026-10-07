"""broker 연결 없이 직렬화와 전달 확인 경계를 검증하는 오프라인 테스트."""

from base64 import b64decode
from copy import deepcopy
from dataclasses import replace
import json
from unittest.mock import Mock

from confluent_kafka import KafkaError, KafkaException, Message
import pytest

from streaming.quarantine.quarantine_publisher import (
    publish_quarantine_record,
    serialize_quarantine_record,
)
from streaming.quarantine.quarantine_record import build_quarantine_record
from streaming.validation.domain_validator import (
    ValidationError,
    ValidationResult,
)


VERSION = "validation-test-revision-3"


@pytest.fixture
def record():
    error = ValidationError("UNIT_MISMATCH", "unit", "단위 오류")
    return build_quarantine_record(
        ValidationResult((error, error)),
        source_topic="kitchen.sensor.raw", source_partition=2,
        source_offset=42, raw_key=b"\xff\x00key", raw_value=b"broken-avro",
        event_id="event-1", schema_id=1,
    )


class FakeProducer:
    """먼저 큐에 넣고 flush가 callback을 실행할 때만 전달 결과를 알린다."""

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


def publish(record, producer, **kwargs):
    return publish_quarantine_record(
        record, producer, processing_version=VERSION, **kwargs,
    )


def test_complete_serialization_contract(record):
    key, value = serialize_quarantine_record(
        record, processing_version=VERSION,
    )
    assert key == record.quarantine_id.encode("utf-8")
    assert isinstance(value, bytes)
    payload = json.loads(value)
    assert payload == {
        "quarantine_id": record.quarantine_id,
        "processing_version": VERSION,
        "source_topic": "kitchen.sensor.raw",
        "source_partition": 2, "source_offset": 42,
        "event_id": "event-1", "schema_id": 1,
        "errors": [
            {"code": "UNIT_MISMATCH", "field": "unit", "details": "단위 오류"},
            {"code": "UNIT_MISMATCH", "field": "unit", "details": "단위 오류"},
        ],
        "raw_key_base64": "/wBrZXk=",
        "raw_value_sha256": record.raw_value_sha256,
    }
    assert b64decode(payload["raw_key_base64"]) == record.raw_key
    assert value == json.dumps(
        payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
    ).encode("utf-8")


@pytest.mark.parametrize("raw_key, expected", [(None, None), (b"", "")])
def test_null_and_empty_bytes_remain_distinct(record, raw_key, expected):
    record = replace(record, raw_key=raw_key, raw_value_sha256=None)
    _, value = serialize_quarantine_record(record, processing_version=VERSION)
    payload = json.loads(value)
    assert payload["raw_key_base64"] == expected
    assert payload["raw_value_sha256"] is None


@pytest.mark.parametrize("code, schema_id", [
    ("INVALID_CONFLUENT_FRAME", None), ("AVRO_DECODE_FAILED", 1),
    ("UNKNOWN_SCHEMA_ID", 999),
])
def test_corrupt_avro_without_event_id(record, code, schema_id):
    record = replace(
        record, event_id=None, schema_id=schema_id,
        errors=(ValidationError(code, "raw_value", "Cannot decode."),),
    )
    producer = FakeProducer()
    publish(record, producer)
    payload = json.loads(producer.calls[0]["value"])
    assert payload["event_id"] is None
    assert payload["schema_id"] == schema_id
    assert payload["errors"][0]["code"] == code
    assert producer.calls[0]["key"] == record.quarantine_id.encode()


def test_returns_callback_message_after_ack_and_passes_topic_timeout(record):
    producer = FakeProducer()
    result = publish(record, producer, topic="custom.quarantine", timeout=2.5)
    assert result is producer.message
    assert producer.calls[0]["topic"] == "custom.quarantine"
    assert producer.timeouts == [2.5]


def test_republish_deterministic_and_does_not_mutate_record(record):
    original = deepcopy(record)
    producer = FakeProducer()
    publish(record, producer)
    publish(record, producer)
    assert record == original
    assert len(producer.calls) == 2
    assert producer.calls[0]["topic"] == "kitchen.sensor.quarantine"
    for name in ("key", "value"):
        assert producer.calls[0][name] == producer.calls[1][name]


def test_processing_version_changes_payload_not_identity(record):
    first = serialize_quarantine_record(record, processing_version=VERSION)
    second = serialize_quarantine_record(
        record, processing_version="next-code",
    )
    assert first[0] == second[0]
    assert first[1] != second[1]


@pytest.mark.parametrize("operation", ["produce", "flush"])
def test_client_exception_propagates_without_retry(record, operation):
    producer = FakeProducer()
    failure = (
        BufferError("Queue full") if operation == "produce"
        else RuntimeError("Flush failed")
    )
    method = Mock(side_effect=failure)
    setattr(producer, operation, method)
    with pytest.raises(type(failure)) as caught:
        publish(record, producer)
    assert caught.value is failure
    assert method.call_count == 1
    if operation == "produce":
        assert producer.timeouts == []
    else:
        assert len(producer.calls) == 1


def test_delivery_error_propagates_even_when_flush_returns_zero(record):
    error = KafkaError(KafkaError._MSG_TIMED_OUT)
    producer = FakeProducer(error=error)
    with pytest.raises(KafkaException) as caught:
        publish(record, producer)
    assert caught.value.args[0] is error
    assert len(producer.calls) == 1


@pytest.mark.parametrize("remaining", [0, 1])
def test_missing_callback_is_never_success(record, remaining):
    producer = FakeProducer(callback=False, remaining=remaining)
    with pytest.raises(TimeoutError):
        publish(record, producer)
    assert len(producer.calls) == 1


def test_nonempty_queue_is_not_success(record):
    with pytest.raises(TimeoutError):
        publish(record, FakeProducer(remaining=1))


def test_unacknowledged_offset_is_not_success(record):
    with pytest.raises(RuntimeError, match="acknowledged offset"):
        publish(record, FakeProducer(offset=-1))


def test_zero_offset_is_valid(record):
    producer = FakeProducer(offset=0)
    assert publish(record, producer) is producer.message


@pytest.mark.parametrize("name", ["topic", "processing_version"])
@pytest.mark.parametrize("value", ["", " \t", None, 123])
def test_blank_or_invalid_settings_fail_before_io(record, name, value):
    producer = FakeProducer()
    settings = {"processing_version": VERSION, name: value}
    with pytest.raises(ValueError, match=name):
        publish_quarantine_record(record, producer, **settings)
    assert producer.calls == producer.timeouts == []


@pytest.mark.parametrize("version", ["", " \t", None, 123])
def test_serializer_rejects_missing_processing_version(record, version):
    with pytest.raises(ValueError, match="processing_version"):
        serialize_quarantine_record(record, processing_version=version)


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan"), True])
def test_invalid_timeout_fails_before_io(record, timeout):
    producer = FakeProducer()
    with pytest.raises(ValueError, match="timeout"):
        publish(record, producer, timeout=timeout)
    assert producer.calls == producer.timeouts == []
