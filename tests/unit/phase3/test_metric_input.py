"""Kafka 접속 없이 생산 validated Avro 입력 어댑터의 경계를 검증한다."""

from datetime import datetime, timezone
from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace

from fastavro import schemaless_writer
import pytest

from streaming.jobs.metric_aggregation import (
    VALIDATED_FIELDS, decode_validated_record, read_validated_kafka,
)


ROOT = Path(__file__).resolve().parents[3]
SCHEMA = json.loads(
    (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text()
)


class Registry:
    def get_schema(self, schema_id):
        assert schema_id == 7
        return SimpleNamespace(schema_str=json.dumps(SCHEMA))


@pytest.fixture
def framed_record():
    event = {
        "event_id": "canonical-a", "event_time": "2026-10-20T21:00:00+09:00",
        "store_id": "store-001", "equipment_id": "fridge-001",
        "equipment_type": "fridge", "metric_name": "temperature_celsius",
        "metric_value": 4.0, "unit": "celsius", "schema_version": "1.0",
        "source": "local-fixture",
    }
    buffer = BytesIO()
    schemaless_writer(buffer, SCHEMA, event)
    return b"\x00\x00\x00\x00\x07" + buffer.getvalue()


def test_existing_avro_contract_and_utc_are_preserved(framed_record):
    result = decode_validated_record(framed_record, b"fridge-001", Registry())
    assert set(result) == set(VALIDATED_FIELDS)
    assert result["metric_value"] == 4.0
    assert datetime.fromisoformat(result["event_time"]) == datetime(
        2026, 10, 20, 12, tzinfo=timezone.utc,
    )
    assert result["event_time"].endswith("+00:00")


def test_equipment_key_mismatch_is_rejected(framed_record):
    with pytest.raises(ValueError, match="equipment_id"):
        decode_validated_record(framed_record, b"wrong-key", Registry())


@pytest.mark.parametrize("value", [None, b"bad-frame"])
def test_decode_failure_propagates(value):
    with pytest.raises(ValueError):
        decode_validated_record(value, b"fridge-001", Registry())


@pytest.mark.parametrize("options", [
    {}, {"subscribe": "kitchen.sensor.raw"},
    {"subscribe": "kitchen.sensor.validated"},
    {"subscribe": "kitchen.sensor.validated", "kafka.bootstrap.servers": "x",
     "assign": "{}"},
])
def test_invalid_options_fail_before_spark_access(options):
    with pytest.raises(ValueError):
        read_validated_kafka(None, options, lambda: None)


def test_registry_transport_error_propagates(framed_record):
    class UnavailableRegistry:
        def get_schema(self, schema_id):
            raise ConnectionError("OFFLINE_REGISTRY_UNAVAILABLE")

    with pytest.raises(ConnectionError, match="OFFLINE_REGISTRY_UNAVAILABLE"):
        decode_validated_record(
            framed_record, b"fridge-001", UnavailableRegistry(),
        )


def test_callback_rejects_timezone_missing_boundary():
    from streaming.jobs.metric_aggregation import gold_batch_callback

    class Batch:
        def toLocalIterator(self):
            yield SimpleNamespace(asDict=lambda: {
                "window_start": "2026-10-20T12:00:00",
                "window_end": "2026-10-20T12:01:00+00:00",
            })

    class Sink:
        def merge_rows(self, rows):
            list(rows)
            raise AssertionError("Invalid input must fail before storage")

    with pytest.raises(ValueError, match="include timezone"):
        gold_batch_callback(Sink())(Batch(), 0)
