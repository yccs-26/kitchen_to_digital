"""저장 fixture를 Phase 2 serializer → 기존 Phase 3 decoder로 왕복한다.

Registry는 오프라인 stub이다. 실제 Kafka, schema 등록, offset commit은 없다.
"""

from copy import deepcopy
from datetime import datetime
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from confluent_kafka.schema_registry import (
    RegisteredSchema, Schema, topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry.error import SchemaRegistryError
import pytest

from streaming.jobs.metric_aggregation import (
    VALIDATED_FIELDS, decode_validated_record,
)
from streaming.sinks.validated_publisher import serialize_validated_event
from streaming.validation.avro_decoder import SensorDecodeError


ROOT = Path(__file__).resolve().parents[3]
SCHEMA = (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text()
FIXTURE = json.loads((ROOT / "tests/fixtures/phase3/"
                     "sliding_window_events.json").read_text())
EVENTS = [event for step in FIXTURE["steps"][:3] for event in step["events"]]


class OfflineRegistry:
    def get_schema(self, schema_id):
        assert schema_id == 7
        return SimpleNamespace(schema_str=SCHEMA)


@pytest.fixture
def serializer():
    registry = Mock()
    registry.lookup_schema.return_value = RegisteredSchema(
        schema_id=7, guid=None, schema=Schema(SCHEMA, "AVRO"),
        subject="kitchen.sensor.validated-value", version=1,
    )
    return AvroSerializer(registry, SCHEMA, conf={
        "auto.register.schemas": False, "use.latest.version": False,
        "subject.name.strategy": topic_subject_name_strategy,
    })


def wire(event, serializer):
    logical = deepcopy(event)
    logical["event_time"] = datetime.fromisoformat(logical["event_time"])
    return serialize_validated_event(logical, serializer)


@pytest.mark.parametrize("event", EVENTS, ids=lambda event: event["event_id"])
def test_stored_fixture_phase2_to_phase3_wire_contract(event, serializer):
    original = deepcopy(event)
    key, value = wire(event, serializer)
    decoded = decode_validated_record(value, key, OfflineRegistry())
    assert value[:5] == b"\x00\x00\x00\x00\x07"
    assert key == event["equipment_id"].encode("utf-8")
    assert set(decoded) == set(VALIDATED_FIELDS)
    assert datetime.fromisoformat(decoded["event_time"]) == (
        datetime.fromisoformat(event["event_time"])
    )
    assert decoded["event_time"].endswith("+00:00")
    assert {k: v for k, v in decoded.items() if k != "event_time"} == {
        k: v for k, v in original.items() if k != "event_time"
    }
    assert event == original


def test_duplicate_and_conflicting_id_are_not_rewritten(serializer):
    first = EVENTS[0]
    key, value = wire(first, serializer)
    a = decode_validated_record(value, key, OfflineRegistry())
    assert decode_validated_record(value, key, OfflineRegistry()) == a
    changed = {**first, "metric_value": first["metric_value"] + 1.0}
    key, value = wire(changed, serializer)
    b = decode_validated_record(value, key, OfflineRegistry())
    assert b["event_id"] == a["event_id"]
    assert b["metric_value"] != a["metric_value"]
    # Native dedup의 payload 충돌 탐지 한계를 adapter가 숨기지 않는다.


@pytest.mark.parametrize("value,code", [
    (b"\x01\x00\x00\x00\x07bad", "INVALID_CONFLUENT_FRAME"),
    (b"\x00\x00\x00\x00\x07", "AVRO_DECODE_FAILED"),
])
def test_malformed_framing_or_avro_propagates(value, code):
    with pytest.raises(SensorDecodeError) as failure:
        decode_validated_record(value, b"fridge-001", OfflineRegistry())
    assert failure.value.code == code


def test_unknown_schema_id_propagates(serializer):
    class MissingSchema:
        def get_schema(self, schema_id):
            raise SchemaRegistryError(404, 40403, "Offline unknown schema")

    key, value = wire(EVENTS[0], serializer)
    with pytest.raises(SensorDecodeError) as failure:
        decode_validated_record(value, key, MissingSchema())
    assert failure.value.code == "UNKNOWN_SCHEMA_ID"
    assert failure.value.schema_id == 7
