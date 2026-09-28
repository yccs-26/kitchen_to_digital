"""Read-only compatibility checks and invalid-input serialization checks."""

import os
from pathlib import Path

import pytest
from confluent_kafka.schema_registry import Schema

from simulator.equipment import EQUIPMENTS
from simulator.main import generate_event
from simulator.producer import KafkaEventProducer

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.getenv("KTD_RUN_INTEGRATION") != "1", reason="Requires local Registry"),
]
SCHEMAS = Path(__file__).resolve().parents[2] / "schemas/avro"


def test_backward_compatibility_preserves_registered_versions():
    producer = KafkaEventProducer()
    registry = producer.registry
    subject = f"{producer.topic}-value"
    before = registry.get_versions(subject)
    assert registry.get_compatibility(subject) == "BACKWARD"
    base = Schema((SCHEMAS / "sensor_metric_event.avsc").read_text(), "AVRO")
    base_version = registry.lookup_schema(subject, base).version
    compatible = Schema((SCHEMAS / "compatibility/sensor_metric_event_v2_compatible.avsc").read_text(), "AVRO")
    compatible_version = registry.lookup_schema(subject, compatible).version
    incompatible = Schema((SCHEMAS / "compatibility/sensor_metric_event_v3_incompatible.avsc").read_text(), "AVRO")
    assert registry.test_compatibility(subject, compatible, version=base_version) is True
    assert registry.test_compatibility(subject, incompatible, version=compatible_version) is False
    assert registry.get_versions(subject) == before
    print(f"[COMPATIBILITY OK] BACKWARD v2 vs v{base_version}=True; v3 vs v{compatible_version}=False; versions={before} unchanged")


@pytest.mark.parametrize("case", ["missing_source", "unit_wrong_type", "metric_wrong_type"])
def test_invalid_avro_input_is_not_enqueued(case):
    producer = KafkaEventProducer()
    payload = generate_event(EQUIPMENTS[0]).to_dict()
    if case == "missing_source":
        del payload["source"]
    elif case == "unit_wrong_type":
        payload["unit"] = 42
    else:
        payload["metric_value"] = "not-a-double"
    with pytest.raises((ValueError, TypeError)) as error:
        producer.send(payload["equipment_id"], payload)
    assert len(producer.producer) == 0
    assert producer.delivered == producer.failed == 0
    print(f"[INVALID INPUT OK] {case}: {type(error.value).__name__}; queued=0")
