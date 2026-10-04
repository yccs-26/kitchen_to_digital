import argparse
import base64
import json
from pathlib import Path

from simulator.equipment import EQUIPMENTS
from simulator.main import generate_event
from simulator.producer import KafkaEventProducer

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = Path("tmp/phase1/avro-fixture.json")


def export_fixture(output: Path = DEFAULT_OUTPUT) -> Path:
    output = REPOSITORY_ROOT / output
    if output.exists():
        raise FileExistsError(f"Fixture already exists: {output}")

    equipment = next(
        equipment for equipment in EQUIPMENTS if equipment.metric_type == "numeric"
    )
    event = generate_event(equipment)
    payload = event.to_dict()
    key = event.equipment_id
    producer = KafkaEventProducer(serialization_only=True)
    serialized_key, value = producer.serialize(key, payload)
    if serialized_key is None or value is None:
        raise ValueError("SensorMetricEvent fixture requires non-null key/value bytes")
    fixture = {
        "topic": producer.topic,
        "equipment_id": key,
        "key_base64": base64.b64encode(serialized_key).decode("ascii"),
        "value_base64": base64.b64encode(value).decode("ascii"),
        "event_id": event.event_id,
        "schema_version": event.schema_version,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("x", encoding="utf-8") as stream:
        stream.write(json.dumps(fixture, indent=2) + "\n")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(f"Exported fixture: {export_fixture(args.output)}")


if __name__ == "__main__":
    main()
