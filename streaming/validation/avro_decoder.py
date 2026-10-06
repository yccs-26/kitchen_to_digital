import io
import json

from fastavro import parse_schema, schemaless_reader
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming.validation.event_time_normalizer import normalize_event_time


def parse_confluent_frame(value: bytes) -> tuple[int, bytes]:
    if len(value) < 5:
        raise ValueError("Confluent frame must be at least 5 bytes")

    if value[0] != 0:
        raise ValueError("Invalid Confluent magic byte")

    schema_id = int.from_bytes(value[1:5], byteorder="big")
    avro_body = value[5:]

    return schema_id, avro_body


def decode_sensor_event(
        value: bytes, 
        registry_client: SchemaRegistryClient
        ) -> dict:

    schema_id, avro_body = parse_confluent_frame(value)

    try:
        schema = registry_client.get_schema(schema_id)
    except SchemaRegistryError as e:
        if e.http_status_code == 404:
            raise ValueError(f"Unknown schema id: {schema_id}")
        
        raise

    schema_dict = json.loads(schema.schema_str)
    parsed_schema = parse_schema(schema_dict)

    try:
        event = schemaless_reader(
            io.BytesIO(avro_body),
            parsed_schema
        )
    except Exception as e:
        raise ValueError("Failed to decode Avro payload") from e

    event['event_time'] = normalize_event_time(event['event_time'])

    return event
