"""기존 Avro 해석·UTC 정규화를 수행하고 데이터 실패만 구분한다."""

import io
import json

from fastavro import parse_schema, schemaless_reader
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming.validation.event_time_normalizer import normalize_event_time


class SensorDecodeError(ValueError):
    """Quarantine 가능한 데이터 오류이며 기존 ValueError 계약도 유지한다."""

    def __init__(
        self, code: str, field: str, details: str, *,
        schema_id: int | None = None, event_id: str | None = None,
    ) -> None:
        super().__init__(details)
        self.code = code
        self.field = field
        self.schema_id = schema_id
        self.event_id = event_id


def parse_confluent_frame(value: bytes) -> tuple[int, bytes]:
    if len(value) < 5:
        raise ValueError("Confluent frame must be at least 5 bytes")
    if value[0] != 0:
        raise ValueError("Invalid Confluent magic byte")
    schema_id = int.from_bytes(value[1:5], byteorder="big")
    return schema_id, value[5:]


def decode_sensor_event(
    value: bytes | None,
    registry_client: SchemaRegistryClient,
) -> dict:
    if value is None:
        raise SensorDecodeError(
            "INVALID_CONFLUENT_FRAME", "raw_value", "Kafka value is null"
        )
    try:
        schema_id, avro_body = parse_confluent_frame(value)
    except ValueError as exc:
        raise SensorDecodeError(
            "INVALID_CONFLUENT_FRAME", "raw_value", str(exc)
        ) from exc

    try:
        schema = registry_client.get_schema(schema_id)
    except SchemaRegistryError as exc:
        if exc.http_status_code == 404:
            raise SensorDecodeError(
                "UNKNOWN_SCHEMA_ID", "schema_id",
                f"Unknown schema id: {schema_id}", schema_id=schema_id,
            ) from exc
        raise

    # Registry 응답 해석·스키마 설정 오류는 데이터 오류로 격리하지 않는다.
    schema_dict = json.loads(schema.schema_str)
    parsed_schema = parse_schema(schema_dict)
    try:
        event = schemaless_reader(io.BytesIO(avro_body), parsed_schema)
    except (EOFError, ValueError, IndexError, OverflowError) as exc:
        raise SensorDecodeError(
            "AVRO_DECODE_FAILED", "raw_value", "Failed to decode Avro payload",
            schema_id=schema_id,
        ) from exc
    if not isinstance(event, dict):
        raise SensorDecodeError(
            "AVRO_DECODE_FAILED", "raw_value", "Expected an Avro record",
            schema_id=schema_id,
        )
    # 누락·잘못된 타입은 기존 domain validator가 전체 오류와 함께 판정한다.
    if isinstance(event.get("event_time"), str):
        try:
            event["event_time"] = normalize_event_time(event["event_time"])
        except (ValueError, OverflowError) as exc:
            event_id = event.get("event_id")
            raise SensorDecodeError(
                "INVALID_EVENT_TIME", "event_time", str(exc),
                schema_id=schema_id,
                event_id=event_id if isinstance(event_id, str) else None,
            ) from exc
    return event
