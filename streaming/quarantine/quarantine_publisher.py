"""Quarantine 레코드를 직렬화하고 Kafka 전달 결과를 동기적으로 확인한다.

전용 confluent_kafka.Producer를 주입하며 acks='all',
delivery.report.only.error=False, allow.auto.create.topics=False로 설정한다.
호출자가 설정과 수명 주기를 관리하며, 이 모듈은 클라이언트를 생성하지 않는다.
전달 성공은 트랜잭션 커밋이 아니므로 트랜잭션 producer는 사용하지 않는다.
클라이언트 내부 재시도는 해당 설정을 따른다. 애플리케이션 수준의 재시도,
중복 제거, checkpoint 갱신, 원본 보관은 여기서 수행하지 않는다.
"""

from base64 import b64encode
from collections.abc import Callable
import json
import math
from typing import Protocol

from confluent_kafka import KafkaError, KafkaException, Message

from streaming.quarantine.quarantine_record import QuarantineRecord


DEFAULT_QUARANTINE_TOPIC = "kitchen.sensor.quarantine"


class DeliveryProducer(Protocol):
    """이 발행 경계에서 사용하는 confluent-kafka 동기 API 계약."""

    def produce(
        self, *, topic: str, key: bytes, value: bytes,
        on_delivery: Callable[[KafkaError | None, Message], None],
    ) -> None: ...

    def flush(self, timeout: float) -> int: ...


def _require_text(name: str, value: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-blank string.")


def serialize_quarantine_record(
    record: QuarantineRecord, *, processing_version: str,
) -> tuple[bytes, bytes]:
    """레코드를 변경하지 않고 결정적인 UTF-8 key와 JSON value를 반환한다.

    raw_key_base64는 표준 Base64를 사용하며 null과 빈 bytes를 구별한다.
    errors의 순서, 중복, 상세 내용을 보존한다. 현재 계약에는 datetime 필드가 없다.
    processing_version은 레코드를 생성한 코드의 버전이다. 과거 데이터를
    재발행할 때도 호출자는 생성 당시 버전을 유지해야 한다.
    이 버전은 v1 식별자 알고리즘과 별개이며 key에 포함되지 않는다.
    """
    _require_text("processing_version", processing_version)
    payload = {
        "quarantine_id": record.quarantine_id,
        "processing_version": processing_version,
        "source_topic": record.source_topic,
        "source_partition": record.source_partition,
        "source_offset": record.source_offset,
        "event_id": record.event_id,
        "schema_id": record.schema_id,
        "errors": [
            {"code": error.code, "field": error.field,
             "details": error.details}
            for error in record.errors
        ],
        "raw_key_base64": (
            b64encode(record.raw_key).decode("ascii")
            if record.raw_key is not None else None
        ),
        "raw_value_sha256": record.raw_value_sha256,
    }
    value = json.dumps(
        payload, sort_keys=True, ensure_ascii=True, separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return record.quarantine_id.encode("utf-8"), value


def publish_quarantine_record(
    record: QuarantineRecord,
    producer: DeliveryProducer,
    *,
    processing_version: str,
    topic: str = DEFAULT_QUARANTINE_TOPIC,
    timeout: float = 15.0,
) -> Message:
    """전달 성공을 확인한 뒤에만 이 레코드의 delivery Message를 반환한다.

    위에 명시한 producer 설정이 필요하다. 주입된 클라이언트는 설정 조회를
    지원하지 않으므로 유효하지 않은 delivery offset을 방어적으로 거부한다.
    특히 acks=0은 broker의 수신 확인이 아니다.
    flush는 전용 producer의 큐를 비우며 delivery callback을 실행한다.
    전달 오류가 발생해도 큐는 비워지므로 큐 길이 0만으로 성공을 판단하지 않는다.

    timeout은 취소가 아니라 결과 미확정 상태다. 큐의 메시지가 나중에 도착할 수 있다.
    timeout이나 호출자 실패 후 다시 처리하면 같은 key로 중복 발행될 수 있다.
    대기 시간은 제한하며 애플리케이션 수준의 재시도는 수행하지 않는다.
    """
    _require_text("topic", topic)
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be a positive finite number.")
    key, value = serialize_quarantine_record(
        record, processing_version=processing_version,
    )
    delivered: Message | None = None
    delivery_error: KafkaError | None = None

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        nonlocal delivered, delivery_error
        delivery_error = error
        delivered = message

    producer.produce(
        topic=topic, key=key, value=value, on_delivery=on_delivery,
    )
    remaining = producer.flush(timeout)
    if delivery_error is not None:
        raise KafkaException(delivery_error)
    if remaining or delivered is None:
        raise TimeoutError(
            "Quarantine delivery was not confirmed before timeout."
        )
    if delivered.offset() < 0:
        raise RuntimeError("Quarantine delivery has no acknowledged offset.")
    return delivered
