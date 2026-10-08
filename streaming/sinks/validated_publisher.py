"""Silver 저장 후 validated 전달과 부분 실패 복구 계약.

검증·UTC 정규화를 마친 canonical 입력만 받는다. 기존 센서 Avro 스키마의
AvroSerializer를 호출자가 주입한다. 기존 sensor_metric_event.avsc를 사용하며
auto.register.schemas=False, use.latest.version=False 및
topic_subject_name_strategy를 설정하고 topic별 등록을 준비한다.
전용 비트랜잭션 producer는 acks='all', delivery.report.only.error=False,
allow.auto.create.topics=False로 구성한다. 설정과 수명은 호출자가 관리한다.

Silver와 Kafka는 원자적 트랜잭션이 아니며 exactly-once를 보장하지 않는다.
Silver 성공 후 발행 실패는 예외로 전달하며 호출자는 같은 이벤트를 재처리한다.
DUPLICATE_NOOP는 발행 완료 증거가 아니므로 다시 발행한다. 발행 성공 후
호출자/checkpoint 실패 또는 timeout 뒤 재처리하면 중복 전달될 수 있다.
누락보다 중복을 허용하며 downstream은 payload의 event_id로 중복을 식별한다.
Kafka key는 장비별 파티션/기록 순서용이며 event_time 순서를 보장하지 않는다.
애플리케이션 내부 재시도나 checkpoint 갱신은 수행하지 않는다.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
import math

from confluent_kafka import KafkaError, KafkaException, Message
from confluent_kafka.serialization import MessageField, SerializationContext

from streaming.quarantine.quarantine_publisher import DeliveryProducer
from streaming.sinks.silver import (
    SilverSink, SilverWriteResult, SilverWriteStatus,
)
from streaming.validation.event_identity import PAYLOAD_FIELDS


DEFAULT_VALIDATED_TOPIC = "kitchen.sensor.validated"
ValueSerializer = Callable[
    [dict[str, object], SerializationContext], bytes | None
]


@dataclass(frozen=True)
class ValidatedDeliveryResult:
    """충돌은 message가 없으며, 나머지는 ACK 확인 후에만 반환한다."""

    silver: SilverWriteResult
    message: Message | None


def serialize_validated_event(
    event: Mapping[str, object],
    serializer: ValueSerializer,
    *,
    topic: str = DEFAULT_VALIDATED_TOPIC,
) -> tuple[bytes, bytes]:
    """기존 10필드를 복사하고 UTC 시각을 Avro string 계약으로 직렬화한다.

    같은 스키마 ID와 serializer 설정에서는 동일 입력이 같은 bytes가 된다.
    transport metadata는 제외하며 원본 mapping을 변경하지 않는다.
    """
    payload = {field: event[field] for field in ("event_id", *PAYLOAD_FIELDS)}
    event_time = payload["event_time"]
    if not isinstance(event_time, datetime) or event_time.utcoffset() is None:
        raise ValueError("event_time must be a timezone-aware datetime")
    payload["event_time"] = event_time.astimezone(timezone.utc).isoformat(
        timespec="microseconds"
    )
    equipment_id = payload["equipment_id"]
    if not isinstance(equipment_id, str) or not equipment_id.strip():
        raise ValueError("equipment_id must be a non-blank string")
    key = equipment_id.encode("utf-8")
    value = serializer(
        payload, SerializationContext(topic, MessageField.VALUE),
    )
    if not isinstance(value, bytes) or not value:
        raise ValueError("validated serializer must return non-empty bytes")
    return key, value


def publish_validated_event(
    event: Mapping[str, object],
    producer: DeliveryProducer,
    serializer: ValueSerializer,
    *,
    topic: str = DEFAULT_VALIDATED_TOPIC,
    timeout: float = 15.0,
) -> Message:
    """Quarantine와 동일하게 flush·개별 callback·ACK offset을 확인한다.

    큐가 비어도 callback 오류나 미확정 전달은 성공이 아니다.
    timeout은 취소를 뜻하지 않으며 나중에 전달될 가능성이 남는다.
    """
    if not isinstance(topic, str) or not topic.strip():
        raise ValueError("topic must be a non-blank string")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or timeout <= 0
    ):
        raise ValueError("timeout must be a positive finite number")
    key, value = serialize_validated_event(event, serializer, topic=topic)
    delivered: Message | None = None
    delivery_error: KafkaError | None = None

    def on_delivery(error: KafkaError | None, message: Message) -> None:
        nonlocal delivered, delivery_error
        delivered = message
        delivery_error = error

    producer.produce(
        topic=topic, key=key, value=value, on_delivery=on_delivery,
    )
    remaining = producer.flush(timeout)
    if delivery_error is not None:
        raise KafkaException(delivery_error)
    if remaining or delivered is None:
        raise TimeoutError(
            "Validated delivery was not confirmed before timeout"
        )
    offset = delivered.offset()
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise RuntimeError("Validated delivery has no acknowledged offset")
    return delivered


def deliver_validated_event(
    event: Mapping[str, object],
    silver: SilverSink,
    producer: DeliveryProducer,
    serializer: ValueSerializer,
    *,
    topic: str = DEFAULT_VALIDATED_TOPIC,
    timeout: float = 15.0,
) -> ValidatedDeliveryResult:
    """Silver 확정 후 발행하며 모든 저장·직렬화·전달 실패를 전파한다.

    충돌 결과와 differing_fields는 보존하되 충돌 이벤트는 발행하지 않는다.
    호출자는 충돌 결과를 정상 발행 완료와 구분하여 처리해야 한다.
    """
    result = silver.write(event)
    if result.status is SilverWriteStatus.CONFLICT:
        return ValidatedDeliveryResult(result, None)
    if result.status not in (
        SilverWriteStatus.INSERTED, SilverWriteStatus.DUPLICATE_NOOP,
    ):
        raise ValueError("Unexpected Silver write status")
    # Silver 중복은 이전 Kafka 발행의 성공 여부를 알려주지 않는다.
    message = publish_validated_event(
        event, producer, serializer, topic=topic, timeout=timeout,
    )
    return ValidatedDeliveryResult(result, message)
