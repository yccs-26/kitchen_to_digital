"""검증에 실패한 Kafka 원본 레코드마다 불변 Quarantine 레코드 하나를 생성한다.

도메인 검증 실패는 ValidationResult를 그대로 재사용한다. 호출자가 명시적으로
분류한 디코딩 실패도 같은 타입으로 전달할 수 있으며, code/field 조합은
INVALID_CONFLUENT_FRAME/raw_value, AVRO_DECODE_FAILED/raw_value,
UNKNOWN_SCHEMA_ID/schema_id를 사용한다. 이는 계약상 오류 코드이며 예외를
자동으로 매핑하는 규칙이 아니다. 현재 디코더는 여러 원인에 대해 ValueError를
발생시킨다. Registry 장애와 그 밖의 시스템 실패는 여기서 분류하면 안 된다.
"""

from dataclasses import dataclass
from hashlib import sha256
import json

from streaming.validation.domain_validator import (
    ValidationError,
    ValidationResult,
)


@dataclass(frozen=True)
class QuarantineRecord:
    """원본 value 사본 없이 Kafka 원본 위치와 오류 정보를 보관한다.

    Kafka null value의 raw_value_sha256은 None이며 빈 bytes의 해시와 구별된다.
    해시는 bytes의 일치 여부를 확인할 수 있지만 원본을 복원하지는 못한다.
    원본 복구에는 Kafka 보존 기간 내 데이터 또는 같은 lineage에 대응하여
    별도로 보관한 원본 bytes가 필요하다. 이 생성 함수는 해당 레코드가
    Bronze에 이미 저장되어 있음을 보장하지 않는다.
    """

    quarantine_id: str
    source_topic: str
    source_partition: int
    source_offset: int
    event_id: str | None
    schema_id: int | None
    errors: tuple[ValidationError, ...]
    raw_key: bytes | None
    raw_value_sha256: str | None


def _require_non_negative_int(name: str, value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer.")


def _quarantine_id(
    source_topic: str,
    source_partition: int,
    source_offset: int,
    event_id: str | None,
    errors: tuple[ValidationError, ...],
) -> str:
    """lineage, 선택적 event ID, 오류 위치로 버전이 명시된 식별자를 생성한다.

    오류의 순서, 중복, 진단 문구는 식별자에 영향을 주지 않는다.
    서로 다른 필드의 MISSING_FIELD는 별개의 실패이므로 field도 생성에 사용한다.
    Schema ID, key, payload 해시는 메타데이터이며 식별자 생성에 사용하지 않는다.
    이 KTD Kafka 소스 내에서는 lineage가 원본 레코드를 식별한다.
    """
    material = [
        "ktd-quarantine-v1",
        source_topic,
        source_partition,
        source_offset,
        event_id,
        sorted({(error.code, error.field) for error in errors}),
    ]
    encoded = json.dumps(
        material, ensure_ascii=True, separators=(",", ":")
    ).encode("utf-8")
    return sha256(encoded).hexdigest()


def build_quarantine_record(
    validation_result: ValidationResult,
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    raw_key: bytes | None,
    raw_value: bytes | None,
    event_id: str | None = None,
    schema_id: int | None = None,
) -> QuarantineRecord:
    """I/O, 입력 변경, 식별자 추정 없이 검증 실패 결과로 레코드를 생성한다.

    잘못된 생성 인자는 프로그래밍 오류로 간주하여 ValueError를 발생시킨다.
    event_id는 문자열로 디코딩된 경우에만 전달한다. 도메인 규칙에 맞지 않는
    문자열도 전달할 수 있으며, 값을 얻을 수 없으면 None을 전달한다.
    이벤트, 장비, 시각은 추정하지 않는다. 원본 오류의 중복과 순서를 모두 보존하며,
    식별자 생성에 사용하는 데이터만 정규화한다.
    """
    if not isinstance(source_topic, str) or not source_topic.strip():
        raise ValueError("source_topic must be a non-blank string.")
    _require_non_negative_int("source_partition", source_partition)
    _require_non_negative_int("source_offset", source_offset)
    if schema_id is not None:
        _require_non_negative_int("schema_id", schema_id)
        if schema_id > 0xFFFFFFFF:
            raise ValueError("schema_id must fit the four-byte wire field.")
    if event_id is not None and not isinstance(event_id, str):
        raise ValueError("event_id must be a decoded string or None.")
    for name, value in (("raw_key", raw_key), ("raw_value", raw_value)):
        if value is not None and not isinstance(value, bytes):
            raise ValueError(f"{name} must be bytes or None.")
    if not isinstance(validation_result, ValidationResult):
        raise ValueError("validation_result must be a ValidationResult.")
    if not isinstance(validation_result.errors, tuple):
        raise ValueError("validation_result.errors must be a tuple.")
    errors = validation_result.errors
    if not errors:
        raise ValueError("Quarantine requires at least one error.")
    for error in errors:
        if not isinstance(error, ValidationError):
            raise ValueError("Each error must be a ValidationError.")
        for name in ("code", "field", "details"):
            value = getattr(error, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"Error {name} must be a non-blank string.")

    return QuarantineRecord(
        quarantine_id=_quarantine_id(
            source_topic, source_partition, source_offset, event_id, errors
        ),
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        event_id=event_id,
        schema_id=schema_id,
        errors=errors,
        raw_key=raw_key,
        raw_value_sha256=(
            sha256(raw_value).hexdigest() if raw_value is not None else None
        ),
    )
