"""Build one immutable Quarantine record per failed Kafka source record.

Domain failures reuse ValidationResult unchanged. Callers may also supply
explicitly classified decode failures through the same type, using
INVALID_CONFLUENT_FRAME/raw_value, AVRO_DECODE_FAILED/raw_value, or
UNKNOWN_SCHEMA_ID/schema_id. These are contract codes, not automatic exception
mapping: the current decoder raises ValueError for several different causes.
Registry outages and other system failures must not be classified here.
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
    """Transport lineage and failures, without a copy of the raw value.

    raw_value_sha256 is None for a Kafka null value, distinct from empty bytes.
    A hash verifies bytes but cannot reconstruct them. Source recovery depends
    on Kafka retention or separately retained raw bytes matching the lineage.
    This builder does not guarantee that Bronze already contains that record.
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
    """Versioned identity: lineage, optional event ID, and failure locations.

    Order, repeated errors, and diagnostic wording do not change identity.
    Field participates because MISSING_FIELD on two different fields describes
    different failures. Schema ID, key, and payload hash are metadata, not ID
    material: lineage identifies the source within this KTD Kafka source.
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
    """Build from a failed result without I/O, mutation, or inferred identity.

    Invalid builder arguments are programmer errors and raise ValueError.
    Pass event_id only when decoded as a string (even if domain-invalid), or
    None when unavailable. No event, equipment, or timestamp is inferred.
    All original errors, including duplicates and their order, are preserved;
    only the identity material is canonicalized.
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
