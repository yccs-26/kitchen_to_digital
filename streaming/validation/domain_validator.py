from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import isfinite
from typing import cast


STRING_FIELDS = (
    "event_id", "store_id", "equipment_id", "equipment_type",
    "metric_name", "unit", "schema_version", "source",
)
REQUIRED_FIELDS = (
    "event_id", "event_time", "store_id", "equipment_id",
    "equipment_type", "metric_name", "metric_value", "unit",
    "schema_version", "source",
)


@dataclass(frozen=True)
class EquipmentSpec:

    store_id: str
    equipment_type: str


@dataclass(frozen=True)
class ValidationError:
    code: str
    field: str
    details: str

# 검증 결과 저장 객체
# ! 오류가 있지만 valid한 값이 있을 수 있어 상태 꼬임 방지
@dataclass(frozen=True)
class ValidationResult:
    errors: tuple[ValidationError, ...]

    @property
    def is_valid(self) -> bool:
        return not self.errors


def _validate_required_fields(
    event: Mapping[str, object], errors: list[ValidationError]
) -> set[str]:
    
    usable: set[str] = set()
    for field in REQUIRED_FIELDS:
        if field not in event:
            errors.append(ValidationError(
                "MISSING_FIELD", field, "Required field is absent."
            ))
        elif event[field] is None:
            errors.append(ValidationError(
                "NULL_FIELD", field, "Required field must not be None."
            ))
        elif field in STRING_FIELDS and not isinstance(event[field], str):
            errors.append(ValidationError(
                "INVALID_FIELD_TYPE", field, "Expected a string."
            ))
        elif field in STRING_FIELDS and not cast(str, event[field]).strip():
            errors.append(ValidationError(
                "EMPTY_FIELD", field, "Expected a non-blank string."
            ))
        else:
            usable.add(field)
    return usable


def _as_utc(value: object) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError("Expected a timezone-aware datetime.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Datetime must have a UTC offset.")
    return value.astimezone(timezone.utc)


def _validate_event_time(
    value: object,
    reference_time: datetime,
    max_future_skew: timedelta,
    errors: list[ValidationError],
) -> None:
    try:
        event_time = _as_utc(value)
    except (ValueError, OverflowError) as exc:
        errors.append(ValidationError(
            "INVALID_EVENT_TIME", "event_time", str(exc)
        ))
        return
    if event_time - reference_time > max_future_skew:
        errors.append(ValidationError(
            "FUTURE_EVENT_TIME", "event_time",
            f"Event exceeds the allowed future skew of {max_future_skew}."
        ))


def _validate_metric_value(
    value: object, errors: list[ValidationError]
) -> None:
    # bool은 int의 subclass라서 (int, float)만 검사하면 bool도 통과해서 True, False도 정상 숫자로 통과하기에 먼저 막아야 한다
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        errors.append(ValidationError(
            "INVALID_METRIC_VALUE", "metric_value",
            "Expected an int or float, excluding bool."
        ))
    elif isinstance(value, float) and not isfinite(value):
        errors.append(ValidationError(
            "NON_FINITE_METRIC_VALUE", "metric_value",
            "Metric value must be finite."
        ))


def _validate_equipment(
    event: Mapping[str, object],
    usable: set[str],
    equipment_registry: Mapping[str, EquipmentSpec],
    metric_units: Mapping[str, Mapping[str, str]],
    errors: list[ValidationError],
) -> None:
    if "equipment_id" in usable:
        equipment = equipment_registry.get(cast(str, event["equipment_id"]))
        if equipment is None:
            errors.append(ValidationError(
                "UNKNOWN_EQUIPMENT", "equipment_id",
                "Equipment ID is not in the registry."
            ))
        else:
            for field, expected, code in (
                ("store_id", equipment.store_id, "STORE_MISMATCH"),
                ("equipment_type", equipment.equipment_type,
                 "EQUIPMENT_TYPE_MISMATCH"),
            ):
                if field in usable and event[field] != expected:
                    errors.append(ValidationError(
                        code, field, f"Registry requires {expected!r}."
                    ))

    if {"equipment_type", "metric_name"} <= usable:
        allowed = metric_units.get(cast(str, event["equipment_type"]), {})
        metric = cast(str, event["metric_name"])
        if metric not in allowed: 
            errors.append(ValidationError(
                "UNSUPPORTED_METRIC", "metric_name",
                "Metric is not configured for the payload equipment type."
            ))
        elif "unit" in usable and event["unit"] != allowed[metric]:
            errors.append(ValidationError(
                "UNIT_MISMATCH", "unit",
                f"Metric requires unit {allowed[metric]!r}."
            ))


def _validate_kafka_key(
    kafka_key: bytes | None,
    equipment_id: object,
    errors: list[ValidationError],
) -> None:
    if kafka_key is None:
        errors.append(ValidationError(
            "MISSING_KAFKA_KEY", "kafka_key", "Kafka key is required."
        ))
        return
    if not isinstance(kafka_key, bytes):
        errors.append(ValidationError(
            "INVALID_KAFKA_KEY", "kafka_key", "Expected raw bytes."
        ))
        return
    try:
        decoded_key = kafka_key.decode("utf-8")
    except UnicodeDecodeError:
        errors.append(ValidationError(
            "INVALID_KAFKA_KEY", "kafka_key", "Key is not valid UTF-8."
        ))
        return
    if equipment_id is not None and decoded_key != equipment_id:
        errors.append(ValidationError(
            "KEY_MISMATCH", "kafka_key",
            "Decoded key must exactly match payload equipment_id."
        ))


def validate_domain_event(
    event: Mapping[str, object],
    kafka_key: bytes | None,
    *,
    supported_schema_versions: Collection[str],
    equipment_registry: Mapping[str, EquipmentSpec],
    metric_units: Mapping[str, Mapping[str, str]],
    reference_time: datetime,
    max_future_skew: timedelta,
) -> ValidationResult:

    try:
        reference_utc = _as_utc(reference_time)
    except (ValueError, OverflowError) as exc:
        raise ValueError(
            "reference_time must be UTC-convertible and aware."
        ) from exc
    if max_future_skew < timedelta(0):
        raise ValueError("max_future_skew must be non-negative.")

    errors: list[ValidationError] = []
    usable = _validate_required_fields(event, errors)
    if "schema_version" in usable:
        if event["schema_version"] not in supported_schema_versions:
            errors.append(ValidationError(
                "UNSUPPORTED_SCHEMA_VERSION", "schema_version",
                "Schema version is not supported by the supplied rules."
            ))
    if "event_time" in usable:
        _validate_event_time(
            event["event_time"], reference_utc, max_future_skew, errors
        )
    if "metric_value" in usable:
        _validate_metric_value(event["metric_value"], errors)
    _validate_equipment(
        event, usable, equipment_registry, metric_units, errors
    )
    _validate_kafka_key(
        kafka_key,
        event["equipment_id"] if "equipment_id" in usable else None,
        errors,
    )
    return ValidationResult(errors=tuple(errors))
