"""Pure identity classification for validated canonical sensor events."""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum


# Current sensor payload contract, excluding the separately checked event_id.
# Explicit projection keeps transport/processing metadata out of identity.
PAYLOAD_FIELDS = (
    "event_time",
    "store_id",
    "equipment_id",
    "equipment_type",
    "metric_name",
    "metric_value",
    "unit",
    "schema_version",
    "source",
)


class EventIdentityStatus(Enum):
    NEW = "NEW"
    DUPLICATE = "DUPLICATE"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class EventIdentityResult:
    status: EventIdentityStatus
    differing_fields: tuple[str, ...] = ()


def classify_event_identity(
    event: Mapping[str, object],
    canonical_event: Mapping[str, object] | None,
) -> EventIdentityResult:

    if canonical_event is None:
        return EventIdentityResult(EventIdentityStatus.NEW)
    if event["event_id"] != canonical_event["event_id"]:
        raise ValueError("canonical_event must have the same event_id")

    differing_fields = tuple(
        field for field in PAYLOAD_FIELDS
        if event[field] != canonical_event[field]
    )
    status = (
        EventIdentityStatus.CONFLICT
        if differing_fields else EventIdentityStatus.DUPLICATE
    )
    return EventIdentityResult(status, differing_fields)
