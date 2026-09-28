from datetime import datetime


REQUIRED_FIELDS = {
    "event_id",
    "event_time",
    "store_id",
    "equipment_id",
    "equipment_type",
    "metric_name",
    "metric_value",
    "schema_version",
    "source",
}

def validate_event(event: dict) -> tuple[bool, str | None]:
    missing_fields = REQUIRED_FIELDS - event.keys()

    if missing_fields:
        return False, f"MISSING FIELDS: {sorted(missing_fields)}"

    if event["schema_version"] != "1.0.0":
        return False, "SCEHMA_VERSION_MISMATCH"

    try:
        datetime.fromisoformat(event["event_time"])
    except (ValueError, TypeError):
        return False, "INVALID_TIMESTAMP"

    if not isinstance(event["store_id"], str):
        return False, "INVALID_STORE_ID_TYPE"

    if not isinstance(event["equipment_id"], str):
        return False, "INVALID_EQUIPMENT_ID_TYPE"

    if not isinstance(event["metric_name"], str):
        return False, "INVALID_METRIC_NAME_TYPE"

    return True, None
