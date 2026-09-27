import copy
import random


def inject_fault(
        event: dict,
        fault_rate: float = 0.1,
) -> dict:
    if random.random() >= fault_rate:
        return event

    faulty_event = copy.deepcopy(event)

    fault_type = random.choice(
        [
            "missing_equipment_id",
            "invalid_timestamp",
            "schema_version_mismatch",
        ]
    )

    if fault_type == "missing_equipment_id":
        faulty_event.pop("equipment_id", None)

    elif fault_type == "invalid_timestamp":
        faulty_event["event_time"] = "invalid-timestamp"

    elif fault_type == "schema_version_mismatch":
        faulty_event["schema_version"] = "999.0.0"

    return faulty_event