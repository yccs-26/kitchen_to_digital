import random

from equipment import EquipmentConfig

def generate_metric_value(equipment: EquipmentConfig):
    if equipment.metric_type == "numeric":
        if equipment.min_value is None or equipment.max_value is None:
            raise ValueError(
                f"numeric metric requires min/max: {equipment.equipment_id}"
            )

        return round(
            random.uniform(
                equipment.min_value,
                equipment.max_value,
            ),
            2
        )

    if equipment.metric_type == "categorical":
        return random.choice(
            ["idle", "washing", "rinsing", "drying"]
        )

    raise ValueError(
        f"unsupported metric_type: {equipment.metric_type}"
    )