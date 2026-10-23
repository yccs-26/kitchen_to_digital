"""검증된 Gold 물리 행의 자연키와 확정 속성을 정확히 비교한다."""

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from streaming.aggregations.gold_row_mapper import GoldPhysicalRow


KEY_COLUMNS = (
    "window_start", "window_type", "store_id", "equipment_id", "metric_name",
)
VALUE_COLUMNS = (
    "window_end", "unit", "sample_count", "sum_value", "min_value",
    "max_value", "avg_value",
)
GoldKey = tuple[datetime, str, str, str, str]


@dataclass(frozen=True)
class GoldComparison:
    """저장 작업 없이 반환하는 신규·동일·충돌 결정이다."""

    decision: Literal["INSERT", "NO-OP", "CONFLICT"]
    key: GoldKey
    differing_fields: tuple[str, ...] = ()


def gold_natural_key(row: GoldPhysicalRow) -> GoldKey:
    """Mapper로 검증한 행의 5개 자연키 필드를 순서대로 투영한다."""
    return (
        row["window_start"], row["window_type"], row["store_id"],
        row["equipment_id"], row["metric_name"],
    )


def compare_gold_rows(
    existing: GoldPhysicalRow | None, incoming: GoldPhysicalRow,
) -> GoldComparison:
    """동일 자연키에 대해서만 비교하며 epsilon이나 값을 갱신하지 않는다.

    두 입력 모두 Gold Row Mapper로 검증된 물리 행이어야 한다.
    저장소 조회와 영속화는 호출자 책임이다.
    """
    key = gold_natural_key(incoming)
    if existing is None:
        return GoldComparison("INSERT", key)
    if gold_natural_key(existing) != key:
        raise ValueError("Gold comparison requires the same natural key")
    differing = tuple(
        field for field in VALUE_COLUMNS
        if existing[field] != incoming[field]
    )
    return GoldComparison(
        "CONFLICT" if differing else "NO-OP", key, differing,
    )
