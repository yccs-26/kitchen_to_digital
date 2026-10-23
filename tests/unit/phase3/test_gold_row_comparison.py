"""생산 물리 행 비교와 독립적인 기존 참조 저장소의 결정을 대조한다."""

from copy import deepcopy
import math

import pytest

from streaming.aggregations.gold_row_comparison import (
    KEY_COLUMNS,
    compare_gold_rows,
    gold_natural_key,
)
from streaming.aggregations.gold_row_mapper import map_gold_row
from test_gold_sink_contract import MemoryGoldReference, minute_row


@pytest.mark.parametrize("changes,expected", [
    ({}, "NO-OP"),
    ({"count": 4, "sum": 16.0}, "CONFLICT"),
    ({"sum": 15.0, "avg": 5.0}, "CONFLICT"),
    ({"unit": "fahrenheit"}, "CONFLICT"),
    ({"min": 1.0}, "CONFLICT"),
    ({"max": 7.0}, "CONFLICT"),
])
def test_decision_matches_independent_reference(minute_row, changes, expected):
    reference = MemoryGoldReference()
    canonical = map_gold_row(minute_row)
    incoming = {**minute_row, **changes}
    assert compare_gold_rows(None, canonical).decision == "INSERT"
    reference.apply_batch([minute_row])
    before = deepcopy(canonical)
    result = compare_gold_rows(canonical, map_gold_row(incoming))
    assert result.decision == expected == reference.classify(incoming)[0]
    assert bool(result.differing_fields) == (expected == "CONFLICT")
    assert canonical == before
    assert result.key == tuple(canonical[name] for name in KEY_COLUMNS)


def test_exact_ulp_and_signed_zero_policy(minute_row):
    canonical = map_gold_row(minute_row)
    total = math.nextafter(minute_row["sum"], math.inf)
    incoming = map_gold_row({
        **minute_row, "sum": total, "avg": total / minute_row["count"],
    })
    assert compare_gold_rows(canonical, incoming).differing_fields == (
        "sum_value", "avg_value",
    )
    zero = {**minute_row, **dict.fromkeys(("sum", "min", "max", "avg"), 0.0)}
    negative = {**zero, "sum": -0.0, "min": -0.0}
    assert compare_gold_rows(
        map_gold_row(zero), map_gold_row(negative),
    ).decision == "NO-OP"


def test_key_mismatch_is_not_a_same_key_comparison(minute_row):
    canonical = map_gold_row(minute_row)
    other = map_gold_row({**minute_row, "equipment_id": "fridge-002"})
    assert gold_natural_key(canonical) != gold_natural_key(other)
    with pytest.raises(ValueError, match="same natural key"):
        compare_gold_rows(canonical, other)
