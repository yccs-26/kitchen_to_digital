"""Multi-batch fixture와 기존 유한 입력 oracle의 계약만 검증한다."""

from copy import deepcopy
from datetime import datetime, timezone
import json
from pathlib import Path

import pytest

from streaming.aggregations.minute_aggregation import (
    EventConflictError,
    aggregate_minutes,
)


ROOT = Path(__file__).resolve().parents[3]
CONTRACT = json.loads(
    (ROOT / "tests/fixtures/phase3/multi_batch_events.json").read_text()
)
SCENARIOS = CONTRACT["scenarios"]
RULES = json.loads(
    (ROOT / "config/validation_rules.json").read_text()
)["metric_units"]


def target_events(scenario):
    """시간 진행용 장비를 제외하고 검사 대상의 입력을 모은다."""
    return [
        event
        for step in scenario["steps"]
        for event in step["events"]
        if event["equipment_id"] == CONTRACT["target_equipment_id"]
    ]


def serialize_result(result):
    """기존 결과를 fixture의 명시적 UTC 표현으로 바꾼다."""
    return [
        {
            "window_start": row.key.window_start.isoformat(),
            "window_end": row.window_end.isoformat(),
            "window_type": row.key.window_type,
            "store_id": row.key.store_id,
            "equipment_id": row.key.equipment_id,
            "metric_name": row.key.metric_name,
            "unit": row.unit,
            **{name: getattr(row, name) for name in (
                "count", "sum", "avg", "min", "max",
            )},
        }
        for row in result
    ]


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s["name"])
def test_fixture_preserves_validated_wire_shape(scenario):
    schema = json.loads(
        (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text()
    )
    fields = {field["name"] for field in schema["fields"]}
    for step in scenario["steps"]:
        assert len(step["events"]) == len(step["expected_decisions"])
        for event in step["events"]:
            assert set(event) == fields
            instant = datetime.fromisoformat(event["event_time"])
            assert instant.utcoffset() is not None
            assert isinstance(event["metric_value"], float)


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda s: s["name"])
def test_finite_oracle_expectations(scenario):
    original = deepcopy(scenario)
    events = target_events(scenario)
    conflict = scenario.get("expected_conflict")
    if conflict:
        with pytest.raises(EventConflictError) as caught:
            aggregate_minutes(events, metric_units=RULES)
        assert caught.value.event_id == conflict["event_id"]
        assert list(caught.value.differing_fields) == conflict[
            "differing_fields"
        ]
    else:
        result = aggregate_minutes(events, metric_units=RULES)
        assert serialize_result(result) == scenario["expected_finite"]
    assert scenario == original


def test_per_batch_python_calls_do_not_provide_cross_batch_dedup():
    scenario = next(s for s in SCENARIOS
                    if s["name"] == "cross_batch_duplicate")
    batches = [step["events"] for step in scenario["steps"][:2]]
    independent_count = sum(
        sum(row.count for row in aggregate_minutes(batch, metric_units=RULES))
        for batch in batches
    )
    whole_count = sum(
        row.count for row in aggregate_minutes(
            target_events(scenario), metric_units=RULES,
        )
    )
    assert independent_count == 4
    assert whole_count == 3


def test_finite_oracle_has_no_watermark_or_finalization():
    late = next(s for s in SCENARIOS if s["name"] == "watermark_late")
    assert late["expected_finite"][0]["count"] == 3
    assert late["expected_stream_final"][0]["count"] == 2
    idle = next(s for s in SCENARIOS if s["name"] == "idle")
    assert idle["expected_finite"][0]["count"] == 2
    assert idle["expected_stream_final"] == []
    assert idle["completion"] == "OPEN_PENDING_EVENT_TIME"


def test_batch_replay_finite_oracle_is_idempotent_not_checkpoint_proof():
    scenario = next(s for s in SCENARIOS if s["name"] == "batch_replay")
    events = target_events(scenario)
    replayed = events + deepcopy(scenario["steps"][0]["events"])
    assert serialize_result(
        aggregate_minutes(replayed, metric_units=RULES),
    ) == scenario["expected_finite"]


def test_utc_boundary_is_not_local_wall_clock_boundary():
    scenario = next(s for s in SCENARIOS if s["name"] == "utc_boundary")
    event = target_events(scenario)[1]
    instant = datetime.fromisoformat(event["event_time"])
    assert instant.astimezone(timezone.utc).isoformat() == (
        "2026-10-16T00:01:00+00:00"
    )


def test_all_required_scenarios_and_stream_policy_are_explicit():
    assert {s["name"] for s in SCENARIOS} == {
        "normal", "cross_batch_duplicate", "payload_conflict",
        "utc_boundary", "watermark_late", "idle", "batch_replay",
    }
    assert CONTRACT["watermark_delay"] == "10 minutes"
    assert CONTRACT["window_duration"] == "1 minute"
    assert CONTRACT["output_mode"] == "append"
