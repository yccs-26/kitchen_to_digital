"""synthetic 후보의 형태와 기존 numeric 입력과의 경계를 검증한다."""

from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pytest

from streaming.state.contracts import (
    CleaningSignal,
    FaultSignal,
    HealthStatus,
    ModeSignal,
    OperationalState,
    ReportedMode,
    StateObservation,
    SyntheticSignal,
)
from streaming.validation.domain_validator import (
    EquipmentSpec,
    validate_domain_event,
)


ROOT = Path(__file__).resolve().parents[3]
FIXTURE = ROOT / "tests/fixtures/phase4/synthetic_signals.json"
NOW = datetime(2026, 10, 16, tzinfo=timezone.utc)


@pytest.fixture
def signal():
    return SyntheticSignal(
        signal_id="synthetic-1",
        event_time=NOW,
        store_id="synthetic-store",
        equipment_id="synthetic-burner",
        source="synthetic-controller",
        payload=CleaningSignal(active=True),
        provenance="synthetic_test_only",
    )


@pytest.mark.parametrize("operational", list(OperationalState))
@pytest.mark.parametrize("health", list(HealthStatus))
def test_operational_state_and_health_are_independent(operational, health):
    state = StateObservation(operational, health)
    changed = replace(state, health_status=HealthStatus.STALE)
    assert changed.operational_state is operational
    assert state.health_status is health


@pytest.mark.parametrize(
    "operational, health",
    [
        (HealthStatus.FAULT, HealthStatus.HEALTHY),
        (OperationalState.IDLE, OperationalState.CLEANING),
        ("IDLE", HealthStatus.HEALTHY),
        (OperationalState.IDLE, "HEALTHY"),
        (None, HealthStatus.HEALTHY),
    ],
)
def test_state_axes_cannot_be_mixed(operational, health):
    with pytest.raises(TypeError):
        StateObservation(operational, health)


def test_unknown_initial_state_is_not_silently_defaulted():
    with pytest.raises(TypeError):
        StateObservation()


def test_synthetic_fixture_covers_reports_without_executing_transitions():
    fixture = json.loads(FIXTURE.read_text())
    context = dict(fixture["context"])
    context["event_time"] = datetime.fromisoformat(context["event_time"])
    payloads = []
    for case in fixture["cases"]:
        if case["kind"] == "mode":
            payload = ModeSignal(ReportedMode(case["mode"]))
        elif case["kind"] == "cleaning":
            payload = CleaningSignal(case["active"])
        else:
            assert case["kind"] == "fault"
            payload = FaultSignal(case["code"], case["active"])
        candidate = SyntheticSignal(
            **context,
            signal_id=case["signal_id"],
            payload=payload,
            provenance=fixture["provenance"],
        )
        assert candidate.event_time == NOW
        assert candidate.event_time.tzinfo is timezone.utc
        payloads.append(payload)
    assert set(payloads) == {
        *(ModeSignal(mode) for mode in ReportedMode),
        CleaningSignal(True),
        CleaningSignal(False),
        FaultSignal("SYNTHETIC_E1", True),
        FaultSignal("SYNTHETIC_E1", False),
    }


@pytest.mark.parametrize("value", [None, 0, 1, "false", "true", 180.0])
def test_explicit_activity_requires_boolean(value):
    with pytest.raises(TypeError):
        CleaningSignal(value)
    with pytest.raises(TypeError):
        FaultSignal("SYNTHETIC_E1", value)


@pytest.mark.parametrize(
    "value", [None, "COOKING", OperationalState.COOKING, 180.0]
)
def test_mode_requires_reported_mode(value):
    with pytest.raises(TypeError):
        ModeSignal(value)


@pytest.mark.parametrize("value", [None, "", " \t", 123])
def test_fault_requires_nonblank_code(value):
    with pytest.raises(ValueError):
        FaultSignal(value, True)


@pytest.mark.parametrize(
    "field", ["signal_id", "store_id", "equipment_id", "source"]
)
@pytest.mark.parametrize("value", [None, "", " \t", 123])
def test_signal_requires_traceable_identity(signal, field, value):
    with pytest.raises(ValueError):
        replace(signal, **{field: value})


@pytest.mark.parametrize("value", [None, "production", "simulator", True])
def test_synthetic_marker_is_mandatory(signal, value):
    with pytest.raises(ValueError):
        replace(signal, provenance=value)


@pytest.mark.parametrize(
    "value", [None, datetime(2026, 10, 16), "2026-10-16T00:00:00Z"]
)
def test_signal_requires_aware_internal_time(signal, value):
    with pytest.raises(ValueError):
        replace(signal, event_time=value)


@pytest.mark.parametrize(
    "value",
    [None, 180.0, {"metric_value": 180.0}, {"active": True}],
)
def test_numeric_or_untyped_payload_cannot_be_explicit_signal(signal, value):
    with pytest.raises(TypeError):
        replace(signal, payload=value)


def test_candidate_and_payload_are_immutable(signal):
    with pytest.raises(FrozenInstanceError):
        signal.payload = FaultSignal("SYNTHETIC_E1", True)
    with pytest.raises(FrozenInstanceError):
        signal.payload.active = False


def test_current_avro_has_no_explicit_signal_fields():
    schema = json.loads(
        (ROOT / "schemas/avro/sensor_metric_event.avsc").read_text()
    )
    fields = {field["name"]: field["type"] for field in schema["fields"]}
    assert len(fields) == 10
    assert fields["metric_value"] == "double"
    assert not {"mode", "cleaning", "fault"} & fields.keys()


@pytest.mark.parametrize("metric_name", ["mode", "cleaning", "fault"])
def test_current_domain_rules_reject_candidate_metric_names(metric_name):
    rules = json.loads(
        (ROOT / "config/validation_rules.json").read_text()
    )
    registry = {
        item["equipment_id"]: EquipmentSpec(
            item["store_id"], item["equipment_type"]
        )
        for item in rules["equipment_registry"]
    }
    for equipment in rules["equipment_registry"]:
        event = {
            **equipment,
            "event_id": "synthetic-boundary-test",
            "event_time": NOW,
            "metric_name": metric_name,
            "metric_value": 1.0,
            "unit": "celsius",
            "schema_version": "1.0.0",
            "source": "synthetic-test",
        }
        result = validate_domain_event(
            event,
            equipment["equipment_id"].encode("utf-8"),
            supported_schema_versions=rules["supported_schema_versions"],
            equipment_registry=registry,
            metric_units=rules["metric_units"],
            reference_time=NOW,
            max_future_skew=timedelta(0),
        )
        assert [error.code for error in result.errors] == [
            "UNSUPPORTED_METRIC"
        ]
