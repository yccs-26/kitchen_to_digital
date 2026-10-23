"""Local Application/Timer/Codec/Avro/Fake integration; no external marker.

Each assertion compares expected snapshots, IDs, pending records and sink data.
Subprocess exits leave real SQLite files for a newly constructed application.
"""

from dataclasses import replace
import os
from pathlib import Path
import subprocess
import sys

import pytest

from streaming.state.application_timer import LocalHealthTimers
from streaming.state.change_wire import encode_change
from streaming.state.contracts import (
    CleaningCompletedSignal, CleaningSignal, ModeSignal, ReportedMode,
)
from tests.fixtures.phase4.application_fixture import (
    BASE, TIMEOUT, bundle, numeric, signal,
)


ROOT = Path(__file__).resolve().parents[2]


def observe(local, second, value=800, command=None):
    event = numeric(second, value)
    return local.app.process_validated(
        event, command_ref=command or f"e{second}",
        reference_time=event["event_time"],
    )


CHILD = '''
import os, sys
from pathlib import Path
from tests.fixtures.phase4.application_fixture import (
    BASE, TIMEOUT, bundle, numeric,
)
from streaming.state.application_timer import LocalHealthTimers
local = bundle(Path(sys.argv[1]))
mode, point = sys.argv[2:]
def crash(stage):
    if stage == point:
        os._exit(81)
if mode == 'observe':
    local.app.process_validated(numeric(), command_ref='initial',
                                reference_time=BASE, hook=crash)
elif mode == 'timer':
    timers = LocalHealthTimers(local.app)
    token = timers.plan('hood-001', evaluation_time=BASE).token
    timers.evaluate('hood-001', token, command_ref='timer',
                    reference_time=BASE + TIMEOUT, hook=crash)
else:
    output = next(oid for oid in local.store.pending()
                  if local.store.output(oid).change is not None)
    local.coordinator.dispatch(output, hook=crash)
raise SystemExit('failure point was not reached')
'''


def crash_child(path, mode, point):
    process = subprocess.run(
        [sys.executable, "-c", CHILD, str(path), mode, point], cwd=ROOT,
        env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=20,
    )
    assert process.returncode == 81, process.stderr


@pytest.mark.parametrize("point,op_revision,health_revision", [
    ("after_calculation", 0, 0), ("before_operational_commit", 0, 0),
    ("after_operational_commit", 1, 0), ("before_health_commit", 1, 0),
    ("after_health_commit", 1, 1),
])
def test_application_process_death_resumes_each_atomic_axis(
    tmp_path, point, op_revision, health_revision,
):
    local = bundle(tmp_path, initialize=True)
    local.close()
    crash_child(tmp_path, "observe", point)
    local = bundle(tmp_path)
    for axis, expected in (("OPERATIONAL", op_revision),
                           ("HEALTH", health_revision)):
        actual = local.store.state(("store-001", "hood-001", axis)).revision
        assert actual == expected
    result = local.app.process_validated(numeric(), command_ref="initial",
                                         reference_time=BASE)
    ids = tuple(item.output_id for item in result)
    assert all(ids)
    assert local.app.load_operational("hood-001").state.candidate.value == (
        "ROTATING"
    )
    assert local.app.load_health("hood-001").health_status.value == "HEALTHY"
    local.app.reconcile(local.coordinator)
    assert local.store.pending() == ()
    assert len(local.history.rows()) == len(local.kafka.messages()) == 1
    repeat = local.app.process_validated(numeric(), command_ref="initial",
                                         reference_time=BASE)
    assert tuple(item.output_id for item in repeat) == ids
    assert all(item.replayed for item in repeat)
    local.close()


@pytest.mark.parametrize("point,stored", [
    ("after_calculation", False), ("before_health_commit", False),
    ("after_health_commit", True),
])
def test_timer_process_death_preserves_deadline_and_intent(
    tmp_path, point, stored,
):
    local = bundle(tmp_path, initialize=True)
    observe(local, 0)
    local.app.reconcile(local.coordinator)
    token = LocalHealthTimers(local.app).plan("hood-001",
                                              evaluation_time=BASE).token
    local.close()
    crash_child(tmp_path, "timer", point)
    local = bundle(tmp_path)
    assert local.app.load_health("hood-001").health_status.value == (
        "STALE" if stored else "HEALTHY"
    )
    timer = LocalHealthTimers(local.app)
    plan = timer.plan("hood-001", evaluation_time=BASE + TIMEOUT)
    assert plan.token == token
    result = timer.evaluate("hood-001", token, command_ref="timer",
                            reference_time=BASE + TIMEOUT)
    assert result.replayed is stored
    native_id = local.store.output(result.output_id).change.identity
    local.app.reconcile(local.coordinator)
    assert local.history.rows()[-1].identity == native_id
    assert len(local.history.rows()) == len(local.kafka.messages()) == 2
    local.close()


@pytest.mark.parametrize("point,duplicates", [
    ("after_history", 1), ("before_kafka", 1), ("after_kafka", 2),
    ("after_current", 1), ("before_complete", 1),
])
def test_application_reconciliation_after_actual_sink_ack_process_exit(
    tmp_path, point, duplicates,
):
    local = bundle(tmp_path, initialize=True)
    result = observe(local, 0)
    health_id = result[1].output_id
    change = local.store.output(health_id).change
    original_wire = encode_change(change, schema_id=17)
    local.close()
    crash_child(tmp_path, "dispatch", point)
    local = bundle(tmp_path)
    local.app.reconcile(local.coordinator)
    assert local.store.pending() == ()
    assert local.history.rows() == (change,)
    assert local.kafka.messages() == (change,) * duplicates
    recovered_wire = encode_change(local.history.rows()[0], schema_id=17)
    assert recovered_wire == original_wire
    assert local.current.get(("store-001", "hood-001", "HEALTH")).data[
        "current"
    ]["state"] == "HEALTHY"
    local.close()


def test_multi_equipment_states_recover_after_every_event(tmp_path):
    local = bundle(tmp_path, initialize=True)
    expected_ids = []
    for second, payload, expected in (
        (0, ModeSignal(ReportedMode.PREHEATING), "PREHEATING"),
        (1, ModeSignal(ReportedMode.COOKING), "COOKING"),
        (2, CleaningSignal(True), "CLEANING"),
        (3, CleaningCompletedSignal(), "IDLE"),
    ):
        event = signal(second, payload)
        result = local.app.process_signal(event, equipment_type="fryer",
                                           command_ref=f"s{second}",
                                           reference_time=event.event_time)
        output = local.store.output(result.output_id)
        expected_ids.append(output.change.identity)
        local.close()
        local = bundle(tmp_path)
        snapshot = local.app.load_operational("fryer-001")
        assert snapshot.state.observation.operational_state.value == expected
        repeat = local.app.process_signal(event, equipment_type="fryer",
                                           command_ref=f"s{second}",
                                           reference_time=event.event_time)
        assert repeat.output_id == result.output_id and repeat.replayed
    # Numeric fryer input establishes Health but cannot alter Operational.
    event = numeric(3, 180, "fryer-001")
    before = local.app.load_operational("fryer-001")
    local.app.process_validated(event, command_ref="oil",
                                reference_time=event["event_time"])
    assert local.app.load_operational("fryer-001") == before
    for second, rpm in ((0, 0), (1, 0), (2, 0), (3, 800), (4, 800), (6, 800),
                        (7, 300), (8, 0), (9, 0), (10, 0)):
        result = observe(local, second, rpm)
        if result[0].decision == "TRANSITION":
            expected_ids.append(local.store.output(result[0].output_id).
                                change.identity)
        local.close()
        local = bundle(tmp_path)
    snapshot = local.app.load_operational("hood-001")
    assert snapshot.state.current.value == "STOPPED"
    timer = LocalHealthTimers(local.app)
    plan = timer.plan("hood-001", evaluation_time=BASE + TIMEOUT)
    timer.evaluate("hood-001", plan.token, command_ref="late-timeout",
                    reference_time=BASE + 2 * TIMEOUT)
    recovery = numeric(20, 0, event_id="recovered")
    local.app.process_validated(recovery, command_ref="recovery",
                                reference_time=recovery["event_time"])
    local.app.reconcile(local.coordinator)
    assert local.store.pending() == ()
    assert all(identity in [row.identity for row in local.history.rows()]
               for identity in expected_ids)
    assert len(expected_ids) == len(set(expected_ids)) == 6
    assert local.current.get(("store-001", "hood-001", "HEALTH")).data[
        "current"
    ]["state"] == "HEALTHY"
    assert local.current.get(("store-001", "fryer-001", "OPERATIONAL")).data[
        "current"
    ]["state"] == "IDLE"
    local.close()


@pytest.mark.parametrize("equipment", [
    "refrigerator", "freezer", "dishwasher",
])
def test_unimplemented_equipment_has_no_fryer_fallback(tmp_path, equipment):
    local = bundle(tmp_path, initialize=True)
    with pytest.raises(ValueError):
        local.app.process_validated({**numeric(), "equipment_type": equipment,
                                     "equipment_id": equipment + "-001"},
                                    command_ref="unsupported",
                                    reference_time=BASE)
    assert local.store.pending() == ()
    snapshot = local.app.load_operational("fryer-001")
    assert snapshot.state.observation.operational_state.value == "IDLE"
    local.close()


def test_existing_validated_avro_decoder_feeds_local_application(tmp_path):
    from io import BytesIO
    import json
    from types import SimpleNamespace
    from fastavro import parse_schema, schemaless_writer
    from streaming.validation.avro_decoder import decode_sensor_event

    schema_path = ROOT / "schemas/avro/sensor_metric_event.avsc"
    schema = json.loads(schema_path.read_text())
    event = numeric()
    body = {**event, "event_time": event["event_time"].isoformat()}
    stream = BytesIO()
    schemaless_writer(stream, parse_schema(schema), body)

    class LocalRegistry:
        def get_schema(self, schema_id):
            assert schema_id == 23
            return SimpleNamespace(schema_str=json.dumps(schema))

    decoded = decode_sensor_event(
        b"\x00" + (23).to_bytes(4, "big") + stream.getvalue(), LocalRegistry(),
    )
    assert decoded["event_time"] == BASE
    local = bundle(tmp_path, initialize=True)
    result = local.app.process_validated(decoded, command_ref="decoded",
                                         reference_time=BASE)
    assert [item.decision for item in result] == ["PENDING", "INITIALIZED"]
    assert local.app.load_health("hood-001").last_accepted_event_id == (
        event["event_id"]
    )
    local.app.reconcile(local.coordinator)
    assert local.kafka.messages()[0].data["source_ref"]["event_id"] == (
        event["event_id"]
    )
    local.close()


def test_recorded_sequence_in_two_fresh_stores_has_same_state_ids_and_wire(
    tmp_path,
):
    def run(path):
        path.mkdir()
        local = bundle(path, initialize=True)
        for second in (0, 1, 3):
            observe(local, second)
        event = signal()
        local.app.process_signal(event, equipment_type="fryer",
                                  command_ref="signal", reference_time=BASE)
        oil = numeric(3, 180, "fryer-001")
        local.app.process_validated(oil, command_ref="oil",
                                    reference_time=oil["event_time"])
        timer = LocalHealthTimers(local.app)
        at = numeric(13)["event_time"]
        token = timer.plan("hood-001", evaluation_time=at).token
        timer.evaluate("hood-001", token, command_ref="timeout",
                        reference_time=at)
        observe(local, 13, command="recovery")
        local.app.reconcile(local.coordinator)
        axes = [("store-001", equipment, axis)
                for equipment in ("fryer-001", "hood-001")
                for axis in ("OPERATIONAL", "HEALTH")]
        result = (
            tuple(local.store.state(axis) for axis in axes),
            local.history.rows(), local.kafka.messages(),
            tuple(encode_change(row, schema_id=17)
                  for row in local.history.rows()),
            tuple(local.current.get(axis) for axis in axes),
        )
        assert local.store.pending() == ()
        local.close()
        return result

    assert run(tmp_path / "first") == run(tmp_path / "replay")
