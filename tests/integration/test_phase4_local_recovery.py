"""Local SQLite recovery: fixed clocks, injected failures, process death.

No integration marker: this file needs no Kafka/Registry/cloud service and is
included in `-m 'not integration'`. External integration tests remain excluded.
"""

from datetime import datetime, timedelta, timezone
import os
from pathlib import Path
import subprocess
import sys

import pytest

from streaming.state.bindings import (
    EquipmentBinding, FRYER_REFERENCE, HOOD_PROFILE, InputKind, ProfileKey,
)
from streaming.state.change_envelope import (
    OutputConflict, OutputContext, canonical, digest, health_output,
    operational_output, snapshot_bytes,
)
from streaming.state.contracts import (
    HealthStatus, ModeSignal, OperationalState, ReportedMode, StateObservation,
    SyntheticSignal,
)
from streaming.state.fsm import TwinState, transition
from streaming.state.health import (
    HealthObservation, HealthPolicy, HealthTick, initialize_health,
    reduce_health, timer_token,
)
from streaming.state.health_codec import decode_health_snapshot
from streaming.state.health_timer_coordinator import plan_health_timer
from streaming.state.local_sinks import LocalCurrent, LocalHistory, LocalKafka
from streaming.state.publication import PublicationCoordinator
from streaming.state.publication_store import PreparedCommand, PublicationStore


BASE = datetime(2026, 10, 22, tzinfo=timezone.utc)
TIMEOUT = timedelta(seconds=10)  # Explicit fixture, not an operating default.
POLICY = HealthPolicy(
    EquipmentBinding("store", "hood", HOOD_PROFILE,
                     InputKind.NUMERIC_TELEMETRY),
    "fixture-binding", "numeric-v1", ProfileKey("hood", "fixture-health", "1"),
    TIMEOUT, "fan_rpm", "rpm", ("1.0.0",), ("fixture",),
)
AXIS = ("store", "hood", "HEALTH")
ROOT = Path(__file__).resolve().parents[2]


def initial():
    return initialize_health(
        POLICY, monitoring_epoch="fixture-epoch", monitoring_started_at=BASE,
    )


def sample(at=BASE, event_id="e1"):
    return HealthObservation(
        event_id, at, "store", "hood", "hood", "fan_rpm", 800,
        "rpm", "1.0.0", "fixture",
    )


def command_for(before, item, at, name, root_revision):
    result = reduce_health(before, item, policy=POLICY, reference_time=at)
    context = OutputContext(
        name, 1, result.snapshot.health_revision,
        result.snapshot.snapshot_revision, before.last_health_change_id,
    )
    output = health_output(
        before, result, item, context=context, reference_time=at,
    )
    command = PreparedCommand(
        AXIS, name, 1, root_revision, digest(snapshot_bytes(before)),
        snapshot_bytes(result.snapshot), canonical({"input": item,
                                                  "evaluation_time": at}),
        result.decision.value, output,
    )
    return command, result.snapshot


def resources(path):
    return (
        PublicationStore(path / "publication.sqlite"),
        LocalHistory(path / "history.sqlite"),
        LocalKafka(path / "kafka.sqlite"),
        LocalCurrent(path / "current.sqlite"),
    )


def close(resources):
    for resource in resources:
        resource.close()


def setup_pending(path):
    items = resources(path)
    command, state = command_for(initial(), sample(), BASE, "c1", 0)
    items[0].initialize(AXIS, 1, snapshot_bytes(initial()))
    output_id = items[0].commit(command)
    return items, command, state, output_id


def assert_recovered(items, output_id, expected, *, kafka_count=1):
    store, history, kafka, current = items
    expected_change = store.output(output_id).change
    assert store.pending() == ()
    assert history.rows() == (expected_change,)
    assert kafka.messages() == (expected_change,) * kafka_count
    assert current.get(AXIS) == store.output(output_id).projection
    restored = decode_health_snapshot(
        store.state(AXIS).snapshot, policy=POLICY,
    )
    assert restored == expected
    assert timer_token(restored) == timer_token(expected)
    assert plan_health_timer(
        restored, policy=POLICY, evaluation_time=BASE + TIMEOUT,
    ) == plan_health_timer(
        expected, policy=POLICY, evaluation_time=BASE + TIMEOUT,
    )


@pytest.mark.parametrize("order,fail,successful", [
    (("history", "kafka", "current"), "kafka", ("history",)),
    (("history", "kafka", "current"), "current", ("history", "kafka")),
    (("kafka", "history", "current"), "history", ("kafka",)),
    (("kafka", "current", "history"), "current", ("kafka",)),
    (("current", "kafka", "history"), "kafka", ("current",)),
])
def test_partial_failure_reopen_pending_and_converge(tmp_path, order, fail,
                                                    successful):
    items, command, expected, oid = setup_pending(tmp_path)

    def inject(stage):
        if stage == "before_" + fail:
            raise RuntimeError("injected before side effect")

    with pytest.raises(RuntimeError):
        PublicationCoordinator(*items).dispatch(oid, order=order, hook=inject)
    # Actual failure state: successful stages persisted, failed stage pending.
    for sink in successful:
        assert items[0].delivery(oid, sink)["status"] == "ACKED"
    assert items[0].delivery(oid, fail)["status"] == "FAILED"
    assert items[0].pending() == (oid,)
    close(items)
    items = resources(tmp_path)
    # Retry behavior: reopening real files retains records and successful ACKs.
    assert items[0].commit(command) == oid
    PublicationCoordinator(*items).reconcile()
    assert_recovered(items, oid, expected)
    close(items)


def test_all_sinks_success_completion_failure_does_not_republish(tmp_path):
    items, _, expected, oid = setup_pending(tmp_path)

    def fail(stage):
        if stage == "before_complete":
            raise RuntimeError("completion record failure")

    with pytest.raises(RuntimeError):
        PublicationCoordinator(*items).dispatch(oid, hook=fail)
    assert all(items[0].delivery(oid, sink)["status"] == "ACKED"
               for sink in ("history", "kafka", "current"))
    close(items)
    items = resources(tmp_path)
    PublicationCoordinator(*items).reconcile()
    assert_recovered(items, oid, expected)
    close(items)


DISPATCH_CHILD = '''
import os, sys
from pathlib import Path
from streaming.state.publication_store import PublicationStore
from streaming.state.publication import PublicationCoordinator
from streaming.state.local_sinks import LocalHistory, LocalKafka, LocalCurrent
path = Path(sys.argv[1])
store = PublicationStore(path / 'publication.sqlite')
sinks = [LocalHistory(path / 'history.sqlite'),
         LocalKafka(path / 'kafka.sqlite'),
         LocalCurrent(path / 'current.sqlite')]
def crash(stage):
    if stage == sys.argv[2]:
        os._exit(73)
PublicationCoordinator(store, *sinks).dispatch(store.pending()[0], hook=crash)
raise SystemExit('expected crash was not reached')
'''


@pytest.mark.parametrize("point,duplicates", [
    ("after_history", 1), ("after_kafka", 2), ("after_current", 1),
    ("before_complete", 1),
])
def test_actual_process_death_after_sink_ack(tmp_path, point, duplicates):
    items, _, expected, oid = setup_pending(tmp_path)
    close(items)
    process = subprocess.run(
        [sys.executable, "-c", DISPATCH_CHILD, str(tmp_path), point],
        cwd=ROOT, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=20,
    )
    assert process.returncode == 73, process.stderr
    items = resources(tmp_path)
    if point != "before_complete":
        sink = point.removeprefix("after_")
        assert items[0].delivery(oid, sink)["status"] == "UNKNOWN"
    PublicationCoordinator(*items).reconcile()
    # Duplicate defense: History/Current converge, Kafka may deliver twice.
    assert_recovered(items, oid, expected, kafka_count=duplicates)
    close(items)


COMMIT_CHILD = '''
import os, sys, base64
from pathlib import Path
from streaming.state.change_envelope import read_json, canonical
from streaming.state.publication_store import (
    PreparedCommand, PublicationStore, decode_output,
)
path = Path(sys.argv[1])
data = read_json((path / 'command.json').read_bytes())
command = PreparedCommand(
    tuple(data['axis_key']), data['command_ref'], data['generation'],
    data['expected_revision'], data['before_sha256'],
    base64.b64decode(data['after_snapshot']), canonical(data['input']),
    data['decision'], decode_output(canonical(data['output'])),
)
store = PublicationStore(path / 'publication.sqlite')
def crash(stage):
    if stage == sys.argv[2]:
        os._exit(74)
store.commit(command, hook=crash)
os._exit(75)
'''


@pytest.mark.parametrize("point,code", [
    ("after_snapshot", 74), ("before_commit", 74), ("after_commit", 75),
])
def test_actual_process_death_at_atomic_snapshot_output_boundary(tmp_path,
                                                               point, code):
    items = resources(tmp_path)
    command, expected = command_for(initial(), sample(), BASE, "c1", 0)
    items[0].initialize(AXIS, 1, snapshot_bytes(initial()))
    (tmp_path / "command.json").write_bytes(command.payload)
    close(items)
    process = subprocess.run(
        [sys.executable, "-c", COMMIT_CHILD, str(tmp_path), point],
        cwd=ROOT, env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, timeout=20,
    )
    assert process.returncode == code, process.stderr
    items = resources(tmp_path)
    if code == 74:
        assert items[0].state(AXIS).revision == 0
        assert items[0].state(AXIS).snapshot == snapshot_bytes(initial())
        assert items[0].pending() == ()
    else:
        assert items[0].state(AXIS).snapshot == snapshot_bytes(expected)
        assert items[0].pending() == (command.output.identity,)
    oid = items[0].commit(command)
    PublicationCoordinator(*items).reconcile()
    assert_recovered(items, oid, expected)
    close(items)


def test_recovery_can_fail_again_without_losing_pending_intent(tmp_path):
    items, _, expected, oid = setup_pending(tmp_path)
    for _ in range(2):
        def fail(stage):
            if stage == "before_kafka":
                raise RuntimeError("retry failed again")
        with pytest.raises(RuntimeError):
            PublicationCoordinator(*items).dispatch(oid, hook=fail)
        close(items)
        items = resources(tmp_path)
    assert items[0].delivery(oid, "kafka")["attempts"] == 2
    assert len(items[1].rows()) == 1
    PublicationCoordinator(*items).reconcile()
    assert_recovered(items, oid, expected)
    close(items)


def test_repeated_batch_pending_lookup_and_duplicate_publish(tmp_path):
    items, command, expected, oid = setup_pending(tmp_path)
    assert items[0].commit(command) == oid
    PublicationCoordinator(*items).reconcile()
    # Explicit consumer-side duplicate.
    items[2].publish(command.output.change)
    close(items)
    items = resources(tmp_path)
    assert items[0].commit(command) == oid
    PublicationCoordinator(*items).reconcile()
    assert_recovered(items, oid, expected, kafka_count=2)
    assert len({record.identity for record in items[2].messages()}) == 1
    close(items)


def test_stale_recovery_and_late_current_do_not_rollback(tmp_path):
    items, first, healthy, oid = setup_pending(tmp_path)
    coordinator = PublicationCoordinator(*items)
    coordinator.dispatch(oid)
    timeout, stale = command_for(
        healthy, HealthTick(timer_token(healthy)), BASE + TIMEOUT, "c2", 1,
    )
    recovery, recovered = command_for(
        stale, sample(BASE + TIMEOUT, "e2"), BASE + TIMEOUT, "c3", 2,
    )
    stale_id = items[0].commit(timeout)
    recovery_id = items[0].commit(recovery)
    # Current latest revision may arrive before the old STALE projection.
    items[3].apply(recovery.output.projection)
    coordinator.reconcile()
    assert items[0].delivery(stale_id, "current")["status"] == "SUPERSEDED"
    assert items[0].delivery(recovery_id, "current")["status"] == (
        "ALREADY_APPLIED"
    )
    assert items[3].apply(first.output.projection).status == "SUPERSEDED"
    assert items[3].get(AXIS).data["current"]["state"] == "HEALTHY"
    assert len(items[1].rows()) == len(items[2].messages()) == 3
    assert [record.identity[1] for record in items[1].rows()] == [
        first.output.change.identity[1], timeout.output.change.identity[1],
        recovery.output.change.identity[1],
    ]
    close(items)
    items = resources(tmp_path)
    assert decode_health_snapshot(items[0].state(AXIS).snapshot,
                                  policy=POLICY) == recovered
    assert items[0].pending() == ()
    close(items)


def test_same_revision_changed_payload_conflicts_and_history_deduplicates(
    tmp_path,
):
    items, command, _, oid = setup_pending(tmp_path)
    PublicationCoordinator(*items).dispatch(oid)
    items[1].write(command.output.change)
    assert len(items[1].rows()) == 1
    different, _ = command_for(initial(), sample(event_id="other"), BASE,
                               "other", 0)
    with pytest.raises(OutputConflict):
        items[3].apply(different.output.projection)
    with pytest.raises(OutputConflict):
        items[1].write(different.output.change)
    assert items[3].get(AXIS) == command.output.projection
    close(items)


def test_operational_and_health_have_independent_recovery(tmp_path):
    items, health_command, _, health_id = setup_pending(tmp_path)
    binding = EquipmentBinding(
        "store", "fryer", FRYER_REFERENCE, InputKind.SYNTHETIC_SIGNAL,
    )
    before = TwinState("store", "fryer", StateObservation(
        OperationalState.IDLE, HealthStatus.STALE,
    ))
    event = SyntheticSignal(
        "signal", BASE, "store", "fryer", "fixture-controller",
        ModeSignal(ReportedMode.PREHEATING), "synthetic_test_only",
    )
    result = transition(before, event, reference_time=BASE)
    output = operational_output(
        before, result, event, binding=binding,
        context=OutputContext("op1", 1, 1, 1), reference_time=BASE,
    )
    axis = ("store", "fryer", "OPERATIONAL")
    items[0].initialize(axis, 1, snapshot_bytes(before))
    op_id = items[0].commit(PreparedCommand(
        axis, "op1", 1, 0, digest(snapshot_bytes(before)),
        snapshot_bytes(result.state), canonical({
            "input": event, "evaluation_time": BASE,
        }), result.decision.value, output,
    ))
    PublicationCoordinator(*items).dispatch(op_id)
    assert items[0].pending() == (health_id,)
    close(items)
    items = resources(tmp_path)
    PublicationCoordinator(*items).reconcile()
    assert items[3].get(axis).data["current"]["state"] == "PREHEATING"
    assert items[3].get(AXIS).data["current"]["state"] == "HEALTHY"
    assert items[0].state(axis).snapshot == snapshot_bytes(result.state)
    assert items[0].state(AXIS).snapshot == health_command.after_snapshot
    assert len(items[1].rows()) == 2
    close(items)
