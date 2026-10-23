"""File-backed doubles: idempotence, order, ACK and axis isolation."""

from dataclasses import replace

import pytest

from streaming.state.change_envelope import (
    OutputConflict, OutputContext, health_output, operational_output,
)
from streaming.state.local_sinks import LocalCurrent, LocalHistory, LocalKafka
from streaming.state.publication import PublicationCoordinator
from streaming.state.sink_protocols import RetryableSinkError
from streaming.state.contracts import ModeSignal, ReportedMode
from streaming.state.fsm import transition
from test_change_envelope import BINDING, fryer_pair, health_pair
from test_health import BASE, EPS, boot, evaluate, observation
from test_health_registry import FRYER_POLICY, fryer_observation
from test_publication import stored


def second_operational():
    pair = fryer_pair()
    before = pair[1].state
    event = replace(
        pair[2], signal_id="s2", event_time=BASE + EPS,
        payload=ModeSignal(ReportedMode.COOKING),
    )
    result = transition(before, event, reference_time=BASE + EPS)
    return operational_output(
        before, result, event, binding=BINDING,
        context=OutputContext("op-2", 1, 2, 2, pair[4].change.identity[1]),
        reference_time=BASE + EPS,
    )


def sinks(tmp_path):
    return (
        LocalHistory(tmp_path / "history.sqlite"),
        LocalKafka(tmp_path / "kafka.sqlite"),
        LocalCurrent(tmp_path / "current.sqlite"),
    )


def test_history_duplicate_order_gap_and_conflict_preserve_canonical(tmp_path):
    history, kafka, current = sinks(tmp_path)
    first = fryer_pair()[4]
    second = second_operational()
    with pytest.raises(RetryableSinkError):
        history.write(second.change)
    history.write(first.change)
    history.write(second.change)
    history.write(first.change)
    assert history.rows() == (first.change, second.change)
    changed_id = fryer_pair("other", "different")[4]
    with pytest.raises(OutputConflict):
        history.write(changed_id.change)
    before, result, event, context, _ = fryer_pair()
    changed_body = operational_output(
        before, result, event, binding=BINDING, context=context,
        reference_time=BASE + EPS,
    )
    assert changed_body.change.identity == first.change.identity
    with pytest.raises(OutputConflict, match="payload"):
        history.write(changed_body.change)
    assert history.rows() == (first.change, second.change)
    for sink in (history, kafka, current):
        sink.close()


def test_current_reverse_repeat_conflict_and_persisted_reopen(tmp_path):
    _, _, current = sinks(tmp_path)
    first = fryer_pair()[4].projection
    second = second_operational().projection
    assert current.apply(second).status == "ACKED"
    assert current.apply(first).status == "SUPERSEDED"
    assert current.apply(second).status == "ALREADY_APPLIED"
    other = fryer_pair("changed", "other")[4].projection
    assert current.apply(other).status == "SUPERSEDED"
    current.close()
    current = LocalCurrent(tmp_path / "current.sqlite")
    assert current.get(first.axis_key) == second
    current.close()


def test_current_same_revision_different_payload_is_conflict(tmp_path):
    _, _, current = sinks(tmp_path)
    current.apply(fryer_pair()[4].projection)
    with pytest.raises(OutputConflict, match="revision"):
        current.apply(fryer_pair("other", "other")[4].projection)
    current.close()


def test_health_and_operational_same_equipment_are_independent(tmp_path):
    history, kafka, current = sinks(tmp_path)
    operational = fryer_pair()[4]
    before = boot(FRYER_POLICY)
    event = fryer_observation()
    result = evaluate(before, event, policy=FRYER_POLICY)
    health = health_output(
        before, result, event, context=OutputContext("h", 1, 1, 1),
        reference_time=BASE,
    )
    for output in (operational, health):
        history.write(output.change)
        kafka.publish(output.change)
        current.apply(output.projection)
    item = current.equipment("store-001", "fryer-001")
    assert item["OPERATIONAL"]["current"]["state"] == "PREHEATING"
    assert item["HEALTH"]["current"]["state"] == "HEALTHY"
    assert len(history.rows()) == 2
    assert len(kafka.messages()) == 2
    assert current.equipment("other", "fryer-001") == {}
    for sink in (history, kafka, current):
        sink.close()


def test_current_new_generation_and_policy_are_fenced(tmp_path):
    _, _, current = sinks(tmp_path)
    pair = health_pair()
    current.apply(pair[4].projection)
    for context in (replace(pair[3], generation=2),):
        incompatible = health_output(
            pair[0], pair[1], pair[2], context=context, reference_time=BASE,
        )
        with pytest.raises(OutputConflict, match="fenced"):
            current.apply(incompatible.projection)
    current.close()


def test_kafka_republication_duplicates_exact_bytes_with_same_key(tmp_path):
    kafka = LocalKafka(tmp_path / "kafka.sqlite")
    change = health_pair()[4].change
    one = kafka.publish(change)
    two = kafka.publish(change)
    assert one.offset == 0 and two.offset == 1
    assert one.key == b"hood-001"
    assert one.topic == "kitchen.state.changes"
    kafka.close()
    kafka = LocalKafka(tmp_path / "kafka.sqlite")
    assert kafka.messages() == (change, change)
    kafka.close()


@pytest.mark.parametrize("changes", [
    {"offset": None}, {"offset": -1}, {"offset": True}, {"offset": 1.0},
    {"callback_received": False}, {"error": "error"},
    {"topic": "wrong"}, {"key": b"wrong"}, {"partition": True},
    {"payload_sha256": "0" * 64},
])
def test_invalid_kafka_ack_leaves_retryable_publication(tmp_path, changes):
    _, store, command = stored(tmp_path)
    oid = store.commit(command)
    history, kafka, current = sinks(tmp_path)

    class BadKafka:
        def publish(self, change):
            return replace(kafka.publish(change), **changes)

    dispatcher = PublicationCoordinator(store, history, BadKafka(), current)
    with pytest.raises(RetryableSinkError):
        dispatcher.dispatch(oid)
    assert store.delivery(oid, "history")["status"] == "ACKED"
    assert store.delivery(oid, "kafka")["status"] == "FAILED"
    assert current.get(command.axis_key) is None
    PublicationCoordinator(store, history, kafka, current).reconcile()
    assert len(history.rows()) == 1
    assert len(kafka.messages()) == 2
    assert store.pending() == ()
    for resource in (store, history, kafka, current):
        resource.close()


def test_current_only_freshness_does_not_publish_history_or_kafka(tmp_path):
    pair = health_pair()
    _, store, command = stored(tmp_path, pair)
    history, kafka, current = sinks(tmp_path)
    coordinator = PublicationCoordinator(store, history, kafka, current)
    coordinator.dispatch(store.commit(command))
    state = pair[1].snapshot
    next_pair = health_pair(
        state, observation(BASE + EPS, "e2"), BASE + EPS, "h2",
    )
    from test_publication import prepare
    next_command = prepare(next_pair, version=1)
    oid = store.commit(next_command)
    coordinator.dispatch(oid)
    assert len(history.rows()) == len(kafka.messages()) == 1
    assert current.get(command.axis_key).data["current"]["last_seen"] == (
        next_pair[4].projection.data["current"]["last_seen"]
    )
    assert store.delivery(oid, "history")["status"] == "NOT_REQUIRED"
    assert store.delivery(oid, "kafka")["attempts"] == 0
    for resource in (store, history, kafka, current):
        resource.close()


def test_failed_sink_conflict_is_visible_and_not_silently_retried(tmp_path):
    _, store, command = stored(tmp_path, fryer_pair())
    history, kafka, current = sinks(tmp_path)
    canonical_change = fryer_pair("other", "different")[4].change
    history.write(canonical_change)
    oid = store.commit(command)
    with pytest.raises(OutputConflict):
        PublicationCoordinator(store, history, kafka, current).dispatch(oid)
    assert store.delivery(oid, "history")["status"] == "CONFLICT"
    assert store.pending() == (oid,)
    assert kafka.messages() == ()
    for resource in (store, history, kafka, current):
        resource.close()


@pytest.mark.parametrize("wrong", ["generation", "revision", "digest",
                                  "foreign_evidence"])
def test_invalid_current_ack_does_not_complete_publication(tmp_path, wrong):
    _, store, command = stored(tmp_path)
    history, kafka, current = sinks(tmp_path)
    oid = store.commit(command)

    class BadCurrent:
        def apply(self, projection):
            ack = current.apply(projection)
            if wrong == "generation":
                return replace(ack, generation=2)
            if wrong == "revision":
                return replace(ack, revision=True)
            if wrong == "digest":
                return replace(ack, payload_sha256="0" * 64)
            return replace(
                ack, canonical_payload=fryer_pair()[4].projection.payload,
            )

    with pytest.raises((RetryableSinkError, ValueError)):
        coordinator = PublicationCoordinator(
            store, history, kafka, BadCurrent(),
        )
        coordinator.dispatch(oid)
    assert store.delivery(oid, "current")["status"] == "FAILED"
    assert store.pending() == (oid,)
    PublicationCoordinator(store, history, kafka, current).reconcile()
    assert store.pending() == ()
    assert len(kafka.messages()) == 1
    for resource in (store, history, kafka, current):
        resource.close()


def test_superseding_current_without_durable_intent_cannot_complete(tmp_path):
    _, store, command = stored(tmp_path)
    history, kafka, current = sinks(tmp_path)
    oid = store.commit(command)
    state = health_pair()[1].snapshot
    newer_pair = health_pair(
        state, observation(BASE + EPS, "e2"), BASE + EPS, "not-committed",
    )
    newer = newer_pair[4]
    # Valid local Current data alone is not evidence of durable publication.
    current.apply(newer.projection)
    with pytest.raises(RetryableSinkError, match="not in this ledger"):
        PublicationCoordinator(store, history, kafka, current).dispatch(oid)
    assert store.delivery(oid, "current")["status"] == "FAILED"
    assert store.pending() == (oid,)
    from test_publication import prepare
    store.commit(prepare(newer_pair, version=1))
    PublicationCoordinator(store, history, kafka, current).reconcile()
    assert store.pending() == ()
    for resource in (store, history, kafka, current):
        resource.close()
