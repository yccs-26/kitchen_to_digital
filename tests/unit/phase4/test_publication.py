"""Atomic local root/output commit, immutable identity and file reopen."""

from dataclasses import replace
import sqlite3
import json

import pytest

from streaming.state.change_envelope import (
    OutputConflict, canonical, digest, snapshot_bytes,
)
from streaming.state.publication_store import PreparedCommand, PublicationStore
from test_change_envelope import fryer_pair, health_pair


def prepare(pair, version=0, before=None):
    previous, result, event, context, output = pair
    state = getattr(result, "snapshot", None) or result.state
    prior = snapshot_bytes(previous) if before is None else before
    return PreparedCommand(
        output.projection.axis_key, context.command_ref,
        context.generation, version, digest(prior), snapshot_bytes(state),
        canonical({"input": event, "evaluation_time":
                   output.change.data["evaluation_time"]
                   if output.change else state.last_evaluated_at}),
        result.decision.value, output,
    )


def stored(tmp_path, pair=None):
    pair = health_pair() if pair is None else pair
    command = prepare(pair)
    path = tmp_path / "publication.sqlite"
    store = PublicationStore(path)
    store.initialize(command.axis_key, 1, snapshot_bytes(pair[0]))
    return path, store, command


@pytest.mark.parametrize("pair_factory", [health_pair, fryer_pair])
def test_atomic_commit_file_reopen_and_exact_retry(tmp_path, pair_factory):
    path, store, command = stored(tmp_path, pair_factory())
    output_id = store.commit(command)
    state = store.state(command.axis_key)
    assert state.snapshot == command.after_snapshot
    assert state.revision == 1
    assert store.output(output_id) == command.output
    assert store.commit(command) == output_id
    assert store.state(command.axis_key) == state
    store.close()
    reopened = PublicationStore(path)
    assert reopened.state(command.axis_key) == state
    assert reopened.pending() == (output_id,)
    assert reopened.command_payload(command.axis_key, command.command_ref)[
        "input"
    ] == json.loads(command.input_payload)
    assert reopened.commit(command) == output_id
    reopened.close()


@pytest.mark.parametrize("stage", ["after_snapshot", "before_commit"])
def test_failure_rolls_back_snapshot_command_and_pending_together(tmp_path,
                                                                stage):
    path, store, command = stored(tmp_path)
    before = store.state(command.axis_key)

    def fail(actual):
        if actual == stage:
            raise RuntimeError("injected transaction failure")

    with pytest.raises(RuntimeError):
        store.commit(command, hook=fail)
    store.close()
    reopened = PublicationStore(path)
    assert reopened.state(command.axis_key) == before
    assert reopened.pending() == ()
    with pytest.raises(KeyError):
        reopened.command_payload(command.axis_key, command.command_ref)
    assert reopened.commit(command) == command.output.identity
    reopened.close()


def test_cas_loser_cannot_change_root_or_publish(tmp_path):
    path, first, command = stored(tmp_path)
    second = PublicationStore(path)
    output_id = first.commit(command)
    before = first.state(command.axis_key)
    loser = replace(command, command_ref="loser", output=None)
    with pytest.raises(OutputConflict, match="CAS"):
        second.commit(loser)
    assert second.state(command.axis_key) == before
    assert second.pending() == (output_id,)
    first.close()
    second.close()


def test_same_command_changed_input_is_conflict(tmp_path):
    _, store, command = stored(tmp_path)
    store.commit(command)
    with pytest.raises(OutputConflict, match="command"):
        store.commit(replace(command, input_payload=canonical({
            **json.loads(command.input_payload), "input": {"changed": 1},
        })))
    assert store.state(command.axis_key).revision == 1
    store.close()


def test_changed_policy_and_output_revision_cannot_rebind_root(tmp_path):
    pair = health_pair()
    _, store, command = stored(tmp_path, pair)
    store.commit(command)
    with pytest.raises(OutputConflict):
        store.commit(replace(command, command_ref="again", output=None))
    with pytest.raises(OutputConflict):
        replace(command, generation=2)
    store.close()


def test_snapshot_digest_mismatch_is_not_committed(tmp_path):
    _, store, command = stored(tmp_path)
    with pytest.raises(OutputConflict, match="snapshot"):
        replace(command, after_snapshot=b"wrong")
    assert store.pending() == ()
    store.close()


def test_unknown_attempt_persists_and_completion_needs_all_sinks(tmp_path):
    path, store, command = stored(tmp_path)
    oid = store.commit(command)
    with pytest.raises(OutputConflict):
        store.complete(oid)
    assert store.begin_attempt(oid, "history")
    store.close()
    store = PublicationStore(path)
    assert store.delivery(oid, "history")["status"] == "UNKNOWN"
    assert store.delivery(oid, "history")["attempts"] == 1
    assert store.begin_attempt(oid, "history")
    store.finish_attempt(oid, "history", "ACKED", receipt={"commit": "local"})
    assert len(store.attempts(oid)) == 2
    assert not store.begin_attempt(oid, "history")
    for sink in ("kafka", "current"):
        store.begin_attempt(oid, sink)
        store.finish_attempt(oid, sink, "ACKED")
    store.complete(oid)
    assert store.pending() == ()
    store.close()


def test_conflict_stays_pending_and_is_not_automatically_retried(tmp_path):
    _, store, command = stored(tmp_path)
    oid = store.commit(command)
    store.begin_attempt(oid, "history")
    store.finish_attempt(oid, "history", "CONFLICT", error="same ID changed")
    with pytest.raises(OutputConflict):
        store.begin_attempt(oid, "history")
    assert store.pending() == (oid,)
    store.close()


@pytest.mark.parametrize("target", ["roots", "outputs", "commands"])
def test_corrupt_persisted_data_is_rejected_not_initialized(tmp_path, target):
    path, store, command = stored(tmp_path)
    oid = store.commit(command)
    column = "snapshot" if target == "roots" else "payload"
    store.db.execute(f"UPDATE {target} SET {column}=?", (b"corrupt",))
    store.close()
    store = PublicationStore(path)
    with pytest.raises(ValueError, match="corrupt"):
        if target == "roots":
            store.state(command.axis_key)
        elif target == "outputs":
            store.output(oid)
        else:
            store.command_payload(command.axis_key, command.command_ref)
    store.close()


def test_unrelated_or_unsupported_database_is_not_migrated(tmp_path):
    for version in (0, 2):
        path = tmp_path / f"other-{version}.sqlite"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE unrelated (value INTEGER)")
            db.execute(f"PRAGMA user_version={version}")
        with pytest.raises(ValueError):
            PublicationStore(path)
        with sqlite3.connect(path) as db:
            count = db.execute("SELECT count(*) FROM unrelated").fetchone()
            assert count == (0,)


def test_changed_snapshot_without_publication_intent_is_forbidden(tmp_path):
    _, store, command = stored(tmp_path)
    with pytest.raises(OutputConflict, match="requires output"):
        store.commit(replace(command, output=None))
    assert store.state(command.axis_key).revision == 0
    assert store.pending() == ()
    store.close()


def test_late_ack_of_reclaimed_attempt_cannot_ack_new_attempt(tmp_path):
    _, store, command = stored(tmp_path)
    oid = store.commit(command)
    first = store.begin_attempt(oid, "kafka")
    second = store.begin_attempt(oid, "kafka")
    with pytest.raises(OutputConflict, match="matching"):
        store.finish_attempt(oid, "kafka", "ACKED", attempt=first)
    assert store.delivery(oid, "kafka")["status"] == "UNKNOWN"
    store.finish_attempt(oid, "kafka", "ACKED", attempt=second)
    assert store.attempts(oid)[0]["status"] == "UNKNOWN"
    assert store.attempts(oid)[1]["status"] == "ACKED"
    store.close()


def test_policy_rebinding_and_revision_gap_preserve_root(tmp_path):
    from test_change_envelope import health_output, OutputContext
    from test_health import BASE, EPS, POLICY, evaluate, observation
    from streaming.state.change_envelope import CurrentProjection, StateOutput
    from streaming.state.change_envelope import _sealed

    pair = health_pair()
    _, store, command = stored(tmp_path, pair)
    store.commit(command)
    before = pair[1].snapshot
    event = observation(BASE + EPS, "e2")
    result = evaluate(before, event, BASE + EPS)
    output = health_output(
        before, result, event,
        context=OutputContext("c2", 1, 1, 2, before.last_health_change_id),
        reference_time=BASE + EPS,
    )
    state_before = store.state(command.axis_key)
    for field in ("policy", "change_revision"):
        data = output.projection.data
        data.pop("content_sha256")
        if field == "policy":
            data[field]["binding_revision"] = "different"
        else:
            data[field] = 3
        bad = StateOutput(None, CurrentProjection(_sealed(data)))
        prepared = PreparedCommand(
            command.axis_key, "c2", 1, 1, digest(snapshot_bytes(before)),
            snapshot_bytes(result.snapshot),
            canonical({"input": event, "evaluation_time": BASE + EPS}),
            result.decision.value, bad,
        )
        with pytest.raises(OutputConflict):
            store.commit(prepared)
        assert store.state(command.axis_key) == state_before
    assert POLICY == before.policy
    store.close()
