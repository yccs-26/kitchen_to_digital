"""Job 4 batches through existing codecs, cores and Fake sinks."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.change_envelope import read_json
from streaming.state.health import timer_token
from streaming.state.job4_batch import BatchExecutionError
from streaming.state.job4_contract import BatchIdentity
from streaming.state.job4_input import TimerInput
from tests.fixtures.phase4.application_fixture import BASE
from tests.fixtures.phase4.job4_fixture import record, resources, writers


DEVICE = ('store-001', 'hood-001')
BATCH = BatchIdentity('query', 'checkpoint-v1', 1)


def execute(local, app, batch=BATCH, records=None, *, tokens=None,
            hook=lambda s: None):
    return app.execute(
        batch, (record(),) if records is None else records,
        writers=tokens if tokens is not None else writers(local),
        coordinator=local.coordinator, owner='dispatcher',
        clock=lambda: BASE + timedelta(seconds=30),
        lease=timedelta(seconds=3), hook=hook,
    )


def test_multi_device_lineage_replay_and_pending_convergence(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        sequence = (record(), record(0, 11, 'fryer-001', 180), record(1, 12))
        result = execute(local, app, records=sequence)
        assert result.checkpoint_allowed
        assert local.store.pending() == ()
        fryer = local.app.load_operational('fryer-001').state
        assert fryer.observation.operational_state.value == 'IDLE'
        assert local.app.load_health('fryer-001').last_seen == BASE
        roots = tuple(local.store.state((store, equipment, axis))
                      for store, equipment
                      in (DEVICE, ('store-001', 'fryer-001'))
                      for axis in ('OPERATIONAL', 'HEALTH'))
        messages = local.kafka.messages()
        replay = execute(local, app, records=sequence)
        again = execute(local, app, replace(BATCH, batch_id=2), sequence)
        assert replay.command_refs == again.command_refs == result.command_refs
        assert roots == tuple(local.store.state(root.axis_key)
                              for root in roots)
        assert local.kafka.messages() == messages
        assert local.store.manifest(result.batch_id)['ranges'] == [
            ['kitchen.sensor.validated', 0, 10, 13],
        ]
        for ref in result.command_refs:
            item = local.store.input(ref)
            request = local.store.db.execute(
                'SELECT request FROM admissions WHERE device=? AND ref=?',
                (f'["{item.device[0]}","{item.device[1]}"]', ref),
            ).fetchone()[0]
            assert read_json(bytes(request))['job4_ingress'] == read_json(
                item.record,
            )
    finally:
        local.close()


def test_partial_failure_progress_and_same_input_recovery(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        def fail(stage):
            if stage == 'before_health_commit':
                raise RuntimeError('injected failure')

        with pytest.raises(BatchExecutionError) as error:
            execute(local, app, hook=fail)
        failure = error.value
        assert failure.completed_refs == ()
        assert local.store.command_status(
            DEVICE, failure.failed_ref,
        ) == 'PARTIAL'
        assert not local.store.evidence(failure.batch_id)[0]
        outcome = execute(local, app)
        assert outcome.checkpoint_allowed
        assert all(local.store.state((*DEVICE, axis)).revision == 1
                   for axis in ('OPERATIONAL', 'HEALTH'))
    finally:
        local.close()


def test_later_error_preserves_successful_record_and_failed_ref(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        # Unknown schema fails preflight without any state mutation.
        sample = record(1, 11)
        bad = replace(sample, value=b'bad')
        with pytest.raises(BatchExecutionError) as error:
            execute(local, app, records=(record(), bad))
        assert error.value.batch_id is None
        assert local.store.state((*DEVICE, 'HEALTH')).revision == 0
        count = [0]

        def fail(stage):
            if stage == 'before_command':
                count[0] += 1
                if count[0] == 2:
                    raise RuntimeError('second command failed')

        with pytest.raises(BatchExecutionError) as error:
            execute(local, app, records=(record(), sample), hook=fail)
        assert len(error.value.completed_refs) == 1
        assert error.value.failed_ref not in error.value.completed_refs
        outcome = execute(local, app, records=(record(), sample))
        assert outcome.checkpoint_allowed
    finally:
        local.close()


@pytest.mark.parametrize('observation_first', [False, True])
def test_timer_race_and_snapshot_restore_keep_recorded_order(tmp_path,
                                                           observation_first):
    local, app = resources(tmp_path, initialize=True)
    execute(local, app)
    token = timer_token(local.app.load_health('hood-001'))
    local.close()
    local, app = resources(tmp_path)
    try:
        tick = TimerInput('due', DEVICE, token, BASE + timedelta(seconds=10))
        observation = record(10, 11)
        sequence = ((observation, tick) if observation_first
                    else (tick, observation))
        result = execute(local, app, replace(BATCH, batch_id=2), sequence)
        assert result.checkpoint_allowed
        health_changes = [item.data['body']['current']
                          for item in local.history.rows()
                          if item.axis_key[2] == 'HEALTH']
        assert health_changes == (['HEALTHY'] if observation_first else
                                  ['HEALTHY', 'STALE', 'HEALTHY'])
        # Replaying the exact mixed command sequence cannot invent changes.
        ids = tuple(item.identity for item in local.history.rows())
        execute(local, app, replace(BATCH, batch_id=2), sequence)
        assert tuple(item.identity for item in local.history.rows()) == ids
    finally:
        local.close()


def test_lost_ack_and_completion_failure_reconcile_without_reemit(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        original = local.coordinator.kafka

        class LostAck:
            def publish(self, change):
                original.publish(change)
                raise ConnectionError('injected lost ACK')

        local.coordinator.kafka = LostAck()
        with pytest.raises(BatchExecutionError):
            execute(local, app)
        local.coordinator.kafka = original

        def fail(stage):
            if stage == 'before_batch_complete':
                raise RuntimeError('completion record failure')

        with pytest.raises(BatchExecutionError):
            execute(local, app, hook=fail)
        assert len(local.kafka.messages()) == 2
        assert len(local.history.rows()) == 1
        assert execute(local, app).checkpoint_allowed
        assert len(local.kafka.messages()) == 2
    finally:
        local.close()


def test_corrupt_snapshot_is_rejected_not_reinitialized(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        local.store.db.execute("UPDATE roots SET snapshot=X'00' "
                               "WHERE axis LIKE '%HEALTH%'")
        with pytest.raises(BatchExecutionError) as error:
            execute(local, app)
        assert error.value.category == 'CONFLICT_OR_INTEGRITY'
        assert not local.store.pending()
    finally:
        local.close()


def test_completed_marker_does_not_regress_on_input_only_replay(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        result = execute(local, app)
        app.apply_inputs(BATCH, (record(),), writers=writers(local))
        assert local.store.db.execute('SELECT complete FROM job4_batches '
                                      'WHERE id=?',
                                      (result.batch_id,)).fetchone()[0] == 1
    finally:
        local.close()


def test_real_hood_stop_rotate_stop_flow_through_one_explicit_batch(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        values = (0, 0, 0, 800, 800, 800, 800, 0, 0, 0)
        records = tuple(record(second, 20 + second, value=value)
                        for second, value in enumerate(values))
        result = execute(local, app, records=records)
        assert result.checkpoint_allowed
        changes = [change for change in local.history.rows()
                   if change.axis_key[2] == 'OPERATIONAL']
        assert [change.data['body']['current'] for change in changes] == (
            ['ROTATING', 'STOPPED']
        )
        assert local.current.get((*DEVICE, 'OPERATIONAL')).data[
            'current'
        ]['state'] == 'STOPPED'
    finally:
        local.close()


def test_explicit_timer_deadline_repeat_and_recovery_use_native_ids(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    try:
        execute(local, app)
        token = timer_token(local.app.load_health('hood-001'))
        operational = local.app.load_operational('hood-001')
        early = TimerInput('early', DEVICE, token, BASE + timedelta(
            seconds=10, microseconds=-1,
        ))
        execute(local, app, replace(BATCH, batch_id=2), (early,))
        health = local.app.load_health('hood-001')
        assert health.health_status.value == 'HEALTHY'
        due = replace(early, timer_ref='due', evaluation_time=BASE + timedelta(
            seconds=10,
        ))
        execute(local, app, replace(BATCH, batch_id=3), (due,))
        assert local.app.load_health('hood-001').health_status.value == 'STALE'
        assert local.app.load_operational('hood-001') == operational
        stale_id = local.app.load_health('hood-001').last_health_change_id
        repeated = replace(due, timer_ref='duplicate-delivery')
        execute(local, app, replace(BATCH, batch_id=4), (repeated,))
        health = local.app.load_health('hood-001')
        assert health.last_health_change_id == stale_id
        execute(local, app, replace(BATCH, batch_id=5), (record(11, 20),))
        health = local.app.load_health('hood-001')
        assert health.health_status.value == 'HEALTHY'
        assert [change.data['body']['current']
                for change in local.history.rows()
                if change.axis_key[2] == 'HEALTH'] == (
                    ['HEALTHY', 'STALE', 'HEALTHY']
                )
    finally:
        local.close()


def test_registered_partial_recovers_without_kafka_or_registry(tmp_path):
    local, app = resources(tmp_path, initialize=True)
    batch_id = None
    try:
        def fail(stage):
            if stage == 'before_health_commit':
                raise RuntimeError('injected partial')

        with pytest.raises(BatchExecutionError) as error:
            execute(local, app, hook=fail)
        batch_id = error.value.batch_id
    finally:
        local.close()
    local, app = resources(tmp_path)
    try:
        class UnavailableRegistry:
            def get_schema(self, schema_id):
                raise AssertionError('recovery must not access Registry')

        app.registry = UnavailableRegistry()
        result = app.recover(
            batch_id, writers=writers(local, 'recovery'),
            coordinator=local.coordinator, owner='dispatcher',
            clock=lambda: BASE + timedelta(seconds=30),
            lease=timedelta(seconds=3),
        )
        assert result.checkpoint_allowed
        assert local.store.pending() == ()
        assert local.store.state((*DEVICE, 'OPERATIONAL')).revision == 1
    finally:
        local.close()


def test_registered_tail_cannot_be_overtaken_after_prefix_command_completed(
    tmp_path,
):
    local, app = resources(tmp_path, initialize=True)
    try:
        def fail(stage):
            if stage == 'after_command':
                raise RuntimeError('prefix committed, tail not admitted')

        with pytest.raises(BatchExecutionError) as error:
            execute(local, app, records=(record(), record(1, 11)), hook=fail)
        original_batch = error.value.batch_id
        assert len(error.value.completed_refs) == 1
        with pytest.raises(BatchExecutionError) as blocked:
            execute(local, app, replace(BATCH, batch_id=2), (record(2, 12),))
        assert blocked.value.category == 'RETRYABLE'
        assert local.store.state((*DEVICE, 'HEALTH')).revision == 1
        other = BatchIdentity('other-query', 'checkpoint-v1', 1)
        assert execute(local, app, other, (
            record(0, 13, 'fryer-001', 180),
        )).checkpoint_allowed
        result = app.recover(
            original_batch, writers=writers(local),
            coordinator=local.coordinator, owner='dispatcher',
            clock=lambda: BASE + timedelta(seconds=30),
            lease=timedelta(seconds=3),
        )
        assert result.checkpoint_allowed
        assert execute(local, app, replace(BATCH, batch_id=2), (
            record(2, 12),
        )).checkpoint_allowed
        assert local.app.load_health('hood-001').last_seen == (
            BASE + timedelta(seconds=2)
        )
    finally:
        local.close()
