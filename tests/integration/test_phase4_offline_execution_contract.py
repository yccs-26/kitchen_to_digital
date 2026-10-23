"""Fake local contract bridge; no Spark query or checkpoint writes."""

from datetime import timedelta

import pytest

from streaming.state.health_registry import HealthRegistryError
from streaming.state.change_envelope import canonical
from streaming.state.job4_contract import (
    BatchIdentity, CommandEvidence, InputPosition, ValidatedCommand,
    checkpoint_ready, validate_input_order,
)
from streaming.state.sink_protocols import RetryableSinkError
from streaming.state.publication_store import decode_output
from streaming.state.writer_fencing import CommandBusy
from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle


DEVICE = ('store-001', 'hood-001')
AXES = ('OPERATIONAL', 'HEALTH')


def command(second=0, offset=10):
    return ValidatedCommand.from_event(
        numeric(second), command_ref=f'input-{offset}',
        position=InputPosition('kitchen.sensor.validated', 0, offset,
                               b'hood-001', 17),
        reference_time=BASE + timedelta(seconds=second),
    )


def completion(resources, refs):
    """Test oracle only: caller manifest and one consistent local DB view."""
    commands, deliveries = [], {}
    with resources.store.transaction():
        for ref in refs:
            output_ids = []
            for axis in AXES:
                try:
                    saved = resources.store.command_payload((*DEVICE, axis),
                                                            ref)
                except KeyError:
                    continue
                if saved['output'] is not None:
                    output_ids.append(decode_output(
                        canonical(saved['output']),
                    ).identity)
            commands.append(CommandEvidence(
                DEVICE, ref, resources.store.command_status(DEVICE, ref),
                tuple(output_ids),
            ))
            for output_id in output_ids:
                deliveries[output_id] = {
                    sink: resources.store.delivery(output_id, sink)['status']
                    for sink in ('history', 'kafka', 'current')
                }
    return checkpoint_ready(tuple(commands), deliveries)


def reconcile(resources):
    return resources.coordinator.reconcile_claimed(
        owner='dispatcher', clock=lambda: BASE + timedelta(seconds=20),
        lease=timedelta(seconds=3),
    )


def test_batch_retry_reopen_preserves_snapshots_and_native_outputs(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    inputs = validate_input_order((command(), command(1, 15)))
    first_batch = BatchIdentity('query', 'checkpoint-epoch', 1)
    token = resources.store.acquire_writer(DEVICE, 'writer')
    results = [app.process_validated(token, **item.application_arguments())
               for item in inputs]
    expected = tuple(resources.store.state((*DEVICE, axis)) for axis in AXES)
    ids = tuple(result.output_id for group in results for result in group)
    refs = tuple(item.command_ref for item in inputs)
    assert not completion(resources, refs)
    resources.close()
    resources, app = fenced_bundle(tmp_path)
    try:
        replay_batch = BatchIdentity('query', 'checkpoint-epoch', 2)
        assert first_batch != replay_batch
        token = resources.store.acquire_writer(DEVICE, 'replacement')
        results = [app.process_validated(token, **item.application_arguments())
                   for item in inputs]
        assert ids == tuple(result.output_id for group in results
                            for result in group)
        assert expected == tuple(resources.store.state((*DEVICE, axis))
                                 for axis in AXES)
        reconcile(resources)
        assert completion(resources, refs)
    finally:
        resources.close()


def test_partial_repeated_failure_and_takeover_requires_complete_evidence(
    tmp_path,
):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')
        item = command()

        def stop(stage):
            if stage == 'before_health_commit':
                raise RuntimeError('injected temporary storage failure')

        for _ in range(2):
            with pytest.raises(RuntimeError):
                app.process_validated(token, **item.application_arguments(),
                                       hook=stop)
            assert not completion(resources, (item.command_ref,))
        with pytest.raises(CommandBusy):
            app.process_validated(
                token, **command(1, 15).application_arguments(),
            )
        token = resources.store.acquire_writer(DEVICE, 'replacement')
        app.process_validated(token, **item.application_arguments())
        assert not completion(resources, (item.command_ref,))
        reconcile(resources)
        assert completion(resources, (item.command_ref,))
    finally:
        resources.close()


def test_network_ack_loss_blocks_progress_until_duplicate_delivery_reconciles(
    tmp_path,
):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')
        item = command()
        app.process_validated(token, **item.application_arguments())
        original = resources.coordinator.kafka

        class LostAck:
            def publish(self, change):
                original.publish(change)
                raise RetryableSinkError('injected ACK loss')

        resources.coordinator.kafka = LostAck()
        with pytest.raises(RetryableSinkError):
            reconcile(resources)
        assert not completion(resources, (item.command_ref,))
        resources.coordinator.kafka = original
        reconcile(resources)
        assert completion(resources, (item.command_ref,))
        assert len(resources.kafka.messages()) == 2
        assert len(resources.history.rows()) == 1
    finally:
        resources.close()


def test_contract_error_requires_manual_review_even_when_pending_is_empty(
    tmp_path,
):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')
        event = numeric()
        event['unit'] = 'wrong'
        item = ValidatedCommand.from_event(
            event, command_ref='invalid', position=command().position,
            reference_time=BASE,
        )
        with pytest.raises(HealthRegistryError):
            app.process_validated(token, **item.application_arguments())
        assert resources.store.pending() == ()
        assert not completion(resources, ('invalid',))
        with pytest.raises(CommandBusy):
            app.process_validated(
                token, **command(1, 15).application_arguments(),
            )
    finally:
        resources.close()


def test_explicit_timer_and_observation_stay_separate_commands(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')
        item = command()
        app.process_validated(token, **item.application_arguments())
        timer = app.timers.plan('hood-001', evaluation_time=BASE).token
        old_operational = resources.app.load_operational('hood-001')
        tick = app.evaluate_timer(
            token, 'hood-001', timer, command_ref='tick',
            reference_time=BASE + timedelta(seconds=10),
        )
        assert tick.decision == 'TRANSITION'
        assert resources.app.load_operational('hood-001') == old_operational
        assert not completion(resources, (item.command_ref, 'tick'))
        reconcile(resources)
        assert completion(resources, (item.command_ref, 'tick'))
        assert resources.app.load_health('hood-001').health_status.value == (
            'STALE'
        )
    finally:
        resources.close()


def test_native_duplicate_decision_completes_without_new_output(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')
        item = command()
        app.process_validated(token, **item.application_arguments())
        reconcile(resources)
        old_health = resources.app.load_health('hood-001')
        result = app.process_validated(
            token, item.application_arguments()['event'],
            command_ref='duplicate', reference_time=BASE,
        )
        assert all(outcome.decision == 'DUPLICATE' for outcome in result)
        assert all(outcome.output_id is None for outcome in result)
        assert resources.app.load_health('hood-001') == old_health
        assert completion(resources, (item.command_ref, 'duplicate'))
        assert len(resources.history.rows()) == 1
    finally:
        resources.close()
