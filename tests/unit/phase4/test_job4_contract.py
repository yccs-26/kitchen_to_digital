"""Pure ingress/order/evidence checks without installing or executing Spark."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.job4_contract import (
    BatchIdentity, CommandEvidence, InputPosition, ValidatedCommand,
    checkpoint_ready, validate_input_order,
)
from tests.fixtures.phase4.application_fixture import BASE, numeric


def command(second=0, offset=10, **kwargs):
    return ValidatedCommand.from_event(
        numeric(second), command_ref=f'command-{offset}',
        position=InputPosition('kitchen.sensor.validated', 0, offset,
                               b'hood-001', 17),
        reference_time=BASE + timedelta(seconds=second), **kwargs,
    )


def test_ingress_normalizes_and_copies_without_native_id_changes():
    event = numeric()
    item = ValidatedCommand.from_event(
        event, command_ref='recorded',
        position=InputPosition('kitchen.sensor.validated', 0, 10,
                               b'hood-001', 17), reference_time=BASE,
    )
    event['metric_value'] = 1
    decoded = item.application_arguments()
    assert decoded['event']['metric_value'] == 800.0
    decoded['event']['metric_value'] = 2
    assert item.application_arguments()['event']['metric_value'] == 800.0
    assert item.device == ('store-001', 'hood-001')
    assert item.command_ref == 'recorded'


@pytest.mark.parametrize('field,value', [
    ('partition', True), ('partition', -1), ('offset', False),
    ('offset', -1), ('writer_schema_id', True), ('writer_schema_id', -1),
    ('key', 'hood-001'), ('topic', 'raw'),
])
def test_bad_lineage_is_rejected(field, value):
    position = dict(topic='kitchen.sensor.validated', partition=0, offset=10,
                    key=b'hood-001', writer_schema_id=17)
    position[field] = value
    with pytest.raises(ValueError):
        InputPosition(**position)


def test_key_mismatch_and_naive_reference_rejected():
    item = command()
    with pytest.raises(ValueError):
        replace(item, position=replace(item.position, key=b'fryer-001'))
    with pytest.raises(ValueError):
        replace(item, reference_time=BASE.replace(tzinfo=None))


def test_offsets_keep_caller_order_and_allow_interleaved_device_gaps():
    first, second = command(), command(1, 15)
    assert validate_input_order((first, second)) == (first, second)
    with pytest.raises(ValueError):
        validate_input_order((second, first))
    with pytest.raises(ValueError):
        validate_input_order((first, first))


def test_partition_migration_requires_explicit_external_cutover():
    first, second = command(), command(1, 15)
    second = replace(second, position=replace(second.position, partition=1))
    with pytest.raises(ValueError):
        validate_input_order((first, second))


@pytest.mark.parametrize('batch_id', [True, -1, '1'])
def test_invalid_batch_identity(batch_id):
    with pytest.raises(ValueError):
        BatchIdentity('query', 'checkpoint-epoch', batch_id)


def test_batch_identity_is_not_domain_command_identity():
    first = BatchIdentity('query', 'epoch', 1)
    assert first != BatchIdentity('query', 'epoch', 2)
    item = command()
    assert item.application_arguments()['command_ref'] == item.command_ref


@pytest.mark.parametrize('status', ['ADMITTED', 'PARTIAL'])
def test_empty_pending_is_not_completed_command(status):
    evidence = CommandEvidence(('store-001', 'hood-001'), 'one', status, ())
    assert not checkpoint_ready((evidence,), {})


@pytest.mark.parametrize('status', [
    'PENDING', 'UNKNOWN', 'FAILED', 'CONFLICT',
])
def test_unconfirmed_or_conflict_sink_blocks_checkpoint(status):
    item = CommandEvidence(('store-001', 'hood-001'), 'one', 'COMPLETE',
                           ('id',))
    deliveries = {'id': {'history': 'ACKED', 'kafka': status,
                         'current': 'ACKED'}}
    assert not checkpoint_ready((item,), deliveries)


def test_all_required_evidence_and_no_fake_change_needed():
    no_output = CommandEvidence(('store-001', 'hood-001'), 'noop',
                                 'COMPLETE', ())
    assert checkpoint_ready((no_output,), {})
    change = replace(no_output, command_ref='change', output_ids=('id',))
    assert not checkpoint_ready((change,), {})
    assert not checkpoint_ready((change,), {'id': {'history': 'ACKED'}})
    assert checkpoint_ready((change,), {'id': {
        'history': 'ACKED', 'kafka': 'ACKED', 'current': 'SUPERSEDED',
    }})
    current_only = {'id': {'history': 'NOT_REQUIRED', 'kafka': 'NOT_REQUIRED',
                           'current': 'ALREADY_APPLIED'}}
    assert checkpoint_ready((change,), current_only)


def test_duplicate_command_evidence_and_unknown_states_fail_closed():
    item = CommandEvidence(('store-001', 'hood-001'), 'one', 'COMPLETE', ())
    with pytest.raises(ValueError):
        checkpoint_ready((item, item), {})
    with pytest.raises(ValueError):
        replace(item, status='SKIPPED')
    with pytest.raises(ValueError):
        replace(item, output_ids=('id', 'id'))


@pytest.mark.parametrize('sink,status', [
    ('current', 'NOT_REQUIRED'), ('history', 'SUPERSEDED'),
    ('kafka', 'SUPERSEDED'), ('kafka', 'ALREADY_APPLIED'),
    ('history', 'NOT_REQUIRED'),
])
def test_sink_roles_cannot_be_satisfied_by_wrong_terminal_status(sink, status):
    item = CommandEvidence(('store-001', 'hood-001'), 'one', 'COMPLETE',
                           ('id',))
    receipt = dict(history='ACKED', kafka='ACKED', current='ACKED')
    receipt[sink] = status
    assert not checkpoint_ready((item,), {'id': receipt})


def test_schema_id_has_wire_bounds():
    with pytest.raises(ValueError):
        InputPosition('kitchen.sensor.validated', 0, 0, b'hood-001', 2 ** 32)


def test_transport_order_does_not_sort_event_time():
    first = command(1, 10)
    second = command(0, 15)
    assert validate_input_order((first, second)) == (first, second)
