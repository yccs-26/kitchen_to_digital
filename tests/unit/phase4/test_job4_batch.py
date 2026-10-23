"""Batch manifest, source cursor and completion policy boundaries."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.change_envelope import OutputConflict
from streaming.state.job4_batch import BatchExecutionError
from streaming.state.job4_contract import BatchIdentity
from streaming.state.job4_input import prepare_input
from streaming.state.job4_store import Job4Store
from streaming.state.publication_store import PublicationStore
from streaming.state.writer_fencing import FencedPublicationStore
from tests.fixtures.phase4.application_fixture import BASE
from tests.fixtures.phase4.job4_fixture import record, resources, writers


BATCH = BatchIdentity('query', 'checkpoint-v1', 1)
DEVICE = ('store-001', 'hood-001')


@pytest.fixture
def local(tmp_path):
    value = resources(tmp_path, initialize=True)
    yield value
    value[0].close()


def run(local, app, batch=BATCH, records=None, **kwargs):
    return app.apply_inputs(batch, (record(),) if records is None else records,
                            writers=writers(local), **kwargs)


def test_input_completion_is_not_publication_or_checkpoint_success(local):
    local, app = local
    result = run(local, app)
    assert result.input_committed
    assert not result.publication_complete and not result.checkpoint_allowed
    with pytest.raises(BatchExecutionError) as error:
        app.complete(result.batch_id, writers=writers(local))
    assert error.value.category == 'RETRYABLE'


def test_identical_offsets_dedup_and_frozen_evaluation_on_replay(local):
    local, app = local
    first = run(local, app, records=(record(), record()))
    roots = tuple(local.store.state((*DEVICE, axis))
                  for axis in ('OPERATIONAL', 'HEALTH'))
    later = replace(record(), evaluation_time=BASE + timedelta(days=1))
    second = run(local, app, records=(later,))
    assert first.command_refs == second.command_refs
    assert len(second.command_refs) == 1
    assert roots == tuple(local.store.state(root.axis_key) for root in roots)
    frozen = local.store.input(first.command_refs[0])
    assert frozen.evaluation_time == BASE


def test_same_batch_changed_order_or_payload_is_conflict(local):
    local, app = local
    run(local, app, records=(record(), record(1, 11)))
    with pytest.raises(BatchExecutionError):
        run(local, app, records=(record(1, 11), record()))
    with pytest.raises(BatchExecutionError) as error:
        run(local, app, records=(record(value=900), record(1, 11)))
    assert error.value.category == 'CONFLICT_OR_INTEGRITY'


def test_cross_batch_unseen_old_offset_and_partition_change_rejected(local):
    local, app = local
    run(local, app)
    second = replace(BATCH, batch_id=2)
    with pytest.raises(BatchExecutionError):
        run(local, app, second, records=(record(1, 9),))
    sample = record(1, 11)
    with pytest.raises(BatchExecutionError):
        run(local, app, second, records=(replace(
            sample, position=replace(sample.position, partition=1),
        ),))
    assert local.store.db.execute(
        'SELECT offset FROM job4_cursors WHERE device=?',
        ('["store-001","hood-001"]',),
    ).fetchone()[0] == 10


def test_stale_epoch_before_manifest_cannot_apply(local):
    local, app = local
    old = writers(local, 'old')
    writers(local, 'replacement')
    with pytest.raises(BatchExecutionError) as error:
        app.apply_inputs(BATCH, (record(),), writers=old)
    assert error.value.category == 'FENCED'
    assert error.value.batch_id is None
    assert local.store.db.execute(
        'SELECT COUNT(*) FROM job4_batches',
    ).fetchone()[0] == 0


def test_unsupported_equipment_and_wrong_store_are_distinct(local):
    local, app = local
    # Real unsupported input reaches the binding guard, not a Fryer fallback.
    with pytest.raises(BatchExecutionError) as error:
        run(local, app, records=(record(equipment_id='unknown'),))
    assert error.value.category == 'UNSUPPORTED'


def test_admission_hook_rollback_keeps_cursor_and_admission_atomic(
    local,
):
    local, app = local
    item = prepare_input(record(), source_epoch=local.store.source_epoch,
                         registry=app.registry)
    batch_id = local.store.register(BATCH, (item,))
    token = writers(local)[DEVICE]
    original = local.store._bind_ingress

    def fail():
        original()
        raise RuntimeError('crash before admission commit')

    local.store._bind_ingress = fail
    with pytest.raises(BatchExecutionError):
        app.apply_inputs(BATCH, (record(),), writers={DEVICE: token})
    assert local.store.db.execute(
        'SELECT COUNT(*) FROM admissions',
    ).fetchone()[0] == 0
    assert local.store.db.execute(
        'SELECT COUNT(*) FROM job4_cursors',
    ).fetchone()[0] == 0
    assert not local.store.evidence(batch_id)[0]


@pytest.mark.parametrize('factory', [PublicationStore, FencedPublicationStore])
def test_existing_databases_are_not_migrated(tmp_path, factory):
    path = tmp_path / 'old.sqlite'
    factory(path).close()
    before = path.read_bytes()
    with pytest.raises(FileExistsError):
        Job4Store.create(path, source_epoch='source')
    with pytest.raises(ValueError):
        Job4Store(path)
    assert path.read_bytes() == before


@pytest.mark.parametrize('updates', [
    {'unit': 'wrong'}, {'metric_name': 'wrong'}, {'schema_version': '2'},
    {'source': 'not-authorized'},
])
def test_registry_header_errors_never_extend_freshness(local, updates):
    local, app = local
    before = local.app.load_health('hood-001')
    with pytest.raises(BatchExecutionError) as error:
        run(local, app, records=(record(updates=updates),))
    assert error.value.category == 'CONTRACT'
    assert local.app.load_health('hood-001') == before
    assert not local.store.evidence(error.value.batch_id)[0]


def test_policy_change_cannot_rewrite_a_partial_command(local):
    local, app = local

    def fail(stage):
        if stage == 'before_health_commit':
            raise RuntimeError('injected partial failure')

    with pytest.raises(BatchExecutionError):
        run(local, app, hook=fail)
    local.app.hood_policies['hood-001'] = replace(
        local.app.hood_policies['hood-001'], enter_rpm=900,
    )
    with pytest.raises(BatchExecutionError) as error:
        run(local, app)
    assert error.value.category == 'CONFLICT_OR_INTEGRITY'
    assert local.store.state((*DEVICE, 'HEALTH')).revision == 0


@pytest.mark.parametrize('table,column', [
    ('job4_batches', 'manifest'), ('job4_inputs', 'record'),
])
def test_catalog_corruption_prevents_completion(local, table, column):
    local, app = local
    result = run(local, app)
    local.store.db.execute(f"UPDATE {table} SET {column}=X'00'")
    with pytest.raises(BatchExecutionError) as error:
        app.complete(result.batch_id, writers=writers(local))
    assert error.value.category == 'CONFLICT_OR_INTEGRITY'


def test_explicit_empty_batch_completes_without_state_or_sink_changes(local):
    local, app = local
    result = app.execute(BATCH, (), writers={})
    assert result.checkpoint_allowed and result.command_refs == ()
    assert not local.store.pending()
    assert local.store.state((*DEVICE, 'HEALTH')).revision == 0


def test_partial_completion_never_calls_partial_commands_successful(local):
    local, app = local

    def fail(stage):
        if stage == 'before_health_commit':
            raise RuntimeError('injected partial')

    with pytest.raises(BatchExecutionError) as first:
        run(local, app, hook=fail)
    with pytest.raises(BatchExecutionError) as error:
        app.complete(first.value.batch_id, writers=writers(local))
    assert error.value.completed_refs == ()
