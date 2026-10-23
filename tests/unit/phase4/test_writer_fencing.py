"""Local fencing, admission and dispatch leases; no external services."""

from dataclasses import replace

import pytest

from streaming.state.change_envelope import OutputConflict, canonical, digest
from streaming.state.publication_store import PreparedCommand, PublicationStore
from streaming.state.writer_fencing import (
    FencedPublicationStore, StaleWriter, CommandBusy, RevisionConflict,
)
from tests.fixtures.phase4.application_fixture import BASE


DEVICE = ('store-001', 'hood-001')
AXIS = (*DEVICE, 'HEALTH')


def prepared(ref='one', rev=0):
    return PreparedCommand(
        AXIS, ref, 1, rev, digest(b'initial'), b'initial',
        canonical({'input': {}, 'evaluation_time': BASE}), 'NO_CHANGE',
    )


def run(store, token, ref='one', command=None):
    return store.run_command(
        token, ref, canonical({'input': ref}), ('HEALTH',),
        lambda: store.commit(command or prepared(ref)),
    )


def test_reject_legacy_database(tmp_path):
    path = tmp_path / 'old.db'
    PublicationStore(path).close()
    before = path.read_bytes()
    with pytest.raises(ValueError):
        FencedPublicationStore(path)
    assert path.read_bytes() == before


def test_stale_writer_even_for_cached_replay(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        first = store.acquire_writer(DEVICE, 'first')
        run(store, first)
        second = store.acquire_writer(DEVICE, 'second')
        with pytest.raises(StaleWriter):
            run(store, first)
        run(store, second)
        assert store.state(AXIS).revision == 1


def test_same_revision_competition_is_retryable(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        token = store.acquire_writer(DEVICE, 'one')
        run(store, token)
        with pytest.raises(RevisionConflict):
            run(store, token, 'two')
        assert store.state(AXIS).revision == 1
        run(store, token, 'two', prepared('two', 1))
        assert store.state(AXIS).revision == 2


def test_command_payload_conflict(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        token = store.acquire_writer(DEVICE, 'one')
        run(store, token)
        with pytest.raises(OutputConflict):
            store.run_command(token, 'one', b'{}', ('HEALTH',), lambda: None)


def test_partial_gate_survives_takeover_and_blocks_overtaking(tmp_path):
    path = tmp_path / 'new.db'
    with FencedPublicationStore(path) as store:
        store.initialize(AXIS, 1, b'initial')
        store.initialize((*DEVICE, 'OPERATIONAL'), 1, b'initial')
        token = store.acquire_writer(DEVICE, 'one')
        with pytest.raises(RuntimeError):
            store.run_command(token, 'one', b'{}',
                              ('HEALTH', 'OPERATIONAL'),
                              lambda: (store.commit(prepared()),
                                       (_ for _ in ()).throw(RuntimeError())))
        assert store.command_status(DEVICE, 'one') == 'PARTIAL'
    with FencedPublicationStore(path) as store:
        token = store.acquire_writer(DEVICE, 'two')
        with pytest.raises(CommandBusy):
            run(store, token, 'two')
        other = PreparedCommand(
            (*DEVICE, 'OPERATIONAL'), 'one', 1, 0, digest(b'initial'),
            b'initial', canonical({'input': {}, 'evaluation_time': BASE}),
            'NO_CHANGE',
        )
        store.run_command(token, 'one', b'{}', ('HEALTH', 'OPERATIONAL'),
                          lambda: store.commit(other))
        assert store.command_status(DEVICE, 'one') == 'COMPLETE'


def test_no_context_cannot_write(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        with pytest.raises(StaleWriter):
            store.commit(prepared())


def test_takeover_during_calculation_fences_commit(tmp_path):
    path = tmp_path / 'new.db'
    with FencedPublicationStore(path) as first, FencedPublicationStore(
        path,
    ) as second:
        first.initialize(AXIS, 1, b'initial')
        token = first.acquire_writer(DEVICE, 'first')

        def operation():
            second.acquire_writer(DEVICE, 'second')
            first.commit(prepared())

        with pytest.raises(StaleWriter):
            first.run_command(token, 'one', b'{}', ('HEALTH',), operation)
        assert first.state(AXIS).revision == 0


def test_owner_aba_does_not_revalidate_old_epoch(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        old = store.acquire_writer(DEVICE, 'A')
        middle = store.acquire_writer(DEVICE, 'B')
        new = store.acquire_writer(DEVICE, 'A')
        assert (old.epoch, middle.epoch, new.epoch) == (1, 2, 3)
        for token in (old, middle):
            with pytest.raises(StaleWriter):
                run(store, token)
        run(store, new)


def test_caught_inner_failure_rolls_back_savepoint(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        token = store.acquire_writer(DEVICE, 'one')

        def fail(stage):
            if stage == 'after_snapshot':
                raise RuntimeError()

        def operation():
            with store.transaction():
                with pytest.raises(RuntimeError):
                    store.commit(prepared(), hook=fail)

        store.run_command(token, 'one', b'{}', ('HEALTH',), operation)
        assert store.state(AXIS).revision == 0
        with pytest.raises(KeyError):
            store.command_payload(AXIS, 'one')
        assert store.command_status(DEVICE, 'one') == 'ADMITTED'


def test_health_first_partial_recovers_without_changing_domain_generation(
    tmp_path,
):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        store.initialize((*DEVICE, 'OPERATIONAL'), 1, b'initial')
        old = store.acquire_writer(DEVICE, 'old')
        store.run_command(old, 'one', b'{}', ('OPERATIONAL', 'HEALTH'),
                          lambda: store.commit(prepared()))
        assert store.command_status(DEVICE, 'one') == 'PARTIAL'
        token = store.acquire_writer(DEVICE, 'new')
        command = prepared()
        command = replace(command, axis_key=(*DEVICE, 'OPERATIONAL'))
        store.run_command(token, 'one', b'{}', ('OPERATIONAL', 'HEALTH'),
                          lambda: store.commit(command))
        assert store.command_status(DEVICE, 'one') == 'COMPLETE'
        assert store.state(AXIS).generation == 1


@pytest.mark.parametrize('wrong', [
    ('other-store', 'hood-001', 'HEALTH'),
    ('store-001', 'fryer-001', 'HEALTH'),
])
def test_cross_identity_commit_rejected(tmp_path, wrong):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        token = store.acquire_writer(DEVICE, 'one')
        with pytest.raises(StaleWriter):
            run(store, token, command=replace(prepared(), axis_key=wrong))
        assert store.state(AXIS).revision == 0


def test_admission_axis_and_domain_generation_conflicts(tmp_path):
    with FencedPublicationStore(tmp_path / 'new.db') as store:
        store.initialize(AXIS, 1, b'initial')
        token = store.acquire_writer(DEVICE, 'one')
        with pytest.raises(OutputConflict):
            run(store, token, command=replace(prepared(), generation=2))
        assert store.command_status(DEVICE, 'one') == 'ADMITTED'
        with pytest.raises(OutputConflict):
            store.run_command(token, 'one', canonical({'input': 'one'}),
                              ('OPERATIONAL',), lambda: None)
        run(store, token)


def test_simultaneous_authority_issuance_serializes_epoch_cas(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    path = tmp_path / 'new.db'
    with FencedPublicationStore(path) as store:
        store.initialize(AXIS, 1, b'initial')
    barrier = Barrier(2)

    def acquire(owner):
        with FencedPublicationStore(path) as store:
            barrier.wait(timeout=5)
            return store.acquire_writer(DEVICE, owner)

    with ThreadPoolExecutor(max_workers=2) as executor:
        tokens = list(executor.map(acquire, ('first', 'second')))
    old, current = sorted(tokens, key=lambda token: token.epoch)
    assert (old.epoch, current.epoch) == (1, 2)
    with FencedPublicationStore(path) as store:
        with pytest.raises(StaleWriter):
            run(store, old)
        run(store, current)
        assert store.state(AXIS).generation == 1
