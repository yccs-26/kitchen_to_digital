"""Audit only the v2 boundaries not already covered by prior process tests."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.change_envelope import OutputConflict, canonical
from streaming.state.health_registry import HealthRegistryError
from streaming.state.publication_store import PublicationStore
from streaming.state.writer_fencing import (
    CommandBusy, FencedPublicationStore, StaleClaim,
)
from tests.fixtures.phase4.application_fixture import BASE, bundle, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle


DEVICE = ('store-001', 'hood-001')
LEASE = timedelta(seconds=3)


def ready(path):
    resources, app = fenced_bundle(path, initialize=True)
    token = resources.store.acquire_writer(DEVICE, 'writer')
    results = app.process_validated(token, numeric(), command_ref='one',
                                    reference_time=BASE)
    output_id = next(r.output_id for r in results
                     if r.axis_key[2] == 'HEALTH')
    return resources, output_id


def test_after_claim_python_exception_releases_own_claim(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        def fail(stage):
            if stage == 'after_claim':
                raise RuntimeError('ordinary exception, not process exit')

        with pytest.raises(RuntimeError):
            resources.coordinator.dispatch_claimed(
                output_id, owner='first', clock=lambda: BASE, lease=LEASE,
                hook=fail,
            )
        # Normal stack unwinding should not require waiting for lease expiry.
        resources.coordinator.dispatch_claimed(
            output_id, owner='second', clock=lambda: BASE, lease=LEASE,
        )
        assert output_id not in resources.store.pending()
    finally:
        resources.close()


@pytest.mark.parametrize('sequence_delta', [-1, 1])
def test_forged_claim_sequence_cannot_complete(tmp_path, sequence_delta):
    resources, output_id = ready(tmp_path)
    try:
        store = resources.store
        claim = store.claim(output_id, 'worker', evaluation_time=BASE,
                            lease=LEASE)
        invalid = replace(claim, sequence=claim.sequence + sequence_delta)
        with store.dispatch_scope(invalid, lambda: BASE):
            with pytest.raises(StaleClaim):
                store.complete(output_id)
        assert output_id in store.pending()
    finally:
        resources.close()


def test_old_dispatcher_complete_at_expiry_cannot_complete_replacement(
    tmp_path,
):
    resources, output_id = ready(tmp_path)
    try:
        store = resources.store
        old = store.claim(output_id, 'old', evaluation_time=BASE, lease=LEASE)
        current = store.claim(output_id, 'new', evaluation_time=BASE + LEASE,
                              lease=LEASE)
        with store.dispatch_scope(current, lambda: BASE + LEASE):
            store.begin_attempt(output_id, 'history')
        with store.dispatch_scope(old, lambda: BASE + LEASE):
            with pytest.raises(StaleClaim):
                store.complete(output_id)
        assert store.delivery(output_id, 'history')['status'] == 'UNKNOWN'
        assert output_id in store.pending()
    finally:
        resources.close()


@pytest.mark.parametrize('factory,wrong', [
    (PublicationStore, FencedPublicationStore),
    (FencedPublicationStore, PublicationStore),
])
def test_same_file_v1_v2_mix_rejected_without_migration(tmp_path, factory,
                                                       wrong):
    path = tmp_path / 'state.db'
    store = factory(path)
    store.initialize((*DEVICE, 'HEALTH'), 1, b'fixture')
    store.close()
    before = path.read_bytes()
    with pytest.raises(ValueError):
        wrong(path)
    assert path.read_bytes() == before


def test_conflicting_admission_while_original_operation_is_live(tmp_path):
    path = tmp_path / 'state.db'
    with FencedPublicationStore(path) as first, FencedPublicationStore(
        path,
    ) as second:
        token = first.acquire_writer(DEVICE, 'shared')

        def operation():
            with pytest.raises(OutputConflict):
                second.run_command(token, 'same', canonical({'value': 2}),
                                   ('HEALTH',), lambda: None)

        first.run_command(token, 'same', canonical({'value': 1}),
                          ('HEALTH',), operation)
        assert first.command_status(DEVICE, 'same') == 'ADMITTED'


def test_invalid_admitted_command_stays_blocked_for_explicit_review(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')
        invalid = numeric()
        invalid['unit'] = 'invalid'
        with pytest.raises(HealthRegistryError):
            app.process_validated(token, invalid, command_ref='invalid',
                                   reference_time=BASE)
        assert resources.store.command_status(DEVICE, 'invalid') == 'ADMITTED'
        with pytest.raises(CommandBusy):
            app.process_validated(token, numeric(1), command_ref='next',
                                   reference_time=BASE + timedelta(seconds=1))
        assert resources.coordinator.reconcile_claimed(
            owner='worker', clock=lambda: BASE, lease=LEASE,
        ) == ()
        # Empty pending is not evidence that all admitted commands completed.
        assert resources.store.command_status(DEVICE, 'invalid') != 'COMPLETE'
    finally:
        resources.close()


def test_conflict_pending_requires_review_not_blind_retry(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        class ConflictingHistory:
            def write(self, change):
                raise OutputConflict('injected immutable payload conflict')

        resources.coordinator.history = ConflictingHistory()
        with pytest.raises(OutputConflict):
            resources.coordinator.dispatch_claimed(
                output_id, owner='first', clock=lambda: BASE, lease=LEASE,
            )
        attempts = resources.store.delivery(output_id, 'history')['attempts']
        with pytest.raises(OutputConflict):
            resources.coordinator.dispatch_claimed(
                output_id, owner='second', clock=lambda: BASE, lease=LEASE,
            )
        assert resources.store.delivery(output_id, 'history')['attempts'] == (
            attempts
        )
        assert output_id in resources.store.pending()
    finally:
        resources.close()


def test_separate_v1_v2_databases_do_not_share_writer_authority(tmp_path):
    legacy_path = tmp_path / 'v1'
    fenced_path = tmp_path / 'v2'
    legacy_path.mkdir()
    fenced_path.mkdir()
    old = bundle(legacy_path, initialize=True)
    new, app = fenced_bundle(fenced_path, initialize=True)
    try:
        device = ('store-001', 'fryer-001')
        sample = numeric(equipment_id=device[1])
        token = new.store.acquire_writer(device, 'v2-writer')
        old_result = old.app.process_validated(
            sample, command_ref='shared', reference_time=BASE,
        )
        new_result = app.process_validated(
            token, sample, command_ref='shared', reference_time=BASE,
        )
        assert old_result[0].output_id == new_result[0].output_id
        assert old.store.state((*device, 'HEALTH')).revision == 1
        assert new.store.state((*device, 'HEALTH')).revision == 1
        # Same equipment in independent DBs is NOT globally fenced.
        # Deployment must prevent dual authorities; no policy is invented here.
    finally:
        old.close()
        new.close()
