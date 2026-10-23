"""Explicit-clock leases retain at-least-once Fake Kafka behavior."""

from datetime import timedelta

import pytest

from streaming.state.writer_fencing import ClaimBusy, StaleClaim
from streaming.state.fenced_application import FencedStateApplication
from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle


LEASE = timedelta(seconds=3)
DEVICE = ('store-001', 'hood-001')


def ready(path):
    resources, app = fenced_bundle(path, initialize=True)
    token = resources.store.acquire_writer(DEVICE, 'writer')
    result = app.process_validated(token, numeric(), command_ref='one',
                                   reference_time=BASE)
    return resources, next(r.output_id for r in result if r.axis_key[2]
                           == 'HEALTH')


def test_duplicate_claim_and_exact_expiry(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        store = resources.store
        first = store.claim(output_id, 'first', evaluation_time=BASE,
                            lease=LEASE)
        with pytest.raises(ClaimBusy):
            store.claim(output_id, 'second', evaluation_time=BASE + LEASE
                        - timedelta(microseconds=1), lease=LEASE)
        second = store.claim(output_id, 'second', evaluation_time=BASE + LEASE,
                             lease=LEASE)
        assert second.sequence == first.sequence + 1
        with store.dispatch_scope(first, lambda: BASE):
            with pytest.raises(StaleClaim):
                store.begin_attempt(output_id, 'history')
        store.release(first)
        with store.dispatch_scope(second, lambda: BASE + LEASE):
            assert store.begin_attempt(output_id, 'history') == 1
    finally:
        resources.close()


def test_expired_ack_does_not_finish_new_attempt(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        store = resources.store
        first = store.claim(output_id, 'owner', evaluation_time=BASE,
                            lease=LEASE)
        with store.dispatch_scope(first, lambda: BASE):
            attempt = store.begin_attempt(output_id, 'kafka')
        second = store.claim(output_id, 'owner', evaluation_time=BASE + LEASE,
                             lease=LEASE)
        with store.dispatch_scope(second, lambda: BASE + LEASE):
            assert store.begin_attempt(output_id, 'kafka') == attempt + 1
        with store.dispatch_scope(first, lambda: BASE + LEASE):
            with pytest.raises(StaleClaim):
                store.finish_attempt(output_id, 'kafka', 'ACKED',
                                     attempt=attempt)
        assert store.delivery(output_id, 'kafka')['status'] == 'UNKNOWN'
    finally:
        resources.close()


@pytest.mark.parametrize('stage', ['before_history', 'after_history',
                                   'after_kafka', 'after_current',
                                   'before_complete'])
def test_ack_uncertainty_retries_converge(tmp_path, stage):
    resources, output_id = ready(tmp_path)
    try:
        def stop(at):
            if at == stage:
                raise KeyboardInterrupt('simulate lost ACK')

        with pytest.raises(KeyboardInterrupt):
            resources.coordinator.dispatch_claimed(
                output_id, owner='first', clock=lambda: BASE, lease=LEASE,
                hook=stop,
            )
        resources.coordinator.dispatch_claimed(
            output_id, owner='second', clock=lambda: BASE + LEASE,
            lease=LEASE,
        )
        assert output_id not in resources.store.pending()
        rows = resources.history.rows()
        assert len(rows) == 1
        messages = resources.kafka.messages()
        assert len(messages) == (2 if stage == 'after_kafka' else 1)
        assert all(msg.payload == messages[0].payload for msg in messages)
        current = resources.current.get((*DEVICE, 'HEALTH'))
        assert current == resources.store.output(output_id).projection
    finally:
        resources.close()


def test_long_sink_call_lease_loss_cannot_ack(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        now = [BASE]

        def advance(stage):
            if stage == 'after_kafka':
                now[0] = BASE + LEASE

        with pytest.raises(StaleClaim):
            resources.coordinator.dispatch_claimed(
                output_id, owner='first', clock=lambda: now[0], lease=LEASE,
                hook=advance,
            )
        assert resources.store.delivery(output_id, 'kafka')['status'] == (
            'UNKNOWN'
        )
        resources.coordinator.dispatch_claimed(
            output_id, owner='second', clock=lambda: now[0], lease=LEASE,
        )
        assert len(resources.kafka.messages()) == 2
    finally:
        resources.close()


def test_clock_reversal_rejected_and_other_output_independent(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        store = resources.store
        claim = store.claim(output_id, 'one', evaluation_time=BASE,
                            lease=LEASE)
        now = [BASE + timedelta(seconds=1)]
        with store.dispatch_scope(claim, lambda: now[0]):
            store.begin_attempt(output_id, 'history')
            now[0] = BASE
            with pytest.raises(StaleClaim):
                store.begin_attempt(output_id, 'kafka')
        with pytest.raises(StaleClaim):
            store.claim(output_id, 'two', evaluation_time=BASE, lease=LEASE)
        other = next(i for i in store.pending() if i != output_id)
        independent = store.claim(other, 'two', evaluation_time=BASE,
                                  lease=LEASE)
        assert independent.output_id == other
    finally:
        resources.close()


def test_old_current_delivery_cannot_roll_back_newer_current(tmp_path):
    resources, old = ready(tmp_path)
    try:
        app = FencedStateApplication(resources.app)
        token = resources.store.acquire_writer(DEVICE, 'new')
        result = app.process_validated(
            token, numeric(1), command_ref='two',
            reference_time=BASE + timedelta(seconds=1),
        )
        latest = next(r.output_id for r in result if r.axis_key[2] == 'HEALTH')
        for output_id in (latest, old):
            resources.coordinator.dispatch_claimed(
                output_id, owner='worker', clock=lambda: BASE + LEASE,
                lease=LEASE,
            )
        assert resources.current.get((*DEVICE, 'HEALTH')) == (
            resources.store.output(latest).projection
        )
        assert resources.store.delivery(old, 'current')['status'] == (
            'SUPERSEDED'
        )
    finally:
        resources.close()


def test_bounded_scan_reports_claim_blocked_then_converges(tmp_path):
    resources, output_id = ready(tmp_path)
    try:
        resources.store.claim(output_id, 'dead', evaluation_time=BASE,
                              lease=LEASE)
        first = resources.coordinator.reconcile_claimed(
            owner='replacement', clock=lambda: BASE, lease=LEASE,
        )
        assert (output_id, 'BLOCKED') in first
        assert any(status == 'COMPLETE' for _, status in first)
        second = resources.coordinator.reconcile_claimed(
            owner='replacement', clock=lambda: BASE + LEASE, lease=LEASE,
        )
        assert second == ((output_id, 'COMPLETE'),)
        assert resources.store.pending() == ()
    finally:
        resources.close()


def test_partial_device_does_not_starve_other_device_publication(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer')

        def stop(stage):
            if stage == 'after_operational_commit':
                raise RuntimeError()

        with pytest.raises(RuntimeError):
            app.process_validated(token, numeric(), command_ref='partial',
                                   reference_time=BASE, hook=stop)
        token = resources.store.acquire_writer(('store-001', 'fryer-001'),
                                               'other')
        app.process_validated(token, numeric(equipment_id='fryer-001'),
                               command_ref='other', reference_time=BASE)
        result = resources.coordinator.reconcile_claimed(
            owner='dispatcher', clock=lambda: BASE, lease=LEASE,
        )
        assert {status for _, status in result} == {'BLOCKED', 'COMPLETE'}
        assert resources.current.get(('store-001', 'fryer-001', 'HEALTH'))
        assert len(resources.store.pending()) == 1
    finally:
        resources.close()
