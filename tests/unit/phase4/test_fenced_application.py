"""Cross-axis barriers retain existing snapshots, IDs and caller order."""

from dataclasses import replace
from datetime import timedelta

import pytest

from streaming.state.change_envelope import OutputConflict
from streaming.state.writer_fencing import CommandBusy, StaleWriter
from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle


DEVICE = ('store-001', 'hood-001')


def process(app, token, second=0, **kwargs):
    return app.process_validated(
        token, numeric(second), command_ref=f'event-{second}',
        reference_time=BASE + timedelta(seconds=second), **kwargs,
    )


@pytest.mark.parametrize('stage', ['after_operational_commit',
                                   'after_health_commit'])
def test_resume_partial_and_replay(tmp_path, stage):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'writer-1')

        def stop(at):
            if at == stage:
                raise RuntimeError('injected crash')

        with pytest.raises(RuntimeError):
            process(app, token, hook=stop)
        status = resources.store.command_status(DEVICE, 'event-0')
        assert status == ('PARTIAL' if stage == 'after_operational_commit'
                          else 'COMPLETE')
        if status == 'PARTIAL':
            with pytest.raises(CommandBusy):
                process(app, token, 1)
            for output_id in resources.store.pending():
                with pytest.raises(CommandBusy):
                    resources.coordinator.dispatch_claimed(
                        output_id, owner='dispatcher', clock=lambda: BASE,
                        lease=timedelta(seconds=3),
                    )
        token = resources.store.acquire_writer(DEVICE, 'writer-2')
        original = process(app, token)
        roots = tuple(resources.store.state((*DEVICE, axis)) for axis in (
            'OPERATIONAL', 'HEALTH',
        ))
        replay = process(app, token)
        assert [r.output_id for r in original] == [r.output_id for r in replay]
        assert roots == tuple(resources.store.state(root.axis_key)
                              for root in roots)
        assert resources.store.command_status(DEVICE, 'event-0') == 'COMPLETE'
    finally:
        resources.close()


def test_other_device_not_blocked_and_policy_change_rejected(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'one')

        def stop(stage):
            if stage == 'after_operational_commit':
                raise RuntimeError()

        with pytest.raises(RuntimeError):
            process(app, token, hook=stop)
        fryer = resources.store.acquire_writer(('store-001', 'fryer-001'),
                                              'other')
        app.process_validated(fryer, numeric(equipment_id='fryer-001'),
                              command_ref='other', reference_time=BASE)
        policies = resources.app.hood_policies
        policies['hood-001'] = replace(policies['hood-001'],
                                       enter_rpm=900)
        with pytest.raises(OutputConflict):
            process(app, token)
    finally:
        resources.close()


def test_stale_writer_and_wrong_device(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        old = resources.store.acquire_writer(DEVICE, 'one')
        process(app, old)
        resources.store.acquire_writer(DEVICE, 'two')
        with pytest.raises(StaleWriter):
            process(app, old)
        wrong = resources.store.acquire_writer(('elsewhere', 'hood-001'),
                                              'wrong')
        with pytest.raises(StaleWriter):
            process(app, wrong)
    finally:
        resources.close()


def test_repeated_partial_failure_and_timer_gate(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'one')

        def stop(stage):
            if stage == 'before_health_commit':
                raise RuntimeError()

        for _ in range(2):
            with pytest.raises(RuntimeError):
                process(app, token, hook=stop)
            root = resources.store.state((*DEVICE, 'OPERATIONAL'))
            assert root.revision == 1
        timer = app.timers.plan('hood-001', evaluation_time=BASE).token
        with pytest.raises(CommandBusy):
            app.evaluate_timer(token, 'hood-001', timer, command_ref='tick',
                               reference_time=BASE + timedelta(seconds=10))
        process(app, token)
        app.evaluate_timer(token, 'hood-001', timer, command_ref='tick',
                           reference_time=BASE + timedelta(seconds=10))
    finally:
        resources.close()


@pytest.mark.parametrize('tick_first', [False, True])
def test_recorded_timer_observation_order_replays_after_reopen(tmp_path,
                                                              tick_first):
    records = []
    for name in ('first', 'second'):
        path = tmp_path / name
        path.mkdir()
        resources, app = fenced_bundle(path, initialize=True)
        token = resources.store.acquire_writer(DEVICE, 'first-writer')
        process(app, token)
        timer = app.timers.plan('hood-001', evaluation_time=BASE).token
        resources.close()
        resources, app = fenced_bundle(path)
        try:
            token = resources.store.acquire_writer(DEVICE, 'replacement')

            def tick():
                app.evaluate_timer(
                    token, 'hood-001', timer, command_ref='tick',
                    reference_time=BASE + timedelta(seconds=10),
                )

            def observation():
                process(app, token, 10)

            actions = ((tick, observation) if tick_first
                       else (observation, tick))
            for action in actions:
                action()
            health = resources.app.load_health('hood-001')
            assert health.health_revision == (3 if tick_first else 1)
            state = resources.store.state((*DEVICE, 'HEALTH'))
            envelopes = tuple(resources.store.output(row[0]) for row in
                              resources.store.db.execute(
                                  'SELECT id FROM outputs ORDER BY rowid',
                              ))
            records.append((state, envelopes))
        finally:
            resources.close()
    assert records[0] == records[1]
