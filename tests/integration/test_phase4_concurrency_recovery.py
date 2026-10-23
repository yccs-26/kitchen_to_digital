"""Real child exits and local file recovery, never external integration.

All subprocesses have a 20-second timeout; subprocess.run kills/reaps timeouts.
Fixed clocks and caller order make every scenario reproducible without sleeps.
"""

from datetime import timedelta
from pathlib import Path
import os
import subprocess
import sys

import pytest

from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle


DEVICE = ('store-001', 'hood-001')
WORKER = r'''
from datetime import timedelta
from pathlib import Path
import os
import sys
from streaming.state.writer_fencing import StaleWriter, WriterToken, ClaimBusy
from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle
path, mode, stage, seconds = sys.argv[1:]
resources, app = fenced_bundle(Path(path))
store = resources.store

def stop(at):
    if at == stage:
        os._exit(81)

try:
    if mode == 'command':
        token = store.acquire_writer(('store-001', 'hood-001'), 'child')
        original = store.commit
        store.commit = lambda command: original(command, hook=stop)
        app.process_validated(token, numeric(), command_ref='one',
                              reference_time=BASE, hook=stop)
    elif mode == 'stale':
        token = WriterToken(('store-001', 'hood-001'), 'old', 1)
        app.process_validated(token, numeric(), command_ref='one',
                              reference_time=BASE)
    else:
        output_id = next(i for i in store.pending()
                         if store.output(i).projection.axis_key[2] == 'HEALTH')
        resources.coordinator.dispatch_claimed(
            output_id, owner='child',
            clock=lambda: BASE + timedelta(seconds=int(seconds)),
            lease=timedelta(seconds=3), hook=stop,
        )
except StaleWriter:
    sys.exit(82)
except ClaimBusy:
    sys.exit(83)
finally:
    resources.close()
'''


def child(path, mode, stage='', seconds=0, expected=81):
    result = subprocess.run(
        [sys.executable, '-c', WORKER, str(path), mode, stage, str(seconds)],
        cwd=Path(__file__).resolve().parents[2], capture_output=True,
        text=True, timeout=20,
        env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
    )
    assert result.returncode == expected, result.stderr


def snapshots(resources):
    return tuple(resources.store.state((*DEVICE, axis)) for axis in (
        'OPERATIONAL', 'HEALTH',
    ))


def outputs(resources):
    return tuple(resources.store.output(row[0]) for row in
                 resources.store.db.execute('SELECT id FROM outputs '
                                            'ORDER BY rowid'))


def finish(resources):
    for output_id in resources.store.pending():
        resources.coordinator.dispatch_claimed(
            output_id, owner='parent',
            clock=lambda: BASE + timedelta(seconds=9),
            lease=timedelta(seconds=3),
        )
    assert resources.store.pending() == ()


@pytest.mark.parametrize('stage', [
    'after_calculation', 'before_operational_commit', 'after_snapshot',
    'before_commit', 'after_operational_commit', 'after_health_commit',
])
def test_command_process_exit_matches_uninterrupted(tmp_path, stage):
    crash = tmp_path / 'crash'
    normal = tmp_path / 'normal'
    crash.mkdir()
    normal.mkdir()
    resources, _ = fenced_bundle(crash, initialize=True)
    resources.close()
    child(crash, 'command', stage)
    resources, app = fenced_bundle(crash)
    control, normal_app = fenced_bundle(normal, initialize=True)
    try:
        token = resources.store.acquire_writer(DEVICE, 'parent')
        app.process_validated(token, numeric(), command_ref='one',
                              reference_time=BASE)
        normal_token = control.store.acquire_writer(DEVICE, 'uninterrupted')
        normal_app.process_validated(
            normal_token, numeric(), command_ref='one', reference_time=BASE,
        )
        assert snapshots(resources) == snapshots(control)
        assert outputs(resources) == outputs(control)
        finish(resources)
        finish(control)
        assert resources.current.equipment(*DEVICE) == (
            control.current.equipment(*DEVICE)
        )
        assert resources.history.rows() == control.history.rows()
    finally:
        resources.close()
        control.close()


@pytest.mark.parametrize('stage', [
    'after_claim', 'before_history', 'after_history', 'after_kafka',
    'after_current', 'before_complete',
])
def test_dispatch_process_exit_lease_recovery(tmp_path, stage):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    token = resources.store.acquire_writer(DEVICE, 'writer')
    app.process_validated(token, numeric(), command_ref='one',
                          reference_time=BASE)
    before_roots, before_outputs = snapshots(resources), outputs(resources)
    resources.close()
    child(tmp_path, 'dispatch', stage)
    # An independent process sees the live claim after the first process dies.
    child(tmp_path, 'dispatch', seconds=1, expected=83)
    resources, _ = fenced_bundle(tmp_path)
    try:
        assert snapshots(resources) == before_roots
        assert outputs(resources) == before_outputs
        finish(resources)
        assert len(resources.history.rows()) == 1
        assert len(resources.kafka.messages()) == (
            2 if stage == 'after_kafka' else 1
        )
        messages = resources.kafka.messages()
        assert all(item == messages[0] for item in messages)
    finally:
        resources.close()


def test_stale_process_cannot_replay_after_takeover(tmp_path):
    resources, _ = fenced_bundle(tmp_path, initialize=True)
    resources.store.acquire_writer(DEVICE, 'old')
    resources.store.acquire_writer(DEVICE, 'new')
    before = snapshots(resources)
    resources.close()
    child(tmp_path, 'stale', expected=82)
    resources, _ = fenced_bundle(tmp_path)
    try:
        assert snapshots(resources) == before
    finally:
        resources.close()


def test_recovery_process_dies_again_then_converges(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    token = resources.store.acquire_writer(DEVICE, 'writer')
    app.process_validated(token, numeric(), command_ref='one',
                          reference_time=BASE)
    resources.close()
    child(tmp_path, 'dispatch', 'after_kafka', seconds=0)
    child(tmp_path, 'dispatch', 'after_kafka', seconds=3)
    resources, _ = fenced_bundle(tmp_path)
    try:
        finish(resources)
        assert len(resources.kafka.messages()) == 3
        assert len(resources.history.rows()) == 1
        assert not resources.store.pending()
    finally:
        resources.close()


def test_partial_command_recovery_killed_again_before_remaining_axis(tmp_path):
    resources, _ = fenced_bundle(tmp_path, initialize=True)
    resources.close()
    child(tmp_path, 'command', 'after_operational_commit')
    child(tmp_path, 'command', 'before_health_commit')
    resources, _ = fenced_bundle(tmp_path)
    try:
        assert resources.store.command_status(DEVICE, 'one') == 'PARTIAL'
        assert resources.store.state((*DEVICE, 'OPERATIONAL')).revision == 1
        assert resources.store.state((*DEVICE, 'HEALTH')).revision == 0
    finally:
        resources.close()
    child(tmp_path, 'command', 'after_health_commit')
    resources, app = fenced_bundle(tmp_path)
    try:
        token = resources.store.acquire_writer(DEVICE, 'last')
        result = app.process_validated(token, numeric(), command_ref='one',
                                       reference_time=BASE)
        assert all(item.replayed for item in result)
        assert all(resources.store.state((*DEVICE, axis)).revision == 1
                   for axis in ('OPERATIONAL', 'HEALTH'))
        finish(resources)
    finally:
        resources.close()
