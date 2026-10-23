"""Overlapping independent processes with bounded file rendezvous."""

from datetime import timedelta
from pathlib import Path
import os
import subprocess
import sys
import time

from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle


DEVICE = ('store-001', 'hood-001')
WORKER = r'''
from datetime import timedelta
from pathlib import Path
import sys
import time
from streaming.state.writer_fencing import WriterToken, StaleClaim
from tests.fixtures.phase4.application_fixture import BASE, numeric
from tests.fixtures.phase4.concurrency_fixture import fenced_bundle
path, mode, name = sys.argv[1:]
path = Path(path)
resources, app = fenced_bundle(path)

def rendezvous(stage):
    target = 'after_calculation' if mode == 'writer' else 'after_kafka'
    if stage != target:
        return
    (path / (name + '.ready')).touch()
    end = time.monotonic() + 12
    while not (path / (name + '.release')).exists():
        if time.monotonic() >= end:
            raise TimeoutError('test rendezvous expired')
        time.sleep(.01)

try:
    if mode == 'writer':
        token = WriterToken(('store-001', 'hood-001'), 'shared', 1)
        app.process_validated(token, numeric(), command_ref='one',
                              reference_time=BASE, hook=rendezvous)
    else:
        output_id = next(i for i in resources.store.pending()
                         if resources.store.output(i).projection.axis_key[2]
                         == 'HEALTH')
        resources.coordinator.dispatch_claimed(
            output_id, owner=name, clock=lambda: BASE,
            lease=timedelta(seconds=3), hook=rendezvous,
        )
except StaleClaim:
    sys.exit(84)
finally:
    resources.close()
'''


def start(path, mode, name):
    return subprocess.Popen(
        [sys.executable, '-c', WORKER, str(path), mode, name],
        cwd=Path(__file__).resolve().parents[2], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, text=True,
        env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
    )


def wait_ready(path, name, process):
    until = time.monotonic() + 15
    while not (path / (name + '.ready')).exists():
        if process.poll() is not None or time.monotonic() >= until:
            raise AssertionError('worker did not reach rendezvous')
        time.sleep(.01)


def cleanup(process):
    if process.poll() is None:
        process.kill()
    process.communicate(timeout=5)


def test_two_live_writers_same_prepared_command(tmp_path):
    resources, _ = fenced_bundle(tmp_path, initialize=True)
    resources.store.acquire_writer(DEVICE, 'shared')
    resources.close()
    first = start(tmp_path, 'writer', 'first')
    second = start(tmp_path, 'writer', 'second')
    try:
        wait_ready(tmp_path, 'first', first)
        wait_ready(tmp_path, 'second', second)
        (tmp_path / 'first.release').touch()
        (tmp_path / 'second.release').touch()
        for process in (first, second):
            _, error = process.communicate(timeout=20)
            assert process.returncode == 0, error
    finally:
        cleanup(first)
        cleanup(second)
    resources, _ = fenced_bundle(tmp_path)
    try:
        assert all(resources.store.state((*DEVICE, axis)).revision == 1
                   for axis in ('OPERATIONAL', 'HEALTH'))
        assert resources.store.db.execute(
            'SELECT COUNT(*) FROM commands',
        ).fetchone()[0] == 2
        assert resources.store.command_status(DEVICE, 'one') == 'COMPLETE'
    finally:
        resources.close()


def test_live_dispatcher_failover_rejects_late_ack(tmp_path):
    resources, app = fenced_bundle(tmp_path, initialize=True)
    token = resources.store.acquire_writer(DEVICE, 'writer')
    app.process_validated(token, numeric(), command_ref='one',
                          reference_time=BASE)
    resources.close()
    process = start(tmp_path, 'dispatcher', 'slow')
    try:
        wait_ready(tmp_path, 'slow', process)
        resources, _ = fenced_bundle(tmp_path)
        try:
            output_id = next(
                i for i in resources.store.pending()
                if resources.store.output(i).projection.axis_key[2] == 'HEALTH'
            )
            resources.coordinator.dispatch_claimed(
                output_id, owner='replacement',
                clock=lambda: BASE + timedelta(seconds=3),
                lease=timedelta(seconds=3),
            )
            assert output_id not in resources.store.pending()
        finally:
            resources.close()
        (tmp_path / 'slow.release').touch()
        _, error = process.communicate(timeout=20)
        assert process.returncode == 84, error
    finally:
        cleanup(process)
    resources, _ = fenced_bundle(tmp_path)
    try:
        assert len(resources.kafka.messages()) == 2
        assert len(resources.history.rows()) == 1
        assert all(row['status'] == 'ACKED' for row in
                   resources.store.db.execute('SELECT status FROM deliveries '
                                              "WHERE status!='NOT_REQUIRED' "
                                              "AND output_id LIKE 'HEALTH:%'"))
    finally:
        resources.close()
