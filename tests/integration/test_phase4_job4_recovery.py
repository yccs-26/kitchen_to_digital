"""Actual Job 4 child exits, file reopen and native result convergence."""

from datetime import timedelta
from dataclasses import replace
from pathlib import Path
import os
import subprocess
import sys

import pytest

from streaming.state.job4_batch import BatchExecutionError
from streaming.state.job4_contract import BatchIdentity
from tests.fixtures.phase4.application_fixture import BASE
from tests.fixtures.phase4.job4_fixture import record, resources, writers


BATCH = BatchIdentity('query', 'checkpoint-v1', 1)
DEVICE = ('store-001', 'hood-001')
WORKER = r'''
from datetime import timedelta
from pathlib import Path
import os
import sys
from streaming.state.job4_batch import BatchExecutionError
from streaming.state.job4_contract import BatchIdentity
from streaming.state.writer_fencing import WriterToken
from tests.fixtures.phase4.application_fixture import BASE
from tests.fixtures.phase4.job4_fixture import record, resources, writers
path, stage = sys.argv[1:]
local, app = resources(Path(path))

def stop(at):
    if at == stage:
        os._exit(87)

original_bind = local.store._bind_ingress

def bind():
    original_bind()
    stop('after_ingress_bound')

local.store._bind_ingress = bind
original_dispatch = local.coordinator.dispatch_claimed
local.coordinator.dispatch_claimed = lambda *a, **kw: original_dispatch(
    *a, **kw, hook=stop,
)
try:
    tokens = writers(local, 'child') if stage != 'stale' else {
        ('store-001', name): WriterToken(('store-001', name), 'old', 1)
        for name in ('hood-001', 'fryer-001')
    }
    app.execute(
        BatchIdentity('query', 'checkpoint-v1', 1),
        (record(), record(1, 11)), writers=tokens,
        coordinator=local.coordinator, owner='child-dispatcher',
        clock=lambda: BASE + timedelta(seconds=30),
        lease=timedelta(seconds=3), hook=stop,
    )
except BatchExecutionError as exc:
    if stage == 'stale' and exc.category == 'FENCED':
        sys.exit(88)
    raise
finally:
    local.close()
'''


def child(path, stage, expected=87):
    process = subprocess.run(
        [sys.executable, '-c', WORKER, str(path), stage], timeout=20,
        capture_output=True, text=True,
        cwd=Path(__file__).resolve().parents[2],
        env={**os.environ, 'PYTHONDONTWRITEBYTECODE': '1'},
    )
    assert process.returncode == expected, process.stderr


def execute(local, app, second=33, records=None):
    return app.execute(
        BATCH, (record(), record(1, 11)) if records is None else records,
        writers=writers(local, 'parent'),
        coordinator=local.coordinator, owner='parent-dispatcher',
        clock=lambda: BASE + timedelta(seconds=second),
        lease=timedelta(seconds=3),
    )


def roots(local):
    return tuple(local.store.state((*DEVICE, axis))
                 for axis in ('OPERATIONAL', 'HEALTH'))


def outputs(local):
    return tuple(local.store.output(row[0]) for row in local.store.db.execute(
        'SELECT id FROM outputs ORDER BY rowid',
    ))


@pytest.mark.parametrize('stage', [
    'after_manifest', 'after_ingress_bound', 'before_operational_commit',
    'after_operational_commit', 'after_health_commit', 'after_command',
    'after_input_complete', 'after_claim', 'after_kafka',
    'before_batch_complete', 'after_batch_complete',
])
def test_real_exit_matches_continuous_domain_and_batch(tmp_path, stage):
    crash, normal = tmp_path / 'crash', tmp_path / 'normal'
    crash.mkdir()
    normal.mkdir()
    local, _ = resources(crash, initialize=True)
    local.close()
    child(crash, stage)
    local, app = resources(crash)
    control, normal_app = resources(normal, initialize=True)
    try:
        delayed = None
        if stage == 'after_manifest':
            delayed = tuple(replace(item, evaluation_time=(
                item.evaluation_time + timedelta(days=1)
            )) for item in (record(), record(1, 11)))
        recovered = execute(local, app, records=delayed)
        continuous = execute(control, normal_app)
        assert recovered == continuous
        assert recovered.checkpoint_allowed
        assert roots(local) == roots(control)
        assert outputs(local) == outputs(control)
        assert local.history.rows() == control.history.rows()
        assert local.current.equipment(*DEVICE) == control.current.equipment(
            *DEVICE,
        )
        assert {item.payload for item in local.kafka.messages()} == {
            item.payload for item in control.kafka.messages()
        }
        assert len(local.kafka.messages()) == (
            len(control.kafka.messages()) + int(stage == 'after_kafka')
        )
        assert local.store.manifest(recovered.batch_id) == (
            control.store.manifest(continuous.batch_id)
        )
        assert local.store.pending() == ()
        if stage == 'after_ingress_bound':
            # Recovery succeeded after the killed admission transaction rolled
            # back cursor/admission/ingress binding together.
            assert local.store.db.execute(
                'SELECT COUNT(*) FROM job4_cursors',
            ).fetchone()[0] == 1
    finally:
        local.close()
        control.close()


def test_live_claim_after_death_blocks_batch_until_exact_expiry(tmp_path):
    local, _ = resources(tmp_path, initialize=True)
    local.close()
    child(tmp_path, 'after_claim')
    local, app = resources(tmp_path)
    try:
        with pytest.raises(BatchExecutionError) as error:
            execute(local, app, second=32)
        assert error.value.category == 'RETRYABLE'
        assert execute(local, app, second=33).checkpoint_allowed
    finally:
        local.close()


def test_old_writer_process_cannot_create_batch_manifest(tmp_path):
    local, _ = resources(tmp_path, initialize=True)
    writers(local, 'old')
    writers(local, 'replacement')
    before = roots(local)
    local.close()
    child(tmp_path, 'stale', expected=88)
    local, _ = resources(tmp_path)
    try:
        assert roots(local) == before
        assert local.store.db.execute(
            'SELECT COUNT(*) FROM job4_batches',
        ).fetchone()[0] == 0
    finally:
        local.close()


def test_partial_recovery_can_die_again_without_reapplying_axis(tmp_path):
    local, _ = resources(tmp_path, initialize=True)
    local.close()
    child(tmp_path, 'after_operational_commit')
    child(tmp_path, 'before_health_commit')
    local, app = resources(tmp_path)
    try:
        assert local.store.state((*DEVICE, 'OPERATIONAL')).revision == 1
        assert local.store.state((*DEVICE, 'HEALTH')).revision == 0
        assert execute(local, app).checkpoint_allowed
        assert local.store.state((*DEVICE, 'OPERATIONAL')).revision == 2
        assert local.store.state((*DEVICE, 'HEALTH')).revision == 2
    finally:
        local.close()
