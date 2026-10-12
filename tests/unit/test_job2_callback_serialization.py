"""실제 C-extension을 둔 채 callback을 별도 Python worker로 전달한다."""

import dis
import inspect
import json
from pathlib import Path
import subprocess
import sys

import pytest

try:
    from pyspark import cloudpickle
except ImportError:
    import cloudpickle

from streaming.jobs.runtime_config import load_runtime_config
from streaming.jobs.sensor_validation_job import make_batch_handler
from streaming.jobs import runtime_config, sensor_validation_job


@pytest.mark.parametrize('use_factory', [False, True])
@pytest.mark.parametrize('by_value', [False, True])
def test_callback_round_trip_in_fresh_worker(tmp_path, use_factory, by_value):
    values = json.loads(Path('config/job2-runtime.example.json').read_text())
    values['checkpoint'] = '/Volumes/ktd/bronze/checkpoints/job2/pickle-test'
    values['processing_version'] = 'pickle-test'
    values['rules_path'] = str(Path('config/validation_rules.json').resolve())
    config = tmp_path / 'runtime.json'
    config.write_text(json.dumps(values))
    settings = load_runtime_config(config)
    kwargs = (
        {'client_config_factory': settings.client_configs} if use_factory else
        {'registry_config': {'url': 'http://unused'},
         'producer_config': {'bootstrap.servers': 'unused'}}
    )
    handler = make_batch_handler(settings.job, settings.rules, **kwargs)
    captured = inspect.getclosurevars(handler)
    # Python 3.11 inspect는 IMPORT_FROM 이름도 globals로 오인하므로
    # cloudpickle이 추적하는 실제 전역 접근 opcode를 확인한다.
    assert not any(
        instruction.opname == 'LOAD_GLOBAL'
        for instruction in dis.get_instructions(handler)
    )
    assert set(captured.nonlocals) == {
        'config', 'rules', 'schema', 'registry_settings', 'producer_settings',
        'client_config_factory',
    }
    factory = captured.nonlocals['client_config_factory']
    if use_factory:
        assert factory.__self__ is settings
    else:
        assert factory is None
    modules = (runtime_config, sensor_validation_job) if by_value else ()
    for module in modules:
        cloudpickle.register_pickle_by_value(module)
    try:
        payload = cloudpickle.dumps(handler)
    finally:
        for module in modules:
            cloudpickle.unregister_pickle_by_value(module)
    # by-value 클래스 복원은 class tracker를 통해 현재 프로세스의 클래스를
    # 갱신할 수 있으므로 실제 worker처럼 아래 별도 프로세스에서만 수행한다.
    # 실제 Producer 클래스를 mock하지 않는다.
    if not by_value:
        assert callable(cloudpickle.loads(payload))
    path = tmp_path / 'callback.bin'
    path.write_bytes(payload)
    worker = '''
import sys
from pathlib import Path
try:
    from pyspark import cloudpickle
except ImportError:
    import cloudpickle
from streaming.jobs import sensor_validation_job as job

handler = cloudpickle.loads(Path(sys.argv[1]).read_bytes())
def execute(batch, batch_id, **settings):
    assert batch == 'worker-batch' and batch_id == 7
    assert settings['config'].processing_version == 'pickle-test'
    factory = settings['client_config_factory']
    if factory is not None:
        assert factory.__self__.job.processing_version == 'pickle-test'
    return 'worker-import-OK'
job._execute_batch = execute
assert handler('worker-batch', 7) == 'worker-import-OK'
print('roundtrip-OK')
'''
    result = subprocess.run(
        [sys.executable, '-c', worker, str(path)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'roundtrip-OK'
