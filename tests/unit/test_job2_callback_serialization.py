"""실제 C-extension을 둔 채 callback을 별도 Python worker로 전달한다."""

import dis
import inspect
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest

try:
    from pyspark import cloudpickle
except ImportError:
    import cloudpickle

from streaming.jobs.runtime_config import load_runtime_config
from streaming.jobs.sensor_validation_job import make_batch_handler
from streaming.jobs import runtime_config, sensor_validation_job


@pytest.mark.parametrize('resolve_on_driver', [False, True])
@pytest.mark.parametrize('by_value', [False, True])
def test_callback_round_trip_in_fresh_worker(
    tmp_path, resolve_on_driver, by_value,
):
    values = json.loads(Path('config/job2-runtime.example.json').read_text())
    values['checkpoint'] = '/Volumes/ktd/bronze/checkpoints/job2/pickle-test'
    values['processing_version'] = 'pickle-test'
    values['rules_path'] = str(Path('config/validation_rules.json').resolve())
    config = tmp_path / 'runtime.json'
    config.write_text(json.dumps(values))
    settings = load_runtime_config(config)
    utility = Mock()
    utility.secrets.get.return_value = 'test-user:test-secret'
    if resolve_on_driver:
        resolved = settings.resolve_clients(utility)
        kwargs = {
            'registry_config': resolved.registry,
            'producer_config': resolved.producer,
        }
        utility.secrets.get.assert_called_once()
        json.dumps([resolved.registry, resolved.producer])
    else:
        kwargs = {
            'registry_config': {'url': 'http://unused'},
            'producer_config': {'bootstrap.servers': 'unused'},
        }
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
    }
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
    return 'worker-import-OK'
original_execute = job._execute_batch
job._execute_batch = execute
assert handler('worker-batch', 7) == 'worker-import-OK'
# 직렬화가 끝난 뒤에만 외부 클라이언트를 fake로 대체한다.
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, Mock
from streaming.jobs import runtime_config as runtime
runtime.get_dbutils = Mock(side_effect=AssertionError('DBUtils forbidden'))
service = ModuleType('databricks.service_credentials')
service.getServiceCredentialsProvider = Mock()
sys.modules[service.__name__] = service
runtime._msk_callback_from_session = Mock(
    return_value=lambda _: ('token', 9999),
)
job.SchemaRegistryClient = Mock(return_value=MagicMock())
job.AvroSerializer = Mock()
job.Producer = Mock()
job.process_batch = Mock(return_value='clients-created')
job._execute_batch = original_execute
assert handler(SimpleNamespace(sparkSession=Mock()), 8) == 'clients-created'
assert job.Producer.call_count == 2
assert 'ktd.msk.credential' not in job.Producer.call_args.args[0]
runtime.get_dbutils.assert_not_called()
print('roundtrip-OK')
'''
    result = subprocess.run(
        [sys.executable, '-c', worker, str(path)],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == 'roundtrip-OK'
