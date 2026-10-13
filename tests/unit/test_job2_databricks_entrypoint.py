"""Databricks 세션 선택과 repo 외부 CWD에서의 실행 계약을 검증한다."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from scripts import run_job2_databricks as launcher
from streaming.jobs import runtime_config as runtime
from streaming.jobs import sensor_validation_job as job


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def deployed_repo(tmp_path):
    """실제 배포 파일만 복사하여 로컬 import 경로의 우연한 성공을 배제한다."""
    root = tmp_path / "workspace" / "ktd"
    for directory in ("streaming", "schemas", "config"):
        shutil.copytree(REPO_ROOT / directory, root / directory)
    (root / "scripts").mkdir()
    shutil.copy2(
        REPO_ROOT / "scripts/run_job2_databricks.py", root / "scripts",
    )
    values = json.loads(
        (root / "config/job2-runtime.example.json").read_text(),
    )
    values["checkpoint"] = "/Volumes/ktd/bronze/checkpoints/job2/unit"
    values["processing_version"] = "unit-only"
    config = root / "config/job2-runtime.json"
    config.write_text(json.dumps(values))
    return root, config


@pytest.mark.parametrize("equals_form", [False, True])
def test_script_imports_from_other_cwd_without_pythonpath(
    deployed_repo, tmp_path, equals_form,
):
    root, _ = deployed_repo
    arguments = (
        ["--runtime-config=config/job2-runtime.json"] if equals_form else
        ["--runtime-config", "config/job2-runtime.json"]
    )
    # -I는 PYTHONPATH와 현재 디렉터리의 암묵적 import를 모두 배제한다.
    result = subprocess.run(
        [sys.executable, "-I", str(root / "scripts/run_job2_databricks.py"),
         *arguments, "--check-config"],
        cwd=tmp_path, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "config OK (offline" in result.stderr


@pytest.mark.parametrize("filename_source", ["compiled", "argv"])
def test_exec_without_file_imports_from_other_cwd(
    deployed_repo, tmp_path, filename_source,
):
    root, _ = deployed_repo
    script = root / "scripts/run_job2_databricks.py"
    harness = """
from pathlib import Path
import sys

script, filename_source = sys.argv[1:]
source = Path(script).read_text()
filename = script if filename_source == 'compiled' else '<databricks-exec>'
sys.argv = [
    '/runtime/python-task-wrapper.py' if filename_source == 'compiled'
    else script,
    '--runtime-config', 'config/job2-runtime.json', '--check-config',
]
namespace = {'__name__': '__main__'}
exec(compile(source, filename, 'exec'), namespace)
assert '__file__' not in namespace
assert namespace['REPO_ROOT'] == Path(script).parent.parent
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", harness, str(script), filename_source],
        cwd=tmp_path, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "config OK (offline" in result.stderr


def test_exec_does_not_guess_repo_from_cwd(deployed_repo, monkeypatch):
    root, _ = deployed_repo
    source = (root / "scripts/run_job2_databricks.py").read_text()
    monkeypatch.chdir(root)
    monkeypatch.setattr(sys, "argv", ["/runtime/python-task-wrapper.py"])
    with pytest.raises(RuntimeError, match="Cannot locate Job 2 repository"):
        exec(compile(source, "<databricks-exec>", "exec"),
             {"__name__": "job2_exec_test"})


def test_compiled_filename_precedes_wrapper_file(deployed_repo, monkeypatch):
    root, _ = deployed_repo
    script = root / "scripts/run_job2_databricks.py"
    monkeypatch.setattr(sys, "argv", [str(launcher.__file__)])
    namespace = {
        "__name__": "job2_exec_test", "__file__": str(launcher.__file__),
    }
    exec(compile(script.read_text(), str(script), "exec"), namespace)
    assert namespace["REPO_ROOT"] == root


def test_example_routes_distinct_credentials(deployed_repo):
    _, config = deployed_repo
    settings = runtime.load_runtime_config(config)
    source = settings.kafka.spark_options(settings.job.source)
    assert source["databricks.serviceCredential"] == "ktd-msk-consumer"
    assert "kafka.security.protocol" not in source
    utility = Mock()
    utility.secrets.get.return_value = "fake-user:fake-secret"
    clients = settings.resolve_clients(utility)
    utility.credentials.getServiceCredentialsProvider.assert_called_once_with(
        "ktd-msk-job2-producer",
    )
    assert clients.producer["ktd.msk.credential"] == "ktd-msk-job2-producer"
    assert clients.producer["ktd.msk.region"] == "ap-northeast-2"
    assert "oauth_cb" not in clients.producer
    assert "databricks.serviceCredential" not in clients.producer
    assert "kafka.security.protocol" not in clients.producer
    assert clients.producer["security.protocol"] == "SASL_SSL"
    utility.secrets.get.assert_called_once_with(
        scope="ktd-job2", key="schema-registry-basic-auth",
    )
    assert settings.registry.url == (
        "https://psrc-1oqld6g.ap-northeast-2.aws.confluent.cloud"
    )
    assert settings.job.silver_table == "ktd.silver.sensor_validated"


def test_offline_check_never_creates_session_or_clients(
    deployed_repo, monkeypatch,
):
    _, config = deployed_repo
    create = Mock(side_effect=AssertionError("Spark must not start"))
    resolve = Mock(side_effect=AssertionError("No secret/network access"))
    monkeypatch.setattr(runtime.RuntimeSettings, "resolve_clients", resolve)
    job.main(
        ["--runtime-config", str(config), "--check-config"],
        spark_factory=create,
    )
    create.assert_not_called()
    resolve.assert_not_called()


@pytest.mark.parametrize("failed", [False, True])
def test_launcher_uses_native_session_and_owns_only_query(
    deployed_repo, monkeypatch, failed,
):
    _, config = deployed_repo
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = Mock()
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(
        job, "_connect_session", Mock(side_effect=AssertionError("Connect")),
    )
    query = Mock(isActive=failed)
    if failed:
        query.awaitTermination.side_effect = RuntimeError("batch failed")
    start = Mock(return_value=query)
    monkeypatch.setattr(runtime, "start_configured_validation", start)
    if failed:
        with pytest.raises(RuntimeError, match="batch failed"):
            launcher.main(["--runtime-config", str(config), "--available-now"])
    else:
        launcher.main(["--runtime-config", str(config), "--available-now"])
    spark = sql.SparkSession.builder.getOrCreate.return_value
    start.assert_called_once_with(spark, str(config), available_now=True)
    query.awaitTermination.assert_called_once_with()
    assert query.stop.call_count == int(failed)
    spark.stop.assert_not_called()


def test_launcher_requires_explicit_config():
    with pytest.raises(SystemExit, match="--runtime-config is required"):
        launcher.main([])


def test_offline_check_requires_config():
    with pytest.raises(SystemExit) as error:
        job.main(["--check-config"])
    assert error.value.code == 2


def test_invalid_config_does_not_create_native_session(tmp_path):
    factory = Mock()
    with pytest.raises(runtime.RuntimeConfigError):
        job.main(
            ["--runtime-config", str(tmp_path / "missing.json")],
            spark_factory=factory,
        )
    factory.assert_not_called()


@pytest.mark.parametrize('failure_stage', [None, 'setup', 'producer', 'metadata'])
def test_metadata_diagnostic_reuses_auth_without_publishing(
    monkeypatch, capsys, failure_stage,
):
    import confluent_kafka
    from types import SimpleNamespace

    private = 'private-key private-secret private-session private-oauth'
    settings = Mock()
    settings.job.validated_topic = 'kitchen.sensor.validated'
    source_options = {'bootstrap.servers': 'unused', 'ktd.msk.credential': 'test'}
    settings.kafka.producer_options.return_value = source_options
    resolved = {'bootstrap.servers': 'unused', 'oauth_cb': Mock()}
    resolve = Mock(return_value=resolved)
    monkeypatch.setattr(runtime, 'load_runtime_config', Mock(return_value=settings))
    monkeypatch.setattr(runtime, 'runtime_producer_options', resolve)
    utility = Mock()
    monkeypatch.setattr(runtime, 'get_dbutils', Mock(return_value=utility))
    producer = Mock()
    producer.list_topics.return_value.topics = {
        'kitchen.sensor.validated': SimpleNamespace(error=None, partitions={
            1: SimpleNamespace(id=1, leader=7),
            0: SimpleNamespace(id=0, leader=-1),
        }),
    }
    factory = Mock(return_value=producer)
    monkeypatch.setattr(confluent_kafka, 'Producer', factory)
    error = confluent_kafka.KafkaException(confluent_kafka.KafkaError(
        confluent_kafka.KafkaError._TRANSPORT, private,
    ))
    if failure_stage == 'setup':
        resolve.side_effect = RuntimeError(private)
    elif failure_stage == 'producer':
        factory.side_effect = error
    elif failure_stage == 'metadata':
        producer.list_topics.side_effect = error
    spark = Mock()
    before = runtime.LOGGER.disabled
    result = launcher.check_kafka_metadata('unused.json', spark)
    assert runtime.LOGGER.disabled == before
    settings.kafka.producer_options.assert_called_once_with(utility)
    resolve.assert_called_once_with(source_options, spark=spark)
    settings.resolve_clients.assert_not_called()
    settings.registry.resolve.assert_not_called()
    producer.produce.assert_not_called()
    producer.flush.assert_not_called()
    if failure_stage is None:
        assert result == {
            'metadata_lookup': 'succeeded', 'topic': 'kitchen.sensor.validated',
            'topic_exists': True, 'topic_error_code': None,
            'topic_error_name': None, 'partition_count': 2,
            'partitions': [{'partition_id': 0, 'leader_id': -1},
                           {'partition_id': 1, 'leader_id': 7}],
        }
    else:
        assert result['metadata_lookup'] == 'failed'
        assert result['error_type'] == (
            'RuntimeError' if failure_stage == 'setup' else 'KafkaException'
        )
        if failure_stage != 'setup':
            assert result['code'] == confluent_kafka.KafkaError._TRANSPORT
            assert result['name'] == '_TRANSPORT'
    if failure_stage not in ('setup', 'producer'):
        producer.list_topics.assert_called_once_with(
            topic='kitchen.sensor.validated', timeout=10,
        )
    if failure_stage != 'setup':
        assert factory.call_args.args[0]['oauth_cb'] is resolved['oauth_cb']
        assert factory.call_args.args[0]['log_level'] == 0
    assert all(value not in str(result) for value in private.split())
    assert capsys.readouterr() == ('', '')


@pytest.mark.parametrize('success', [True, False])
def test_metadata_cli_never_starts_stream(monkeypatch, capsys, success):
    spark = Mock()
    monkeypatch.setattr(launcher, 'runtime_session', Mock(return_value=spark))
    result = {'metadata_lookup': 'succeeded' if success else 'failed'}
    diagnose = Mock(return_value=result)
    monkeypatch.setattr(launcher, 'check_kafka_metadata', diagnose)
    start = Mock()
    monkeypatch.setattr(job, 'main', start)
    args = ['--runtime-config', 'config/job2-runtime.json',
            '--check-kafka-metadata']
    if success:
        launcher.main(args)
    else:
        with pytest.raises(SystemExit) as caught:
            launcher.main(args)
        assert caught.value.code == 1
    diagnose.assert_called_once_with(
        str(launcher.REPO_ROOT / 'config/job2-runtime.json'), spark,
    )
    start.assert_not_called()
    assert json.loads(capsys.readouterr().out) == result


@pytest.mark.parametrize('outcome', ['success', 'missing', 'duplicate',
                                    'delivery_error', 'timeout'])
def test_produce_probe_reads_silver_and_publishes_once(
    monkeypatch, capsys, outcome,
):
    from io import BytesIO
    from types import SimpleNamespace

    import confluent_kafka
    import confluent_kafka.schema_registry as sr
    from fastavro import schemaless_reader

    event_id = '30008ab5-8a00-43d9-bc4a-94493ceaaea7'
    row = {
        'event_id': event_id, 'event_time': 1700000000000000,
        'store_id': 'from-silver', 'equipment_id': 'silver-equipment',
        'equipment_type': 'refrigerator', 'metric_name': 'temperature_celsius',
        'metric_value': 4.2, 'unit': 'celsius', 'schema_version': '1.0.0',
        'source': 'simulator',
    }
    spark = Mock()
    silver_row = Mock()
    silver_row.asDict.return_value = row
    spark.sql.return_value.collect.return_value = (
        [] if outcome == 'missing' else
        [silver_row] * (2 if outcome == 'duplicate' else 1)
    )
    settings = Mock()
    settings.job.silver_table = 'ktd.silver.sensor_validated'
    settings.job.validated_topic = 'kitchen.sensor.validated'
    settings.resolve_clients.return_value = SimpleNamespace(
        producer={'ktd.msk.credential': 'writer'}, registry={'url': 'unused'},
    )
    monkeypatch.setattr(runtime, 'load_runtime_config', Mock(return_value=settings))
    monkeypatch.setattr(runtime, 'get_dbutils', Mock())
    resolve = Mock(return_value={'oauth_cb': Mock()})
    monkeypatch.setattr(runtime, 'runtime_producer_options', resolve)
    schema_text = (REPO_ROOT / 'schemas/avro/sensor_metric_event.avsc').read_text()
    registry = Mock()
    registry.lookup_schema.return_value = sr.RegisteredSchema(
        schema_id=1, guid=None, schema=sr.Schema(schema_text, 'AVRO'),
        subject='kitchen.sensor.validated-value', version=1,
    )
    context = Mock()
    context.__enter__ = Mock(return_value=registry)
    context.__exit__ = Mock(return_value=False)
    registry_factory = Mock(return_value=context)
    monkeypatch.setattr(sr, 'SchemaRegistryClient', registry_factory)
    producer = Mock()
    factory = Mock(return_value=producer)
    monkeypatch.setattr(confluent_kafka, 'Producer', factory)
    message = Mock()
    message.partition.return_value = 1
    message.offset.return_value = 42

    def flush(timeout):
        assert timeout == 15.0
        if outcome == 'timeout':
            return 1
        error = None
        if outcome == 'delivery_error':
            error = confluent_kafka.KafkaError(
                confluent_kafka.KafkaError.TOPIC_AUTHORIZATION_FAILED,
                'private-token private-secret',
            )
        producer.produce.call_args.kwargs['on_delivery'](error, message)
        return 0

    producer.flush.side_effect = flush
    result = launcher.check_kafka_produce('unused.json', spark)
    spark.sql.assert_called_once()
    sql = spark.sql.call_args.args[0]
    assert sql.startswith('SELECT ') and 'LIMIT 2' in sql
    assert 'unix_micros(`event_time`)' in sql
    assert spark.sql.call_args.kwargs['args'] == {'event_id': event_id}
    if outcome in ('missing', 'duplicate'):
        factory.assert_not_called()
        settings.resolve_clients.assert_not_called()
        assert result['produce_probe'] == 'failed'
    else:
        resolve.assert_called_once_with(
            settings.resolve_clients.return_value.producer, spark=spark,
        )
        assert factory.call_args.args[0]['log_level'] == 0
        producer.produce.assert_called_once()
        producer.flush.assert_called_once_with(15.0)
        record = producer.produce.call_args.kwargs
        assert record['topic'] == 'kitchen.sensor.validated'
        assert record['key'] == b'silver-equipment'
        assert record['value'][:5] == b'\x00\x00\x00\x00\x01'
        payload = schemaless_reader(BytesIO(record['value'][5:]),
                                    json.loads(schema_text))
        assert payload == dict(row, event_time='2023-11-14T22:13:20.000000+00:00')
        registry.register_schema.assert_not_called()
        registry.register_schema_full_response.assert_not_called()
    diagnostics = result.pop('diagnostics')
    stages = [item['stage'] for item in diagnostics]
    if outcome not in ('missing', 'duplicate'):
        assert stages[:4] == [
            'producer_create', 'producer_ready',
            'produce_enter', 'produce_enqueued',
        ]
        assert 'flush_enter' in stages and 'flush_return' in stages
        assert ('delivery_callback' in stages) == (outcome != 'timeout')
        producer.poll.assert_not_called()
        producer.list_topics.assert_not_called()
        assert factory.call_args.args[0]['statistics.interval.ms'] == 1000
        assert callable(factory.call_args.args[0]['error_cb'])
        assert callable(factory.call_args.args[0]['stats_cb'])
    if outcome == 'success':
        assert result == {
            'produce_probe': 'succeeded', 'topic': 'kitchen.sensor.validated',
            'partition': 1, 'offset': 42, 'event_id': event_id,
        }
    elif outcome == 'delivery_error':
        assert result['name'] == 'TOPIC_AUTHORIZATION_FAILED'
        assert result['error_type'] == 'KafkaException'
    elif outcome == 'timeout':
        assert result['timeout'] is True
        assert result['callback_received'] is False
        assert result['remaining'] == 1
    assert 'private' not in json.dumps(result)
    assert capsys.readouterr() == ('', '')


def test_produce_probe_cli_is_exclusive_and_does_not_start_job(monkeypatch, capsys):
    spark = Mock()
    monkeypatch.setattr(launcher, 'runtime_session', Mock(return_value=spark))
    probe = Mock(return_value={'produce_probe': 'succeeded'})
    monkeypatch.setattr(launcher, 'check_kafka_produce', probe)
    start = Mock()
    monkeypatch.setattr(job, 'main', start)
    args = ['--runtime-config=config/job2-runtime.json', '--check-kafka-produce']
    launcher.main(args)
    probe.assert_called_once_with(
        str(REPO_ROOT / 'config/job2-runtime.json'), spark,
    )
    assert json.loads(capsys.readouterr().out) == {'produce_probe': 'succeeded'}
    start.assert_not_called()
    with pytest.raises(SystemExit):
        launcher.main(args + ['--check-kafka-metadata'])
    assert probe.call_count == 1


def test_produce_diagnostics_strip_native_secrets_and_addresses():
    from confluent_kafka import KafkaError

    diagnostic = launcher._ProduceProbeDiagnostics()
    private = 'secret-token broker.private:9098 access-key payload'
    diagnostic.error_cb(KafkaError(KafkaError.SASL_AUTHENTICATION_FAILED, private))
    diagnostic.stats_cb(json.dumps({
        'name': private, 'msg_cnt': 1, 'replyq': 0, 'txmsgs': 0,
        'brokers': {private: {
            'name': private, 'nodeid': 1, 'state': 'AUTH', 'tx': 4,
            'rx': 3, 'outbuf_cnt': 0, 'waitresp_cnt': 1,
            'req': {'SaslAuthenticate': 1, 'Metadata': 1, 'Produce': 0,
                    private: 123},
        }},
        'topics': {'kitchen.sensor.validated': {
            'metadata_age': 1000, 'partitions': {'0': {
                'partition': 0, 'leader': 1, 'msgq_cnt': 1,
                'xmit_msgq_cnt': 0, 'msgs_inflight': 0, 'payload': private,
            }},
        }, private: {'payload': private}},
    }))
    error, stats = diagnostic.events
    assert error['name'] == 'SASL_AUTHENTICATION_FAILED'
    assert error['code'] == KafkaError.SASL_AUTHENTICATION_FAILED
    assert stats['brokers'][0]['state'] == 'AUTH'
    assert stats['brokers'][0]['requests']['Produce'] == 0
    assert stats['partitions'][0]['msgq_cnt'] == 1
    assert stats['metadata_age'] == 1000
    for value in private.split():
        assert value not in json.dumps(diagnostic.events)
    diagnostic.stats_cb(json.dumps({'brokers': {'private': {
        'state': private, 'nodeid': private, 'tx': private,
    }}}))
    assert diagnostic.events[-1]['brokers'] == [
        {'state': 'UNKNOWN', 'requests': {}},
    ]
    diagnostic.stats_cb(private)
    assert diagnostic.events[-1]['stage'] == 'statistics_unavailable'
    assert private not in json.dumps(diagnostic.events)


@pytest.mark.parametrize('kind,exists', [
    ('missing_response', None), ('UNKNOWN_TOPIC_OR_PART', False),
    ('TOPIC_AUTHORIZATION_FAILED', None),
])
def test_topic_metadata_errors_are_safe(monkeypatch, kind, exists):
    from types import SimpleNamespace
    import confluent_kafka

    settings = Mock()
    settings.job.validated_topic = 'kitchen.sensor.validated'
    monkeypatch.setattr(runtime, 'load_runtime_config', Mock(return_value=settings))
    monkeypatch.setattr(runtime, 'get_dbutils', Mock())
    monkeypatch.setattr(runtime, 'runtime_producer_options', Mock(return_value={}))
    producer = Mock()
    monkeypatch.setattr(confluent_kafka, 'Producer', Mock(return_value=producer))
    code = None if kind == 'missing_response' else getattr(confluent_kafka.KafkaError, kind)
    producer.list_topics.return_value.topics = {} if code is None else {
        'kitchen.sensor.validated': SimpleNamespace(
            error=confluent_kafka.KafkaError(code, 'private-token private-secret'),
            partitions={},
        ),
    }
    result = launcher.check_kafka_metadata('unused.json', Mock())
    assert result['metadata_lookup'] == 'failed'
    assert result['topic_exists'] is exists
    assert result['topic_error_code'] == code
    assert result['topic_error_name'] == (kind if code is not None else None)
    assert result['partition_count'] == (0 if code is not None else None)
    assert result['partitions'] == []
    assert 'private' not in json.dumps(result)
    producer.produce.assert_not_called()
    producer.flush.assert_not_called()
