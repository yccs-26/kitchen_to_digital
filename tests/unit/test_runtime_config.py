"""네트워크 없이 JSON 계약, 인증 경계와 클라이언트 옵션을 검증한다."""

from copy import deepcopy
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

from streaming.jobs import runtime_config as runtime
from streaming.jobs import sensor_validation_job as job
from streaming.jobs.raw_ingestion import kafka_source_options


@pytest.fixture
def config_file(tmp_path):
    rules = Path('config/validation_rules.json').read_text()
    (tmp_path / 'rules.json').write_text(rules)
    values = {
        'kafka': {
            'bootstrap_servers': 'broker.example:9098',
            'security_protocol': 'SASL_SSL', 'mechanism': 'AWS_MSK_IAM',
            'region': 'ap-northeast-2',
            'source_service_credential': 'reader',
            'producer_service_credential': 'writer',
        },
        'registry': {
            'provider': 'confluent', 'url': 'https://registry.example',
            'auth_mode': 'basic',
            'credentials': {'scope': 'test-scope', 'key': 'registry-auth'},
        },
        'topics': {
            'raw': 'raw', 'validated': 'validated', 'quarantine': 'bad',
        },
        'silver_table': 'ktd.silver.sensor_validated',
        'checkpoint': '/Volumes/ktd/bronze/checkpoints/job2/test',
        'processing_version': 'test', 'rules_path': 'rules.json',
    }
    path = tmp_path / 'runtime.json'
    path.write_text(json.dumps(values))
    return path


def change(path, keys, value, *, delete=False):
    data = json.loads(path.read_text())
    node = data
    for key in keys[:-1]:
        node = node[key]
    if delete:
        del node[keys[-1]]
    else:
        node[keys[-1]] = value
    path.write_text(json.dumps(data))


def test_parse_reuses_rules_and_does_not_mutate_files(config_file):
    before = config_file.read_bytes()
    settings = runtime.load_runtime_config(config_file)
    assert settings.job.silver_table == 'ktd.silver.sensor_validated'
    assert settings.rules.supported_schema_versions == frozenset({'1.0.0'})
    fridge = settings.rules.equipment_registry['fridge-001']
    assert fridge.store_id == 'store-001'
    assert settings.rules_path == str(config_file.parent / 'rules.json')
    assert config_file.read_bytes() == before


@pytest.mark.parametrize('keys', [
    ('kafka', 'bootstrap_servers'), ('registry', 'url'),
    ('registry', 'provider'), ('topics', 'raw'), ('topics', 'validated'),
    ('topics', 'quarantine'), ('silver_table',), ('checkpoint',),
    ('processing_version',), ('rules_path',),
    ('registry', 'credentials', 'scope'), ('registry', 'credentials', 'key'),
    ('kafka', 'producer_service_credential'),
    ('kafka', 'source_service_credential'), ('kafka', 'region'),
])
@pytest.mark.parametrize('delete', [False, True])
def test_missing_required_config_fails_before_secret_access(
    config_file, keys, delete,
):
    change(config_file, keys, '', delete=delete)
    with pytest.raises(runtime.RuntimeConfigError):
        runtime.load_runtime_config(config_file)


@pytest.mark.parametrize('contents', [None, '{broken', '[]'])
def test_missing_or_invalid_file(tmp_path, contents):
    path = tmp_path / 'missing.json'
    if contents is not None:
        path.write_text(contents)
    with pytest.raises(runtime.RuntimeConfigError):
        runtime.load_runtime_config(path)


@pytest.mark.parametrize('provider', ['aws_glue', 'other', None])
def test_registry_provider_is_not_guessed(config_file, provider):
    change(config_file, ('registry', 'provider'), provider)
    with pytest.raises(runtime.RuntimeConfigError, match='Confluent'):
        runtime.load_runtime_config(config_file)


@pytest.mark.parametrize('url', [
    'https://user:secret@registry.example',
    'https://registry.example?token=secret',
    'https://registry.example#secret', 'file:///secret',
    'http://registry.example',
])
def test_secret_in_url_or_insecure_basic_auth_is_rejected(config_file, url):
    change(config_file, ('registry', 'url'), url)
    with pytest.raises(runtime.RuntimeConfigError) as error:
        runtime.load_runtime_config(config_file)
    assert 'user:secret' not in str(error.value)


def test_inline_secret_is_not_an_accepted_setting(config_file):
    change(config_file, ('registry', 'password'), 'do-not-print')
    with pytest.raises(runtime.RuntimeConfigError) as error:
        runtime.load_runtime_config(config_file)
    assert 'do-not-print' not in str(error.value)


@pytest.mark.parametrize('mutation', ['missing', 'empty', 'duplicate'])
def test_bad_rules_fail_before_runtime(config_file, mutation):
    path = config_file.parent / 'rules.json'
    if mutation == 'missing':
        path.unlink()
    elif mutation == 'empty':
        path.write_text('{}')
    else:
        data = json.loads(path.read_text())
        data['equipment_registry'].append(data['equipment_registry'][0])
        path.write_text(json.dumps(data))
    with pytest.raises(runtime.RuntimeConfigError, match='Rules'):
        runtime.load_runtime_config(config_file)


def test_job1_checkpoint_cannot_be_reused(config_file):
    data = json.loads(config_file.read_text())
    change(config_file, ('bronze_checkpoint',), data['checkpoint'])
    with pytest.raises(runtime.RuntimeConfigError, match='checkpoints'):
        runtime.load_runtime_config(config_file)


def test_spark_and_producer_configs_have_distinct_apis(
    config_file, monkeypatch,
):
    original_file = config_file.read_bytes()
    settings = runtime.load_runtime_config(config_file)
    original = deepcopy(settings)
    callback = Mock()
    monkeypatch.setattr(
        runtime, 'make_msk_oauth_callback', Mock(return_value=callback),
    )
    spark = settings.kafka.spark_options(settings.job.source)
    producer = settings.kafka.producer_options(Mock())
    assert spark['kafka.bootstrap.servers'] == producer['bootstrap.servers']
    assert 'kafka.security.protocol' not in spark
    assert spark['databricks.serviceCredential'] == 'reader'
    assert 'sasl.mechanism' not in spark
    assert 'kafka.sasl.jaas.config' not in spark
    assert 'kafka.sasl.mechanism' not in spark
    assert producer['sasl.mechanism'] == 'OAUTHBEARER'
    assert producer['oauth_cb'] is callback
    assert 'databricks.serviceCredential' not in producer
    assert producer['acks'] == 'all'
    assert producer['allow.auto.create.topics'] is False
    assert producer == {
        'bootstrap.servers': 'broker.example:9098',
        'security.protocol': 'SASL_SSL',
        'acks': 'all',
        'delivery.report.only.error': False,
        'allow.auto.create.topics': False,
        'sasl.mechanism': 'OAUTHBEARER',
        'oauth_cb': callback,
    }
    assert settings == original
    assert config_file.read_bytes() == original_file


def test_service_credential_source_matches_job1_options(config_file):
    settings = runtime.load_runtime_config(config_file)
    expected = kafka_source_options(settings.job.source)
    expected['kafka.allow.auto.create.topics'] = 'false'
    assert settings.kafka.spark_options(settings.job.source) == expected


@pytest.mark.parametrize('protocol', ['PLAINTEXT', 'SSL'])
def test_source_without_service_credential_keeps_protocol(
    config_file, protocol,
):
    change(config_file, ('kafka',), {
        'bootstrap_servers': 'broker.example:9092',
        'security_protocol': protocol, 'mechanism': 'NONE',
    })
    settings = runtime.load_runtime_config(config_file)
    options = settings.kafka.spark_options(settings.job.source)
    assert options['kafka.security.protocol'] == protocol
    assert 'databricks.serviceCredential' not in options
    assert options['kafka.allow.auto.create.topics'] == 'false'


@pytest.mark.parametrize('protocol', ['PLAINTEXT', 'SSL'])
def test_msk_config_still_requires_sasl_ssl(config_file, protocol):
    change(config_file, ('kafka', 'security_protocol'), protocol)
    with pytest.raises(runtime.RuntimeConfigError, match='requires SASL_SSL'):
        runtime.load_runtime_config(config_file)


def test_registry_secret_retrieval_and_repr_masking(
    config_file, monkeypatch, capsys,
):
    settings = runtime.load_runtime_config(config_file)
    utility = Mock()
    utility.secrets.get.return_value = 'api-user:private-value'
    monkeypatch.setattr(runtime, 'make_msk_oauth_callback', Mock())
    clients = settings.resolve_clients(utility)
    utility.secrets.get.assert_called_once_with(
        scope='test-scope', key='registry-auth',
    )
    assert clients.registry['basic.auth.user.info'] == 'api-user:private-value'
    assert 'private-value' not in repr(clients)
    assert 'private-value' not in repr(settings)
    assert capsys.readouterr().out == ''


@pytest.mark.parametrize(
    'response', ['', 'missing-colon', ':password', 'user:'],
)
def test_invalid_secret_is_rejected(config_file, response):
    settings = runtime.load_runtime_config(config_file)
    utility = Mock()
    utility.secrets.get.return_value = response
    with pytest.raises(runtime.RuntimeConfigError):
        settings.registry.resolve(utility)


def test_secret_exception_does_not_expose_original(config_file, capsys):
    settings = runtime.load_runtime_config(config_file)
    utility = Mock()
    utility.secrets.get.side_effect = RuntimeError('private-value')
    with pytest.raises(runtime.RuntimeConfigError) as error:
        settings.registry.resolve(utility)
    assert 'private-value' not in str(error.value)
    assert error.value.__suppress_context__
    assert capsys.readouterr().out == ''


def test_explicit_local_no_auth_mode(config_file):
    change(config_file, ('kafka',), {
        'bootstrap_servers': 'localhost:9092',
        'security_protocol': 'PLAINTEXT', 'mechanism': 'NONE',
    })
    change(config_file, ('registry',), {
        'provider': 'confluent', 'url': 'http://localhost:8081',
        'auth_mode': 'none',
    })
    settings = runtime.load_runtime_config(config_file)
    utility = Mock()
    clients = settings.resolve_clients(utility)
    assert clients.registry == {'url': 'http://localhost:8081', 'timeout': 10}
    assert 'oauth_cb' not in clients.producer
    utility.secrets.get.assert_not_called()
    utility.credentials.getServiceCredentialsProvider.assert_not_called()


@pytest.mark.parametrize(
    'mechanism', ['SCRAM-SHA-512', 'PLAIN', 'OAUTHBEARER'],
)
def test_unsupported_auth_is_not_silently_downgraded(config_file, mechanism):
    change(config_file, ('kafka', 'mechanism'), mechanism)
    with pytest.raises(runtime.RuntimeConfigError):
        runtime.load_runtime_config(config_file)


def test_msk_token_refresh_uses_uc_session_and_seconds(monkeypatch):
    signer_module = ModuleType('aws_msk_iam_sasl_signer')
    signer = Mock()
    signer.generate_auth_token_from_credentials_provider.side_effect = [
        ('token1', 1700000000000), ('token2', 1700000001000),
    ]
    signer_module.MSKAuthTokenProvider = signer
    credentials_module = ModuleType('botocore.credentials')
    credentials_module.CredentialProvider = type('CredentialProvider', (), {})
    monkeypatch.setitem(sys.modules, 'aws_msk_iam_sasl_signer', signer_module)
    monkeypatch.setitem(
        sys.modules, 'botocore.credentials', credentials_module,
    )
    utility = Mock()
    session = utility.credentials.getServiceCredentialsProvider.return_value
    callback = runtime.make_msk_oauth_callback(utility, 'writer', 'region')
    assert callback(None) == ('token1', 1700000000.0)
    assert callback(None) == ('token2', 1700000001.0)
    sign = signer.generate_auth_token_from_credentials_provider
    region, provider = sign.call_args.args
    assert region == 'region'
    assert provider.load() is session.get_credentials.return_value
    get_provider = utility.credentials.getServiceCredentialsProvider
    get_provider.assert_called_once_with('writer')
    sign.side_effect = RuntimeError('token2')
    with pytest.raises(runtime.RuntimeConfigError) as error:
        callback(None)
    assert 'token2' not in str(error.value)


def test_secret_preflight_failure_never_starts_query(config_file, monkeypatch):
    utility = Mock()
    utility.secrets.get.side_effect = RuntimeError('private')
    start = Mock()
    monkeypatch.setattr(job, 'start_validation', start)
    with pytest.raises(runtime.RuntimeConfigError):
        runtime.start_configured_validation(
            Mock(), config_file, dbutils=utility,
        )
    start.assert_not_called()


def test_start_passes_only_references_to_callback(config_file, monkeypatch):
    utility = Mock()
    utility.secrets.get.return_value = 'user:secret-value'
    monkeypatch.setattr(runtime, 'make_msk_oauth_callback', Mock())
    start = Mock()
    monkeypatch.setattr(job, 'start_validation', start)
    runtime.start_configured_validation(
        Mock(), config_file, dbutils=utility, available_now=True,
    )
    kwargs = start.call_args.kwargs
    assert kwargs['available_now'] is True
    assert 'registry_config' not in kwargs
    factory = kwargs['client_config_factory']
    assert isinstance(factory.__self__, runtime.RuntimeSettings)
    assert 'secret-value' not in repr(factory.__self__)
    assert kwargs['source_options']['databricks.serviceCredential'] == 'reader'


def test_batch_resolves_references_at_execution(config_file, monkeypatch):
    settings = runtime.load_runtime_config(config_file)
    factory = Mock(return_value=runtime.ClientConfigs(
        {'url': 'https://registry.example'}, {'bootstrap.servers': 'broker'},
    ))
    registry = MagicMock()
    monkeypatch.setattr(
        job, 'SchemaRegistryClient', Mock(return_value=registry),
    )
    producer = Mock()
    monkeypatch.setattr(job, 'Producer', producer)
    process = Mock()
    monkeypatch.setattr(job, 'process_batch', process)
    handler = job.make_batch_handler(
        settings.job, settings.rules, client_config_factory=factory,
    )
    factory.assert_not_called()
    batch = SimpleNamespace(sparkSession=Mock())
    handler(batch, 1)
    factory.assert_called_once_with(batch.sparkSession)
    assert producer.call_args.args[0]['acks'] == 'all'
    registry_client = process.call_args.kwargs['registry_client']
    assert registry_client is registry.__enter__.return_value


def test_example_requires_explicit_runtime_values():
    with pytest.raises(runtime.RuntimeConfigError):
        runtime.load_runtime_config('config/job2-runtime.example.json')


def test_rules_match_current_numeric_simulator_contract():
    from simulator.equipment import EQUIPMENTS

    rules = job.load_rules('config/validation_rules.json')
    numeric = [e for e in EQUIPMENTS if e.metric_type == 'numeric']
    assert set(rules.equipment_registry) == {e.equipment_id for e in numeric}
    for equipment in numeric:
        units = rules.metric_units[equipment.equipment_type]
        assert units[equipment.metric_name] == equipment.unit


@pytest.mark.parametrize('field,value', [
    ('supported_schema_versions', '1.0.0'),
    ('supported_schema_versions', ['']),
    ('equipment_registry', {'fridge-001': 'store-001'}),
    ('equipment_registry', [None]),
    ('equipment_registry', [{
        'equipment_id': 'fridge-001', 'store_id': '',
        'equipment_type': 'refrigerator',
    }]),
    ('metric_units', []),
    ('metric_units', {'refrigerator': 'celsius'}),
    ('metric_units', {'refrigerator': {'temperature_celsius': ''}}),
])
def test_malformed_rules_fail_before_batch(config_file, field, value):
    change(config_file.parent / 'rules.json', (field,), value)
    with pytest.raises(runtime.RuntimeConfigError, match='Rules'):
        runtime.load_runtime_config(config_file)


def test_source_options_reach_spark_without_mutation(config_file, monkeypatch):
    settings = runtime.load_runtime_config(config_file)
    options = settings.kafka.spark_options(settings.job.source)
    before = deepcopy(options)
    spark = MagicMock()
    handler = Mock()
    monkeypatch.setattr(job, 'make_batch_handler', Mock(return_value=handler))
    job.start_validation(
        spark, settings.job, settings.rules,
        client_config_factory=Mock(), source_options=options,
    )
    spark.readStream.format.assert_called_once_with('kafka')
    spark.readStream.format.return_value.options.assert_called_once_with(
        **options,
    )
    assert options == before


def test_resolved_configs_cannot_mix_with_secret_factory(config_file):
    settings = runtime.load_runtime_config(config_file)
    with pytest.raises(ValueError, match='either'):
        job.make_batch_handler(
            settings.job, settings.rules, registry_config={'url': 'unused'},
            client_config_factory=Mock(),
        )


def test_cli_invalid_config_never_creates_session(tmp_path, monkeypatch):
    connect = ModuleType('databricks.connect')
    connect.DatabricksSession = Mock()
    monkeypatch.setitem(sys.modules, 'databricks.connect', connect)
    monkeypatch.setattr(sys, 'argv', [
        'job2', '--runtime-config', str(tmp_path / 'absent.json'),
    ])
    with pytest.raises(runtime.RuntimeConfigError):
        job.main()
    connect.DatabricksSession.builder.getOrCreate.assert_not_called()


def test_cli_runtime_route_waits_and_stops_only_owned_query(
    config_file, monkeypatch,
):
    connect = ModuleType('databricks.connect')
    connect.DatabricksSession = Mock()
    monkeypatch.setitem(sys.modules, 'databricks.connect', connect)
    monkeypatch.setattr(sys, 'argv', [
        'job2', '--runtime-config', str(config_file), '--available-now',
    ])
    start = Mock()
    monkeypatch.setattr(runtime, 'start_configured_validation', start)
    job.main()
    spark = connect.DatabricksSession.builder.getOrCreate.return_value
    start.assert_called_once_with(
        spark, str(config_file), available_now=True,
    )
    start.return_value.awaitTermination.assert_called_once_with()
    start.return_value.stop.assert_called_once_with()
    spark.stop.assert_not_called()
