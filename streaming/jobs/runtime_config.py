"""Job 2의 비밀값 없는 JSON 설정과 실행 위치별 인증 연결을 담당한다."""

from dataclasses import dataclass, field
import json
from pathlib import Path
from urllib.parse import urlsplit

from streaming.jobs.raw_ingestion import IngestionConfig, kafka_source_options


class RuntimeConfigError(ValueError):
    """설정 값이나 외부 예외 내용을 노출하지 않는 설정 오류다."""


def _text(value, label):
    if not isinstance(value, str) or not value.strip():
        raise RuntimeConfigError(f"Required setting missing: {label}")
    return value.strip()


def _object(value, label, required, optional=()):
    if not isinstance(value, dict):
        raise RuntimeConfigError(f"Expected object: {label}")
    if set(value) - set(required) - set(optional):
        raise RuntimeConfigError(f"Unknown setting in {label}")
    if set(required) - set(value):
        raise RuntimeConfigError(f"Required setting missing in {label}")
    return value


@dataclass(frozen=True)
class SecretReference:
    """비밀값 대신 Databricks scope/key만 보관한다."""

    scope: str
    key: str

    @classmethod
    def parse(cls, value):
        value = _object(value, 'secret_reference', ('scope', 'key'))
        return cls(_text(value['scope'], 'secret.scope'),
                   _text(value['key'], 'secret.key'))

    def read(self, dbutils):
        try:
            value = dbutils.secrets.get(scope=self.scope, key=self.key)
        except Exception:
            raise RuntimeConfigError('Secret retrieval failed') from None
        if not isinstance(value, str) or not value.strip():
            raise RuntimeConfigError('Secret value is empty')
        return value


@dataclass(frozen=True)
class RegistrySettings:
    """현재 decoder와 호환되는 Confluent API만 명시적으로 허용한다."""

    url: str
    auth_mode: str
    credentials: SecretReference | None = None

    def resolve(self, dbutils):
        options = {'url': self.url, 'timeout': 10}
        if self.auth_mode == 'basic':
            value = self.credentials.read(dbutils)
            if ':' not in value or not all(value.split(':', 1)):
                raise RuntimeConfigError('Registry basic secret is invalid')
            options['basic.auth.user.info'] = value
        return options


@dataclass(frozen=True)
class KafkaSettings:
    """동일 broker와 보안 모드를 서로 다른 Kafka API 옵션으로 변환한다."""

    bootstrap_servers: str
    security_protocol: str
    mechanism: str
    region: str | None = None
    source_service_credential: str | None = None
    producer_service_credential: str | None = None

    def spark_options(self, source):
        options = kafka_source_options(source)
        options['kafka.security.protocol'] = self.security_protocol
        options['kafka.allow.auto.create.topics'] = 'false'
        # MSK IAM의 JVM 인증은 기존 Databricks service credential에 맡긴다.
        return options

    def producer_options(self, dbutils):
        options = {
            'bootstrap.servers': self.bootstrap_servers,
            'security.protocol': self.security_protocol,
            'acks': 'all',
            'delivery.report.only.error': False,
            'allow.auto.create.topics': False,
        }
        if self.mechanism == 'AWS_MSK_IAM':
            options['sasl.mechanism'] = 'OAUTHBEARER'
            options['oauth_cb'] = make_msk_oauth_callback(
                dbutils, self.producer_service_credential, self.region,
            )
        return options


def make_msk_oauth_callback(dbutils, credential_name, region):
    """UC 자격 증명을 토큰 갱신 시 읽으며 토큰은 closure에 저장하지 않는다."""
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
        from botocore.credentials import CredentialProvider

        # Databricks의 반환값은 botocore session이므로 provider로 감싼다.
        session = dbutils.credentials.getServiceCredentialsProvider(
            credential_name,
        )
    except Exception:
        raise RuntimeConfigError('MSK credential setup failed') from None

    class Provider(CredentialProvider):
        def load(self):
            return session.get_credentials()

    provider = Provider()
    sign = MSKAuthTokenProvider.generate_auth_token_from_credentials_provider

    def oauth_callback(_config):
        try:
            token, expiry_ms = sign(region, provider)
        except Exception:
            raise RuntimeConfigError('MSK token retrieval failed') from None
        return token, expiry_ms / 1000.0

    return oauth_callback


def get_dbutils(spark):
    """foreachBatch 실행 위치에서 dbutils를 생성하고 외부 객체를 운반하지 않는다."""
    from pyspark.dbutils import DBUtils

    return DBUtils(spark)


@dataclass(frozen=True)
class ClientConfigs:
    """인증이 해석된 설정은 repr에 포함하지 않는다."""

    registry: dict = field(repr=False)
    producer: dict = field(repr=False)


@dataclass(frozen=True)
class RuntimeSettings:
    """closure에는 비밀값이 아닌 참조와 비보안 설정만 전달한다."""

    job: object
    rules: object
    kafka: KafkaSettings
    registry: RegistrySettings
    rules_path: str

    def client_configs(self, spark):
        return self.resolve_clients(get_dbutils(spark))

    def resolve_clients(self, dbutils):
        return ClientConfigs(
            self.registry.resolve(dbutils),
            self.kafka.producer_options(dbutils),
        )


def load_runtime_config(path):
    """설정·규칙을 먼저 검사하며 secret 조회나 네트워크 요청을 하지 않는다."""
    from streaming.jobs.sensor_validation_job import (
        ValidationJobConfig, load_rules,
    )

    path = Path(path)
    try:
        values = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        raise RuntimeConfigError(
            'Runtime config file missing or invalid',
        ) from None
    values = _object(values, 'runtime', (
        'kafka', 'registry', 'topics', 'silver_table', 'checkpoint',
        'processing_version', 'rules_path',
    ), ('starting_offsets', 'max_offsets_per_trigger', 'bronze_checkpoint'))
    kafka = _object(values['kafka'], 'kafka', (
        'bootstrap_servers', 'security_protocol', 'mechanism',
    ), ('region', 'source_service_credential', 'producer_service_credential'))
    servers = _text(kafka['bootstrap_servers'], 'bootstrap_servers')
    if any(not part.strip() or '://' in part or '@' in part
           for part in servers.split(',')):
        raise RuntimeConfigError('Invalid bootstrap servers')
    protocol = _text(kafka['security_protocol'], 'security_protocol')
    mechanism = _text(kafka['mechanism'], 'mechanism')
    if mechanism == 'AWS_MSK_IAM':
        if protocol != 'SASL_SSL':
            raise RuntimeConfigError('MSK IAM requires SASL_SSL')
        settings = KafkaSettings(
            servers, protocol, mechanism,
            _text(kafka.get('region'), 'kafka.region'),
            _text(kafka.get('source_service_credential'), 'source credential'),
            _text(kafka.get('producer_service_credential'),
                  'producer credential'),
        )
    elif mechanism == 'NONE' and protocol in ('PLAINTEXT', 'SSL'):
        if any(kafka.get(k) is not None for k in (
            'region', 'source_service_credential',
            'producer_service_credential',
        )):
            raise RuntimeConfigError('Unexpected credentials for NONE mode')
        settings = KafkaSettings(servers, protocol, mechanism)
    else:
        raise RuntimeConfigError('Unsupported Kafka authentication mode')
    registry = _object(values['registry'], 'registry', (
        'provider', 'url', 'auth_mode',
    ), ('credentials',))
    if registry['provider'] != 'confluent':
        raise RuntimeConfigError(
            'Explicit Confluent provider required by decoder',
        )
    url = _text(registry['url'], 'registry.url')
    try:
        parsed = urlsplit(url)
        valid = (parsed.scheme in ('http', 'https') and parsed.hostname
                 and not parsed.username and not parsed.password
                 and not parsed.query and not parsed.fragment)
    except ValueError:
        valid = False
    if not valid:
        raise RuntimeConfigError('Registry URL must not contain credentials')
    mode = registry['auth_mode']
    if mode == 'basic':
        if parsed.scheme != 'https':
            raise RuntimeConfigError('Registry basic auth requires HTTPS')
        ref = SecretReference.parse(registry.get('credentials'))
    elif mode == 'none' and registry.get('credentials') is None:
        ref = None
    else:
        raise RuntimeConfigError('Invalid Registry auth mode or reference')
    topics = _object(
        values['topics'], 'topics', ('raw', 'validated', 'quarantine'),
    )
    topic_names = {k: _text(v, f'topics.{k}') for k, v in topics.items()}
    checkpoint = _text(values['checkpoint'], 'checkpoint')
    if checkpoint == values.get('bronze_checkpoint'):
        raise RuntimeConfigError('Job 1 and Job 2 checkpoints must differ')
    maximum = values.get('max_offsets_per_trigger', 1000)
    if type(maximum) is not int:
        raise RuntimeConfigError('max_offsets_per_trigger must be an integer')
    try:
        job = ValidationJobConfig(
            source=IngestionConfig(
                bootstrap_servers=servers, checkpoint=checkpoint,
                topic=topic_names['raw'],
                starting_offsets=values.get('starting_offsets', 'earliest'),
                max_offsets_per_trigger=maximum,
                service_credential=settings.source_service_credential,
            ),
            silver_table=_text(values['silver_table'], 'silver_table'),
            processing_version=_text(
                values['processing_version'], 'processing_version',
            ),
            validated_topic=topic_names['validated'],
            quarantine_topic=topic_names['quarantine'],
        )
    except (TypeError, ValueError):
        raise RuntimeConfigError('Invalid Job 2 settings') from None
    rules_path = Path(_text(values['rules_path'], 'rules_path'))
    if not rules_path.is_absolute():
        rules_path = path.resolve().parent / rules_path
    try:
        rules = load_rules(str(rules_path))
        if (not rules.supported_schema_versions or not rules.equipment_registry
                or not rules.metric_units):
            raise ValueError('Empty rules')
    except (OSError, ValueError, TypeError, KeyError):
        raise RuntimeConfigError('Rules file missing or invalid') from None
    return RuntimeSettings(
        job, rules, settings, RegistrySettings(url, mode, ref),
        str(rules_path),
    )


def start_configured_validation(
    spark, path, *, dbutils=None, available_now=False,
):
    """query 시작 전에 설정과 인증 참조를 검사하고 실제 secret은 출력하지 않는다."""
    from streaming.jobs.sensor_validation_job import start_validation

    settings = load_runtime_config(path)
    utility = dbutils if dbutils is not None else get_dbutils(spark)
    # 참조 접근 실패를 query 시작 전에 알리되 해석된 비밀값은 보관하지 않는다.
    settings.resolve_clients(utility)
    return start_validation(
        spark, settings.job, settings.rules,
        client_config_factory=settings.client_configs,
        source_options=settings.kafka.spark_options(settings.job.source),
        available_now=available_now,
    )
