"""Job 2의 비밀값 없는 JSON 설정과 실행 위치별 인증 연결을 담당한다."""

from dataclasses import dataclass, field
import json
import logging
import math
from pathlib import Path
import re
import time
from threading import Lock
from urllib.parse import urlsplit

from streaming.jobs.raw_ingestion import IngestionConfig, kafka_source_options

LOGGER = logging.getLogger(__name__)


def _oauth_progress(stage, started):
    """인증 데이터 없이 실행 단계와 경과 시간만 기록한다."""
    LOGGER.warning(
        'Job2 OAuth stage=%s elapsed_ms=%.0f',
        stage, (time.monotonic() - started) * 1000,
    )


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
        # 서비스 자격 증명이 있으면 Job 1처럼 JVM 인증 옵션을 DBR에 맡긴다.
        if not source.service_credential:
            options['kafka.security.protocol'] = self.security_protocol
        options['kafka.allow.auto.create.topics'] = 'false'
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
            # driver에서 접근을 확인하되 session/provider는 운반하지 않는다.
            try:
                dbutils.credentials.getServiceCredentialsProvider(
                    self.producer_service_credential,
                )
            except Exception:
                raise RuntimeConfigError(
                    'MSK credential setup failed',
                ) from None
            options['ktd.msk.credential'] = self.producer_service_credential
            options['ktd.msk.region'] = self.region
        return options


def make_msk_oauth_callback(dbutils, credential_name, region):
    """UC 자격 증명을 토큰 갱신 시 읽으며 토큰은 closure에 저장하지 않는다."""
    try:
        # Databricks의 반환값은 botocore session이므로 provider로 감싼다.
        session = dbutils.credentials.getServiceCredentialsProvider(
            credential_name,
        )
    except Exception:
        raise RuntimeConfigError('MSK credential setup failed') from None

    return _msk_callback_from_session(session, region)


def _msk_callback_from_session(session, region):
    """worker 내부에만 갱신 가능한 session과 signer를 보관한다."""
    from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    from botocore.credentials import CredentialProvider

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


def _mint_msk_token(credential, region):
    """scalar UDF 안에서만 service credential을 읽고 단기 토큰을 발급한다."""
    started = time.monotonic()
    stage = 'service_credentials_import'
    _oauth_progress(stage, started)
    try:
        from databricks.service_credentials import (
            getServiceCredentialsProvider,
        )

        stage = 'service_credentials_provider'
        _oauth_progress(stage, started)
        session = getServiceCredentialsProvider(credential)
        stage = 'oauth_callback_setup'
        _oauth_progress(stage, started)
        callback = _msk_callback_from_session(session, region)
        stage = 'token_signing'
        _oauth_progress(stage, started)
        result = callback(None)
        _oauth_progress('udf_return', started)
        return result
    except Exception as error:
        _oauth_progress(stage + '_failed', started)
        # 원문에는 credential/token이 섞일 수 있어 허용된 원인만 전달한다.
        reason = 'unclassified_error_message_redacted'
        if isinstance(error, ImportError):
            reason = 'dependency_import_failed'
        elif 'can only be called in a Python UDF' in str(error):
            reason = 'python_udf_context_required'
        elif isinstance(error, PermissionError):
            reason = 'permission_denied'
        raise RuntimeConfigError(
            'MSK worker credential setup failed '
            f'[stage={stage}; type={type(error).__name__}; '
            f'reason={reason}]',
        ) from None


def runtime_producer_options(options, *, spark=None):
    """배치의 OAuth 갱신 요청을 지원되는 scalar UDF 실행 위치로 전달한다."""
    options = dict(options)
    credential = options.pop('ktd.msk.credential', None)
    region = options.pop('ktd.msk.region', None)
    if credential is None:
        return options
    if spark is None:
        raise RuntimeConfigError('MSK authentication requires batch SparkSession')

    from pyspark.sql.functions import udf

    def mint():
        # UDF에는 이름과 region만 전달하고 session/credential은 전달하지 않는다.
        from streaming.jobs.runtime_config import _mint_msk_token

        return _mint_msk_token(credential, region)

    mint_udf = udf(mint, 'struct<token:string,expiry:double>').asNondeterministic()

    # Spark 계획은 호출 스레드에서 만든다. 갱신 스레드의 active session에 의존하지 않는다.
    query = spark.range(1).select(mint_udf().alias('oauth'))

    def fetch_token():
        started = time.monotonic()
        stage = 'udf_collect'
        try:
            _oauth_progress(stage, started)
            row = query.first().oauth
            stage = 'token_validate'
            _oauth_progress(stage, started)
            token, expiry = row.token, row.expiry
            if (not isinstance(token, str) or not token
                    or not isinstance(expiry, (int, float))
                    or not math.isfinite(expiry) or expiry <= time.time() + 30):
                raise ValueError('Invalid token response')
            _oauth_progress('token_ready', started)
            return token, float(expiry)
        except Exception as error:
            _oauth_progress(stage + '_failed', started)
            LOGGER.warning('Job2 OAuth error_type=%s', type(error).__name__)
            # Spark가 감싼 UDF 진단은 허용된 필드만 복구한다.
            safe = re.search(
                r'MSK worker credential setup failed '
                r'\[stage=(service_credentials_import|'
                r'service_credentials_provider|oauth_callback_setup|'
                r'token_signing); type=[A-Za-z_][A-Za-z0-9_]*; '
                r'reason=(dependency_import_failed|python_udf_context_required|'
                r'permission_denied|unclassified_error_message_redacted)\]',
                str(error),
            )
            raise RuntimeConfigError(
                safe.group(0) if safe else 'MSK token retrieval failed '
                f'[stage=scalar_udf; type={type(error).__name__}]',
            ) from None

    # Producer 생성의 10초 대기 전에 발급을 마친다. 배치 메모리에만 보관한다.
    cached = fetch_token()
    now = time.time()
    refresh_at = now + (cached[1] - now) * 0.75
    lock = Lock()
    LOGGER.warning('Job2 OAuth stage=prefetch_ready')

    def oauth_callback(_config):
        nonlocal cached, refresh_at
        started = time.monotonic()
        _oauth_progress('callback_enter', started)
        # 두 Producer의 초기 호출은 같은 토큰을 사용하고 갱신은 직렬화한다.
        with lock:
            if time.time() >= refresh_at:
                refreshed = fetch_token()
                now = time.time()
                cached = refreshed
                refresh_at = now + (cached[1] - now) * 0.75
            _oauth_progress('callback_return', started)
            return cached

    options['oauth_cb'] = oauth_callback
    LOGGER.warning('Job2 OAuth stage=callback_registered')
    return options


def get_dbutils(spark):
    """Job 시작 위치에서만 dbutils를 생성한다."""
    from pyspark.dbutils import DBUtils

    return DBUtils(spark)


@dataclass(frozen=True)
class ClientConfigs:
    """인증이 해석된 설정은 repr에 포함하지 않는다."""

    registry: dict = field(repr=False)
    producer: dict = field(repr=False)


@dataclass(frozen=True)
class RuntimeSettings:
    """driver에서 해석할 설정과 secret 참조를 보관한다."""

    job: object
    rules: object
    kafka: KafkaSettings
    registry: RegistrySettings
    rules_path: str

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
    # secret은 driver에서 한 번 해석하고 직렬화 가능한 설정만 넘긴다.
    resolved = settings.resolve_clients(utility)
    return start_validation(
        spark, settings.job, settings.rules,
        registry_config=resolved.registry, producer_config=resolved.producer,
        source_options=settings.kafka.spark_options(settings.job.source),
        available_now=available_now,
    )
