"""기존 Kafka reader가 재사용하는 wire 변환과 executor Registry factory."""

from dataclasses import dataclass
import os
import re


@dataclass(frozen=True)
class ExecutorRegistryFactory:
    """승인 compute의 Secret 참조로 주입된 환경변수만 worker에서 읽는다.

    closure에는 환경변수 이름/URL만 있고 credential/client/dbutils는 없다.
    실제 Secret scope/key와 환경변수 binding은 배포 수용 절차에서 확인한다.
    """

    url: str
    credential_env: str

    def __post_init__(self):
        from urllib.parse import urlsplit

        parsed = urlsplit(self.url)
        if (parsed.scheme != "https" or not parsed.hostname
                or parsed.username or parsed.password or parsed.query
                or parsed.fragment or parsed.path not in ("", "/")
                or not re.fullmatch(r"[A-Z][A-Z0-9_]*", self.credential_env)):
            raise ValueError("Invalid Registry reference")

    def __call__(self):
        from confluent_kafka.schema_registry import SchemaRegistryClient

        credentials = os.environ.get(self.credential_env)
        if (not credentials or ":" not in credentials
                or "{{secrets/" in credentials):
            raise RuntimeError("REGISTRY_EXECUTOR_SECRET_UNAVAILABLE")
        return SchemaRegistryClient({
            "url": self.url, "basic.auth.user.info": credentials,
        })


class _RegistryBoundary:
    """이전 checkout에서도 P2 최신 40403 구분을 보존하는 얇은 위임 경계."""

    def __init__(self, client):
        self.client = client

    def get_schema(self, schema_id):
        from confluent_kafka.schema_registry.error import SchemaRegistryError

        try:
            return self.client.get_schema(schema_id)
        except SchemaRegistryError as error:
            if error.http_status_code == 404 and error.error_code != 40403:
                # Phase2 decoder는 건드리지 않고 transport/config 오류를 분리한다.
                raise RuntimeError("REGISTRY_SERVICE_UNAVAILABLE") from None
            raise


def decode_record(value, key, registry):
    """Phase2 decoder/UTC를 재사용한다. offset commit이나 domain 재검증은 없다."""
    from streaming.jobs.metric_aggregation import VALIDATED_FIELDS
    from streaming.validation.avro_decoder import decode_sensor_event

    decoded = decode_sensor_event(
        bytes(value) if value is not None else None,
        _RegistryBoundary(registry),
    )
    if bytes(key or b"") != decoded["equipment_id"].encode("utf-8"):
        raise ValueError("Validated Kafka key must match equipment_id")
    return {name: decoded[name].isoformat() if name == "event_time"
            else decoded[name] for name in VALIDATED_FIELDS}


def transform_kafka_records(source, registry_factory):
    """실제 Kafka source와 로컬 binary DataFrame이 같은 UDF 경로를 사용한다."""
    from pyspark.sql import functions as functions
    from streaming.jobs.metric_aggregation import validated_schema

    registry = None

    def decode(value, key):
        nonlocal registry
        if registry is None:
            registry = registry_factory()
        return decode_record(value, key, registry)

    decoded = functions.udf(decode, validated_schema())
    return source.select(decoded("value", "key").alias("event")).select(
        "event.*"
    )
