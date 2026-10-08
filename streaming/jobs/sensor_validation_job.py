"""Job 2의 주입 가능한 레코드 처리와 순차 foreachBatch 연결.

Spark가 checkpoint를 관리한다. 개별 sink ACK가 모두 확인된 배치만 반환하며
실패 시 이미 완료된 레코드도 재처리될 수 있다. Silver는 영속 멱등 경계로,
Kafka는 event_id 또는 quarantine_id로 중복을 식별한다. exactly-once는 아니다.
단일 query·단일 Silver writer가 전제이며 실제 클라우드 동작은 미검증이다.
"""

import argparse
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
import json
import logging
import math
import os
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import (
    SchemaRegistryClient, topic_subject_name_strategy,
)
from confluent_kafka.schema_registry.avro import AvroSerializer

from streaming.jobs.raw_ingestion import (
    IngestionConfig, kafka_source, quoted_table,
)
from streaming.quarantine.quarantine_publisher import (
    DeliveryProducer, publish_quarantine_record,
)
from streaming.quarantine.quarantine_record import (
    QuarantineRecord, build_quarantine_record,
)
from streaming.sinks.delta_silver import DeltaSilverStorage
from streaming.sinks.silver import (
    SilverSink, SilverWriteResult, SilverWriteStatus,
)
from streaming.sinks.validated_publisher import (
    ValueSerializer, deliver_validated_event,
)
from streaming.validation.avro_decoder import (
    SensorDecodeError, decode_sensor_event, parse_confluent_frame,
)
from streaming.validation.domain_validator import (
    EquipmentSpec, ValidationError, ValidationResult, validate_domain_event,
)
from streaming.validation.event_time_policy import (
    EventTimeClassification, EventTimeStatus, classify_event_time,
)


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationRules:
    """기존 validator에 전달할 설정이며 새로운 검증 규칙을 정의하지 않는다."""

    supported_schema_versions: frozenset[str]
    equipment_registry: Mapping[str, EquipmentSpec]
    metric_units: Mapping[str, Mapping[str, str]]


@dataclass(frozen=True)
class ValidationJobConfig:
    """Job 1의 소스 옵션을 재사용하되 Job 2 checkpoint를 분리한다."""

    source: IngestionConfig
    silver_table: str
    processing_version: str
    quarantine_topic: str = "kitchen.sensor.quarantine"
    validated_topic: str = "kitchen.sensor.validated"
    allowed_lateness: timedelta = timedelta(minutes=10)
    max_future_skew: timedelta = timedelta(minutes=5)
    delivery_timeout: float = 15.0

    def __post_init__(self) -> None:
        quoted_table(self.silver_table)
        # Job 1과 경로를 공유하지 않도록 전용 job2 하위 경로를 요구한다.
        if "job2" not in self.source.checkpoint.split("/")[5:]:
            raise ValueError("checkpoint must use a dedicated job2 directory")
        for name in (
            "processing_version", "quarantine_topic", "validated_topic",
        ):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a non-blank string")
        if len({self.source.topic, self.quarantine_topic,
                self.validated_topic}) != 3:
            raise ValueError(
                "raw, quarantine and validated topics must differ"
            )
        if self.allowed_lateness < timedelta(0):
            raise ValueError("allowed_lateness must be non-negative")
        if self.max_future_skew < timedelta(0):
            raise ValueError("max_future_skew must be non-negative")
        if (
            isinstance(self.delivery_timeout, bool)
            or not isinstance(self.delivery_timeout, (int, float))
            or not math.isfinite(self.delivery_timeout)
            or self.delivery_timeout <= 0
        ):
            raise ValueError("delivery_timeout must be positive and finite")


class RecordStatus(Enum):
    VALIDATED = "VALIDATED"
    QUARANTINED = "QUARANTINED"


@dataclass(frozen=True)
class RecordResult:
    """발행 또는 격리 ACK 완료 결과이며 Silver 충돌 증거도 보존한다."""

    status: RecordStatus
    lineage: tuple[str, int, int]
    silver: SilverWriteResult | None = None
    event_time: EventTimeClassification | None = None
    quarantine: QuarantineRecord | None = None

    @property
    def completed(self) -> bool:
        return self.status in (
            RecordStatus.VALIDATED, RecordStatus.QUARANTINED,
        )


def _raw_bytes(value: object) -> bytes | None:
    """Spark BINARY의 bytearray 표현을 복사하고 잘못된 입력은 거부한다."""
    if value is None or isinstance(value, bytes):
        return value
    if isinstance(value, bytearray):
        return bytes(value)
    raise TypeError("Kafka key/value must be bytes, bytearray or None")


def process_record(
    raw: Mapping[str, object], *, registry_client, silver: SilverSink,
    quarantine_producer: DeliveryProducer,
    validated_producer: DeliveryProducer, serializer: ValueSerializer,
    config: ValidationJobConfig, rules: ValidationRules,
    reference_time: datetime,
) -> RecordResult:
    """decode부터 ACK까지 처리하며 데이터 오류 외의 실패는 그대로 전파한다.

    동일 reference_time과 의존성 상태를 주면 같은 분류를 얻는다. 재시작 때
    처리 시각이 달라지면 late/future 분류도 달라질 수 있으며 영속 상태가 아니다.
    """
    topic, partition, offset = (raw[name] for name in (
        "topic", "partition", "offset",
    ))
    if topic != config.source.topic:
        raise ValueError("Unexpected raw topic")
    for name, value in (("partition", partition), ("offset", offset)):
        if type(value) is not int or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
    key, value = _raw_bytes(raw["key"]), _raw_bytes(raw["value"])
    lineage = (topic, partition, offset)

    def quarantine(
        validation: ValidationResult, *, schema_id=None, event_id=None,
    ) -> RecordResult:
        record = build_quarantine_record(
            validation, source_topic=topic, source_partition=partition,
            source_offset=offset, raw_key=key, raw_value=value,
            event_id=event_id, schema_id=schema_id,
        )
        publish_quarantine_record(
            record, quarantine_producer,
            processing_version=config.processing_version,
            topic=config.quarantine_topic, timeout=config.delivery_timeout,
        )
        return RecordResult(
            RecordStatus.QUARANTINED, lineage, quarantine=record,
        )

    try:
        event = decode_sensor_event(value, registry_client)
    except SensorDecodeError as exc:
        return quarantine(
            ValidationResult((
                ValidationError(exc.code, exc.field, str(exc)),
            )),
            schema_id=exc.schema_id, event_id=exc.event_id,
        )
    schema_id, _ = parse_confluent_frame(value)
    validation = validate_domain_event(
        event, key,
        supported_schema_versions=rules.supported_schema_versions,
        equipment_registry=rules.equipment_registry,
        metric_units=rules.metric_units, reference_time=reference_time,
        max_future_skew=config.max_future_skew,
    )
    if not validation.is_valid:
        event_id = event.get("event_id")
        return quarantine(
            validation, schema_id=schema_id,
            event_id=event_id if isinstance(event_id, str) else None,
        )
    timing = classify_event_time(
        event["event_time"], reference_time=reference_time,
        allowed_lateness=config.allowed_lateness,
        max_future_skew=config.max_future_skew,
    )
    # 정상적으로는 앞선 공유 시각 검증에서 제외되며 계약 불일치도 막는다.
    if timing.status is EventTimeStatus.FUTURE_INVALID:
        raise RuntimeError("Future event passed domain validation")
    delivery = deliver_validated_event(
        event, silver, validated_producer, serializer,
        topic=config.validated_topic, timeout=config.delivery_timeout,
    )
    if delivery.silver.status is SilverWriteStatus.CONFLICT:
        # 충돌 필드마다 오류를 담되 원본 레코드당 Quarantine은 한 번만 발행한다.
        # 격리 ACK 전 실패는 전파하므로 증거 없이 checkpoint가 진행되지 않는다.
        isolated = quarantine(
            ValidationResult(tuple(
                ValidationError(
                    "EVENT_ID_CONFLICT", field,
                    "Incoming payload differs from "
                    "the stored Silver canonical.",
                )
                for field in delivery.silver.differing_fields
            )),
            schema_id=schema_id, event_id=event["event_id"],
        )
        return replace(isolated, silver=delivery.silver, event_time=timing)
    return RecordResult(
        RecordStatus.VALIDATED, lineage, delivery.silver, timing,
    )


def process_batch(batch, batch_id: int, **dependencies) -> dict[str, int]:
    """배치 내 순차 처리로 단일 writer를 유지하고 전체 성공 때만 계수를 반환한다.

    입력은 파티션·offset 순서로 처리한다. 전체 collect 대신 iterator를 쓰지만
    최대 파티션 크기만큼 메모리를 사용할 수 있다. 병렬 sink 쓰기는 하지 않는다.
    계수는 이번 배치 시도 기준이며 재처리 중복을 제거한 운영 누계가 아니다.
    """
    counts = dict(
        received=0, validated=0, quarantine=0, conflict=0, duplicate=0, late=0,
    )
    records = batch.select("topic", "partition", "offset", "key", "value")
    ordered = records.orderBy("topic", "partition", "offset")
    for row in ordered.toLocalIterator():
        result = process_record(row.asDict(), **dependencies)
        if not result.completed:
            raise RuntimeError(f"Record did not complete at {result.lineage}")
        counts["received"] += 1
        if result.status is RecordStatus.QUARANTINED:
            counts["quarantine"] += 1
            counts["conflict"] += (
                result.silver is not None
                and result.silver.status is SilverWriteStatus.CONFLICT
            )
        else:
            counts["validated"] += 1
            counts["duplicate"] += (
                result.silver.status is SilverWriteStatus.DUPLICATE_NOOP
            )
            counts["late"] += result.event_time.status is EventTimeStatus.LATE
    LOGGER.info("Job 2 completed batch_id=%s counts=%s", batch_id, counts)
    return counts


def make_batch_handler(
    config: ValidationJobConfig, rules: ValidationRules, *,
    registry_config: Mapping[str, object] | None = None,
    producer_config: Mapping[str, object] | None = None,
    client_config_factory=None,
):
    """직렬화 가능한 설정만 캡처하고 클라이언트는 배치 실행 위치에서 생성한다."""
    schema = (Path(__file__).resolve().parents[2]
              / "schemas/avro/sensor_metric_event.avsc").read_text(
                  encoding="utf-8"
              )
    if client_config_factory is not None and (
        registry_config is not None or producer_config is not None
    ):
        raise ValueError("Use either config references or resolved configs")
    registry_settings = dict(registry_config or {})
    producer_settings = {
        **(producer_config or {}),
        "acks": "all", "delivery.report.only.error": False,
        "allow.auto.create.topics": False,
    }
    if "transactional.id" in producer_settings:
        raise ValueError("Transactional producers are not supported")

    def handle_batch(batch, batch_id):
        # SparkSession과 native producer를 closure에 캡처하지 않는다.
        if client_config_factory is None:
            batch_registry = registry_settings
            batch_producer = producer_settings
        else:
            # 비밀값은 foreachBatch 실행 위치에서 조회하며 closure에 넣지 않는다.
            resolved = client_config_factory(batch.sparkSession)
            batch_registry = resolved.registry
            batch_producer = {
                **resolved.producer,
                "acks": "all", "delivery.report.only.error": False,
                "allow.auto.create.topics": False,
            }
            if "transactional.id" in batch_producer:
                raise ValueError("Transactional producers are not supported")
        with SchemaRegistryClient(batch_registry) as registry:
            serializer = AvroSerializer(
                registry, schema,
                conf={
                    "auto.register.schemas": False,
                    "use.latest.version": False,
                    "subject.name.strategy": topic_subject_name_strategy,
                },
            )
            return process_batch(
                batch, batch_id, registry_client=registry,
                silver=SilverSink(DeltaSilverStorage(
                    spark=batch.sparkSession, table_name=config.silver_table,
                )),
                quarantine_producer=Producer(batch_producer),
                validated_producer=Producer(batch_producer),
                serializer=serializer, config=config, rules=rules,
                reference_time=datetime.now(timezone.utc),
            )

    return handle_batch


def start_validation(
    spark, config: ValidationJobConfig, rules: ValidationRules, *,
    registry_config: Mapping[str, object] | None = None,
    producer_config: Mapping[str, object] | None = None,
    client_config_factory=None,
    source_options: Mapping[str, object] | None = None,
    available_now: bool = False,
):
    """raw를 독립 소비하고 Spark의 전용 checkpoint에 진행 상태를 맡긴다."""
    handler = make_batch_handler(
        config, rules, registry_config=registry_config,
        producer_config=producer_config,
        client_config_factory=client_config_factory,
    )
    if source_options is None:
        source = kafka_source(spark, config.source)
    else:
        source = spark.readStream.format("kafka").options(**source_options).load()
    writer = (
        source.writeStream.outputMode("append")
        .option("checkpointLocation", config.source.checkpoint)
        .queryName("ktd_sensor_validation")
        .foreachBatch(handler)
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")
    return writer.start()


def load_rules(path: str) -> ValidationRules:
    """명시된 JSON 설정만 읽으며 simulator 목록을 운영 규칙으로 추정하지 않는다."""
    values = json.loads(Path(path).read_text(encoding="utf-8"))
    # 잘못된 규칙 구조가 배치 처리 중 드러나지 않도록 파일 경계에서 검사한다.
    def nonblank(value):
        return isinstance(value, str) and bool(value.strip())

    if not isinstance(values, dict):
        raise ValueError("Rules must be a JSON object")
    versions = values.get("supported_schema_versions")
    entries = values.get("equipment_registry")
    units = values.get("metric_units")
    if (not isinstance(versions, list) or not versions
            or not all(nonblank(version) for version in versions)):
        raise ValueError("Invalid supported schema versions")
    if not isinstance(entries, list) or not entries:
        raise ValueError("Invalid equipment registry")
    if not isinstance(units, dict) or not units:
        raise ValueError("Invalid metric units")
    for equipment_type, metrics in units.items():
        if (not nonblank(equipment_type) or not isinstance(metrics, dict)
                or not metrics
                or not all(nonblank(k) and nonblank(v)
                           for k, v in metrics.items())):
            raise ValueError("Invalid metric units")
    equipment = {}
    for item in entries:
        if (not isinstance(item, dict)
                or not all(nonblank(item.get(key)) for key in (
                    "equipment_id", "store_id", "equipment_type",
                )) or item["equipment_type"] not in units):
            raise ValueError("Invalid equipment registry entry")
        equipment_id = item["equipment_id"]
        if equipment_id in equipment:
            raise ValueError("equipment_id must be globally unique in rules")
        equipment[equipment_id] = EquipmentSpec(
            item["store_id"], item["equipment_type"],
        )
    return ValidationRules(
        frozenset(values["supported_schema_versions"]), equipment,
        values["metric_units"],
    )


def main() -> None:
    """환경 설정을 읽은 뒤에만 세션을 만들며 소유한 query만 종료한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--runtime-config")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.runtime_config:
        from streaming.jobs.runtime_config import (
            load_runtime_config, start_configured_validation,
        )
        # 파일 누락과 계약 오류는 세션 생성 전에 드러낸다.
        load_runtime_config(args.runtime_config)
        from databricks.connect import DatabricksSession

        spark = DatabricksSession.builder.getOrCreate()
        query = start_configured_validation(
            spark, args.runtime_config, available_now=args.available_now,
        )
        try:
            query.awaitTermination()
        finally:
            if query.isActive:
                query.stop()
        return
    config = ValidationJobConfig(
        source=IngestionConfig(
            bootstrap_servers=os.environ["KTD_KAFKA_BOOTSTRAP_SERVERS"],
            checkpoint=os.environ["KTD_VALIDATION_CHECKPOINT"],
            topic=os.getenv("KTD_RAW_TOPIC", "kitchen.sensor.raw"),
            starting_offsets=os.getenv("KTD_STARTING_OFFSETS", "earliest"),
            max_offsets_per_trigger=int(os.getenv("KTD_MAX_OFFSETS", "1000")),
            service_credential=os.getenv("KTD_KAFKA_SERVICE_CREDENTIAL"),
        ),
        silver_table=os.environ["KTD_SILVER_TABLE"],
        processing_version=os.environ["KTD_PROCESSING_VERSION"],
        quarantine_topic=os.getenv(
            "KTD_QUARANTINE_TOPIC", "kitchen.sensor.quarantine",
        ),
        validated_topic=os.getenv(
            "KTD_VALIDATED_TOPIC", "kitchen.sensor.validated",
        ),
        allowed_lateness=timedelta(seconds=float(os.getenv(
            "KTD_ALLOWED_LATENESS_SECONDS", "600",
        ))),
        max_future_skew=timedelta(seconds=float(os.getenv(
            "KTD_MAX_FUTURE_SKEW_SECONDS", "300",
        ))),
    )
    if config.source.checkpoint == os.getenv("KTD_BRONZE_CHECKPOINT"):
        raise ValueError("Job 1 and Job 2 must not share a checkpoint")
    rules = load_rules(os.environ["KTD_VALIDATION_RULES_PATH"])
    registry_config = json.loads(os.environ["KTD_SCHEMA_REGISTRY_CONFIG_JSON"])
    producer_config = json.loads(os.environ["KTD_PRODUCER_CONFIG_JSON"])

    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    query = start_validation(
        spark, config, rules, registry_config=registry_config,
        producer_config=producer_config, available_now=args.available_now,
    )
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()


if __name__ == "__main__":
    main()
