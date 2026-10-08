"""Spark·Kafka·Registry 없이 실제 Phase 2 컴포넌트 연결을 검증한다."""

from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
import json
from pathlib import Path
from unittest.mock import MagicMock, Mock

from confluent_kafka import KafkaError, KafkaException, Message
from confluent_kafka.schema_registry import RegisteredSchema, Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
from fastavro import schemaless_writer
import pytest

from streaming.jobs import sensor_validation_job as job
from streaming.jobs.raw_ingestion import IngestionConfig
from streaming.sinks.silver import SilverSink, SilverWriteStatus
from streaming.validation.domain_validator import (
    EquipmentSpec, validate_domain_event,
)
from streaming.validation.event_time_policy import EventTimeStatus


NOW = datetime(2026, 10, 8, 12, tzinfo=timezone.utc)
SCHEMA_TEXT = Path("schemas/avro/sensor_metric_event.avsc").read_text()
SCHEMA = json.loads(SCHEMA_TEXT)


class FakeStorage:
    """단일 호출 흐름용 테스트 저장소이며 영속 저장 증빙은 아니다."""

    def __init__(self):
        self.rows = {}
        self.lookups = 0

    def lookup(self, event_id):
        self.lookups += 1
        return self.rows.get(event_id)

    def insert_if_absent(self, event):
        if event["event_id"] in self.rows:
            return False
        self.rows[event["event_id"]] = dict(event)
        return True


class FakeProducer:
    """실제 ACK 대신 flush 시점에 callback을 실행한다."""

    def __init__(self):
        self.calls = []
        self.error = None
        self.callback = True
        self.message = Mock(spec=Message)
        self.message.offset.return_value = 3

    def produce(self, **kwargs):
        self.calls.append(kwargs)

    def flush(self, timeout):
        if self.callback:
            self.calls[-1]["on_delivery"](self.error, self.message)
        return 0


@pytest.fixture
def event():
    return {
        "event_id": "job2-test-1", "event_time": NOW.isoformat(),
        "store_id": "store-001", "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius", "metric_value": 4.2,
        "unit": "celsius", "schema_version": "1.0.0", "source": "simulator",
    }


def raw_record(event, **changes):
    stream = BytesIO()
    schemaless_writer(stream, SCHEMA, event)
    return {
        "topic": "kitchen.sensor.raw", "partition": 2, "offset": 42,
        "key": b"fridge-001", "value": b"\x00\x00\x00\x00\x01"
        + stream.getvalue(), **changes,
    }


@pytest.fixture
def config():
    return job.ValidationJobConfig(
        source=IngestionConfig(
            "broker:9092", "/Volumes/ktd/silver/checkpoints/job2/sensor_raw",
        ),
        silver_table="ktd.silver.sensor", processing_version="p2-7-test",
    )


@pytest.fixture
def rules():
    return job.ValidationRules(
        frozenset({"1.0.0"}),
        {"fridge-001": EquipmentSpec("store-001", "refrigerator")},
        {"refrigerator": {"temperature_celsius": "celsius"}},
    )


@pytest.fixture
def dependencies(config, rules):
    registry = Mock()
    registry.get_schema.return_value = Schema(SCHEMA_TEXT, "AVRO")
    return dict(
        registry_client=registry, silver=SilverSink(FakeStorage()),
        quarantine_producer=FakeProducer(), validated_producer=FakeProducer(),
        serializer=Mock(return_value=b"serialized-avro"),
        config=config, rules=rules, reference_time=NOW,
    )


def test_valid_insert_and_lineage(event, dependencies):
    result = job.process_record(raw_record(event), **dependencies)
    assert result.status is job.RecordStatus.VALIDATED
    assert result.completed
    assert result.lineage == ("kitchen.sensor.raw", 2, 42)
    assert result.silver.status is SilverWriteStatus.INSERTED
    assert result.event_time.status is EventTimeStatus.ON_TIME
    assert dependencies["quarantine_producer"].calls == []
    sent = dependencies["validated_producer"].calls[0]
    assert sent["topic"] == "kitchen.sensor.validated"
    assert sent["key"] == b"fridge-001"
    payload = dependencies["serializer"].call_args.args[0]
    assert payload["event_id"] == event["event_id"]
    assert payload["event_time"] == "2026-10-08T12:00:00.000000+00:00"


def test_invalid_preserves_all_errors_in_one_record(event, dependencies):
    event.update(unit="wrong", store_id="wrong", schema_version="wrong")
    raw = raw_record(event, key=b"other")
    result = job.process_record(raw, **dependencies)
    expected = validate_domain_event(
        {**event, "event_time": NOW}, raw["key"],
        supported_schema_versions=(
            dependencies["rules"].supported_schema_versions
        ),
        equipment_registry=dependencies["rules"].equipment_registry,
        metric_units=dependencies["rules"].metric_units,
        reference_time=NOW, max_future_skew=timedelta(minutes=5),
    )
    assert result.status is job.RecordStatus.QUARANTINED
    assert result.completed
    assert result.quarantine.errors == expected.errors
    assert len(expected.errors) == 4
    assert result.quarantine.raw_value_sha256 == (
        sha256(raw["value"]).hexdigest()
    )
    assert result.quarantine.raw_key == raw["key"]
    assert result.quarantine.schema_id == 1
    assert result.quarantine.event_id == event["event_id"]
    assert dependencies["silver"]._storage.lookups == 0
    assert dependencies["validated_producer"].calls == []
    calls = dependencies["quarantine_producer"].calls
    assert len(calls) == 1
    payload = json.loads(calls[0]["value"])
    assert len(payload["errors"]) == 4
    assert payload["processing_version"] == "p2-7-test"
    assert (payload["source_topic"], payload["source_partition"],
            payload["source_offset"]) == result.lineage


@pytest.mark.parametrize("value,code,schema_id", [
    (None, "INVALID_CONFLUENT_FRAME", None),
    (b"", "INVALID_CONFLUENT_FRAME", None),
    (b"broken", "INVALID_CONFLUENT_FRAME", None),
    (b"\x00\x00\x00\x00\x01", "AVRO_DECODE_FAILED", 1),
    (b"\x00\x00\x00\x00\x01\x10a", "AVRO_DECODE_FAILED", 1),
])
def test_corrupt_quarantine_without_event_id(
    event, dependencies, value, code, schema_id,
):
    raw = raw_record(event, value=value, key=None)
    first = job.process_record(raw, **dependencies)
    second = job.process_record(raw, **dependencies)
    assert first == second
    assert first.completed
    assert first.quarantine.event_id is None
    assert first.quarantine.schema_id == schema_id
    assert first.quarantine.errors[0].code == code
    assert first.quarantine.raw_value_sha256 == (
        sha256(value).hexdigest() if value is not None else None
    )
    assert dependencies["validated_producer"].calls == []
    assert dependencies["silver"]._storage.lookups == 0


def test_unknown_schema_is_quarantined(event, dependencies):
    dependencies["registry_client"].get_schema.side_effect = (
        SchemaRegistryError(404, 40403, "Unknown schema")
    )
    result = job.process_record(raw_record(event), **dependencies)
    assert result.quarantine.errors[0].code == "UNKNOWN_SCHEMA_ID"
    assert result.quarantine.schema_id == 1
    assert result.quarantine.event_id is None


@pytest.mark.parametrize("error", [
    SchemaRegistryError(401, 40101, "Unauthorized"),
    SchemaRegistryError(503, 50001, "Unavailable"),
    TimeoutError("registry timeout"), ValueError("registry config error"),
])
def test_registry_system_failure_is_not_quarantined(
    event, dependencies, error,
):
    dependencies["registry_client"].get_schema.side_effect = error
    with pytest.raises(type(error)) as caught:
        job.process_record(raw_record(event), **dependencies)
    assert caught.value is error
    assert dependencies["quarantine_producer"].calls == []
    assert dependencies["validated_producer"].calls == []


def test_invalid_registry_schema_is_system_failure(event, dependencies):
    dependencies["registry_client"].get_schema.return_value = Schema(
        "{", "AVRO",
    )
    with pytest.raises(json.JSONDecodeError):
        job.process_record(raw_record(event), **dependencies)
    assert dependencies["quarantine_producer"].calls == []


@pytest.mark.parametrize("timestamp", ["bad", "2026-10-08T12:00:00"])
def test_time_normalization_failure_is_quarantined(
    event, dependencies, timestamp,
):
    result = job.process_record(
        raw_record({**event, "event_time": timestamp}), **dependencies,
    )
    assert result.quarantine.errors[0].code == "INVALID_EVENT_TIME"
    assert result.quarantine.event_id == event["event_id"]
    assert dependencies["validated_producer"].calls == []


@pytest.mark.parametrize("minutes,expected", [
    (-11, EventTimeStatus.LATE), (-10, EventTimeStatus.ON_TIME),
    (5, EventTimeStatus.ON_TIME),
])
def test_late_valid_and_time_boundaries(
    event, dependencies, minutes, expected,
):
    event["event_time"] = (NOW + timedelta(minutes=minutes)).isoformat()
    result = job.process_record(raw_record(event), **dependencies)
    assert result.completed
    assert result.event_time.status is expected
    assert result.silver.status is SilverWriteStatus.INSERTED
    assert len(dependencies["validated_producer"].calls) == 1


def test_future_invalid_never_reaches_silver(event, dependencies):
    event["event_time"] = (NOW + timedelta(minutes=6)).isoformat()
    result = job.process_record(raw_record(event), **dependencies)
    assert result.quarantine.errors[0].code == "FUTURE_EVENT_TIME"
    assert dependencies["silver"]._storage.lookups == 0
    assert dependencies["validated_producer"].calls == []


def test_duplicate_is_republished_and_conflict_is_quarantined(
    event, dependencies,
):
    raw = raw_record(event)
    job.process_record(raw, **dependencies)
    duplicate = job.process_record(raw, **dependencies)
    conflict = job.process_record(
        raw_record({**event, "metric_value": 7.2}), **dependencies,
    )
    assert duplicate.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert duplicate.completed
    assert conflict.status is job.RecordStatus.QUARANTINED
    assert conflict.silver.differing_fields == ("metric_value",)
    assert conflict.completed
    assert len(dependencies["validated_producer"].calls) == 2
    assert len(dependencies["quarantine_producer"].calls) == 1


@pytest.mark.parametrize("destination", ["quarantine", "validated"])
@pytest.mark.parametrize("failure", ["produce", "flush", "ack", "timeout"])
def test_publish_failure_never_completes(
    event, dependencies, destination, failure,
):
    if destination == "quarantine":
        event["unit"] = "wrong"
    producer = dependencies[f"{destination}_producer"]
    error_type = RuntimeError
    if failure in ("produce", "flush"):
        setattr(producer, failure, Mock(side_effect=RuntimeError("failed")))
    elif failure == "ack":
        producer.error = KafkaError(KafkaError._MSG_TIMED_OUT)
        error_type = KafkaException
    else:
        producer.callback = False
        error_type = TimeoutError
    with pytest.raises(error_type):
        job.process_record(raw_record(event), **dependencies)
    if destination == "quarantine":
        assert dependencies["silver"]._storage.lookups == 0
        assert dependencies["validated_producer"].calls == []


@pytest.mark.parametrize("operation", ["lookup", "insert_if_absent"])
def test_silver_failure_propagates(event, dependencies, operation):
    failure = OSError("Silver storage failed")
    setattr(
        dependencies["silver"]._storage, operation, Mock(side_effect=failure),
    )
    with pytest.raises(OSError) as caught:
        job.process_record(raw_record(event), **dependencies)
    assert caught.value is failure
    assert dependencies["validated_producer"].calls == []


def test_serialization_failure_and_retry(event, dependencies):
    raw = raw_record(event)
    dependencies["serializer"].side_effect = ValueError("serialization failed")
    with pytest.raises(ValueError):
        job.process_record(raw, **dependencies)
    assert dependencies["validated_producer"].calls == []
    dependencies["serializer"].side_effect = None
    result = job.process_record(raw, **dependencies)
    assert result.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert result.completed


def test_publish_failure_then_same_record_retry(event, dependencies):
    raw = raw_record(event)
    producer = dependencies["validated_producer"]
    producer.callback = False
    with pytest.raises(TimeoutError):
        job.process_record(raw, **dependencies)
    producer.callback = True
    result = job.process_record(raw, **dependencies)
    assert result.silver.status is SilverWriteStatus.DUPLICATE_NOOP
    assert result.completed
    assert len(producer.calls) == 2


@pytest.mark.parametrize("invalid", [False, True])
def test_spark_binary_input_is_copied_without_mutation(
    event, dependencies, invalid,
):
    if invalid:
        event["unit"] = "wrong"
    raw = raw_record(event)
    raw["key"], raw["value"] = bytearray(raw["key"]), bytearray(raw["value"])
    original = deepcopy(raw)
    assert job.process_record(raw, **dependencies).completed
    assert raw == original


def test_topic_injection_and_processing_version(event, dependencies):
    config = replace(
        dependencies["config"],
        source=replace(dependencies["config"].source, topic="custom.raw"),
        quarantine_topic="custom.quarantine",
        validated_topic="custom.validated",
        processing_version="custom-version",
    )
    dependencies["config"] = config
    job.process_record(raw_record(event, topic="custom.raw"), **dependencies)
    job.process_record(raw_record(
        {**event, "unit": "bad"}, topic="custom.raw",
    ), **dependencies)
    assert dependencies["validated_producer"].calls[0]["topic"] == (
        "custom.validated"
    )
    call = dependencies["quarantine_producer"].calls[0]
    assert call["topic"] == "custom.quarantine"
    assert json.loads(call["value"])["processing_version"] == "custom-version"


class FakeBatch:
    """Spark 변환 호출과 iterator 소비만 확인하며 Spark 실행은 모사하지 않는다."""

    def __init__(self, *records):
        self.records = records
        self.sparkSession = Mock()
        self.selected = None
        self.ordered = None

    def select(self, *columns):
        self.selected = columns
        return self

    def orderBy(self, *columns):
        self.ordered = columns
        return self

    def toLocalIterator(self):
        for record in self.records:
            yield Mock(asDict=Mock(return_value=dict(record)))


def test_batch_counts_and_projection(event, dependencies):
    batch = FakeBatch(
        raw_record(event), raw_record(event, offset=43),
        raw_record({**event, "unit": "bad"}, offset=44),
        raw_record({**event, "event_id": "late", "event_time": (
            NOW - timedelta(hours=1)
        ).isoformat()}, offset=45),
    )
    assert job.process_batch(batch, 3, **dependencies) == {
        "received": 4, "validated": 3, "quarantine": 1,
        "duplicate": 1, "late": 1, "conflict": 0,
    }
    assert batch.selected == ("topic", "partition", "offset", "key", "value")
    assert batch.ordered == ("topic", "partition", "offset")


def test_empty_batch_has_no_sink_io(dependencies):
    assert job.process_batch(FakeBatch(), 0, **dependencies) == dict(
        received=0, validated=0, quarantine=0, conflict=0, duplicate=0, late=0,
    )
    assert dependencies["validated_producer"].calls == []
    assert dependencies["quarantine_producer"].calls == []


def test_conflict_is_isolated_and_batch_continues(event, dependencies):
    batch = FakeBatch(
        raw_record(event),
        raw_record({**event, "metric_value": 7.2}, offset=43),
        raw_record({**event, "event_id": "next-event"}, offset=44),
    )
    counts = job.process_batch(batch, 1, **dependencies)
    assert counts == dict(
        received=3, validated=2, quarantine=1, conflict=1, duplicate=0, late=0,
    )
    assert len(dependencies["validated_producer"].calls) == 2
    assert len(dependencies["quarantine_producer"].calls) == 1
    assert dependencies["silver"]._storage.rows[event["event_id"]] == {
        **event, "event_time": NOW,
    }


def test_partial_batch_retry_republishes_completed_records(
    event, dependencies,
):
    second = raw_record({**event, "unit": "bad"}, offset=43)
    batch = FakeBatch(raw_record(event), second)
    dependencies["quarantine_producer"].callback = False
    with pytest.raises(TimeoutError):
        job.process_batch(batch, 2, **dependencies)
    dependencies["quarantine_producer"].callback = True
    counts = job.process_batch(batch, 2, **dependencies)
    assert counts["duplicate"] == 1
    assert len(dependencies["validated_producer"].calls) == 2


def test_handler_creates_clients_at_execution_and_uses_batch_session(
    event, config, rules, monkeypatch,
):
    monkeypatch.setattr(job, "datetime", Mock(now=Mock(return_value=NOW)))
    registry = MagicMock()
    registry.__enter__.return_value = registry
    registry.get_schema.return_value = Schema(SCHEMA_TEXT, "AVRO")
    registry.lookup_schema.return_value = RegisteredSchema(
        schema_id=1, guid=None, schema=Schema(SCHEMA_TEXT, "AVRO"),
        subject="kitchen.sensor.validated-value", version=1,
    )
    registry_factory = Mock(return_value=registry)
    storage_factory = Mock(return_value=FakeStorage())
    producers = [FakeProducer(), FakeProducer()]
    producer_factory = Mock(side_effect=producers)
    monkeypatch.setattr(job, "SchemaRegistryClient", registry_factory)
    monkeypatch.setattr(job, "DeltaSilverStorage", storage_factory)
    monkeypatch.setattr(job, "Producer", producer_factory)
    handler = job.make_batch_handler(
        config, rules, registry_config={"url": "http://unused"},
        producer_config={"bootstrap.servers": "unused", "acks": "0"},
    )
    registry_factory.assert_not_called()
    producer_factory.assert_not_called()
    batch = FakeBatch(raw_record(event))
    assert handler(batch, 1)["validated"] == 1
    storage_factory.assert_called_once_with(
        spark=batch.sparkSession, table_name="ktd.silver.sensor",
    )
    assert producer_factory.call_count == 2
    settings = producer_factory.call_args.args[0]
    assert settings["acks"] == "all"
    assert settings["delivery.report.only.error"] is False
    assert settings["allow.auto.create.topics"] is False
    registry.__exit__.assert_called_once()
    assert producers[1].calls[0]["value"][:5] == b"\x00\x00\x00\x00\x01"


@pytest.mark.parametrize("available_now", [False, True])
def test_stream_wiring_reuses_source_and_dedicated_checkpoint(
    config, rules, monkeypatch, available_now,
):
    spark, source, writer, query = Mock(), Mock(), Mock(), Mock()
    source.writeStream = writer
    for name in (
        "outputMode", "option", "queryName", "foreachBatch", "trigger",
    ):
        getattr(writer, name).return_value = writer
    writer.start.return_value = query
    source_factory = Mock(return_value=source)
    monkeypatch.setattr(job, "kafka_source", source_factory)
    result = job.start_validation(
        spark, config, rules, registry_config={"url": "unused"},
        producer_config={}, available_now=available_now,
    )
    assert result is query
    source_factory.assert_called_once_with(spark, config.source)
    writer.option.assert_called_once_with(
        "checkpointLocation", config.source.checkpoint,
    )
    writer.foreachBatch.assert_called_once()
    writer.trigger.assert_called_once_with(**(
        {"availableNow": True} if available_now
        else {"processingTime": "10 seconds"}
    ))


@pytest.mark.parametrize("changes", [
    {"processing_version": ""}, {"silver_table": "bad"},
    {"quarantine_topic": "kitchen.sensor.raw"},
    {"validated_topic": "kitchen.sensor.quarantine"},
    {"allowed_lateness": timedelta(seconds=-1)},
    {"max_future_skew": timedelta(seconds=-1)},
    {"delivery_timeout": 0}, {"delivery_timeout": float("nan")},
])
def test_invalid_config_fails_before_io(config, changes):
    with pytest.raises(ValueError):
        replace(config, **changes)


def test_job1_checkpoint_rejected(config):
    with pytest.raises(ValueError, match="job2"):
        replace(config, source=replace(config.source, checkpoint=(
            "/Volumes/ktd/bronze/checkpoints/job1/sensor_raw"
        )))


def test_transactions_rejected(config, rules):
    with pytest.raises(ValueError, match="Transactional"):
        job.make_batch_handler(
            config, rules, registry_config={},
            producer_config={"transactional.id": "not-supported"},
        )


@pytest.mark.parametrize("duplicate", [False, True])
def test_load_rules_checks_equipment_uniqueness(tmp_path, duplicate):
    item = dict(equipment_id="fridge-001", store_id="store-001",
                equipment_type="refrigerator")
    values = {
        "supported_schema_versions": ["1.0.0"],
        "equipment_registry": [item, item] if duplicate else [item],
        "metric_units": {"refrigerator": {"temperature_celsius": "celsius"}},
    }
    path = tmp_path / "rules.json"
    path.write_text(json.dumps(values))
    if duplicate:
        with pytest.raises(ValueError, match="globally unique"):
            job.load_rules(str(path))
    else:
        assert job.load_rules(str(path)).equipment_registry == {
            "fridge-001": EquipmentSpec("store-001", "refrigerator"),
        }


def test_real_delta_adapter_is_called_before_publish(event, dependencies):
    calls = []
    canonical = {**event, "event_time": int(NOW.timestamp()) * 1_000_000}
    responses = [[], [], [], [Mock(asDict=Mock(return_value=canonical))]]

    def sql(statement, *, args):
        assert dependencies["validated_producer"].calls == []
        calls.append((statement, args))
        return Mock(collect=Mock(return_value=responses.pop(0)))

    spark = Mock(sql=Mock(side_effect=sql))
    dependencies["silver"] = SilverSink(job.DeltaSilverStorage(
        spark=spark, table_name="ktd.silver.sensor",
    ))
    result = job.process_record(raw_record(event), **dependencies)
    assert result.silver.status is SilverWriteStatus.INSERTED
    assert "MERGE INTO" in calls[2][0]
    assert calls[2][1]["event_time"] == NOW
    assert len(dependencies["validated_producer"].calls) == 1


def test_delta_sql_failure_is_not_quarantined(event, dependencies):
    error = RuntimeError("MERGE unavailable")
    spark = Mock(sql=Mock(side_effect=error))
    dependencies["silver"] = SilverSink(job.DeltaSilverStorage(
        spark=spark, table_name="ktd.silver.sensor",
    ))
    with pytest.raises(RuntimeError) as caught:
        job.process_record(raw_record(event), **dependencies)
    assert caught.value is error
    assert dependencies["validated_producer"].calls == []
    assert dependencies["quarantine_producer"].calls == []


def test_unexpected_decoder_failure_propagates(
    event, dependencies, monkeypatch,
):
    from streaming.validation import avro_decoder

    error = RuntimeError("unexpected decoder failure")
    monkeypatch.setattr(
        avro_decoder, "schemaless_reader", Mock(side_effect=error),
    )
    with pytest.raises(RuntimeError) as caught:
        job.process_record(raw_record(event), **dependencies)
    assert caught.value is error
    assert dependencies["quarantine_producer"].calls == []


@pytest.mark.parametrize("changes", [
    {"topic": "unexpected"}, {"partition": -1}, {"partition": True},
    {"offset": None}, {"key": "text"}, {"value": "text"},
])
def test_malformed_source_row_fails_before_any_io(
    event, dependencies, changes,
):
    with pytest.raises((TypeError, ValueError)):
        job.process_record(raw_record(event, **changes), **dependencies)
    dependencies["registry_client"].get_schema.assert_not_called()
    assert dependencies["quarantine_producer"].calls == []


def test_batch_stops_at_first_failure(event, dependencies):
    dependencies["validated_producer"].callback = False
    batch = FakeBatch(raw_record(event), raw_record(
        {**event, "event_id": "must-not-process"}, offset=43,
    ))
    with pytest.raises(TimeoutError):
        job.process_batch(batch, 1, **dependencies)
    assert len(dependencies["validated_producer"].calls) == 1
    assert "must-not-process" not in dependencies["silver"]._storage.rows


def test_handler_closes_registry_on_batch_failure(config, rules, monkeypatch):
    registry = MagicMock()
    registry.__enter__.return_value = registry
    monkeypatch.setattr(
        job, "SchemaRegistryClient", Mock(return_value=registry),
    )
    monkeypatch.setattr(job, "Producer", Mock())
    error = OSError("batch failed")
    monkeypatch.setattr(job, "process_batch", Mock(side_effect=error))
    handler = job.make_batch_handler(
        config, rules, registry_config={}, producer_config={},
    )
    with pytest.raises(OSError) as caught:
        handler(FakeBatch(), 1)
    assert caught.value is error
    registry.__exit__.assert_called_once()


@pytest.mark.parametrize("failed", [False, True])
def test_entrypoint_owns_query_lifecycle_only(
    config, rules, monkeypatch, failed,
):
    import sys
    from types import ModuleType

    fake_connect = ModuleType("databricks.connect")
    session = Mock()
    fake_connect.DatabricksSession = Mock()
    fake_connect.DatabricksSession.builder.getOrCreate.return_value = session
    monkeypatch.setitem(sys.modules, "databricks.connect", fake_connect)
    monkeypatch.setattr(
        sys, "argv", ["sensor_validation_job", "--available-now"],
    )
    monkeypatch.setenv("KTD_KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("KTD_VALIDATION_CHECKPOINT", config.source.checkpoint)
    monkeypatch.setenv("KTD_SILVER_TABLE", config.silver_table)
    monkeypatch.setenv("KTD_PROCESSING_VERSION", "test")
    monkeypatch.setenv("KTD_VALIDATION_RULES_PATH", "unused.json")
    monkeypatch.setenv("KTD_SCHEMA_REGISTRY_CONFIG_JSON", '{"url":"unused"}')
    monkeypatch.setenv("KTD_PRODUCER_CONFIG_JSON", '{}')
    monkeypatch.delenv("KTD_BRONZE_CHECKPOINT", raising=False)
    monkeypatch.setattr(job, "load_rules", Mock(return_value=rules))
    query = Mock(isActive=True)
    if failed:
        query.awaitTermination.side_effect = RuntimeError("query failed")
    start = Mock(return_value=query)
    monkeypatch.setattr(job, "start_validation", start)
    if failed:
        with pytest.raises(RuntimeError, match="query failed"):
            job.main()
    else:
        job.main()
    assert start.call_args.args[0] is session
    assert start.call_args.kwargs["available_now"] is True
    query.stop.assert_called_once()
    session.stop.assert_not_called()


def test_conflict_evidence_preserves_fields_lineage_and_canonical(
    event, dependencies,
):
    job.process_record(raw_record(event), **dependencies)
    canonical = deepcopy(dependencies["silver"]._storage.rows)
    incoming = {**event, "metric_value": 9.5, "source": "other-source"}
    raw = raw_record(incoming, offset=43)
    original = deepcopy(raw)
    result = job.process_record(raw, **dependencies)
    assert result.completed
    assert result.status is job.RecordStatus.QUARANTINED
    assert result.silver.status is SilverWriteStatus.CONFLICT
    assert result.silver.differing_fields == ("metric_value", "source")
    assert tuple(error.field for error in result.quarantine.errors) == (
        result.silver.differing_fields
    )
    assert all(error.code == "EVENT_ID_CONFLICT"
               for error in result.quarantine.errors)
    calls = dependencies["quarantine_producer"].calls
    assert len(calls) == 1
    payload = json.loads(calls[0]["value"])
    assert [error["field"] for error in payload["errors"]] == [
        "metric_value", "source",
    ]
    assert payload["event_id"] == event["event_id"]
    assert payload["schema_id"] == 1
    assert payload["processing_version"] == "p2-7-test"
    assert (payload["source_topic"], payload["source_partition"],
            payload["source_offset"]) == ("kitchen.sensor.raw", 2, 43)
    assert result.quarantine.raw_key == raw["key"]
    assert payload["raw_value_sha256"] == sha256(raw["value"]).hexdigest()
    assert dependencies["silver"]._storage.rows == canonical
    assert len(dependencies["validated_producer"].calls) == 1
    assert raw == original


@pytest.mark.parametrize("failure", ["produce", "flush", "ack", "timeout"])
def test_conflict_quarantine_failure_blocks_batch_until_ack(
    event, dependencies, failure,
):
    job.process_record(raw_record(event), **dependencies)
    canonical = deepcopy(dependencies["silver"]._storage.rows)
    producer = dependencies["quarantine_producer"]
    if failure in ("produce", "flush"):
        setattr(producer, failure, Mock(side_effect=RuntimeError("failed")))
        error_type = RuntimeError
    elif failure == "ack":
        producer.error = KafkaError(KafkaError._MSG_TIMED_OUT)
        error_type = KafkaException
    else:
        producer.callback = False
        error_type = TimeoutError
    batch = FakeBatch(
        raw_record({**event, "metric_value": 9.5}, offset=43),
        raw_record({**event, "event_id": "next-event"}, offset=44),
    )
    with pytest.raises(error_type):
        job.process_batch(batch, 2, **dependencies)
    assert dependencies["silver"]._storage.rows == canonical
    assert len(dependencies["validated_producer"].calls) == 1
    dependencies["quarantine_producer"] = FakeProducer()
    counts = job.process_batch(batch, 2, **dependencies)
    assert counts["conflict"] == counts["quarantine"] == 1
    assert counts["validated"] == 1
    assert counts["received"] == 2
    assert dependencies["silver"]._storage.rows[event["event_id"]] == (
        canonical[event["event_id"]]
    )


def test_conflict_replay_after_checkpoint_failure_has_stable_evidence(
    event, dependencies,
):
    job.process_record(raw_record(event), **dependencies)
    raw = raw_record({**event, "metric_value": 9.5}, offset=43)
    first = job.process_record(raw, **dependencies)
    assert first.completed
    # 격리 ACK 후 checkpoint 실패를 가정하고 같은 원본을 다시 처리한다.
    second = job.process_record(raw, **dependencies)
    assert second == first
    calls = dependencies["quarantine_producer"].calls
    assert len(calls) == 2
    assert calls[0]["key"] == calls[1]["key"]
    assert calls[0]["value"] == calls[1]["value"]
    assert len(dependencies["validated_producer"].calls) == 1
    other = job.process_record({**raw, "offset": 44}, **dependencies)
    assert other.quarantine.quarantine_id != first.quarantine.quarantine_id
