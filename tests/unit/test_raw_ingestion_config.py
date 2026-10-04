import pytest

from streaming.jobs.raw_ingestion import (
    IngestionConfig,
    kafka_source_options,
    quoted_table,
)


@pytest.mark.parametrize(
    "name", ["sensor_raw", "ktd.bronze.x; DROP TABLE x", "ktd..x", "a.b.c.d"]
)
def test_rejects_invalid_table_identifiers(name):
    with pytest.raises(ValueError, match="catalog.schema.table"):
        quoted_table(name)


@pytest.mark.parametrize(
    "path",
    [
        "/tmp/checkpoint",
        "dbfs:/checkpoints",
        "s3://bucket/checkpoint",
        "/Volumes/ktd/bronze/checkpoints",
        "/Volumes/ktd/bronze/v/../job",
        "/Volumes/ktd/bronze/v/job/",
        "/Volumes/ktd//v/job",
    ],
)
def test_requires_dedicated_volume_subdirectory(path):
    with pytest.raises(ValueError, match="checkpoint"):
        IngestionConfig("broker:9092", path)


def test_valid_config_preserves_table_and_checkpoint():
    path = "/Volumes/ktd/bronze/checkpoints/job1/sensor_raw"
    config = IngestionConfig("broker:9092", path)
    assert config.checkpoint == path
    assert quoted_table(config.table) == "`ktd`.`bronze`.`sensor_raw`"
    assert config.service_credential is None


@pytest.mark.parametrize("credential", [None, "", "test-msk-consumer"])
def test_kafka_source_options_with_optional_service_credential(credential):
    config = IngestionConfig(
        "broker:9092",
        "/Volumes/ktd/bronze/checkpoints/job1",
        topic="test.raw",
        starting_offsets="latest",
        max_offsets_per_trigger=123,
        service_credential=credential,
    )
    expected = {
        "kafka.bootstrap.servers": "broker:9092",
        "subscribe": "test.raw",
        "includeHeaders": "true",
        "startingOffsets": "latest",
        "failOnDataLoss": "true",
        "maxOffsetsPerTrigger": 123,
        "kafka.default.api.timeout.ms": "15000",
        "kafka.request.timeout.ms": "10000",
    }
    if credential:
        expected["databricks.serviceCredential"] = credential
    assert kafka_source_options(config) == expected


@pytest.mark.parametrize(
    "options",
    [
        {"bootstrap_servers": ""},
        {"topic": ""},
        {"max_offsets_per_trigger": 0},
        {"starting_offsets": "typo"},
    ],
)
def test_rejects_invalid_source_configuration(options):
    values = {
        "bootstrap_servers": "broker:9092",
        "checkpoint": "/Volumes/ktd/bronze/checkpoints/job1",
        **options,
    }
    with pytest.raises(ValueError):
        IngestionConfig(**values)
