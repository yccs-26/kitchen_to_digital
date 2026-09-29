"""Validate configuration without importing Spark or contacting the cloud."""

import pytest

from streaming.jobs.raw_ingestion import IngestionConfig, quoted_table


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
