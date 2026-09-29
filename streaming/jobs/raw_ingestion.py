"""Job 1: preserve Kafka transport records in UC managed Iceberg."""

import argparse
import os
import re
from dataclasses import dataclass

BRONZE_DDL = """
    topic STRING,
    partition INT,
    offset BIGINT,
    kafka_timestamp TIMESTAMP,
    key BINARY,
    value BINARY,
    headers ARRAY<STRUCT<key: STRING, value: BINARY>>,
    ingested_at TIMESTAMP
"""


def quoted_table(name: str) -> str:
    """Accept a three-part UC name; never interpolate arbitrary SQL."""
    parts = name.split(".")
    if len(parts) != 3 or any(
        not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", part) for part in parts
    ):
        raise ValueError("table must be catalog.schema.table identifiers")
    return ".".join(f"`{part}`" for part in parts)


@dataclass(frozen=True)
class IngestionConfig:
    bootstrap_servers: str
    checkpoint: str
    table: str = "ktd.bronze.sensor_raw"
    topic: str = "kitchen.sensor.raw"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int = 10000

    def __post_init__(self) -> None:
        quoted_table(self.table)
        if not self.bootstrap_servers.strip() or not self.topic.strip():
            raise ValueError("bootstrap_servers and topic are required")
        parts = self.checkpoint.split("/")
        if (
            len(parts) < 6
            or parts[:2] != ["", "Volumes"]
            or any(part in ("", ".", "..") for part in parts[2:])
        ):
            raise ValueError(
                "checkpoint must be /Volumes/catalog/schema/volume/job-path"
            )
        if self.starting_offsets not in ("earliest", "latest"):
            raise ValueError("starting_offsets must be earliest or latest")
        if self.max_offsets_per_trigger < 1:
            raise ValueError("max_offsets_per_trigger must be positive")


def ensure_bronze_table(spark, table: str) -> None:
    """Create only the table; the UC schema must already exist."""
    from pyspark.sql.types import StructType

    target = quoted_table(table)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({BRONZE_DDL}) USING ICEBERG")
    details = {
        row.col_name: row.data_type
        for row in spark.sql(f"DESCRIBE TABLE EXTENDED {target}").collect()
    }
    if details.get("Type") != "MANAGED" or (
        details.get("Provider", "").lower() != "iceberg"
    ):
        raise ValueError("existing target must be a UC managed Iceberg table")
    expected = StructType.fromDDL(BRONZE_DDL).simpleString()
    actual = spark.table(table).schema.simpleString()
    if actual != expected:
        raise ValueError(f"Bronze schema mismatch: {actual}; expected {expected}")


def kafka_source(spark, config: IngestionConfig):
    """Let Spark own offsets; retain headers including repeated keys."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.bootstrap_servers)
        .option("subscribe", config.topic)
        .option("includeHeaders", "true")
        .option("startingOffsets", config.starting_offsets)
        .option("failOnDataLoss", "true")
        .option("maxOffsetsPerTrigger", config.max_offsets_per_trigger)
        .option("kafka.default.api.timeout.ms", "15000")
        .option("kafka.request.timeout.ms", "10000")
        .load()
    )


def bronze_records(source):
    """No decoding, filtering, event identity, watermark, or deduplication."""
    from pyspark.sql import functions as F

    return source.select(
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        "key",
        "value",
        "headers",
        F.current_timestamp().alias("ingested_at"),
    )


def start_ingestion(spark, config: IngestionConfig, *, available_now=False):
    """Use the native sink and a stable checkpoint across normal restarts."""
    return start_bronze_sink(
        spark,
        bronze_records(kafka_source(spark, config)),
        config,
        available_now=available_now,
    )


def start_bronze_sink(spark, records, config, *, available_now=False):
    """Start the same native sink for ingestion and isolated sink tests."""
    ensure_bronze_table(spark, config.table)
    writer = (
        records.writeStream.format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", config.checkpoint)
        .queryName("ktd_raw_ingestion")
    )
    if available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="10 seconds")
    return writer.toTable(config.table)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()
    config = IngestionConfig(
        bootstrap_servers=os.environ["KTD_KAFKA_BOOTSTRAP_SERVERS"],
        checkpoint=os.environ["KTD_BRONZE_CHECKPOINT"],
        table=os.getenv("KTD_BRONZE_TABLE", "ktd.bronze.sensor_raw"),
        starting_offsets=os.getenv("KTD_STARTING_OFFSETS", "earliest"),
    )
    from databricks.connect import DatabricksSession

    spark = DatabricksSession.builder.getOrCreate()
    query = start_ingestion(spark, config, available_now=args.available_now)
    try:
        query.awaitTermination()
    finally:
        if query.isActive:
            query.stop()


if __name__ == "__main__":
    main()
