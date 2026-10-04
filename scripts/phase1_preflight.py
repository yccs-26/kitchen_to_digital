"""Read-only checks from Databricks compute, not from the local Kafka client."""

import os

from databricks.connect import DatabricksSession


def main() -> None:
    spark = DatabricksSession.builder.getOrCreate()
    print(f"Spark: {spark.version}", flush=True)
    print(f"Range: {spark.range(3).collect()}", flush=True)
    print(f"Catalogs: {spark.sql('SHOW CATALOGS').collect()}", flush=True)
    print("Checking Kafka metadata and a bounded read from compute", flush=True)
    source = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KTD_KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", "kitchen.sensor.raw")
        .option("includeHeaders", "true")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .option("kafka.default.api.timeout.ms", "10000")
        .option("kafka.request.timeout.ms", "5000")
        .option("fetchOffset.numRetries", "0")
        .load()
    )
    print(source.select("topic", "partition", "offset").limit(1).collect())
    print("Kafka preflight passed")


if __name__ == "__main__":
    main()
