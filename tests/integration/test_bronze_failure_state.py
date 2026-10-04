import json
import os
from pathlib import Path
from uuid import uuid4

import pytest

from scripts.phase1_failure_state import (
    assert_audit,
    audit_lineage,
    batch_progress,
    checkpoint_batch,
    guard_resources,
    quarantine_commit,
    snapshot_tree,
)
from streaming.jobs.raw_ingestion import (
    IngestionConfig,
    kafka_source_options,
    start_ingestion,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("KTD_RUN_BRONZE_FAILURE_STATE") != "1",
        reason="Explicit MSK/Databricks failure-state opt-in required",
    ),
]


def records(rows):
    def encode(value):
        if isinstance(value, (bytes, bytearray)):
            return value.hex()
        if isinstance(value, dict):
            return {k: encode(v) for k, v in value.items()}
        if isinstance(value, list):
            return [encode(v) for v in value]
        return value

    return sorted(
        [encode(row.asDict(recursive=True)) for row in rows],
        key=lambda r: (r["topic"], r["partition"], r["offset"]),
    )


def test_msk_native_sink_failure_state():
    assert os.getenv("DATABRICKS_RUNTIME_VERSION"), (
        "Run inside a Databricks notebook driver, not local Connect/pytest"
    )
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    assert not spark.streams.active, (
        "Use an idle session; do not stop other queries"
    )
    bootstrap = os.environ["KTD_KAFKA_BOOTSTRAP_SERVERS"]
    assert all(
        ".kafka" in host and ".amazonaws.com:9098" in host
        for host in bootstrap.split(",")
    ), "Expected MSK IAM endpoints"
    credential = os.environ["KTD_KAFKA_SERVICE_CREDENTIAL"]
    assert credential.strip(), "An MSK consumer Service Credential is required"
    run_id = uuid4().hex
    checkpoint = (
        "/Volumes/ktd/bronze/checkpoints/job1-failure-test/"
        f"{run_id}/sensor_raw"
    )
    config = IngestionConfig(
        bootstrap_servers=bootstrap,
        table=f"ktd.bronze.sensor_raw_failure_test_{run_id}",
        checkpoint=checkpoint,
        service_credential=credential,
        max_offsets_per_trigger=1,
    )
    guard_resources(config.table, checkpoint, run_id)
    root = Path(checkpoint)
    assert Path("/Volumes/ktd/bronze/checkpoints").is_dir(), (
        "UC Volume unavailable"
    )
    assert not root.parent.exists(), "Run directory already exists"
    assert not spark.catalog.tableExists(config.table), (
        "Test table already exists"
    )
    evidence = root.parent / "evidence"
    evidence.mkdir(parents=True, exist_ok=False)

    def save(name, value):
        with (evidence / f"{name}.json").open("x", encoding="utf-8") as stream:
            json.dump(value, stream, default=str, indent=2, sort_keys=True)

    print(f"[RESOURCES] table={config.table} checkpoint={checkpoint}")
    save(
        "resources",
        {
            "table": config.table,
            "checkpoint": checkpoint,
            "spark_version": spark.version,
            "dbr": os.environ["DATABRICKS_RUNTIME_VERSION"],
            "kind": "failure-state reproduction, not real crash",
        },
    )

    def source_records():
        options = kafka_source_options(config)
        options.pop("maxOffsetsPerTrigger")
        try:
            rows = (
                spark.read.format("kafka")
                .options(**options)
                .option("endingOffsets", "latest")
                .load()
                .select(
                    "topic",
                    "partition",
                    "offset",
                    "key",
                    "value",
                    "headers",
                    "timestamp",
                )
                .limit(1001)
                .collect()
            )
        except Exception as error:
            raise AssertionError(
                "MSK read failed: check Kafka retention/offset availability "
                "and the underlying authentication/network error; "
                "failOnDataLoss remains true"
            ) from error
        assert len(rows) <= 1000, (
            "Bounded fixture test: source exceeds 1000 rows"
        )
        result = records(rows)
        positions = {(r["partition"], r["offset"]): r for r in result}
        assert all((1, n) in positions for n in (0, 1, 2)), (
            "Expected source partition=1 offsets=0/1/2 unavailable. "
            "Kafka retention or changed topic history blocks this experiment; "
            "do not substitute Bronze as expected source"
        )
        assert positions[1, 2]["value"] == "0000", (
            "Corrupt fixture bytes changed"
        )
        return result

    expected = source_records()
    save("expected-source", expected)

    def run(label):
        guard_resources(config.table, checkpoint, run_id)
        query = start_ingestion(spark, config, available_now=True)
        try:
            assert query.awaitTermination(600), "AvailableNow timeout"
            progress = [
                json.loads(p.json) if hasattr(p, "json") else p
                for p in query.recentProgress
            ]
            result = {
                "id": str(query.id),
                "runId": str(query.runId),
                "progress": progress,
            }
            save(label, result)
            return result
        finally:
            if query.isActive:
                query.stop()

    def audit(label):
        rows = records(spark.table(config.table).collect())
        report = audit_lineage(expected, rows)
        save(label, {"rows": rows, **report})
        assert_audit(report)
        transport = [dict(r) for r in rows]
        for row in transport:
            row.pop("ingested_at")
            row["timestamp"] = row.pop("kafka_timestamp")
        assert transport == expected, (
            "Raw source bytes/headers/timestamps changed"
        )
        return rows

    first = run("first-progress")
    before = audit("before")
    original = snapshot_tree(root)
    save("checkpoint-layout", sorted(original))
    batch, query_id, start, end = checkpoint_batch(original, config.topic)
    assert query_id == first["id"], "Checkpoint/query identity mismatch"

    batch_progress(first, batch, start, end)
    assert source_records() == expected, (
        "MSK source changed/expired; quiesce producers before the experiment"
    )
    moved = quarantine_commit(
        config.table,
        checkpoint,
        run_id,
        batch,
        original,
        lambda: bool(spark.streams.active),
    )
    save(
        "injected-state",
        {
            "batch": batch,
            "moved_to": str(moved),
            "files": sorted(snapshot_tree(root)),
        },
    )
    assert audit("after-injection") == before, "Injection changed sink results"

    second = run("restart-progress")
    assert second["id"] == first["id"] and second["runId"] != first["runId"]

    recovery_progress = batch_progress(second, batch, start, end)
    assert (root / "commits" / str(batch)).is_file(), (
        "commits/N not regenerated"
    )
    assert (root / "offsets" / str(batch)).read_bytes() == original[
        f"offsets/{batch}"
    ]
    assert (root / "metadata").read_bytes() == original["metadata"]

    checkpoint_batch(snapshot_tree(root), config.topic)
    assert audit("after-restart") == before, "Restart changed existing rows"
    assert source_records() == expected, (
        "MSK source changed/expired during restart"
    )

    run("no-input-progress")
    assert audit("after-no-input") == before
    save(
        "result",
        {
            "status": "PASS",
            "batch": batch,
            "missing": 0,
            "duplicates": 0,
            "unexpected": 0,
            "numInputRows": recovery_progress["sources"][0]["numInputRows"],
            "retry_evidence": "same batch/start/end",
        },
    )
    print(f"[PASS] isolated failure-state reproduction; evidence={evidence}")
