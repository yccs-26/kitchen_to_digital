"""Cloud 도구의 오프라인 계약 검사. cloud client는 생성하지 않는다."""

import base64
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from confluent_kafka.schema_registry import Schema
from confluent_kafka.schema_registry.error import SchemaRegistryError
import pytest

from scripts.phase2 import cloud_e2e as tools

RUN_ID = "p2i-" + "1" * 32
CONFIG = Path("config/job2-runtime.example.json")
NOW = "2026-10-23T00:00:00+00:00"


@pytest.fixture
def manifest():
    return tools.make_manifest(CONFIG, RUN_ID, NOW, 1)


@pytest.fixture
def readiness(manifest):
    now = datetime.now(timezone.utc)
    proof = tools.readiness_template(manifest)
    proof.update(status="READY", checked_at=now.isoformat(),
                 expires_at=(now + timedelta(minutes=10)).isoformat(),
                 verification_credential="test-reader")
    proof["principals"] = {
        key: ("local-test-user" if key in ("databricks_run_as", "uc_reader")
              else "arn:aws:iam::048715026902:role/local-test-only")
        for key in proof["principals"]
    }
    proof["checks"] = {key: "READY" for key in proof["checks"]}
    proof["writers"] = dict(
        status="NONE_CONFIRMED", checked_at=now.isoformat(),
        evidence_ref="local-test",
    )
    proof["compute"] = dict(cluster_id="0929-113111-cgu7k91c",
                            auto_termination_minutes=15,
                            manual_stop_confirmed=True)
    proof["budget"] = dict(
        project_cash_spent_usd=1, approved_databricks_cash_usd=1,
        approved_aws_usd=1, hourly_databricks_cash_usd=2,
        hourly_aws_cash_usd=1, max_minutes=15,
        rates_and_credits_verified=True, evidence_ref="local-test-only",
    )
    return proof


@pytest.fixture
def registry():
    client = Mock()

    def get_schema(schema_id):
        if schema_id == 2147483647:
            raise SchemaRegistryError(404, 40403, "not found")
        return Schema(tools.SCHEMA_PATH.read_text(), "AVRO")

    client.get_schema.side_effect = get_schema
    client.lookup_schema.return_value = SimpleNamespace(schema_id=1)
    return client


def acks_for(manifest):
    topic = manifest["contract"]["topics"]["raw"]
    return [dict(case=case["name"], topic=topic,
                 partition=1, offset=100+i)
            for i, case in enumerate(manifest["cases"])]


def test_manifest_deterministic_contract(manifest):
    assert tools.make_manifest(CONFIG, RUN_ID, NOW, 1) == manifest
    assert tools.plan(manifest, "publish") == {
        **tools.identity(manifest), "action": "publish", "dry_run": True,
        "cloud_access": False, "input_records": 10, "canonical_ids": 4,
        "expected_validated": 5, "expected_quarantine": 5,
    }
    cases = {case["name"]: case for case in manifest["cases"]}
    assert (cases["normal"]["value_base64"]
            == cases["duplicate"]["value_base64"])
    assert cases["conflict"]["event_id"] == cases["normal"]["event_id"]
    assert cases["late_within"]["expected"]["event_time_status"] == "ON_TIME"
    assert cases["late_outside"]["expected"]["event_time_status"] == "LATE"
    assert cases["business_anomaly"]["expected"]["validated"]
    assert all(case["event_id"].startswith(RUN_ID) for case in cases.values())


@pytest.mark.parametrize("run_id", ["wrong", "p2i-../x", "p2i-" + "A"*32])
def test_invalid_run_id_rejected(run_id):
    with pytest.raises(tools.PreflightError, match="invalid_run_id"):
        tools.make_manifest(CONFIG, run_id, NOW, 1)


def test_manifest_tamper_and_no_overwrite(manifest, tmp_path):
    path = tmp_path / "manifest.json"
    tools.write_json(path, manifest)
    assert tools.load_manifest(path, CONFIG) == manifest
    with pytest.raises(FileExistsError):
        tools.write_json(path, manifest)
    manifest["cases"][0]["expected"]["validated"] = False
    path.write_text(json.dumps(manifest))
    with pytest.raises(tools.PreflightError, match="mismatch"):
        tools.load_manifest(path, CONFIG)


@pytest.mark.parametrize("action", [
    "snapshot", "publish", "run-job", "verify",
])
def test_dry_run_does_not_create_clients(
    manifest, tmp_path, monkeypatch, action,
):
    path = tmp_path / "manifest.json"
    tools.write_json(path, manifest)
    monkeypatch.setattr(tools, "runtime_spark",
                        Mock(side_effect=AssertionError))
    monkeypatch.setattr(tools, "publish", Mock(side_effect=AssertionError))
    assert tools.main([action, "--manifest", str(path),
                       "--runtime-config", str(CONFIG)]) == 0
    assert tools.main([action, "--manifest", str(path),
                       "--runtime-config", str(CONFIG), "--execute",
                       "--approve-run-id", "wrong"]) == 1
    tools.runtime_spark.assert_not_called()
    tools.publish.assert_not_called()


@pytest.mark.parametrize("mode", [
    "permission", "bootstrap", "topic", "schema",
])
def test_preflight_errors_are_fail_closed(manifest, registry, mode):
    consumer = Mock()
    if mode in ("permission", "bootstrap"):
        consumer.list_topics.side_effect = PermissionError("secret-do-not-log")
        with pytest.raises(PermissionError):
            tools.collect_bounds(consumer, ["kitchen.sensor.raw"])
    elif mode == "topic":
        consumer.list_topics.return_value.topics = {}
        with pytest.raises(tools.PreflightError, match="metadata_unavailable"):
            tools.collect_bounds(consumer, ["kitchen.sensor.raw"])
    else:
        registry.get_schema.side_effect = None
        registry.get_schema.return_value = Schema('{"type":"string"}', "AVRO")
        with pytest.raises(tools.PreflightError, match="writer_schema"):
            tools.check_registry(manifest, registry)


def test_unknown_id_requires_exact_registry_not_found(manifest, registry):
    tools.check_registry(manifest, registry)
    registry.get_schema.side_effect = lambda _: Schema(
        tools.SCHEMA_PATH.read_text(), "AVRO",
    )
    with pytest.raises(tools.PreflightError, match="unknown_schema_id_exists"):
        tools.check_registry(manifest, registry)


def test_acks_require_all_cases_and_strict_offsets(manifest):
    acks = acks_for(manifest)
    tools.validate_acks(manifest, acks)
    with pytest.raises(tools.PreflightError, match="missing_or_unordered"):
        tools.validate_acks(manifest, acks[:-1])
    for value in (False, True, -1, "1", None):
        bad = deepcopy(acks)
        bad[0]["offset"] = value
        with pytest.raises(tools.PreflightError, match="invalid_ack"):
            tools.validate_acks(manifest, bad)
    acks[1]["partition"] = 2
    with pytest.raises(tools.PreflightError, match="partition_or_order"):
        tools.validate_acks(manifest, acks)


def test_retention_and_partition_change_are_not_silent():
    start = {"topic": {"0": {"low": 2, "high": 5}}}
    tools.validate_bounds(start, {"topic": {"0": {"low": 3, "high": 7}}})
    with pytest.raises(tools.PreflightError, match="retention"):
        tools.validate_bounds(start, {"topic": {"0": {"low": 6, "high": 7}}})
    with pytest.raises(tools.PreflightError, match="partition"):
        tools.validate_bounds(start, {"topic": {"1": {"low": 2, "high": 7}}})


def test_read_window_starts_at_baseline_without_commit():
    before = {"topic": {"0": {"low": 0, "high": 5}}}
    after = {"topic": {"0": {"low": 0, "high": 6}}}
    consumer = Mock()
    message = Mock()
    message.error.return_value = None
    message.topic.return_value = "topic"
    message.partition.return_value = 0
    message.offset.return_value = 5
    message.key.return_value = b"k"
    message.value.return_value = b"v"
    consumer.poll.return_value = message
    consumer.position.return_value = [SimpleNamespace(
        topic="topic", partition=0, offset=6,
    )]
    records = tools.read_window(consumer, before, after)
    assert len(records) == 1 and records[0]["offset"] == 5
    assert consumer.assign.call_args.args[0][0].offset == 5
    consumer.commit.assert_not_called()
    consumer.subscribe.assert_not_called()
    consumer.position.return_value[0].offset = 5
    with pytest.raises(tools.PreflightError, match="incomplete"):
        tools.read_window(consumer, before, after, timeout=0)


def test_partial_publish_is_journaled_and_not_retried(tmp_path):
    manifest = tools.make_manifest(
        CONFIG, RUN_ID, datetime.now(timezone.utc).isoformat(), 1,
    )
    before = snapshot_for(manifest)
    output = tmp_path / "acks.json"
    produce = Mock(side_effect=[SimpleNamespace(
        topic=manifest["contract"]["topics"]["raw"], partition=1, offset=100,
    ), TimeoutError("secret-never-written")])
    with pytest.raises(tools.PreflightError, match="no_resend"):
        tools.publish(manifest, before, output, produce)
    report = json.loads(output.read_text())
    assert report["status"] == "UNCONFIRMED" and len(report["acks"]) == 1
    assert "secret-never-written" not in output.read_text()
    with pytest.raises(FileExistsError):
        tools.publish(manifest, before, tmp_path / "retry.json", produce)
    assert produce.call_count == 2


def test_fault_wrappers_do_not_change_broker():
    producer = Mock()
    with pytest.raises(TimeoutError, match="before_produce"):
        tools.FaultProducer(producer, "producer_timeout").produce()
    producer.produce.assert_not_called()
    with pytest.raises(TimeoutError, match="after_flush"):
        tools.FaultProducer(producer, "ack_uncertain").flush(1)
    producer.flush.assert_called_once_with(1)


def test_real_components_match_manifest_and_replay(
    manifest, registry, tmp_path,
):
    """실제 Job 2 + 테스트 SQLite/ACK 대역. Delta 증거는 별도 test다."""
    import sys
    sys.path.insert(0, str(tools.ROOT / "tests/unit"))
    from test_sensor_validation_job import config, rules, FakeProducer
    from test_silver_sink import SqliteTestStorage
    from streaming.jobs import sensor_validation_job as job
    from streaming.sinks.silver import SilverSink

    records = []

    class Recorder(FakeProducer):
        def flush(self, timeout):
            result = super().flush(timeout)
            sent = self.calls[-1]
            records.append(dict(
                topic=sent["topic"], partition=0, offset=len(records),
                key_base64=tools.b64(sent["key"]),
                value_base64=tools.b64(sent["value"]),
            ))
            return result

    acks = acks_for(manifest)
    storage = SqliteTestStorage(tmp_path / "silver.db")
    deps = dict(
        registry_client=registry, silver=SilverSink(storage),
        validated_producer=Recorder(), quarantine_producer=Recorder(),
        serializer=lambda event, _: tools.encode(event, 1),
        config=config.__wrapped__(), rules=rules.__wrapped__(),
        reference_time=tools.timestamp(NOW),
    )
    raw_records = []
    for case, ack in zip(manifest["cases"], acks):
        raw = {key: ack[key] for key in ("topic", "partition", "offset")}
        raw_records.append({**raw, "key_base64": case["key_base64"],
                            "value_base64": case["value_base64"]})
        raw.update(key=base64.b64decode(case["key_base64"]),
                   value=base64.b64decode(case["value_base64"]))
        job.process_record(raw, **deps)
    rows = list(storage.connection.execute("SELECT payload FROM canonical"))
    silver = [json.loads(row[0]) for row in rows]
    first = tools.reconcile(
        manifest, acks, raw_records + records, silver, registry,
    )
    assert first["status"] == "PASS", first
    assert len(first["canonical"]) == 4
    assert len(first["canonical_fingerprints"]) == 4
    assert first["failure_reasons_by_case"]["bad_frame"] == [
        ["INVALID_CONFLUENT_FRAME", "raw_value"],
    ]
    assert len(first["validated_lineage_by_event"][RUN_ID + "/normal"]) == 2
    assert first["extra_deliveries"] == 0
    # 같은 manifest 재처리: 연결을 다시 열어 영속 canonical을 재조회한다.
    storage.close()
    storage = SqliteTestStorage(tmp_path / "silver.db")
    deps["silver"] = SilverSink(storage)
    for record in raw_records:
        raw = {key: record[key] for key in ("topic", "partition", "offset")}
        raw.update(key=base64.b64decode(record["key_base64"]),
                   value=base64.b64decode(record["value_base64"]))
        job.process_record(raw, **deps)
    second = tools.reconcile(
        manifest, acks, raw_records + records, silver, registry,
    )
    assert second["status"] == "PASS" and second["extra_deliveries"] == 10
    assert first["canonical_fingerprints"] == second["canonical_fingerprints"]
    comparison = tools.compare_reports(first, second)
    assert comparison["canonical_unchanged"]
    tools.write_json(tmp_path / "canonical-replay.json", {
        "scope": "real Job 2 / SQLite test storage / fake Kafka ACK",
        "first": first, "restart": second, "comparison": comparison,
    })
    missing = tools.reconcile(manifest, acks, raw_records, silver, registry)
    assert missing["status"] == "FAIL"
    corrupted = deepcopy(silver)
    corrupted[0]["metric_value"] = 999
    assert tools.reconcile(manifest, acks, raw_records + records,
                           corrupted, registry)["status"] == "FAIL"
    assert tools.reconcile(manifest, acks, raw_records + records,
                           silver + silver[:1], registry)["status"] == "FAIL"
    storage.close()


def test_permission_error_output_is_redacted(
    manifest, tmp_path, monkeypatch, capsys, readiness,
):
    path = tmp_path / "manifest.json"
    tools.write_json(path, manifest)
    monkeypatch.setattr(tools, "runtime_spark", lambda: Mock())
    monkeypatch.setattr(tools, "open_cloud", Mock(
        side_effect=PermissionError("secret-do-not-log"),
    ))
    proof = tmp_path / "readiness.json"
    tools.write_json(proof, readiness)
    assert tools.main([
        "snapshot", "--manifest", str(path), "--runtime-config", str(CONFIG),
        "--execute", "--approve-run-id", RUN_ID, "--output",
        str(tmp_path / "result.json"),
        "--verification-credential", "test-reader",
        "--readiness", str(proof),
    ]) == 1
    tools.open_cloud.assert_called_once()
    assert "secret-do-not-log" not in capsys.readouterr().out
    result = json.loads((tmp_path / "result.json").read_text())
    assert result["status"] == "BLOCKED"


def test_snapshot_collision_blocks_publish(manifest, registry, monkeypatch):
    monkeypatch.setattr(tools, "collect_bounds", lambda *args: {})
    monkeypatch.setattr(tools, "read_silver",
                        lambda *args: [{"event_id": "old"}])
    with pytest.raises(tools.PreflightError, match="identity_collision"):
        tools.take_snapshot(manifest, Mock(), registry, Mock())


@pytest.mark.parametrize("completion", [True, False])
def test_job_harness_reuses_handler_and_stops_query(
    manifest, monkeypatch, completion,
):
    from unittest.mock import MagicMock
    from streaming.jobs import runtime_config as runtime
    from streaming.jobs import sensor_validation_job as job
    spark = MagicMock()
    spark.streams.active = []
    query = MagicMock()
    query.awaitTermination.return_value = completion
    query.recentProgress = [{"batchId": 0}]
    writer = spark.readStream.format.return_value.options.return_value.load(
    ).writeStream
    writer.foreachBatch.return_value = writer
    writer.option.return_value = writer
    writer.queryName.return_value = writer
    writer.trigger.return_value = writer
    writer.start.return_value = query
    monkeypatch.setattr(tools, "read_silver", lambda *args: [])
    monkeypatch.setattr(runtime, "get_dbutils", lambda *args: Mock())
    monkeypatch.setattr(runtime.RuntimeSettings, "resolve_clients",
                        lambda *args: SimpleNamespace(
                            registry={}, producer={},
                        ))
    handler = Mock(return_value={"received": 10})
    monkeypatch.setattr(job, "make_batch_handler", Mock(return_value=handler))
    before = snapshot_for(manifest)
    result = tools.run_job(manifest, acks_for(manifest), before,
                           CONFIG, spark, timeout=1)
    assert result["status"] == (
        "QUERY_COMPLETED_REQUIRES_RECONCILE" if completion else "FAILED"
    )
    writer.option.assert_called_once_with(
        "checkpointLocation", manifest["contract"]["checkpoint"],
    )
    query.stop.assert_called_once()
    source_options = spark.readStream.format.return_value.options.call_args
    options = source_options.kwargs
    raw = manifest["contract"]["topics"]["raw"]
    assert json.loads(options["assign"]) == {raw: [0, 1, 2]}
    assert json.loads(options["startingOffsets"]) == {
        raw: {"0": 50, "1": 100, "2": 75},
    }
    assert options["failOnDataLoss"] == "true"
    assert "subscribe" not in options and "kafka.group.id" not in options
    writer.trigger.assert_called_once_with(availableNow=True)
    batch = MagicMock()
    batch.where.return_value.isEmpty.return_value = False
    writer.foreachBatch.call_args.args[0](batch, 0)
    handler.assert_called_once_with(batch.where.return_value, 0)
    assert result["batches"][0]["counts"]["received"] == 10
    unsafe = deepcopy(manifest)
    unsafe["contract"]["silver_table"] = unsafe["contract"][
        "production_silver_table"
    ]
    with pytest.raises(tools.PreflightError, match="isolated"):
        tools.run_job(unsafe, acks_for(unsafe),
                      {**before, **tools.identity(unsafe)},
                      CONFIG, spark, fault="after_ack")


def test_bad_bootstrap_rejected_without_network(tmp_path):
    values = json.loads(CONFIG.read_text())
    values["rules_path"] = str(tools.ROOT / "config/validation_rules.json")
    values["kafka"]["bootstrap_servers"] = "broken-bootstrap"
    path = tmp_path / "runtime.json"
    path.write_text(json.dumps(values))
    with pytest.raises(tools.PreflightError, match="invalid_bootstrap"):
        tools.make_manifest(path, RUN_ID, NOW, 1)


def test_consumer_uses_separate_group_and_no_commits(monkeypatch):
    import confluent_kafka
    from streaming.jobs import runtime_config as runtime
    utility = Mock()
    consumer = Mock()
    factory = Mock(return_value=consumer)
    monkeypatch.setattr(confluent_kafka, "Consumer", factory)
    monkeypatch.setattr(runtime, "get_dbutils", lambda *args: utility)
    monkeypatch.setattr(runtime, "runtime_producer_options",
                        lambda options, **kwargs: options)
    monkeypatch.setattr(runtime.RegistrySettings, "resolve",
                        lambda *args: {"url": "http://localhost:8081"})
    client, registry = tools.open_cloud(CONFIG, "verification-only", RUN_ID,
                                        Mock())
    options = factory.call_args.args[0]
    assert options["group.id"] == "ktd-p2e2e-" + RUN_ID[4:]
    assert options["enable.auto.commit"] is False
    assert options["enable.auto.offset.store"] is False
    assert options["allow.auto.create.topics"] is False
    assert "acks" not in options
    utility.credentials.getServiceCredentialsProvider.assert_called_once_with(
        "verification-only",
    )
    client.close()
    registry.__exit__(None, None, None)


def test_query_restart_comparison_requires_same_checkpoint():
    first = dict(run_id=RUN_ID, manifest_sha256="same", checkpoint="isolated",
                 query_id="q", run_id_spark="r1", status="FAILED")
    second = {**first, "run_id_spark": "r2",
              "status": "QUERY_COMPLETED_REQUIRES_RECONCILE"}
    assert tools.check_query_restart(first, second)["status"] == "PASS"
    second["checkpoint"] = "other"
    assert tools.check_query_restart(first, second)["status"] == "FAIL"


@pytest.mark.parametrize("mode", [
    "registry", "delta", "producer_timeout", "ack_uncertain", "after_ack",
    "batch",
])
def test_isolated_faults_fail_and_restore_patches(mode, monkeypatch):
    from unittest.mock import MagicMock
    from streaming.jobs import runtime_config as runtime
    from streaming.jobs import sensor_validation_job as job
    manifest = tools.make_manifest(
        CONFIG, RUN_ID, NOW, 1,
        silver_table="ktd.silver.sensor_validated_e2e_" + RUN_ID[4:],
    )
    before = snapshot_for(manifest)
    spark = MagicMock()
    spark.streams.active = []
    query = MagicMock()
    query.recentProgress = []
    writer = spark.readStream.format.return_value.options.return_value.load(
    ).writeStream
    for method in ("foreachBatch", "option", "queryName", "trigger"):
        getattr(writer, method).return_value = writer
    writer.start.return_value = query
    batch = MagicMock()
    batch.where.return_value.isEmpty.return_value = False
    query.awaitTermination.side_effect = lambda _: (
        writer.foreachBatch.call_args.args[0](batch, 0)
    )
    monkeypatch.setattr(tools, "read_silver", lambda *args: [])
    monkeypatch.setattr(runtime, "get_dbutils", lambda *args: Mock())
    monkeypatch.setattr(runtime.RuntimeSettings, "resolve_clients",
                        lambda *args: SimpleNamespace(
                            registry={}, producer={},
                        ))
    native = Mock()
    native.flush.return_value = 0
    def factory(options):
        return native
    monkeypatch.setattr(job, "Producer", factory)
    original_registry = job.SchemaRegistryClient.get_schema
    original_delta = job.DeltaSilverStorage.insert_if_absent

    def handler(batch, batch_id):
        if mode == "registry":
            job.SchemaRegistryClient.get_schema(Mock(), 1)
        elif mode == "delta":
            job.DeltaSilverStorage.insert_if_absent(Mock(), {})
        elif mode in ("producer_timeout", "ack_uncertain"):
            producer = job.Producer({})
            producer.produce()
            producer.flush(1)
        return {"received": 10}

    monkeypatch.setattr(job, "make_batch_handler", lambda *a, **k: handler)
    result = tools.run_job(manifest, acks_for(manifest), before,
                           CONFIG, spark, fault=mode)
    assert result["status"] == "FAILED"
    assert job.SchemaRegistryClient.get_schema is original_registry
    assert job.DeltaSilverStorage.insert_if_absent is original_delta
    assert job.Producer is factory
    query.stop.assert_called_once()


def test_job_request_is_pure_existing_compute_plan(manifest, readiness):
    parameters = [
        "snapshot", "--execute", "--approve-run-id", RUN_ID,
        "--runtime-config", "/Workspace/example/config/job2-runtime.json",
        "--manifest", "/Volumes/c/s/v/manifest.json",
        "--output", "/Volumes/c/s/v/before.json",
        "--readiness", "/Volumes/c/s/v/readiness.json",
        "--verification-credential", "test-reader",
    ]
    request = tools.job_request(
        manifest, "0929-113111-cgu7k91c",
        "/Workspace/example/scripts/phase2/cloud_e2e.py",
        parameters, readiness,
    )
    assert request["timeout_seconds"] == 600
    task = request["tasks"][0]
    assert task["max_retries"] == 0 and "new_cluster" not in task
    assert len(request["idempotency_token"]) == 64
    with pytest.raises(tools.PreflightError):
        tools.job_request(manifest, "", "/tmp/script.py", parameters)


def test_silver_provider_mismatch_is_blocked(manifest):
    spark = Mock()
    spark.sql.return_value.collect.return_value = [
        SimpleNamespace(col_name="Provider", data_type="iceberg"),
        SimpleNamespace(col_name="Type", data_type="MANAGED"),
    ]
    with pytest.raises(tools.PreflightError, match="managed_delta"):
        tools.read_silver(spark, manifest)
    spark.table.assert_not_called()


def test_silver_metadata_and_all_run_id_lookups(manifest, monkeypatch):
    from streaming.sinks import delta_silver
    spark = Mock()
    spark.sql.return_value.collect.return_value = [
        SimpleNamespace(col_name="Provider", data_type="delta"),
        SimpleNamespace(col_name="Type", data_type="MANAGED"),
    ]
    fields = []
    for field in json.loads(tools.SCHEMA_PATH.read_text())["fields"]:
        name = field["name"]
        data_type = "timestamp" if name == "event_time" else (
            "double" if name == "metric_value" else "string"
        )
        fields.append(SimpleNamespace(name=name, dataType=SimpleNamespace(
            simpleString=lambda data_type=data_type: data_type,
        )))
    spark.table.return_value.schema.fields = fields
    storage = Mock()
    storage.lookup.return_value = None
    monkeypatch.setattr(delta_silver, "DeltaSilverStorage",
                        lambda **k: storage)
    assert tools.read_silver(spark, manifest) == []
    assert storage.lookup.call_count == 8
    fields[0].dataType.simpleString = lambda: "double"
    with pytest.raises(tools.PreflightError, match="schema_mismatch"):
        tools.read_silver(spark, manifest)


def test_production_silver_cannot_be_e2e_target():
    with pytest.raises(tools.PreflightError, match="isolat"):
        tools.make_manifest(CONFIG, RUN_ID, NOW, 1,
                            silver_table="ktd.silver.sensor_validated")


def test_tampered_checkpoint_rejected_before_spark_io(manifest):
    manifest = deepcopy(manifest)
    manifest["contract"]["checkpoint"] = (
        manifest["contract"]["production_checkpoint"] + "/accidental-child"
    )
    before = {**tools.identity(manifest), "bounds": {}}
    with pytest.raises(tools.PreflightError, match="checkpoint"):
        tools.run_job(manifest, acks_for(manifest), before, CONFIG, Mock())


def test_execute_without_cost_writer_readiness_creates_no_clients(
    manifest, tmp_path, monkeypatch,
):
    path = tmp_path / "manifest.json"
    tools.write_json(path, manifest)
    client = Mock(side_effect=RuntimeError("must not create session"))
    monkeypatch.setattr(tools, "runtime_spark", client)
    assert tools.main([
        "snapshot", "--manifest", str(path), "--runtime-config", str(CONFIG),
        "--execute", "--approve-run-id", RUN_ID,
        "--verification-credential", "reader", "--output", str(tmp_path/"out"),
    ]) == 1
    client.assert_not_called()


def test_restart_missing_checkpoint_or_run_identity_is_not_pass():
    first = dict(run_id=RUN_ID, manifest_sha256="sha", query_id="query")
    second = {**first, "run_id_spark": "second",
              "status": "QUERY_COMPLETED_REQUIRES_RECONCILE"}
    assert tools.check_query_restart(first, second)["status"] == "FAIL"


def test_readiness_positive_is_local_attestation_only(manifest, readiness):
    assert tools.validate_readiness(manifest, readiness)["status"] == "READY"
    assert tools.readiness_template(manifest)["status"] == "BLOCKED"


@pytest.mark.parametrize("value", [None, True, float("nan"), -1])
def test_unverified_or_invalid_cost_blocks_execution(
    manifest, readiness, value,
):
    readiness["budget"]["hourly_databricks_cash_usd"] = value
    with pytest.raises(tools.PreflightError, match="cost_inputs"):
        tools.validate_readiness(manifest, readiness)


@pytest.mark.parametrize("change", ["project", "estimate", "missing"])
def test_budget_caps_and_missing_cost_evidence(manifest, readiness, change):
    if change == "project":
        readiness["budget"]["project_cash_spent_usd"] = 9.5
    elif change == "estimate":
        readiness["budget"]["hourly_aws_cash_usd"] = 100
    else:
        readiness["budget"]["rates_and_credits_verified"] = False
    with pytest.raises(tools.PreflightError, match="cost_"):
        tools.validate_readiness(manifest, readiness)


@pytest.mark.parametrize("condition", ["busy", "stale", "unknown"])
def test_writer_clearance_is_required(manifest, readiness, condition):
    if condition == "stale":
        readiness["writers"]["checked_at"] = (
            datetime.now(timezone.utc) - timedelta(minutes=6)
        ).isoformat()
    else:
        readiness["writers"]["status"] = condition
    with pytest.raises(tools.PreflightError, match="writer_clearance"):
        tools.validate_readiness(manifest, readiness)


def test_bad_credential_and_unverified_uc_permissions(manifest, readiness):
    with pytest.raises(tools.PreflightError, match="credential_mismatch"):
        tools.validate_readiness(manifest, readiness, "wrong-reader")
    readiness["checks"]["iam_uc_permissions"] = "UNVERIFIED"
    with pytest.raises(tools.PreflightError, match="prerequisites"):
        tools.validate_readiness(manifest, readiness)


def test_job_request_rejects_missing_readiness_and_wrong_compute(
    manifest, readiness,
):
    parameters = ["snapshot", "--execute", "--approve-run-id", RUN_ID]
    script = "/Workspace/example/scripts/phase2/cloud_e2e.py"
    with pytest.raises(tools.PreflightError, match="readiness_required"):
        tools.job_request(manifest, "candidate", script, parameters)
    with pytest.raises(tools.PreflightError, match="compute_mismatch"):
        tools.job_request(manifest, "wrong-compute", script, parameters,
                          readiness)


def test_active_query_and_missing_table_fail_before_publish(
    manifest, monkeypatch,
):
    from unittest.mock import MagicMock
    spark = MagicMock()
    spark.streams.active = ["other-query"]
    before = {**tools.identity(manifest), "bounds": {}}
    with pytest.raises(tools.PreflightError, match="active_queries"):
        tools.run_job(manifest, acks_for(manifest), before, CONFIG, spark)
    spark.readStream.format.assert_not_called()
    spark.sql.side_effect = RuntimeError("TABLE_OR_VIEW_NOT_FOUND")
    with pytest.raises(RuntimeError, match="NOT_FOUND"):
        tools.read_silver(spark, manifest)


def test_incomplete_manifest_never_creates_cloud_client(tmp_path, monkeypatch):
    manifest = tmp_path / "incomplete.json"
    manifest.write_text("{}")
    client = Mock(side_effect=AssertionError)
    monkeypatch.setattr(tools, "runtime_spark", client)
    assert tools.main(["snapshot", "--manifest", str(manifest),
                       "--runtime-config", str(CONFIG), "--execute"]) == 1
    client.assert_not_called()


def test_topic_contract_and_foreign_schema_cannot_be_selected(tmp_path):
    values = json.loads(CONFIG.read_text())
    values["rules_path"] = str(tools.ROOT / "config/validation_rules.json")
    values["topics"]["raw"] = "other.sensor.raw"
    path = tmp_path / "wrong-topic.json"
    path.write_text(json.dumps(values))
    with pytest.raises(tools.PreflightError, match="topic_contract"):
        tools.make_manifest(path, RUN_ID, NOW, 1)
    with pytest.raises(tools.PreflightError, match="isolated_run_silver"):
        tools.make_manifest(CONFIG, RUN_ID, NOW, 1, silver_table=(
            "ktd.other.sensor_validated_e2e_" + RUN_ID[4:]
        ))


@pytest.mark.parametrize("caller", [
    "arn:aws:iam::048715026902:user/chan",
    "arn:aws:sts::048715026902:assumed-role/wrong-role/session",
])
def test_unexpected_raw_identity_rejected(caller):
    client = Mock()
    client.get_caller_identity.return_value = {"Arn": caller}
    with pytest.raises(tools.PreflightError, match="principal_mismatch"):
        tools.check_raw_principal(
            "arn:aws:iam::048715026902:role/ktd-msk-producer-role",
            "ap-northeast-2", client,
        )
    client.close.assert_called_once()


def test_expected_raw_role_accepted_without_publish():
    client = Mock()
    caller = (
        "arn:aws:sts::048715026902:assumed-role/ktd-msk-producer-role/i-test"
    )
    client.get_caller_identity.return_value = {"Arn": caller}
    assert tools.check_raw_principal(
        "arn:aws:iam::048715026902:role/ktd-msk-producer-role",
        "ap-northeast-2", client,
    ) == caller
    client.close.assert_called_once()


def test_job_request_credential_typo_rejected(manifest, readiness):
    parameters = [
        "snapshot", "--execute", "--approve-run-id", RUN_ID,
        "--runtime-config", "/Workspace/example/config/runtime.json",
        "--manifest", "/Volumes/c/s/v/manifest.json",
        "--output", "/Volumes/c/s/v/before.json",
        "--readiness", "/Volumes/c/s/v/readiness.json",
        "--verification-credential", "typo-reader",
    ]
    with pytest.raises(tools.PreflightError, match="credential_mismatch"):
        tools.job_request(
            manifest, readiness["compute"]["cluster_id"],
            "/Workspace/example/scripts/phase2/cloud_e2e.py",
            parameters, readiness,
        )


def snapshot_for(manifest):
    return dict(
        **tools.identity(manifest), status="READY",
        bounds={topic: {
            str(p): dict(low=0, high=high)
            for p, high in enumerate((50, 100, 75))
        } for topic in manifest["contract"]["topics"].values()},
    )


@pytest.mark.parametrize("problem", [
    "missing_partition", "ack_before_start", "ack_wrong_partition",
    "boolean_offset", "missing_low", "snapshot_not_ready", "missing_topic",
])
def test_invalid_raw_window_rejected_before_spark(
    manifest, monkeypatch, problem,
):
    from unittest.mock import MagicMock
    before, acks = snapshot_for(manifest), acks_for(manifest)
    raw = manifest["contract"]["topics"]["raw"]
    if problem == "missing_partition":
        del before["bounds"][raw]["2"]
    elif problem == "ack_before_start":
        for ack in acks:
            ack["offset"] -= 1
    elif problem == "ack_wrong_partition":
        for ack in acks:
            ack["partition"] = 3
    elif problem == "boolean_offset":
        before["bounds"][raw]["0"]["low"] = False
    elif problem == "missing_low":
        del before["bounds"][raw]["0"]["low"]
    elif problem == "snapshot_not_ready":
        before["status"] = "FAILED"
    else:
        del before["bounds"][manifest["contract"]["topics"]["validated"]]
    spark = MagicMock()
    spark.streams.active = []
    read = Mock(side_effect=AssertionError("must reject before Spark IO"))
    monkeypatch.setattr(tools, "read_silver", read)
    with pytest.raises(tools.PreflightError):
        tools.run_job(manifest, acks, before, CONFIG, spark)
    read.assert_not_called()
    spark.readStream.format.assert_not_called()


def test_publish_invalid_snapshot_never_attempts(manifest, tmp_path):
    producer = Mock()
    before = snapshot_for(manifest)
    del before["bounds"][manifest["contract"]["topics"]["raw"]]["2"]
    with pytest.raises(tools.PreflightError, match="partition_set"):
        tools.publish(manifest, before, tmp_path / "acks.json", producer)
    producer.assert_not_called()
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("partition,offset", [(3, 100), (1, 99)])
def test_publish_invalid_ack_window_stops_after_one_attempt(
    tmp_path, partition, offset,
):
    manifest = tools.make_manifest(
        CONFIG, RUN_ID, datetime.now(timezone.utc).isoformat(), 1,
    )
    producer = Mock(return_value=SimpleNamespace(
        topic=manifest["contract"]["topics"]["raw"],
        partition=partition, offset=offset,
    ))
    output = tmp_path / "acks.json"
    with pytest.raises(tools.PreflightError, match="no_resend"):
        tools.publish(manifest, snapshot_for(manifest), output, producer)
    producer.assert_called_once()
    assert json.loads(output.read_text())["status"] == "UNCONFIRMED"
    assert (tmp_path / (RUN_ID + ".publish.lock")).exists()


@pytest.mark.parametrize("mismatch", ["commit", "source"])
def test_deployed_commit_or_artifact_mismatch_blocks_load(
    manifest, tmp_path, mismatch,
):
    if mismatch == "commit":
        manifest["deployment_sha"] = "0" * 40
    else:
        manifest["source_sha256"]["scripts/phase2/cloud_e2e.py"] = "0" * 64
    path = tmp_path / "manifest.json"
    tools.write_json(path, manifest)
    with pytest.raises(tools.PreflightError, match="mismatch"):
        tools.load_manifest(path, CONFIG)
