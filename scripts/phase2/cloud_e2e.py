"""P2-8i 준비·발행·대사 도구. 기본 실행은 네트워크 없는 dry-run이다."""

import argparse
import base64
from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from io import BytesIO
import json
import math
import os
from pathlib import Path
import re
import subprocess
import sys
import time
from uuid import uuid4


def repository_root():
    """DBR wrapper의 __file__ 대신 실제 compile 경로도 확인한다."""
    for candidate in (sys._getframe(1).f_code.co_filename,
                      globals().get("__file__"), sys.argv[0]):
        if isinstance(candidate, str):
            path = Path(candidate).resolve()
            if (path.name == "cloud_e2e.py" and path.parent.name == "phase2"
                    and (path.parents[2] / "streaming").is_dir()):
                return path.parents[2]
    raise RuntimeError("Cannot locate deployed repository")


ROOT = repository_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from fastavro import schemaless_writer  # noqa: E402
from streaming.jobs.runtime_config import load_runtime_config  # noqa: E402
from streaming.validation.avro_decoder import (  # noqa: E402
    SensorDecodeError, decode_sensor_event,
)

SCHEMA_PATH = ROOT / "schemas/avro/sensor_metric_event.avsc"
RUN_PATTERN = r"p2i-[0-9a-f]{32}"
EXPECTED_TOPICS = dict(raw="kitchen.sensor.raw",
                       validated="kitchen.sensor.validated",
                       quarantine="kitchen.sensor.quarantine")


class PreflightError(ValueError):
    """원문 cloud 예외나 비밀값 없이 중단 사유를 전달한다."""


def digest(value):
    return sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def timestamp(value):
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None:
        raise PreflightError("timezone_required")
    return parsed.astimezone(timezone.utc)


def write_json(path, value):
    """기존 증빙을 덮어쓰지 않으며 디스크에 기록을 완료한다."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as output:
        json.dump(value, output, indent=2)
        output.write("\n")
        output.flush()
        os.fsync(output.fileno())


def append_json(path, value):
    with Path(path).open("a") as output:
        output.write(json.dumps(value) + "\n")
        output.flush()
        os.fsync(output.fileno())


def encode(payload, schema_id):
    body = BytesIO()
    schemaless_writer(body, json.loads(SCHEMA_PATH.read_text()), payload)
    return b"\x00" + schema_id.to_bytes(4, "big") + body.getvalue()


def b64(value):
    return base64.b64encode(value).decode()


def validate_isolation(manifest, settings=None):
    """대소문자 별칭과 운영 checkpoint의 하위 경로도 시험 대상으로 거부한다."""
    from streaming.jobs.raw_ingestion import quoted_table
    run_id, contract = manifest["run_id"], manifest["contract"]
    if not re.fullmatch(RUN_PATTERN, run_id):
        raise PreflightError("invalid_run_id")
    production = (settings.job.silver_table if settings else
                  contract["production_silver_table"])
    original_checkpoint = (settings.job.source.checkpoint if settings else
                           contract["production_checkpoint"])
    table = production + "_e2e_" + run_id[4:]
    checkpoint = original_checkpoint.rsplit("/", 1)[0] + "/e2e/" + run_id
    quoted_table(contract["silver_table"])
    if (contract["production_silver_table"] != production
            or contract["silver_table"].casefold() != table.casefold()):
        raise PreflightError("isolated_run_silver_required")
    if (contract["production_checkpoint"] != original_checkpoint
            or contract["checkpoint"] != checkpoint):
        raise PreflightError("isolated_run_checkpoint_required")
    if contract["topics"] != EXPECTED_TOPICS:
        raise PreflightError("unexpected_topic_contract")


def readiness_template(manifest):
    """미확인 값은 null/UNVERIFIED로 남기는 승인 전 로컬 양식이다."""
    return dict(
        **identity(manifest), scope="local readiness template; not approval",
        status="BLOCKED", checked_at=None, expires_at=None,
        bootstrap=manifest["contract"]["bootstrap"],
        silver_table=manifest["contract"]["silver_table"],
        checkpoint=manifest["contract"]["checkpoint"],
        verification_credential=None,
        principals={name: None for name in (
            "raw_producer", "job2_consumer", "job2_producer",
            "output_reader", "databricks_run_as", "uc_reader",
        )},
        checks={name: "UNVERIFIED" for name in (
            "iam_uc_permissions", "deployed_artifact", "dependencies",
            "network_review", "cleanup_plan",
        )},
        writers=dict(status="UNVERIFIED", checked_at=None, evidence_ref=None),
        compute=dict(cluster_id=None, auto_termination_minutes=None,
                     manual_stop_confirmed=False),
        budget=dict(project_cash_spent_usd=None,
                    approved_databricks_cash_usd=None, approved_aws_usd=None,
                    hourly_databricks_cash_usd=None, hourly_aws_cash_usd=None,
                    max_minutes=None, rates_and_credits_verified=False,
                    evidence_ref=None),
    )


def validate_readiness(manifest, readiness, credential=None, now=None):
    """관리자 검토 증거를 manifest에 묶는다. 실효 권한·분산 lock을 대신하지 않는다."""
    validate_isolation(manifest)
    check_evidence(manifest, readiness)
    now = now or datetime.now(timezone.utc)
    try:
        if readiness["status"] != "READY":
            raise PreflightError("readiness_not_ready")
        checked, expires = (timestamp(readiness[name])
                            for name in ("checked_at", "expires_at"))
        if not (checked <= now < expires <= checked + timedelta(minutes=15)):
            raise PreflightError("readiness_expired_or_future")
        for field in ("bootstrap", "silver_table", "checkpoint"):
            if readiness[field] != manifest["contract"][field]:
                raise PreflightError("readiness_environment_mismatch")
        name = readiness["verification_credential"]
        if (not isinstance(name, str)
                or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", name)
                or "APPROVAL" in name or (credential and credential != name)):
            raise PreflightError("verification_credential_mismatch")
        principals = readiness["principals"]
        for key in ("raw_producer", "job2_consumer", "job2_producer",
                    "output_reader"):
            if not re.fullmatch(r"arn:aws:iam::[0-9]{12}:role/[A-Za-z0-9_/-]+",
                                principals[key] or ""):
                raise PreflightError("confirmed_iam_principal_required")
        for key in ("databricks_run_as", "uc_reader"):
            if not isinstance(principals[key], str) or not principals[key]:
                raise PreflightError("confirmed_databricks_principal_required")
        required = readiness_template(manifest)["checks"]
        if any(readiness["checks"].get(key) != "READY" for key in required):
            raise PreflightError("unverified_execution_prerequisites")
        writers = readiness["writers"]
        age = now - timestamp(writers["checked_at"])
        if (writers["status"] != "NONE_CONFIRMED"
                or not writers["evidence_ref"]
                or not timedelta(0) <= age <= timedelta(minutes=5)):
            raise PreflightError("writer_clearance_missing_stale_or_busy")
        compute = readiness["compute"]
        if (not isinstance(compute["cluster_id"], str)
                or not compute["cluster_id"]
                or compute["manual_stop_confirmed"] is not True
                or type(compute["auto_termination_minutes"]) is not int
                or not 1 <= compute["auto_termination_minutes"] <= 60):
            raise PreflightError("compute_shutdown_plan_required")
        budget = readiness["budget"]
        fields = ("project_cash_spent_usd", "approved_databricks_cash_usd",
                  "approved_aws_usd", "hourly_databricks_cash_usd",
                  "hourly_aws_cash_usd", "max_minutes")
        if (budget["rates_and_credits_verified"] is not True
                or not budget["evidence_ref"]
                or any(type(budget[key]) not in (int, float)
                       or not math.isfinite(budget[key]) or budget[key] < 0
                       for key in fields)):
            raise PreflightError("verified_cost_inputs_required")
        hours = budget["max_minutes"] / 60
        if (not 0 < budget["max_minutes"] <= 30
                or budget["project_cash_spent_usd"]
                + budget["approved_databricks_cash_usd"] > 10
                or hours * budget["hourly_databricks_cash_usd"]
                > budget["approved_databricks_cash_usd"]
                or hours * budget["hourly_aws_cash_usd"]
                > budget["approved_aws_usd"]):
            raise PreflightError("cost_budget_exceeded")
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, PreflightError):
            raise
        raise PreflightError("incomplete_readiness") from None
    return dict(status="READY", scope="operator-reviewed prerequisites only")


def make_manifest(config_path, run_id, reference_time, schema_id,
                  unknown_schema_id=2147483647, silver_table=None,
                  deployment_sha=None):
    """같은 run/time/schema/config는 같은 key·value·event_id를 만든다."""
    if not re.fullmatch(RUN_PATTERN, run_id):
        raise PreflightError("invalid_run_id")
    if any(type(v) is not int or not 0 < v <= 2147483647
           for v in (schema_id, unknown_schema_id)):
        raise PreflightError("invalid_schema_id")
    if schema_id == unknown_schema_id:
        raise PreflightError("schema_ids_must_differ")
    settings = load_runtime_config(config_path)
    for server in settings.kafka.bootstrap_servers.split(","):
        if not re.fullmatch(r"[A-Za-z0-9.-]+:[0-9]{1,5}", server.strip()):
            raise PreflightError("invalid_bootstrap")
        if not 0 < int(server.rsplit(":", 1)[1]) <= 65535:
            raise PreflightError("invalid_bootstrap")
    reference = timestamp(reference_time)
    base = dict(
        event_id=f"{run_id}/normal", event_time=reference.isoformat(),
        store_id="store-001", equipment_id="fridge-001",
        equipment_type="refrigerator", metric_name="temperature_celsius",
        metric_value=4.2, unit="celsius", schema_version="1.0.0",
        source=f"e2e:{run_id}",
    )
    cases = []
    for name in ("normal", "business_anomaly", "domain_invalid", "bad_frame",
                 "unknown_schema", "duplicate", "conflict", "late_within",
                 "late_outside", "future"):
        payload = {**base, "event_id": f"{run_id}/{name}"}
        errors = []
        event_time_status = "ON_TIME"
        if name == "business_anomaly":
            payload["metric_value"] = 30.0
        elif name == "domain_invalid":
            payload["unit"] = "invalid-unit"
            errors = [["UNIT_MISMATCH", "unit"]]
        elif name in ("duplicate", "conflict"):
            payload = dict(base)
            if name == "conflict":
                payload["metric_value"] = 9.5
                errors = [["EVENT_ID_CONFLICT", "metric_value"]]
        elif name.startswith("late_"):
            minutes = 1 if name == "late_within" else 20
            payload["event_time"] = (
                reference - timedelta(minutes=minutes)
            ).isoformat()
            event_time_status = "ON_TIME" if minutes == 1 else "LATE"
        elif name == "future":
            payload["event_time"] = (
                reference + timedelta(days=1)
            ).isoformat()
            errors = [["FUTURE_EVENT_TIME", "event_time"]]
            event_time_status = "FUTURE_INVALID"
        from streaming.validation.domain_validator import validate_domain_event
        domain = validate_domain_event(
            {**payload, "event_time": timestamp(payload["event_time"])},
            b"fridge-001",
            supported_schema_versions=settings.rules.supported_schema_versions,
            equipment_registry=settings.rules.equipment_registry,
            metric_units=settings.rules.metric_units, reference_time=reference,
            max_future_skew=settings.job.max_future_skew,
        )
        expected_domain = errors if name != "conflict" else []
        actual_domain = [[e.code, e.field] for e in domain.errors]
        if actual_domain != expected_domain:
            raise PreflightError("fixture_rules_incompatible")
        value = encode(payload, schema_id)
        if name == "bad_frame":
            value = b"\xffinvalid-frame"
            errors = [["INVALID_CONFLUENT_FRAME", "raw_value"]]
        elif name == "unknown_schema":
            value = encode(payload, unknown_schema_id)
            errors = [["UNKNOWN_SCHEMA_ID", "schema_id"]]
        cases.append(dict(
            name=name, event_id=payload["event_id"], payload=payload,
            key_base64=b64(b"fridge-001"), value_base64=b64(value),
            expected=dict(
                classification="QUARANTINED" if errors else "VALIDATED",
                silver=name not in ("domain_invalid", "bad_frame",
                                     "unknown_schema", "future"),
                validated=not errors, quarantine=bool(errors),
                errors=errors, event_time_status=event_time_status,
                quarantine_event_id=(None if name in (
                    "bad_frame", "unknown_schema",
                ) else payload["event_id"]),
                canonical=dict(base) if name == "conflict" else payload,
            ),
        ))
    table = silver_table or (settings.job.silver_table + "_e2e_" + run_id[4:])
    from streaming.jobs.raw_ingestion import quoted_table
    quoted_table(table)
    contract = dict(
        bootstrap=settings.kafka.bootstrap_servers,
        region=settings.kafka.region,
        topics=dict(raw=settings.job.source.topic,
                    validated=settings.job.validated_topic,
                    quarantine=settings.job.quarantine_topic),
        silver_table=table, production_silver_table=settings.job.silver_table,
        production_checkpoint=settings.job.source.checkpoint,
        checkpoint=settings.job.source.checkpoint.rsplit("/", 1)[0]
        + "/e2e/" + run_id,
        source_credential=settings.kafka.source_service_credential,
        producer_credential=settings.kafka.producer_service_credential,
        registry_url=settings.registry.url,
        registry_reference=(vars(settings.registry.credentials)
                            if settings.registry.credentials else None),
        rules_sha256=sha256(
            Path(settings.rules_path).read_bytes(),
        ).hexdigest(),
        schema_sha256=sha256(SCHEMA_PATH.read_bytes()).hexdigest(),
    )
    manifest = dict(
        version=1, run_id=run_id, reference_time=reference.isoformat(),
        schema_id=schema_id, unknown_schema_id=unknown_schema_id,
        deployment_sha=deployment_sha or subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
        ).strip(),
        source_sha256={str(path.relative_to(ROOT)):
                       sha256(path.read_bytes()).hexdigest()
                       for path in sorted([
                           *ROOT.glob("streaming/**/*.py"),
                           ROOT / "scripts/phase2/cloud_e2e.py",
                           ROOT / "scripts/phase1_msk_fixture_producer.py",
                       ])},
        config_sha256=sha256(Path(config_path).read_bytes()).hexdigest(),
        contract=contract, cases=cases,
        status="OFFLINE_PLAN_NOT_CLOUD_VERIFIED",
    )
    validate_isolation(manifest, settings)
    return manifest


def load_manifest(path, config_path):
    manifest = json.loads(Path(path).read_text())
    if (ROOT / ".git").exists():
        actual_sha = subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True,
        ).strip()
        if actual_sha != manifest["deployment_sha"]:
            raise PreflightError("deployment_head_mismatch")
    rebuilt = make_manifest(
        config_path, manifest["run_id"], manifest["reference_time"],
        manifest["schema_id"], manifest["unknown_schema_id"],
        manifest["contract"]["silver_table"], manifest["deployment_sha"],
    )
    if manifest != rebuilt:
        raise PreflightError("manifest_config_schema_or_deployment_mismatch")
    return manifest


def identity(manifest):
    return dict(run_id=manifest["run_id"], manifest_sha256=digest(manifest))


def check_evidence(manifest, evidence):
    if any(evidence.get(key) != value
           for key, value in identity(manifest).items()):
        raise PreflightError("evidence_manifest_mismatch")


def plan(manifest, action):
    return dict(
        **identity(manifest), action=action, dry_run=True,
        cloud_access=False, input_records=len(manifest["cases"]),
        canonical_ids=len({case["event_id"] for case in manifest["cases"]
                           if case["expected"]["silver"]}),
        expected_validated=sum(case["expected"]["validated"]
                               for case in manifest["cases"]),
        expected_quarantine=sum(case["expected"]["quarantine"]
                                for case in manifest["cases"]),
    )


def validate_bounds(before, after):
    if before.keys() != after.keys():
        raise PreflightError("topic_set_changed")
    for topic, partitions in before.items():
        if partitions.keys() != after[topic].keys():
            raise PreflightError("partition_set_changed")
        for partition, start in partitions.items():
            end = after[topic][partition]
            if any(type(v) is not int or v < 0
                   for row in (start, end) for v in row.values()):
                raise PreflightError("invalid_offsets")
            if not (start["low"] <= start["high"] <= end["high"]
                    and end["low"] <= start["high"]):
                raise PreflightError("retention_or_offset_gap")


def collect_bounds(consumer, topics):
    from confluent_kafka import TopicPartition
    result = {}
    for topic in topics:
        metadata = consumer.list_topics(topic=topic, timeout=15)
        detail = metadata.topics.get(topic)
        if detail is None or detail.error is not None:
            raise PreflightError("topic_metadata_unavailable")
        if len(detail.partitions) != 3:
            raise PreflightError("partition_count_mismatch")
        result[topic] = {}
        for partition in sorted(detail.partitions):
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=15, cached=False,
            )
            result[topic][str(partition)] = dict(low=low, high=high)
    validate_bounds(result, result)
    return result


def validate_input_window(manifest, before, acks=None):
    """세 partition snapshot과 ACK 하한을 Spark/발행 전에 검사한다."""
    check_evidence(manifest, before)
    if before.get("status") != "READY":
        raise PreflightError("snapshot_not_ready")
    bounds = before.get("bounds")
    topics = set(manifest["contract"]["topics"].values())
    if not isinstance(bounds, dict) or set(bounds) != topics:
        raise PreflightError("snapshot_topic_set_mismatch")
    for partitions in bounds.values():
        if (not isinstance(partitions, dict)
                or set(partitions) != {"0", "1", "2"}):
            raise PreflightError("snapshot_partition_set_mismatch")
        for row in partitions.values():
            if not isinstance(row, dict) or set(row) != {"low", "high"}:
                raise PreflightError("snapshot_bounds_incomplete")
    validate_bounds(bounds, bounds)
    raw = bounds[manifest["contract"]["topics"]["raw"]]
    if acks is not None:
        validate_acks(manifest, acks)
        for ack in acks:
            partition = str(ack["partition"])
            if (partition not in raw
                    or ack["offset"] < raw[partition]["high"]):
                raise PreflightError("raw_ack_outside_start_window")
    return raw


def read_window(consumer, before, after, timeout=60, limit=10000):
    """수동 assign과 명시 offset만 사용한다. commit/subscribe 호출은 없다."""
    from confluent_kafka import KafkaError, TopicPartition
    validate_bounds(before, after)
    pending = {}
    for topic, partitions in before.items():
        for partition, start in partitions.items():
            high = after[topic][partition]["high"]
            if high > start["high"]:
                pending[(topic, int(partition))] = high
    assignment = [TopicPartition(t, p, before[t][str(p)]["high"])
                  for t, p in pending]
    if not assignment:
        return []
    consumer.assign(assignment)
    records = []
    deadline = time.monotonic() + timeout
    while pending:
        if time.monotonic() >= deadline:
            raise PreflightError("read_window_incomplete")
        remaining = max(0.01, deadline - time.monotonic())
        message = consumer.poll(min(1.0, remaining))
        if message is not None:
            key = (message.topic(), message.partition())
            if message.error():
                if message.error().code() != KafkaError._PARTITION_EOF:
                    raise PreflightError("consumer_error")
            elif key in pending and message.offset() < pending[key]:
                records.append(dict(
                    topic=key[0], partition=key[1], offset=message.offset(),
                    key_base64=(b64(message.key())
                                if message.key() is not None else None),
                    value_base64=(b64(message.value())
                                  if message.value() is not None else None),
                ))
                if len(records) > limit:
                    raise PreflightError("read_window_limit")
        for position in consumer.position(assignment):
            key = (position.topic, position.partition)
            if key in pending and position.offset >= pending[key]:
                del pending[key]
    return records


def read_silver(spark, manifest):
    from streaming.jobs.raw_ingestion import quoted_table
    from streaming.sinks.delta_silver import DeltaSilverStorage
    table = manifest["contract"]["silver_table"]
    details = {row.col_name: row.data_type for row in spark.sql(
        "DESCRIBE TABLE EXTENDED " + quoted_table(table),
    ).collect()}
    if (details.get("Provider", "").lower() != "delta"
            or details.get("Type", "").upper() != "MANAGED"):
        raise PreflightError("silver_must_be_managed_delta")
    expected_types = {
        field["name"]: (
            "timestamp" if field["name"] == "event_time" else "double"
            if field["name"] == "metric_value" else "string"
        ) for field in json.loads(SCHEMA_PATH.read_text())["fields"]
    }
    fields = spark.table(table).schema.fields
    if (len(fields) != 10 or {field.name: field.dataType.simpleString()
                            for field in fields} != expected_types):
        raise PreflightError("silver_schema_mismatch")
    storage = DeltaSilverStorage(
        spark=spark, table_name=manifest["contract"]["silver_table"],
    )
    return [dict(row) for event_id in sorted({case["event_id"]
            for case in manifest["cases"]})
            if (row := storage.lookup(event_id)) is not None]


def normalized(payload):
    payload = dict(payload)
    value = payload["event_time"]
    if isinstance(value, datetime):
        value = value.isoformat()
    payload["event_time"] = timestamp(value).isoformat(timespec="microseconds")
    return payload


def validate_acks(manifest, acks):
    names = [case["name"] for case in manifest["cases"]]
    if [ack.get("case") for ack in acks] != names:
        raise PreflightError("missing_or_unordered_ack")
    previous = -1
    partition = None
    for ack in acks:
        if (ack.get("topic") != manifest["contract"]["topics"]["raw"]
                or any(type(ack.get(k)) is not int or ack[k] < 0
                       for k in ("partition", "offset"))):
            raise PreflightError("invalid_ack")
        if partition is None:
            partition = ack["partition"]
        if ack["partition"] != partition or ack["offset"] <= previous:
            raise PreflightError("key_partition_or_order_changed")
        previous = ack["offset"]


def reconcile(manifest, acks, records, silver_rows, registry):
    """원본 ACK 위치·실제 bytes와 canonical 10필드를 모두 대사한다."""
    from streaming.quarantine.quarantine_record import build_quarantine_record
    from streaming.validation.domain_validator import ValidationError
    from streaming.validation.domain_validator import ValidationResult
    validate_acks(manifest, acks)
    topics = manifest["contract"]["topics"]
    cases = {case["name"]: case for case in manifest["cases"]}
    positions = {(a["topic"], a["partition"], a["offset"]): cases[a["case"]]
                 for a in acks}
    issues = []
    raw_seen, validated, quarantined = Counter(), Counter(), Counter()
    validated_lineage, quarantine_lineage, observed_reasons = {}, {}, {}
    expected_events = {}
    expected_counts = Counter()
    for case in manifest["cases"]:
        if case["expected"]["silver"]:
            event = normalized(case["expected"]["canonical"])
            expected_events[case["event_id"]] = event
        if case["expected"]["validated"]:
            expected_counts[case["event_id"]] += 1
    for record in records:
        position = (record["topic"], record["partition"], record["offset"])
        if record["topic"] == topics["raw"] and position in positions:
            case = positions[position]
            raw_seen[case["name"]] += 1
            if any(record[field] != case[field]
                   for field in ("key_base64", "value_base64")):
                issues.append("raw_bytes_mismatch:" + case["name"])
        elif record["topic"] == topics["validated"]:
            try:
                event = decode_sensor_event(base64.b64decode(
                    record["value_base64"], validate=True,
                ), registry)
            except SensorDecodeError:
                issues.append("validated_decode_error_in_window")
                continue
            event_id = event.get("event_id", "")
            if not event_id.startswith(manifest["run_id"] + "/"):
                continue
            validated[event_id] += 1
            validated_lineage.setdefault(event_id, []).append(list(position))
            if (event_id not in expected_events
                    or normalized(event) != expected_events[event_id]
                    or record["key_base64"] != b64(b"fridge-001")):
                issues.append("validated_payload_mismatch:" + event_id)
        elif record["topic"] == topics["quarantine"]:
            try:
                value = json.loads(base64.b64decode(record["value_base64"]))
                origin = (value["source_topic"], value["source_partition"],
                          value["source_offset"])
            except (ValueError, KeyError, TypeError):
                issues.append("quarantine_decode_error_in_window")
                continue
            if origin not in positions:
                if str(value.get("event_id", "")).startswith(
                    manifest["run_id"] + "/",
                ):
                    issues.append("unexpected_run_quarantine_lineage")
                continue
            case = positions[origin]
            quarantined[case["name"]] += 1
            quarantine_lineage.setdefault(case["name"], []).append(
                list(position)
            )
            observed_reasons[case["name"]] = [
                [error["code"], error["field"]]
                for error in value.get("errors", [])
            ]
            errors = case["expected"]["errors"]
            raw = base64.b64decode(case["value_base64"])
            expected = build_quarantine_record(
                ValidationResult(tuple(ValidationError(code, field, "expected")
                                       for code, field in errors)),
                source_topic=origin[0], source_partition=origin[1],
                source_offset=origin[2], raw_key=b"fridge-001", raw_value=raw,
                event_id=case["expected"]["quarantine_event_id"],
                schema_id=(int.from_bytes(raw[1:5], "big")
                           if raw[0] == 0 else None),
            ) if errors else None
            if (expected is None
                    or value.get("quarantine_id") != expected.quarantine_id
                    or value.get("event_id") != expected.event_id
                    or value.get("schema_id") != expected.schema_id
                    or value.get("raw_value_sha256") != sha256(raw).hexdigest()
                    or value.get("raw_key_base64") != case["key_base64"]
                    or record["key_base64"] != b64(
                        expected.quarantine_id.encode())
                    or sorted((e["code"], e["field"])
                              for e in value.get("errors", []))
                    != sorted(map(tuple, errors))):
                issues.append("quarantine_payload_mismatch:" + case["name"])
    for name, case in cases.items():
        if raw_seen[name] != 1:
            issues.append("raw_lineage_missing_or_duplicate:" + name)
        if bool(quarantined[name]) != case["expected"]["quarantine"]:
            issues.append("quarantine_delivery_mismatch:" + name)
    for event_id, count in expected_counts.items():
        if validated[event_id] < count:
            issues.append("validated_missing:" + event_id)
    rows = [normalized(row) for row in silver_rows]
    row_ids = Counter(row["event_id"] for row in rows)
    if row_ids != Counter({key: 1 for key in expected_events}):
        issues.append("silver_identity_or_count_mismatch")
    if any(row != expected_events.get(row["event_id"]) for row in rows):
        issues.append("silver_canonical_payload_mismatch")
    extra = sum(max(0, n - expected_counts[key])
                for key, n in validated.items())
    extra += sum(max(0, n - 1) for n in quarantined.values())
    return dict(
        **identity(manifest), status="FAIL" if issues else "PASS",
        issues=issues, extra_deliveries=extra,
        validated_by_event=dict(validated),
        quarantine_by_case=dict(quarantined),
        canonical=sorted(rows, key=lambda row: row["event_id"]),
        raw_acknowledgements=acks,
        canonical_fingerprints={row["event_id"]: digest(row) for row in rows},
        validated_lineage_by_event=validated_lineage,
        quarantine_lineage_by_case=quarantine_lineage,
        failure_reasons_by_case=observed_reasons,
    )


def open_cloud(config_path, credential, run_id, spark):
    """기존 IAM signer를 재사용하고 consumer의 자동 commit을 끈다."""
    from confluent_kafka import Consumer
    from confluent_kafka.schema_registry import SchemaRegistryClient
    from streaming.jobs import runtime_config as runtime
    settings = load_runtime_config(config_path)
    utility = runtime.get_dbutils(spark)
    kafka = replace(settings.kafka, producer_service_credential=credential)
    options = runtime.runtime_producer_options(
        kafka.producer_options(utility), spark=spark,
    )
    for key in ("acks", "delivery.report.only.error"):
        options.pop(key, None)
    options.update({
        "group.id": "ktd-p2e2e-" + run_id[4:],
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "error", "enable.partition.eof": True,
        "log_level": 0,
    })
    consumer = Consumer(options)
    try:
        registry = SchemaRegistryClient(settings.registry.resolve(utility))
    except Exception:
        consumer.close()
        raise
    return consumer, registry


def check_registry(manifest, registry):
    from confluent_kafka.schema_registry import Schema
    from confluent_kafka.schema_registry.error import SchemaRegistryError
    expected = json.loads(SCHEMA_PATH.read_text())
    actual = registry.get_schema(manifest["schema_id"]).schema_str
    if json.loads(actual) != expected:
        raise PreflightError("writer_schema_mismatch")
    for name in ("raw", "validated"):
        schema = registry.lookup_schema(
            manifest["contract"]["topics"][name] + "-value",
            Schema(SCHEMA_PATH.read_text(), "AVRO"),
        )
        if name == "raw" and schema.schema_id != manifest["schema_id"]:
            raise PreflightError("raw_subject_schema_id_mismatch")
    try:
        registry.get_schema(manifest["unknown_schema_id"])
    except SchemaRegistryError as error:
        if error.http_status_code == 404 and error.error_code == 40403:
            return
        raise PreflightError("unknown_schema_probe_unconfirmed") from None
    raise PreflightError("unknown_schema_id_exists")


def take_snapshot(manifest, consumer, registry, spark):
    check_registry(manifest, registry)
    bounds = collect_bounds(consumer, manifest["contract"]["topics"].values())
    if read_silver(spark, manifest):
        raise PreflightError("existing_run_identity_collision")
    return dict(**identity(manifest), bounds=bounds, status="READY",
                captured_at=datetime.now(timezone.utc).isoformat())


def check_raw_principal(expected_role, region, client=None):
    """발행 직전 default credential chain의 실제 STS role만 확인한다."""
    if client is None:
        import botocore.session
        client = botocore.session.get_session().create_client(
            "sts", region_name=region,
        )
    try:
        principal = client.get_caller_identity()["Arn"]
    finally:
        client.close()
    account = expected_role.split(":")[4]
    role = re.escape(expected_role.rsplit("/", 1)[1])
    assumed = rf"arn:aws:sts::{account}:assumed-role/{role}/[^/]+"
    if principal != expected_role and not re.fullmatch(assumed, principal):
        raise PreflightError("raw_producer_principal_mismatch")
    return principal


def publish(manifest, before, output, produce=None, attempt_path=None,
            caller_principal=None):
    """시도 파일을 먼저 저장하고 불명확한 전달은 자동 재전송하지 않는다."""
    raw = validate_input_window(manifest, before)
    age = datetime.now(timezone.utc) - timestamp(manifest["reference_time"])
    if not timedelta(0) <= age <= timedelta(minutes=2):
        raise PreflightError("fixture_expired_regenerate_with_new_run_id")
    if produce is None:
        from scripts.phase1_msk_fixture_producer import produce_fixture
        produce = produce_fixture
    from scripts.phase1_msk_fixture_producer import Fixture
    validate_isolation(manifest)
    output = Path(output)
    lock = attempt_path or output.parent / (
        manifest["run_id"] + ".publish.lock"
    )
    write_json(lock, identity(manifest))
    write_json(output.with_suffix(".attempt.json"), identity(manifest))
    acks = []
    for case in manifest["cases"]:
        append_json(output.with_suffix(".journal.jsonl"), dict(
            case=case["name"], state="ATTEMPTING", **identity(manifest),
        ))
        fixture = Fixture(
            manifest["contract"]["topics"]["raw"], "fridge-001",
            case["event_id"], "1.0.0",
            base64.b64decode(case["key_base64"]),
            base64.b64decode(case["value_base64"]),
        )
        try:
            ack = produce(fixture, manifest["contract"]["bootstrap"],
                          manifest["contract"]["region"])
            acks.append(dict(case=case["name"], topic=ack.topic,
                             partition=ack.partition, offset=ack.offset))
            # 부분 ACK도 즉시 타입·순서를 검사하고 불명확한 결과는 중단한다.
            prefix = {**manifest, "cases": manifest["cases"][:len(acks)]}
            validate_acks(prefix, acks)
            if (str(ack.partition) not in raw
                    or ack.offset < raw[str(ack.partition)]["high"]):
                raise PreflightError("raw_ack_outside_start_window")
            append_json(output.with_suffix(".journal.jsonl"), dict(
                state="ACKED", **acks[-1],
            ))
        except Exception as error:
            write_json(output, dict(
                **identity(manifest), status="UNCONFIRMED", acks=acks,
                failed_case=case["name"], error_type=type(error).__name__,
                caller_principal=caller_principal,
            ))
            raise PreflightError("partial_publish_inspect_journal_no_resend")
    result = dict(**identity(manifest), status="ACKED", acks=acks,
                  caller_principal=caller_principal)
    write_json(output, result)
    return result


def run_job(manifest, acks, before, config_path, spark, fault="none",
            timeout=180):
    """실제 Job 2 handler를 run의 ACK 위치에만 적용하는 격리 harness다."""
    from contextlib import ExitStack
    from unittest.mock import patch
    from confluent_kafka.schema_registry.error import SchemaRegistryError
    from streaming.jobs import runtime_config as runtime
    from streaming.jobs import sensor_validation_job as job
    check_evidence(manifest, before)
    validate_acks(manifest, acks)
    settings = runtime.load_runtime_config(config_path)
    validate_isolation(manifest, settings)
    if type(timeout) is not int or not 1 <= timeout <= 180:
        raise PreflightError("query_timeout_must_not_exceed_180")
    if spark.streams.active:
        raise PreflightError("shared_session_has_active_queries")
    raw_bounds = validate_input_window(manifest, before, acks)
    contract = manifest["contract"]
    # table/Volume 생성은 하지 않는다. grants/schema는 기존 adapter가 검사한다.
    read_silver(spark, manifest)
    resolved = settings.resolve_clients(runtime.get_dbutils(spark))
    config = replace(
        settings.job, silver_table=contract["silver_table"],
        processing_version="git:" + manifest["deployment_sha"],
        source=replace(settings.job.source, checkpoint=contract["checkpoint"]),
    )
    handler = job.make_batch_handler(
        config, settings.rules, registry_config=resolved.registry,
        producer_config=resolved.producer,
    )
    partition = acks[0]["partition"]
    offsets = [ack["offset"] for ack in acks]
    batches = []
    injected = False

    def batch_handler(batch, batch_id):
        nonlocal injected
        selected = batch.where(
            (batch["partition"] == partition) & batch["offset"].isin(offsets)
        )
        if selected.isEmpty():
            return
        mode = fault if not injected else "none"
        injected = True
        with ExitStack() as stack:
            if mode == "registry":
                stack.enter_context(patch.object(
                    job.SchemaRegistryClient, "get_schema",
                    side_effect=SchemaRegistryError(503, 50001, "injected"),
                ))
            elif mode == "delta":
                stack.enter_context(patch.object(
                    job.DeltaSilverStorage, "insert_if_absent",
                    side_effect=OSError("injected_delta_write_failure"),
                ))
            elif mode in ("producer_timeout", "ack_uncertain"):
                factory = job.Producer

                def wrap(options):
                    producer = factory(options)
                    return FaultProducer(producer, mode)

                stack.enter_context(patch.object(job, "Producer", wrap))
            counts = handler(selected, batch_id)
            batches.append(dict(batch_id=batch_id, counts=counts))
        if mode in ("after_ack", "batch"):
            raise RuntimeError("injected_after_ack_before_checkpoint")

    options = settings.kafka.spark_options(config.source)
    options.pop("subscribe", None)
    raw_topic = contract["topics"]["raw"]
    options["assign"] = json.dumps({raw_topic: [int(p) for p in raw_bounds]})
    options["startingOffsets"] = json.dumps({raw_topic: {
        partition: bounds["high"] for partition, bounds in raw_bounds.items()
    }})
    query = None
    result = dict(**identity(manifest), checkpoint=contract["checkpoint"],
                  fault=fault, batches=batches)
    try:
        source = spark.readStream.format("kafka").options(**options).load()
        query = (
            source.writeStream.foreachBatch(batch_handler)
            .option("checkpointLocation", contract["checkpoint"])
            .queryName("ktd_e2e_" + manifest["run_id"][4:])
            .trigger(availableNow=True).start()
        )
        if not query.awaitTermination(timeout):
            raise PreflightError("job_timeout")
        result["status"] = "QUERY_COMPLETED_REQUIRES_RECONCILE"
    except Exception as error:
        result.update(status="FAILED", error_type=type(error).__name__)
        if isinstance(error, PreflightError):
            result["reason"] = str(error)
    finally:
        if query is not None:
            if query.isActive:
                query.stop()
            result.update(
                query_id=str(query.id), run_id_spark=str(query.runId),
                progress=[json.loads(item.json) if hasattr(item, "json")
                          else item for item in query.recentProgress],
            )
    return result


class FaultProducer:
    """해당 격리 harness의 producer만 감싸며 broker/IAM을 변경하지 않는다."""

    def __init__(self, producer, mode):
        self.producer = producer
        self.mode = mode

    def produce(self, **kwargs):
        if self.mode == "producer_timeout":
            raise TimeoutError("injected_before_produce")
        return self.producer.produce(**kwargs)

    def flush(self, timeout):
        remaining = self.producer.flush(timeout)
        if self.mode == "ack_uncertain":
            raise TimeoutError("injected_after_flush_ack_uncertainty")
        return remaining


def compare_reports(first, second):
    if any(first.get(key) != second.get(key)
           for key in ("run_id", "manifest_sha256")):
        raise PreflightError("restart_report_identity_mismatch")
    stable = (first.get("status") == second.get("status") == "PASS"
              and first["canonical"] == second["canonical"])
    return dict(
        run_id=first["run_id"], status="PASS" if stable else "FAIL",
        canonical_unchanged=stable,
        extra_before=first.get("extra_deliveries"),
        extra_after=second.get("extra_deliveries"),
    )


def job_request(manifest, cluster_id, python_file, parameters, readiness=None):
    """기존 Compute용 submit JSON만 만든다. Jobs API를 호출하지 않는다."""
    if (not cluster_id or not python_file.startswith("/Workspace/")
            or not python_file.endswith("/scripts/phase2/cloud_e2e.py")):
        raise PreflightError("explicit_existing_compute_and_script_required")
    if (not parameters
            or parameters[0] not in ("snapshot", "run-job", "verify")):
        raise PreflightError("unsupported_cloud_task")
    try:
        approved = parameters[parameters.index("--approve-run-id") + 1]
    except (ValueError, IndexError):
        raise PreflightError("task_run_approval_parameter_required") from None
    if approved != manifest["run_id"] or "--execute" not in parameters:
        raise PreflightError("task_run_approval_parameter_mismatch")
    if readiness is None:
        raise PreflightError("readiness_required_before_job_request")
    validate_readiness(manifest, readiness)
    if cluster_id != readiness["compute"]["cluster_id"]:
        raise PreflightError("approved_compute_mismatch")
    required = ["--runtime-config", "--manifest", "--output", "--readiness"]
    if parameters[0] in ("run-job", "verify"):
        required += ["--before", "--acks"]
    if parameters[0] in ("snapshot", "verify"):
        required += ["--verification-credential"]
    for flag in required:
        try:
            value = parameters[parameters.index(flag) + 1]
        except (ValueError, IndexError):
            raise PreflightError("task_parameter_missing") from None
        if not value or value.startswith("--"):
            raise PreflightError("task_parameter_missing")
        if flag != "--verification-credential" and not value.startswith("/"):
            raise PreflightError("task_paths_must_be_absolute")
        if (flag == "--verification-credential"
                and value != readiness["verification_credential"]):
            raise PreflightError("verification_credential_mismatch")
    return dict(
        run_name=manifest["run_id"] + "-" + parameters[0],
        idempotency_token=digest([identity(manifest), parameters]),
        timeout_seconds=600,
        tasks=[dict(
            task_key=parameters[0].replace("-", "_"),
            existing_cluster_id=cluster_id, timeout_seconds=600, max_retries=0,
            spark_python_task=dict(python_file=python_file,
                                   parameters=parameters),
        )],
    )


def runtime_spark():
    """Cloud 실행 게이트를 통과한 경우에만 DBR 세션에 접근한다."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


def check_query_restart(first, second):
    stable = (
        first.get("run_id") == second.get("run_id")
        and first.get("manifest_sha256") == second.get("manifest_sha256")
        and first.get("checkpoint") == second.get("checkpoint")
        and all(isinstance(report.get(key), str) and report[key]
                for report in (first, second)
                for key in ("run_id", "manifest_sha256", "checkpoint",
                            "query_id", "run_id_spark"))
        and first["query_id"] == second.get("query_id")
        and first.get("run_id_spark") != second.get("run_id_spark")
        and second.get("status") == "QUERY_COMPLETED_REQUIRES_RECONCILE"
    )
    return dict(status="PASS" if stable else "FAIL",
                same_checkpoint_query=stable)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=(
        "prepare", "snapshot", "publish", "run-job", "verify", "compare",
    ))
    parser.add_argument("--runtime-config", type=Path,
                        default=ROOT / "config/job2-runtime.json")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--run-id")
    parser.add_argument("--reference-time")
    parser.add_argument("--schema-id", type=int)
    parser.add_argument("--unknown-schema-id", type=int, default=2147483647)
    parser.add_argument("--silver-table")
    parser.add_argument("--readiness", type=Path)
    parser.add_argument("--before", type=Path)
    parser.add_argument("--acks", type=Path)
    parser.add_argument("--previous-report", type=Path)
    parser.add_argument("--query-before", type=Path)
    parser.add_argument("--query-after", type=Path)
    parser.add_argument("--verification-credential")
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--approve-run-id")
    parser.add_argument("--fault", default="none", choices=(
        "none", "registry", "delta", "producer_timeout", "ack_uncertain",
        "after_ack", "batch",
    ))
    parser.add_argument("--approve-fault")
    args = parser.parse_args(argv)
    try:
        if args.action == "prepare":
            if args.schema_id is None:
                raise PreflightError("schema_id_required_offline_candidate")
            manifest = make_manifest(
                args.runtime_config, args.run_id or "p2i-" + uuid4().hex,
                args.reference_time or datetime.now(timezone.utc).isoformat(),
                args.schema_id, args.unknown_schema_id, args.silver_table,
            )
            write_json(args.manifest, manifest)
            if args.readiness:
                write_json(args.readiness, readiness_template(manifest))
            print(json.dumps(plan(manifest, "prepare")))
            return 0
        manifest = load_manifest(args.manifest, args.runtime_config)
        if args.action == "compare":
            first = json.loads(args.previous_report.read_text())
            second = json.loads(args.output.read_text())
            check_evidence(manifest, first)
            check_evidence(manifest, second)
            result = compare_reports(first, second)
            result["scope"] = "canonical_only"
            if args.query_before or args.query_after:
                query_first = json.loads(args.query_before.read_text())
                query_second = json.loads(args.query_after.read_text())
                check_evidence(manifest, query_first)
                check_evidence(manifest, query_second)
                restart = check_query_restart(query_first, query_second)
                result.update(scope="canonical_and_query_restart",
                              query_restart=restart)
                if restart["status"] == "FAIL":
                    result["status"] = "FAIL"
        elif not args.execute:
            result = plan(manifest, args.action)
        else:
            if args.approve_run_id != manifest["run_id"]:
                raise PreflightError("explicit_matching_run_approval_required")
            if (args.fault != "none" and args.approve_fault != args.fault):
                raise PreflightError("separate_fault_approval_required")
            if args.readiness is None:
                raise PreflightError("readiness_required_before_cloud_access")
            readiness = json.loads(args.readiness.read_text())
            validate_readiness(
                manifest, readiness, args.verification_credential,
            )
            if args.output is None or args.output.exists():
                raise PreflightError("fresh_output_path_required")
            if args.action == "publish":
                before = json.loads(args.before.read_text())
                caller_principal = check_raw_principal(
                    readiness["principals"]["raw_producer"],
                    manifest["contract"]["region"],
                )
                result = publish(
                    manifest, before, args.output,
                    attempt_path=args.manifest.with_suffix(".publish.lock"),
                    caller_principal=caller_principal,
                )
            else:
                spark = runtime_spark()
                if args.action == "run-job":
                    before = json.loads(args.before.read_text())
                    ack_report = json.loads(args.acks.read_text())
                    check_evidence(manifest, ack_report)
                    result = run_job(
                        manifest, ack_report["acks"], before,
                        args.runtime_config, spark, args.fault,
                    )
                else:
                    if not args.verification_credential:
                        raise PreflightError(
                            "verification_credential_required",
                        )
                    consumer, registry = open_cloud(
                        args.runtime_config, args.verification_credential,
                        manifest["run_id"], spark,
                    )
                    try:
                        with registry:
                            if args.action == "snapshot":
                                result = take_snapshot(
                                    manifest, consumer, registry, spark,
                                )
                            else:
                                before = json.loads(args.before.read_text())
                                ack_report = json.loads(args.acks.read_text())
                                check_evidence(manifest, before)
                                check_evidence(manifest, ack_report)
                                end = collect_bounds(consumer, manifest[
                                    "contract"]["topics"].values())
                                records = read_window(
                                    consumer, before["bounds"], end,
                                )
                                result = reconcile(
                                    manifest, ack_report["acks"], records,
                                    read_silver(spark, manifest), registry,
                                )
                                result.update(bounds_before=before["bounds"],
                                              bounds_after=end,
                                              observed_records=records)
                            result.update(
                                verification_credential=(
                                    args.verification_credential
                                ),
                                verification_group="ktd-p2e2e-"
                                + manifest["run_id"][4:],
                            )
                    finally:
                        consumer.close()
                write_json(args.output, result)
        print(json.dumps(result, default=str))
        return 1 if result.get("status") in ("FAIL", "FAILED") else 0
    except Exception as error:
        # Runtime/HTTP/Kafka 예외 원문에는 secret이 포함될 수 있다.
        result = dict(status="BLOCKED", error_type=type(error).__name__)
        if isinstance(error, PreflightError):
            result["reason"] = str(error)
        if args.output is not None and not args.output.exists():
            write_json(args.output, result)
        print(json.dumps(result))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
