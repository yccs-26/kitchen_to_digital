"""Opt-in: 실제 Kafka/Registry/Spark/Delta E2E와 sink 복구 경계."""

import base64
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient

from scripts.phase2 import cloud_e2e as tools

ROOT = Path(__file__).resolve().parents[2]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.environ.get("KTD_RUN_LOCAL_KAFKA") != "1",
                       reason="requires explicit local Kafka/Delta opt-in"),
]


def wait_for(check, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except Exception:
            pass
        time.sleep(0.2)
    raise TimeoutError("local test dependency did not become ready")


def docker(*args):
    return subprocess.run(["docker", *args], check=True, capture_output=True,
                          text=True, timeout=45).stdout


@pytest.fixture(scope="module")
def fault_services():
    """다른 workload에 영향을 주지 않는 별도 KTD 로컬 fault broker/Registry."""
    suffix = uuid4().hex[:8]
    kafka, registry = "ktd-p2-kafka-" + suffix, "ktd-p2-registry-" + suffix
    docker(
        "run", "-d", "--name", kafka, "--network", "docker_default",
        "-p", "127.0.0.1:19092:9092", "-e", "KAFKA_NODE_ID=1",
        "-e", "KAFKA_PROCESS_ROLES=broker,controller",
        "-e", "KAFKA_LISTENERS=CONTROLLER://:9093,"
        "INTERNAL://:29092,EXTERNAL://:9092",
        "-e", f"KAFKA_ADVERTISED_LISTENERS=INTERNAL://{kafka}:29092,"
        "EXTERNAL://127.0.0.1:19092",
        "-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP="
        "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT",
        "-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e", "KAFKA_INTER_BROKER_LISTENER_NAME=INTERNAL",
        "-e", f"KAFKA_CONTROLLER_QUORUM_VOTERS=1@{kafka}:9093",
        "-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        "apache/kafka:4.1.0",
    )
    try:
        wait_for(lambda: AdminClient({
            "bootstrap.servers": "127.0.0.1:19092",
        }).list_topics(timeout=2))
        docker(
            "run", "-d", "--name", registry, "--network", "docker_default",
            "-p", "127.0.0.1:18081:8081", "-e",
            "SCHEMA_REGISTRY_HOST_NAME=" + registry, "-e",
            "SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081", "-e",
            "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS="
            f"PLAINTEXT://{kafka}:29092",
            "confluentinc/cp-schema-registry:8.1.5",
        )
        wait_for(lambda: SchemaRegistryClient({
            "url": "http://127.0.0.1:18081", "timeout": 1,
        }).get_subjects() is not None)
        yield kafka, registry
    finally:
        # 컨테이너와 모든 Volume/data는 증빙·재실행을 위해 보존한다.
        for name in (registry, kafka):
            subprocess.run(["docker", "stop", name], capture_output=True,
                           timeout=45)


def seed(root, mode, bootstrap, registry_url):
    prefix = "ktd.p2local." + uuid4().hex
    topics = {name: prefix + "." + name
              for name in ("raw", "validated", "quarantine")}
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for future in admin.create_topics([
        NewTopic(t, num_partitions=3, replication_factor=1)
        for t in topics.values()
    ]).values():
        future.result(15)
    with SchemaRegistryClient({"url": registry_url}) as client:
        schema = Schema(tools.SCHEMA_PATH.read_text(), "AVRO")
        schema_id = client.register_schema(topics["raw"] + "-value", schema)
        client.register_schema(topics["validated"] + "-value", schema)
    manifest = tools.make_manifest(
        ROOT / "config/job2-runtime.example.json", "p2i-" + uuid4().hex,
        datetime.now(timezone.utc).isoformat(), schema_id,
    )
    manifest["contract"].update(topics=topics, bootstrap=bootstrap)
    manifest.update(local_registry_url=registry_url,
                    scope="LOCAL controlled clock; not Cloud manifest",
                    injected_fault=mode)
    (root / "manifest.json").write_text(json.dumps(manifest))
    producer = Producer({"bootstrap.servers": bootstrap, "acks": "all",
                         "allow.auto.create.topics": False})
    acks = []
    for case in manifest["cases"]:
        def ack(error, message, case=case):
            assert error is None
            acks.append(dict(case=case["name"], topic=message.topic(),
                             partition=message.partition(),
                             offset=message.offset()))
        producer.produce(
            topics["raw"], partition=1,
            key=base64.b64decode(case["key_base64"]),
            value=base64.b64decode(case["value_base64"]), on_delivery=ack,
        )
        assert producer.flush(15) == 0
    tools.validate_acks(manifest, acks)
    assert len(acks) == 10
    (root / "raw-acks.json").write_text(json.dumps(acks))
    return manifest, acks


def worker(root, mode, expected, services=None, checkpoint="checkpoint"):
    number = len(list(root.glob("worker-*.log")))
    log = root / f"worker-{number}-{mode}.log"
    with log.open("w") as output:
        process = subprocess.Popen(
            [sys.executable, str(Path(__file__).with_name(
                "job2_local_kafka_worker.py")), str(root), mode,
             "--checkpoint", checkpoint], cwd=ROOT,
            env={**os.environ, "SPARK_LOCAL_IP": "127.0.0.1"},
            stdout=output, stderr=subprocess.STDOUT, start_new_session=True,
        )
        try:
            if mode == "kill_after_ack":
                wait_for(lambda: (root / "kill-ready").exists(), 90)
                os.killpg(process.pid, signal.SIGKILL)
            elif mode in ("broker", "registry"):
                wait_for(lambda: (root / "fault-ready").exists(), 90)
                target = services[0 if mode == "broker" else 1]
                docker("stop", target)
                (root / "fault-released").write_text(mode)
            code = process.wait(timeout=150)
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            if mode in ("broker", "registry"):
                docker("start", services[0 if mode == "broker" else 1])
                if mode == "broker":
                    wait_for(lambda: AdminClient({
                        "bootstrap.servers": "127.0.0.1:19092",
                    }).list_topics(timeout=2))
                else:
                    wait_for(lambda: SchemaRegistryClient({
                        "url": "http://127.0.0.1:18081", "timeout": 1,
                    }).get_subjects() is not None)
    if expected == 0:
        assert code == 0, log.read_text()[-6000:]
    else:
        assert code != 0, "fault did not fail query"
    return code


def reconcile(root, manifest, acks):
    consumer = Consumer({
        "bootstrap.servers": manifest["contract"]["bootstrap"],
        "group.id": "ktd-local-observe-" + uuid4().hex,
        "enable.auto.commit": False, "enable.auto.offset.store": False,
        "auto.offset.reset": "error", "enable.partition.eof": True,
    })
    try:
        after = tools.collect_bounds(
            consumer, manifest["contract"]["topics"].values(),
        )
        before = {t: {p: dict(low=0, high=0) for p in bounds}
                  for t, bounds in after.items()}
        records = tools.read_window(consumer, before, after, timeout=30)
    finally:
        consumer.close()
    with SchemaRegistryClient({"url": manifest["local_registry_url"]}) as reg:
        report = tools.reconcile(
            manifest, acks, records,
            json.loads((root / "canonical.json").read_text()), reg,
        )
    report.update(bounds=after, records=records)
    assert report["status"] == "PASS", report["issues"]
    return report


@pytest.mark.parametrize("mode", [
    "normal", "before_merge", "before_publish", "uncertain_ack",
    "quarantine", "kill_after_ack", "broker", "registry",
])
def test_real_kafka_delta_failure_and_recovery(tmp_path, mode, request):
    services = None
    if mode in ("broker", "registry"):
        services = request.getfixturevalue("fault_services")
    bootstrap = "127.0.0.1:19092" if services else "localhost:9092"
    registry_url = ("http://127.0.0.1:18081" if services
                    else "http://localhost:8081")
    manifest, acks = seed(tmp_path, mode, bootstrap, registry_url)
    if mode != "normal":
        worker(tmp_path, mode, 1, services)
        assert not (tmp_path / "checkpoint/commits/0").exists()
    worker(tmp_path, "success", 0)
    first = reconcile(tmp_path, manifest, acks)
    assert len(first["canonical"]) == 4
    assert sum(first["validated_by_event"].values()) >= 5
    assert len(first["quarantine_by_case"]) == 5
    queries = [json.loads(x) for x in
               (tmp_path / "queries.jsonl").read_text().splitlines()]
    if mode != "normal":
        assert queries[0]["id"] == queries[1]["id"]
        assert queries[0]["run_id"] != queries[1]["run_id"]
    # 완료된 Kafka checkpoint 재개는 입력을 되감지 않는다.
    if mode == "normal":
        worker(tmp_path, "success", 0)
        resumed = reconcile(tmp_path, manifest, acks)
        assert resumed["canonical_fingerprints"] == first[
            "canonical_fingerprints"
        ]
        assert resumed["bounds"] == first["bounds"]
        # 같은 오래된 event_id를 새 Kafka 위치로 다시 보낸다. 시간 창 밖에서도
        # 영속 Silver가 canonical을 유지하고 validated는 재발행해야 한다.
        case = manifest["cases"][0]
        producer = Producer({"bootstrap.servers": bootstrap, "acks": "all"})
        duplicate_acks = []

        def acknowledged(error, message):
            assert error is None
            duplicate_acks.append(dict(partition=message.partition(),
                                       offset=message.offset()))

        producer.produce(
            manifest["contract"]["topics"]["raw"], partition=1,
            key=base64.b64decode(case["key_base64"]),
            value=base64.b64decode(case["value_base64"]),
            on_delivery=acknowledged,
        )
        assert producer.flush(15) == 0
        assert duplicate_acks == [dict(partition=1, offset=10)]
        worker(tmp_path, "late_duplicate", 0)
        aged = reconcile(tmp_path, manifest, acks)
        assert aged["canonical_fingerprints"] == first[
            "canonical_fingerprints"
        ]
        batch = json.loads((tmp_path / "batches.jsonl").read_text(
        ).splitlines()[-1])
        assert batch["received"] == batch["duplicate"] == batch["late"] == 1
    worker(tmp_path, "success", 0, checkpoint="fresh_checkpoint")
    replay = reconcile(tmp_path, manifest, acks)
    assert replay["canonical_fingerprints"] == first["canonical_fingerprints"]
    assert replay["extra_deliveries"] >= first["extra_deliveries"] + 10
    (tmp_path / "result.json").write_text(json.dumps(dict(
        scope="REAL local Kafka/Registry/Spark/Delta, not MSK/UC",
        mode=mode, first=first, replay=replay, queries=queries,
    ), indent=2))
