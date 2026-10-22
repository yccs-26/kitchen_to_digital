"""실제 로컬 Delta/checkpoint 검증. broker·Registry와 UC는 검증하지 않는다."""

import base64
from datetime import datetime, timezone
from io import BytesIO
import json
import os
from pathlib import Path
import subprocess
import sys

from fastavro import schemaless_reader
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "tests/unit"))
from test_sensor_validation_job import raw_record, SCHEMA  # noqa: E402

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("KTD_RUN_LOCAL_DELTA") != "1",
        reason="requires isolated local OSS Spark/Delta/JDK",
    ),
]


def write_input(root, name, events):
    records = []
    for offset, event in events:
        record = raw_record(event, offset=offset)
        for field in ("key", "value"):
            record[field] = base64.b64encode(record[field]).decode()
        records.append(json.dumps(record))
    (root / "input" / name).write_text("\n".join(records) + "\n")


def read_lines(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


def run_worker(root, mode, expected, checkpoint="checkpoint"):
    attempt = len(list(root.glob("worker-*.log")))
    log = root / f"worker-{attempt}-{mode}-{checkpoint}.log"
    with log.open("w") as output:
        result = subprocess.run(
            [sys.executable, str(Path(__file__).with_name(
                "job2_local_recovery_worker.py"
            )), str(root), mode, "--checkpoint", checkpoint],
            cwd=ROOT, env={**os.environ, "SPARK_LOCAL_IP": "127.0.0.1"},
            stdout=output, stderr=subprocess.STDOUT, timeout=240,
        )
    assert result.returncode == expected, log.read_text()[-12000:]
    if mode == "before_ack":
        assert "injected: Silver committed, ACK unavailable" in log.read_text()


def test_fresh_process_delta_and_checkpoint_recovery(tmp_path):
    """불확실 ACK·강제 종료·offset 재개·새 checkpoint replay가 수렴한다."""
    (tmp_path / "input").mkdir()
    event = dict(
        event_id="local-recovery-1",
        event_time="2016-10-08T12:00:00.123456+00:00",
        store_id="store-001", equipment_id="fridge-001",
        equipment_type="refrigerator", metric_name="temperature_celsius",
        metric_value=4.2, unit="celsius", schema_version="1.0.0",
        source="simulator",
    )
    write_input(tmp_path, "batch0.json", [
        (0, event), (1, event), (2, {**event, "metric_value": 9.5}),
        (3, {**event, "event_id": "invalid", "unit": "wrong"}),
        (4, {**event, "event_id": "local-recovery-2"}),
    ])
    # 실제 Delta commit 후 ACK 실패: Spark offset 로그는 있으나 commit은 없다.
    run_worker(tmp_path, "before_ack", 1)
    assert (tmp_path / "delta" / "_delta_log").is_dir()
    assert (tmp_path / "checkpoint" / "offsets" / "0").is_file()
    assert not (tmp_path / "checkpoint" / "commits" / "0").exists()
    assert not (tmp_path / "acks.jsonl").exists()

    # 새 Python/JVM: canonical 재조회 후 ACK, foreachBatch 반환 전 프로세스 종료.
    run_worker(tmp_path, "after_ack", 23)
    first_acks = read_lines(tmp_path / "acks.jsonl")
    assert len(first_acks) == 5
    assert not (tmp_path / "checkpoint" / "commits" / "0").exists()

    run_worker(tmp_path, "success", 0)
    assert (tmp_path / "checkpoint" / "commits" / "0").is_file()
    acks = read_lines(tmp_path / "acks.jsonl")
    assert acks[:5] == acks[5:10]  # 재발행과 conflict 증빙까지 동일하다.
    canonical = json.loads((tmp_path / "canonical.json").read_text())
    assert len(canonical) == 2
    assert all(row["metric_value"] == 4.2 for row in canonical)
    assert all(datetime.fromisoformat(row["event_time"]) == datetime(
        2016, 10, 8, 12, 0, 0, 123456, tzinfo=timezone.utc,
    ) for row in canonical)
    batches = read_lines(tmp_path / "batches.jsonl")
    assert [row["batch_id"] for row in batches] == [0, 0]
    assert batches[-1]["duplicate"] == 3
    assert batches[-1]["conflict"] == 1
    assert batches[-1]["late"] == 3

    # 같은 checkpoint에 새 file만 추가: batch0 재처리 없이 batch1만 처리한다.
    write_input(tmp_path, "batch1.json", [(5, event)])
    run_worker(tmp_path, "success", 0)
    assert len(read_lines(tmp_path / "acks.jsonl")) == 11
    assert read_lines(tmp_path / "batches.jsonl")[-1]["batch_id"] == 1
    assert json.loads((tmp_path / "canonical.json").read_text()) == canonical

    # 기존 checkpoint를 삭제하지 않고 독립 새 checkpoint로 전체 replay 모의.
    run_worker(tmp_path, "success", 0, "fresh_checkpoint")
    assert len(read_lines(tmp_path / "acks.jsonl")) == 17
    assert json.loads((tmp_path / "canonical.json").read_text()) == canonical
    queries = read_lines(tmp_path / "queries.jsonl")
    assert len(queries) == 5
    assert len({query["id"] for query in queries[:4]}) == 1
    assert queries[-1]["id"] != queries[0]["id"]
    assert len({query["run_id"] for query in queries}) == 5
    assert len({query["name"] for query in queries}) == 1
    for entry in read_lines(tmp_path / "acks.jsonl"):
        value = bytes.fromhex(entry["value_hex"])
        if entry["topic"] == "kitchen.sensor.validated":
            decoded = schemaless_reader(BytesIO(value[5:]), SCHEMA)
            assert len(decoded) == 10
            assert decoded["metric_value"] == 4.2
            assert entry["key"] == "fridge-001"
        else:
            evidence = json.loads(value)
            assert evidence["source_offset"] in (2, 3)
            assert evidence["errors"]
