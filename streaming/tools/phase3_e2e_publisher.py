"""결정적 dry-run과 승인된 격리 raw→Phase2 발행. validated 직접 쓰기는 없다."""

import argparse
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path


FIXTURE = Path(__file__).resolve().parents[2] / (
    "tests/fixtures/phase3/e2e_validated_events.json"
)


def document_digest(document) -> str:
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def prepare_input(
    *, base_time: datetime, namespace: str, equipment_mapping=None,
) -> dict:
    """시각·ID만 이동한다. 장비 등록/현재 시각/네트워크를 추측하지 않는다."""
    if (base_time.utcoffset() != timedelta(0) or base_time.second
            or base_time.microsecond or not namespace.strip()
            or "<" in namespace):
        raise ValueError("Explicit UTC minute and unique namespace required")
    fixture = json.loads(FIXTURE.read_text())
    shift = base_time - datetime.fromisoformat(fixture["base_time"])
    batches = deepcopy(fixture["batches"])
    for batch in batches:
        for event in batch["events"]:
            event["event_id"] = namespace + ":" + event["event_id"]
            event["event_time"] = (
                datetime.fromisoformat(event["event_time"]) + shift
            ).isoformat()
    expected = deepcopy(fixture["expected_rows"])
    for row in expected:
        for name in ("window_start", "window_end"):
            row[name] = (datetime.fromisoformat(row[name]) + shift).isoformat()
    if equipment_mapping is not None:
        source_ids = {
            e["equipment_id"] for b in batches for e in b["events"]
        }
        if (set(equipment_mapping) != source_ids
                or any(set(v) != {"equipment_id", "store_id", "equipment_type"}
                       or any(not isinstance(x, str) or not x.strip()
                              for x in v.values())
                       for v in equipment_mapping.values())
                or len({v["equipment_id"]
                        for v in equipment_mapping.values()})
                != len(source_ids)):
            raise ValueError(
                "Complete distinct approved equipment map required",
            )
        for batch in batches:
            for event in batch["events"]:
                event.update(equipment_mapping[event["equipment_id"]])
        for row in expected:
            replacement = equipment_mapping[row["equipment_id"]]
            row.update({
                k: replacement[k] for k in ("equipment_id", "store_id")
            })
    return {"mode": "DRY_RUN_NOT_APPROVED", "route": "phase2_raw",
            "topic": "kitchen.sensor.raw", "namespace": namespace,
            "batches": batches, "expected_rows": expected,
            "equipment_mapping": equipment_mapping,
            "notes": fixture["notes"]}


def independent_expected(events) -> list[dict]:
    """테스트 입력용 유한 oracle. 생산 window/mapper 코드를 호출하지 않는다."""
    unique = {}
    for event in events:
        old = unique.setdefault(event["event_id"], event)
        if old != event:
            raise ValueError("Conflicting fixture event_id")
    windows = {}
    for event in unique.values():
        time = datetime.fromisoformat(event["event_time"])
        if time.utcoffset() != timedelta(0):
            raise ValueError("Oracle requires UTC fixture")
        minute = time.replace(second=0, microsecond=0)
        for kind, length in (("1m_tumbling", 1), ("5m_sliding", 5)):
            for offset in range(length):
                start = minute - timedelta(minutes=offset)
                key = (start, kind, event["store_id"], event["equipment_id"],
                       event["metric_name"], event["unit"], length)
                windows.setdefault(key, []).append(event["metric_value"])
    result = []
    for (start, kind, store, equipment, metric, unit, length), values in (
        windows.items()
    ):
        total = math.fsum(values)
        result.append({
            "window_start": start.isoformat(),
            "window_end": (start + timedelta(minutes=length)).isoformat(),
            "window_type": kind, "store_id": store,
            "equipment_id": equipment, "metric_name": metric, "unit": unit,
            "sample_count": len(values), "sum_value": total,
            "min_value": min(values), "max_value": max(values),
            "avg_value": total / len(values),
        })
    return sorted(result, key=str)


def publish_batch(document, batch_index, *, settings, approval,
                  producer_factory, serializer, batch_ready):
    """한 batch만 발행한다. 호출자는 query/ACK/W를 보고 다음 batch를 승인한다."""
    from streaming.sinks.validated_publisher import publish_validated_event

    approval.require(settings, "publish_raw")
    if (document.get("route") != "phase2_raw"
            or document.get("topic") != "kitchen.sensor.raw"
            or document_digest(document) != approval.fixture_sha256
            or type(batch_index) is not int
            or not 0 <= batch_index < len(document["batches"])
            or batch_ready is not True):
        raise ValueError("Isolated Phase2 raw batch approval required")
    # 입력 형식을 먼저 검사한다. 실제 발행은 여러 ACK 사이에 부분 완료될 수 있다.
    prepared = []
    for event in document["batches"][batch_index]["events"]:
        row = dict(event)
        row["event_time"] = datetime.fromisoformat(row["event_time"])
        if not row["event_id"].startswith(document["namespace"] + ":"):
            raise ValueError("Unscoped event_id")
        prepared.append(row)
    producer = producer_factory()
    acknowledgements = []
    for row in prepared:
        message = publish_validated_event(
            row, producer, serializer, topic="kitchen.sensor.raw",
        )
        offset = message.offset()
        if (type(offset) is not int or offset < 0
                or message.topic() != "kitchen.sensor.raw"
                or type(message.partition()) is not int
                or message.partition() < 0):
            raise RuntimeError("Missing scoped Kafka acknowledgement")
        acknowledgements.append({"event_id": row["event_id"],
                                 "partition": message.partition(),
                                 "offset": offset})
    return acknowledgements


def publish_from_phase2_config(
    document, batch_index, *, settings, approval, phase2_config,
    dbutils, batch_ready, reference_time=None,
):
    """승인 후에만 기존 P2 인증/serializer를 연결한다. 실제 실행은 이번에 금지다."""
    approval.require(settings, "publish_raw")
    from streaming.jobs.runtime_config import load_runtime_config

    phase2 = load_runtime_config(phase2_config)
    if (phase2.job.source.bootstrap_servers != settings.bootstrap_servers
            or phase2.job.source.topic != "kitchen.sensor.raw"
            or phase2.job.validated_topic != "kitchen.sensor.validated"
            or phase2.registry.url != settings.registry_url):
        raise ValueError("Phase2 route differs from approved Phase3 scope")
    # 잘못된 document는 Secret 해석/native client 생성 전에도 거부한다.
    if (document_digest(document) != approval.fixture_sha256
            or document.get("topic") != "kitchen.sensor.raw"
            or document.get("route") != "phase2_raw"
            or not document.get("equipment_mapping")
            or batch_ready is not True):
        raise ValueError("Unapproved isolated publisher input")
    if (type(batch_index) is not int
            or not 0 <= batch_index < len(document["batches"])):
        raise ValueError("Invalid batch index")
    from streaming.validation.domain_validator import validate_domain_event

    instant = reference_time or datetime.now(timezone.utc)
    for event in document["batches"][batch_index]["events"]:
        logical = dict(event)
        logical["event_time"] = datetime.fromisoformat(logical["event_time"])
        result = validate_domain_event(
            logical, logical["equipment_id"].encode(),
            supported_schema_versions=phase2.rules.supported_schema_versions,
            equipment_registry=phase2.rules.equipment_registry,
            metric_units=phase2.rules.metric_units, reference_time=instant,
            max_future_skew=phase2.job.max_future_skew,
        )
        if not result.is_valid:
            raise ValueError("Publisher input violates approved Phase2 rules")
    from confluent_kafka import Producer
    from confluent_kafka.schema_registry import (
        SchemaRegistryClient, topic_subject_name_strategy,
    )
    from confluent_kafka.schema_registry.avro import AvroSerializer

    clients = phase2.resolve_clients(dbutils)
    registry = SchemaRegistryClient(clients.registry)
    schema = (FIXTURE.parents[3] / "schemas/avro/sensor_metric_event.avsc")
    serializer = AvroSerializer(registry, schema.read_text(), conf={
        "auto.register.schemas": False, "use.latest.version": False,
        "subject.name.strategy": topic_subject_name_strategy,
    })
    return publish_batch(
        document, batch_index, settings=settings, approval=approval,
        producer_factory=lambda: Producer(clients.producer),
        serializer=serializer, batch_ready=batch_ready,
    )


def main(argv=None):
    """CLI는 항상 dry-run이다. 발행은 위의 승인된 주입 API에서만 가능하다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-time", required=True)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--equipment-map")
    args = parser.parse_args(argv)
    mapping = (json.loads(Path(args.equipment_map).read_text())
               if args.equipment_map else None)
    document = prepare_input(base_time=datetime.fromisoformat(args.base_time),
                             namespace=args.namespace,
                             equipment_mapping=mapping)
    Path(args.output).write_text(json.dumps(document, indent=2) + "\n")
    print(json.dumps({"mode": document["mode"],
                      "batches": len(document["batches"]),
                      "expected_gold_rows": len(document["expected_rows"]),
                      "sha256": document_digest(document)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
