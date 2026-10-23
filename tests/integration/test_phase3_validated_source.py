"""실제 로컬 Spark Python worker의 Avro binary 변환. Kafka 연결은 하지 않는다."""

from datetime import datetime
from io import BytesIO
import json
from pathlib import Path
from types import SimpleNamespace

from fastavro import schemaless_writer
import pytest

from test_phase3_streaming import local_spark
from streaming.sources.validated_kafka import transform_kafka_records


SCHEMA = (Path(__file__).resolve().parents[2] /
          "schemas/avro/sensor_metric_event.avsc").read_text()


def wire(event):
    stream = BytesIO()
    schemaless_writer(stream, json.loads(SCHEMA), event)
    return b"\0\0\0\0\7" + stream.getvalue()


def test_actual_worker_binary_frame_and_utc(local_spark, tmp_path):
    path = (Path(__file__).resolve().parents[1] /
            "fixtures/phase3/e2e_validated_events.json")
    fixture = json.loads(path.read_text())
    events = [e for b in fixture["batches"][:3] for e in b["events"]]
    events[0] = {**events[0], "event_time": "2026-10-20T21:00:00+09:00"}
    source = local_spark.createDataFrame([
        (e["equipment_id"].encode(), wire(e)) for e in events
    ], "key binary, value binary")
    # test 모듈 자체를 worker에서 import하지 않도록 closure를 사용한다.
    schema = SCHEMA

    def factory():
        return SimpleNamespace(get_schema=lambda n: SimpleNamespace(
            schema_str=schema,
        ))
    actual = [r.asDict() for r in transform_kafka_records(
        source, factory,
    ).collect()]
    assert len(actual) == 8
    assert len({r["event_id"] for r in actual}) == 7
    assert all(r["event_time"].endswith("+00:00") for r in actual)
    assert actual[0]["event_time"] == "2026-10-20T12:00:00+00:00"
    assert all(len(r) == 10 for r in actual)
    (tmp_path / "decoded-worker-rows.json").write_text(
        json.dumps(actual, indent=2),
    )


@pytest.mark.parametrize("value", [None, b"bad", b"\0\0\0\0\7"])
def test_worker_decode_error_fails_action(local_spark, value):
    schema = SCHEMA

    def factory():
        return SimpleNamespace(get_schema=lambda n: SimpleNamespace(
            schema_str=schema,
        ))
    source = local_spark.createDataFrame(
        [(b"fridge-001", value)], "key binary, value binary",
    )
    with pytest.raises(
        Exception, match="(SensorDecodeError|AVRO|Kafka value)",
    ):
        transform_kafka_records(source, factory).collect()


def test_streaming_wire_batches_late_and_24_final(local_spark, tmp_path):
    import base64
    from streaming.jobs.metric_aggregation import (
        build_metric_aggregation, gold_batch_callback,
    )
    from test_phase3_metric_aggregation import CaptureSink
    from test_phase3_streaming import run_available

    fixture_path = (Path(__file__).resolve().parents[1] /
                    "fixtures/phase3/e2e_validated_events.json")
    fixture = json.loads(fixture_path.read_text())
    source = tmp_path / "binary-source"
    source.mkdir()
    frame = local_spark.readStream.schema("key binary, value binary").json(
        str(source),
    )
    schema = SCHEMA

    def factory():
        return SimpleNamespace(get_schema=lambda n: SimpleNamespace(
            schema_str=schema,
        ))

    plan = build_metric_aggregation(transform_kafka_records(frame, factory))
    capture = CaptureSink()
    progress = []
    for index, batch in enumerate(fixture["batches"]):
        rows = [{"key": base64.b64encode(e["equipment_id"].encode()).decode(),
                 "value": base64.b64encode(wire(e)).decode()}
                for e in batch["events"]]
        staged = tmp_path / f"staged-{index}.json"
        staged.write_text("\n".join(json.dumps(r) for r in rows))
        staged.replace(source / f"batch-{index}.json")
        recent = run_available(plan, tmp_path / "checkpoint",
                               gold_batch_callback(capture))
        progress.extend(json.loads(p.json) for p in recent)
        target = [r for r in capture.rows
                  if r["equipment_id"] != "progress-fixture-unapproved"]
        (tmp_path / f"wire-stage-{index}.json").write_text(json.dumps({
            "rows": target, "progress": progress,
        }, indent=2))
    assert len(target) == 24

    def key(row):
        return tuple(str(row[k]) for k in (
            "window_start", "window_type", "store_id", "equipment_id",
            "metric_name",
        ))
    expected = [dict(row) for row in fixture["expected_rows"]]
    for row in expected:
        for name in ("window_start", "window_end"):
            row[name] = datetime.fromisoformat(row[name]).isoformat()
    assert sorted(target, key=key) == sorted(expected, key=key)
    assert any(op["numRowsDroppedByWatermark"] > 0
               for p in progress for op in p.get("stateOperators", []))
    (tmp_path / "wire-streaming-24.json").write_text(json.dumps({
        "expected": fixture["expected_rows"], "actual": target,
        "progress": progress,
    }, indent=2))
