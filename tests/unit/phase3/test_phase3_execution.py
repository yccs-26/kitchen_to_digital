"""범위 승인·실행 연결·발행/장애 경계의 오프라인 계약."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import json
from types import SimpleNamespace

import pytest

from test_phase3_preflight import candidate, write_config
from streaming.config.phase3_authorization import (
    ExecutionApproval, ExecutionNotApproved, settings_digest,
)
from streaming.config.phase3_runtime import parse_phase3_runtime
from streaming.jobs.metric_aggregation import (
    start_configured_metric_aggregation,
)
from streaming.tools.phase3_e2e_publisher import (
    document_digest, independent_expected, prepare_input, publish_batch,
)
from streaming.tools.phase3_failure_probe import (
    FailureProbe, await_bounded_query,
)


@pytest.fixture
def approved(candidate):
    candidate["gold_format_approved"] = True
    settings = parse_phase3_runtime(candidate)
    permit = ExecutionApproval(
        "synthetic-local-only", settings_digest(settings),
        datetime.now(timezone.utc) + timedelta(hours=1), "phase3-test",
        ("launch", "publish_raw", "failure_probe"),
        True, True, True, True, True,
        registry_credential_env="TEST_REGISTRY_AUTH",
    )
    return settings, permit


@pytest.mark.parametrize("changes", [
    {"settings_sha256": "wrong"}, {"isolated_target": False},
    {"single_writer_confirmed": False}, {"actions": ()},
    {"uc_acceptance_complete": False}, {"phase2_baseline_reviewed": False},
    {"cost_controls_reviewed": False},
    {"expires_at": datetime(2000, 1, 1, tzinfo=timezone.utc)},
])
def test_scope_or_expiry_blocks(approved, changes):
    settings, permit = approved
    with pytest.raises(ExecutionNotApproved):
        settings.require_deployment_ready(replace(permit, **changes))


def test_launcher_reaches_same_query_checkpoint(
    approved, candidate, tmp_path, monkeypatch,
):
    import streaming.jobs.metric_aggregation as job
    import streaming.sinks.gold_uc_delta as sinks

    settings, permit = approved
    path = write_config(tmp_path, candidate)
    calls = {}
    monkeypatch.setattr(sinks, "GoldUCDeltaSink",
                        lambda *a, **kw: SimpleNamespace())
    monkeypatch.setattr(job, "read_validated_kafka",
                        lambda spark, options, factory: calls.setdefault(
                            "source", options))
    monkeypatch.setattr(job, "build_metric_aggregation", lambda frame: "plan")
    monkeypatch.setattr(job, "gold_batch_callback", lambda sink: "callback")

    def start(plan, sink, checkpoint, **kwargs):
        calls.update(plan=plan, checkpoint=checkpoint, options=kwargs)
        return "test-query"

    monkeypatch.setattr(job, "start_metric_query", start)
    assert start_configured_metric_aggregation(
        None, path, adapter="uc_managed_delta", approval=permit,
        registry_credential_env="TEST_REGISTRY_AUTH",
    ) == "test-query"
    assert calls["plan"] == "plan"
    assert calls["checkpoint"] == settings.checkpoint
    assert calls["source"]["subscribe"] == "kitchen.sensor.validated"
    assert calls["options"]["callback"] == "callback"


def test_fixture_expected_independent_and_dry_run(tmp_path):
    from streaming.tools.phase3_e2e_publisher import main

    doc = prepare_input(
        base_time=datetime(2026, 10, 20, 12, tzinfo=timezone.utc),
        namespace="run",
    )
    events = [e for b in doc["batches"][:3] for e in b["events"]]
    assert len(events) == 8 and len({e["event_id"] for e in events}) == 7
    expected = independent_expected(events)
    assert len(expected) == 24
    assert sorted(doc["expected_rows"], key=str) == expected
    output = tmp_path / "dry.json"
    assert main(["--base-time", "2026-10-20T12:00:00Z", "--namespace", "run",
                 "--output", str(output)]) == 0
    assert json.loads(output.read_text())["mode"] == "DRY_RUN_NOT_APPROVED"


def test_publisher_route_hash_and_ack(approved, monkeypatch):
    import streaming.sinks.validated_publisher as module

    settings, permit = approved
    doc = prepare_input(
        base_time=datetime(2026, 10, 20, 12, tzinfo=timezone.utc),
        namespace="run",
    )
    permit = replace(permit, fixture_sha256=document_digest(doc))
    created = []
    offsets = [0, 1, 2]

    def delivered(row, producer, serializer, **kwargs):
        assert kwargs["topic"] == "kitchen.sensor.raw"
        offset = offsets.pop(0)
        return SimpleNamespace(offset=lambda: offset,
                               topic=lambda: "kitchen.sensor.raw",
                               partition=lambda: 0)

    monkeypatch.setattr(module, "publish_validated_event", delivered)
    result = publish_batch(
        doc, 0, settings=settings, approval=permit,
        producer_factory=lambda: created.append(1), serializer=None,
        batch_ready=True,
    )
    assert len(result) == 3 and created == [1]
    altered = {**doc, "topic": "kitchen.sensor.validated"}
    with pytest.raises(ValueError):
        publish_batch(altered, 0, settings=settings, approval=permit,
                      producer_factory=lambda: pytest.fail("Must not connect"),
                      serializer=None, batch_ready=True)
    with pytest.raises(ValueError):
        publish_batch(doc, 0, settings=settings, approval=permit,
                      producer_factory=lambda: pytest.fail("Must not connect"),
                      serializer=None, batch_ready=False)
    monkeypatch.setattr(module, "publish_validated_event", lambda *a, **kw:
                        SimpleNamespace(offset=lambda: True))
    with pytest.raises(RuntimeError, match="acknowledgement"):
        publish_batch(doc, 0, settings=settings, approval=permit,
                      producer_factory=lambda: None, serializer=None,
                      batch_ready=True)


@pytest.mark.parametrize("stage,expected_calls", [
    ("before_merge", []), ("after_merge", [7]),
])
def test_failure_probe_only_approved_batch(approved, stage, expected_calls):
    settings, permit = approved
    calls = []
    probe = FailureProbe(permit.job_tag, settings.checkpoint, stage, 7)
    callback = probe.wrap(lambda batch, n: calls.append(n), settings, permit)
    with pytest.raises(RuntimeError, match="PHASE3_TEST"):
        callback(None, 7)
    assert calls == expected_calls
    callback(None, 8)
    assert calls[-1] == 8
    with pytest.raises(ValueError):
        replace(probe, checkpoint="wrong").wrap(None, settings, permit)


def test_query_timeout_stops_and_runtime_error_propagates():
    stopped = []
    query = SimpleNamespace(awaitTermination=lambda n: False,
                            stop=lambda: stopped.append(True),
                            exception=lambda: None)
    assert await_bounded_query(query, 1) is False
    assert stopped == [True]
    query.exception = lambda: RuntimeError("QUERY_FAILURE")
    with pytest.raises(RuntimeError, match="QUERY_FAILURE"):
        await_bounded_query(query, 1)


def test_approval_loader_and_local_cli_cannot_start_spark(
    approved, candidate, tmp_path, monkeypatch,
):
    from dataclasses import asdict
    from streaming.config.phase3_authorization import load_execution_approval
    from streaming.jobs.metric_aggregation import main

    settings, permit = approved
    record = asdict(permit)
    record["expires_at"] = permit.expires_at.isoformat()
    approval_file = tmp_path / "synthetic-approval.json"
    approval_file.write_text(json.dumps(record))
    assert load_execution_approval(approval_file) == permit
    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    with pytest.raises(RuntimeError, match="existing DBR"):
        main(["--config", str(write_config(tmp_path, candidate)),
              "--approval", str(approval_file),
              "--adapter", "uc_managed_delta",
              "--registry-credential-env", "TEST_REGISTRY_AUTH"])


def test_phase2_wrapper_rejects_fixture_before_client(approved, monkeypatch):
    from streaming.tools.phase3_e2e_publisher import publish_from_phase2_config
    import streaming.jobs.runtime_config as runtime

    settings, permit = approved
    phase2 = SimpleNamespace(
        job=SimpleNamespace(source=SimpleNamespace(
            bootstrap_servers=settings.bootstrap_servers,
            topic="kitchen.sensor.raw",
        ), validated_topic="kitchen.sensor.validated"),
        registry=SimpleNamespace(url=settings.registry_url),
        resolve_clients=lambda db: pytest.fail("No secret/client access"),
    )
    monkeypatch.setattr(runtime, "load_runtime_config", lambda path: phase2)
    with pytest.raises(ValueError, match="Unapproved"):
        publish_from_phase2_config({}, 0, settings=settings, approval=permit,
                                   phase2_config="offline", dbutils=None,
                                   batch_ready=True)


def test_explicit_equipment_mapping_preserves_numeric_expected():
    base = datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
    original = prepare_input(base_time=base, namespace="run")
    ids = sorted({e["equipment_id"] for b in original["batches"]
                  for e in b["events"]})
    mapping = {old: {"equipment_id": "isolated-" + str(index),
                     "store_id": "isolated-store",
                     "equipment_type": "refrigerator"}
               for index, old in enumerate(ids)}
    shifted = prepare_input(base_time=base, namespace="run",
                            equipment_mapping=mapping)
    assert len(shifted["expected_rows"]) == 24
    for old, new in zip(original["expected_rows"], shifted["expected_rows"]):
        assert new["sample_count"] == old["sample_count"]
        assert new["sum_value"] == old["sum_value"]
        expected_id = mapping[old["equipment_id"]]["equipment_id"]
        assert new["equipment_id"] == expected_id
    with pytest.raises(ValueError, match="Complete distinct"):
        prepare_input(base_time=base, namespace="run", equipment_mapping={})
