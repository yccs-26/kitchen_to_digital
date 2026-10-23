"""새 probe의 실제 로컬 Delta commit/동일 checkpoint 재시도 경계."""

from datetime import datetime, timedelta, timezone
import json

import pytest

from phase3_local_runtime import delta_spark
from test_phase3_gold_uc_delta import named_sink, named_rows
from test_phase3_metric_aggregation import FIXTURE, add_events, local_plan
from test_phase3_streaming import run_available
from streaming.config.phase3_authorization import (
    ExecutionApproval, settings_digest,
)
from streaming.config.phase3_runtime import GoldTarget, Phase3Runtime
from streaming.jobs.metric_aggregation import gold_batch_callback
from streaming.jobs.runtime_config import SecretReference
from streaming.tools.phase3_failure_probe import FailureProbe


@pytest.mark.parametrize("stage,rows_before", [
    ("before_merge", 0), ("after_merge", 24),
])
def test_probe_failure_recovers_same_checkpoint(
    named_sink, tmp_path, stage, rows_before,
):
    checkpoint = tmp_path / "checkpoint"
    source = tmp_path / "source"
    source.mkdir()
    events = [e for step in FIXTURE["steps"][:3] for e in step["events"]]
    events.extend(FIXTURE["steps"][5]["events"])
    add_events(source, events, 0)
    # 로컬 전용 설정은 /Volumes를 사용하지 않는다. live config parser 검증은 unit에 있다.
    settings = Phase3Runtime(
        GoldTarget("delta", *named_sink.table.split(".")),
        "https://workspace.invalid", str(checkpoint), "broker.invalid:9098",
        "test-credential", "earliest", "https://registry.invalid",
        SecretReference("test-scope", "test-key"), "test-owner", "17.3 LTS",
        "STANDARD", 5, 1, True,
    )
    approval = ExecutionApproval(
        "local-probe-only", settings_digest(settings),
        datetime.now(timezone.utc) + timedelta(minutes=5), "local-probe",
        ("failure_probe",), True, True, True, True, True,
    )
    consume = gold_batch_callback(named_sink)
    injected = FailureProbe("local-probe", str(checkpoint), stage, 1).wrap(
        consume, settings, approval,
    )
    plan = local_plan(named_sink.spark, source)
    with pytest.raises(Exception, match="PHASE3_TEST"):
        run_available(plan, checkpoint, injected)
    before = named_rows(named_sink)
    assert len(before) == rows_before
    assert (checkpoint / "offsets/1").exists()
    assert not (checkpoint / "commits/1").exists()
    run_available(plan, checkpoint, consume)
    after = named_rows(named_sink)
    assert len(after) == 24
    if stage == "after_merge":
        assert after == before
    assert (checkpoint / "commits/1").exists()
    (tmp_path / "probe-recovery.json").write_text(json.dumps({
        "stage": stage, "before": before, "after": after,
        "same_checkpoint_committed_batch": 1,
    }, indent=2))
