"""명시적 UC 후보 선택과 live 실행 게이트를 오프라인 검증한다."""

from copy import deepcopy

import pytest

from test_phase3_preflight import candidate, write_config
from streaming.config.phase3_runtime import Phase3DeploymentBlocked
from streaming.jobs.metric_aggregation import (
    prepare_metric_deployment, start_configured_metric_aggregation,
)


def test_candidate_selection_preserves_preflight_and_live_block(
    candidate, tmp_path,
):
    path = write_config(tmp_path, candidate)
    settings, report = prepare_metric_deployment(
        path, adapter="uc_managed_delta",
    )
    assert settings.checkpoint == candidate["checkpoint"]
    assert report["static_validation"] == "PASS"
    assert report["overall"] == "BLOCKED"
    assert report["writer_candidate"]["live_acceptance"] == (
        "REQUIRES_LIVE_CHECK"
    )
    assert settings.candidate_writer_target(adapter="uc_managed_delta") == (
        "`candidate_catalog`.`candidate_schema`.`candidate_gold`"
    )
    for flag in (False, True):
        changed = deepcopy(candidate)
        changed["gold_format_approved"] = flag
        path = write_config(tmp_path, changed)
        with pytest.raises(Phase3DeploymentBlocked, match="NOT_AUTHORIZED"):
            start_configured_metric_aggregation(
                None, path, adapter="uc_managed_delta",
                registry_factory=lambda: pytest.fail("No client creation"),
            )


@pytest.mark.parametrize("adapter,kind", [
    ("local_delta", "delta"), ("uc_managed_delta", "iceberg"),
    ("guess", "delta"),
])
def test_no_implicit_fallback(candidate, tmp_path, adapter, kind):
    candidate["gold"]["format"] = kind
    with pytest.raises(Phase3DeploymentBlocked):
        prepare_metric_deployment(write_config(tmp_path, candidate),
                                  adapter=adapter)


def test_static_failure_blocks_before_selection(candidate, tmp_path):
    candidate["writer"]["exclusive_target"] = False
    with pytest.raises(Phase3DeploymentBlocked, match="Static preflight"):
        prepare_metric_deployment(write_config(tmp_path, candidate),
                                  adapter="uc_managed_delta")
