"""배포 준비의 정적 계약만 검증하며 실제 Kafka/UC 접근은 금지한다."""

from copy import deepcopy
import json
from pathlib import Path

import pytest

from streaming.config.phase3_runtime import (
    Phase3ConfigError, Phase3DeploymentBlocked, load_phase3_runtime,
    parse_phase3_runtime,
)
from streaming.jobs.runtime_config import SecretReference
from streaming.tools.phase3_preflight import main, run_preflight


ROOT = Path(__file__).resolve().parents[3]


@pytest.fixture
def candidate():
    """실존 리소스를 뜻하지 않는 .invalid 도메인과 synthetic 식별자다."""
    values = json.loads((ROOT / "config/phase3_gold.example.json").read_text())
    values.update({
        "workspace_host": "https://workspace.invalid",
        "gold": {"format": "delta", "catalog": "candidate_catalog",
                 "schema": "candidate_schema", "table": "candidate_gold"},
        "checkpoint": (
            "/Volumes/candidate_catalog/candidate_schema/cp/job3/e2e"
        ),
    })
    values["kafka"].update({
        "bootstrap_servers": "broker.invalid:9098",
        "service_credential": "candidate-read-credential",
        "starting_offsets": "earliest",
    })
    values["registry"].update({
        "url": "https://registry.invalid",
        "credentials": {"scope": "candidate-scope", "key": "candidate-key"},
    })
    values["writer"].update({"exclusive_target": True, "owner": "test-owner"})
    values["runtime"].update({"dbr": "17.3 LTS", "access_mode": "STANDARD"})
    values["limits"].update({"max_runtime_minutes": 30, "max_cost_usd": 1})
    return values


def write_config(tmp_path, values):
    path = tmp_path / "candidate.json"
    path.write_text(json.dumps(values))
    return path


def test_complete_static_config_still_blocks_deployment(candidate, tmp_path):
    settings = parse_phase3_runtime(candidate)
    assert settings.target.identifier == (
        "`candidate_catalog`.`candidate_schema`.`candidate_gold`"
    )
    assert settings.kafka_options()["subscribe"] == "kitchen.sensor.validated"
    assert settings.kafka_options()["failOnDataLoss"] == "true"
    with pytest.raises(Phase3DeploymentBlocked, match="NOT_IMPLEMENTED"):
        settings.require_deployment_ready()
    result = run_preflight(write_config(tmp_path, candidate))
    assert result["static_validation"] == "PASS"
    assert result["overall"] == "BLOCKED"
    assert all(c["status"] == "REQUIRES_LIVE_CHECK"
               for c in result["live_checks"])


@pytest.mark.parametrize("path,value", [
    (("kafka", "topic"), "kitchen.sensor.raw"),
    (("kafka", "bootstrap_servers"), "password@broker.invalid:9098"),
    (("kafka", "bootstrap_servers"), "broker.invalid:99999"),
    (("kafka", "auth_mode"), "plaintext_password"),
    (("kafka", "service_credential"), None),
    (("kafka", "fail_on_data_loss"), False),
    (("kafka", "starting_offsets"), None),
    (("gold", "catalog"), "catalog;DROP"),
    (("gold", "format"), "parquet"),
    (("checkpoint",), "/Volumes/a/b/c/job2/shared"),
    (("checkpoint",), "/Volumes/a/b/c/job3/../shared"),
    (("checkpoint",), "s3://unapproved/checkpoint"),
    (("registry", "url"), "https://user:password@registry.invalid"),
    (("registry", "credentials"), {"password": "never-output-this"}),
    (("streaming", "watermark"), "5 minutes"),
    (("streaming", "output_mode"), "update"),
    (("streaming", "timezone"), "Asia/Seoul"),
    (("streaming", "windows"), []),
    (("writer", "max_concurrent_runs"), True),
    (("writer", "max_concurrent_runs"), 2),
    (("writer", "exclusive_target"), False),
    (("runtime", "dbr"), "unknown"),
    (("runtime", "access_mode"), None),
    (("limits", "max_cost_usd"), float("nan")),
    (("limits", "max_runtime_minutes"), 0),
])
def test_static_contract_rejects_unsafe_or_changed_setting(
    candidate, path, value,
):
    modified = deepcopy(candidate)
    target = modified
    for name in path[:-1]:
        target = target[name]
    target[path[-1]] = value
    with pytest.raises(Phase3ConfigError):
        parse_phase3_runtime(modified)


def test_secret_is_never_resolved_and_checkpoint_not_probed(
    candidate, tmp_path, monkeypatch,
):
    path = write_config(tmp_path, candidate)

    def forbidden(*args, **kwargs):
        raise AssertionError("Static preflight must not access live resources")

    monkeypatch.setattr(SecretReference, "read", forbidden)
    monkeypatch.setattr(Path, "exists", forbidden)
    result = run_preflight(path)
    assert result["static_validation"] == "PASS"
    assert isinstance(load_phase3_runtime(path).registry_credentials,
                      SecretReference)


def test_unknown_secret_field_is_redacted(candidate, tmp_path, capsys):
    candidate["kafka"]["password"] = "never-output-this"
    path = write_config(tmp_path, candidate)
    assert main(["--config", str(path)]) == 2
    output = capsys.readouterr().out
    assert "never-output-this" not in output
    assert "kafka.unknown_field" in output


def test_template_and_missing_file_are_blocked(tmp_path):
    report = run_preflight(ROOT / "config/phase3_gold.example.json")
    assert report["static_validation"] == "BLOCKED"
    assert report["overall"] == "BLOCKED"
    assert run_preflight(tmp_path / "absent.json")["configuration_issues"] == [
        "config.file",
    ]


def test_approval_flag_cannot_enable_missing_uc_writer(candidate, tmp_path):
    candidate["gold_format_approved"] = True
    result = run_preflight(write_config(tmp_path, candidate))
    assert result["static_validation"] == "PASS"
    assert result["overall"] == "BLOCKED"
    assert "UC_GOLD_WRITER_NOT_IMPLEMENTED" in result["blockers"]
    with pytest.raises(Phase3DeploymentBlocked):
        parse_phase3_runtime(candidate).require_deployment_ready()
