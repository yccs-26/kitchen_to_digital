"""Databricks 세션 선택과 repo 외부 CWD에서의 실행 계약을 검증한다."""

import json
from pathlib import Path
import shutil
import subprocess
import sys
from types import ModuleType
from unittest.mock import Mock

import pytest

from scripts import run_job2_databricks as launcher
from streaming.jobs import runtime_config as runtime
from streaming.jobs import sensor_validation_job as job


REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture
def deployed_repo(tmp_path):
    """실제 배포 파일만 복사하여 로컬 import 경로의 우연한 성공을 배제한다."""
    root = tmp_path / "workspace" / "ktd"
    for directory in ("streaming", "schemas", "config"):
        shutil.copytree(REPO_ROOT / directory, root / directory)
    (root / "scripts").mkdir()
    shutil.copy2(
        REPO_ROOT / "scripts/run_job2_databricks.py", root / "scripts",
    )
    values = json.loads(
        (root / "config/job2-runtime.example.json").read_text(),
    )
    values["checkpoint"] = "/Volumes/ktd/bronze/checkpoints/job2/unit"
    values["processing_version"] = "unit-only"
    config = root / "config/job2-runtime.json"
    config.write_text(json.dumps(values))
    return root, config


@pytest.mark.parametrize("equals_form", [False, True])
def test_script_imports_from_other_cwd_without_pythonpath(
    deployed_repo, tmp_path, equals_form,
):
    root, _ = deployed_repo
    arguments = (
        ["--runtime-config=config/job2-runtime.json"] if equals_form else
        ["--runtime-config", "config/job2-runtime.json"]
    )
    # -I는 PYTHONPATH와 현재 디렉터리의 암묵적 import를 모두 배제한다.
    result = subprocess.run(
        [sys.executable, "-I", str(root / "scripts/run_job2_databricks.py"),
         *arguments, "--check-config"],
        cwd=tmp_path, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "config OK (offline" in result.stderr


@pytest.mark.parametrize("filename_source", ["compiled", "argv"])
def test_exec_without_file_imports_from_other_cwd(
    deployed_repo, tmp_path, filename_source,
):
    root, _ = deployed_repo
    script = root / "scripts/run_job2_databricks.py"
    harness = """
from pathlib import Path
import sys

script, filename_source = sys.argv[1:]
source = Path(script).read_text()
filename = script if filename_source == 'compiled' else '<databricks-exec>'
sys.argv = [
    '/runtime/python-task-wrapper.py' if filename_source == 'compiled'
    else script,
    '--runtime-config', 'config/job2-runtime.json', '--check-config',
]
namespace = {'__name__': '__main__'}
exec(compile(source, filename, 'exec'), namespace)
assert '__file__' not in namespace
assert namespace['REPO_ROOT'] == Path(script).parent.parent
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", harness, str(script), filename_source],
        cwd=tmp_path, capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "config OK (offline" in result.stderr


def test_exec_does_not_guess_repo_from_cwd(deployed_repo, monkeypatch):
    root, _ = deployed_repo
    source = (root / "scripts/run_job2_databricks.py").read_text()
    monkeypatch.chdir(root)
    monkeypatch.setattr(sys, "argv", ["/runtime/python-task-wrapper.py"])
    with pytest.raises(RuntimeError, match="Cannot locate Job 2 repository"):
        exec(compile(source, "<databricks-exec>", "exec"),
             {"__name__": "job2_exec_test"})


def test_compiled_filename_precedes_wrapper_file(deployed_repo, monkeypatch):
    root, _ = deployed_repo
    script = root / "scripts/run_job2_databricks.py"
    monkeypatch.setattr(sys, "argv", [str(launcher.__file__)])
    namespace = {
        "__name__": "job2_exec_test", "__file__": str(launcher.__file__),
    }
    exec(compile(script.read_text(), str(script), "exec"), namespace)
    assert namespace["REPO_ROOT"] == root


def test_example_routes_distinct_credentials(deployed_repo):
    _, config = deployed_repo
    settings = runtime.load_runtime_config(config)
    source = settings.kafka.spark_options(settings.job.source)
    assert source["databricks.serviceCredential"] == "ktd-msk-consumer"
    assert "kafka.security.protocol" not in source
    utility = Mock()
    utility.secrets.get.return_value = "fake-user:fake-secret"
    clients = settings.resolve_clients(utility)
    utility.credentials.getServiceCredentialsProvider.assert_called_once_with(
        "ktd-msk-job2-producer",
    )
    assert clients.producer["ktd.msk.credential"] == "ktd-msk-job2-producer"
    assert clients.producer["ktd.msk.region"] == "ap-northeast-2"
    assert "oauth_cb" not in clients.producer
    assert "databricks.serviceCredential" not in clients.producer
    assert "kafka.security.protocol" not in clients.producer
    assert clients.producer["security.protocol"] == "SASL_SSL"
    utility.secrets.get.assert_called_once_with(
        scope="ktd-job2", key="schema-registry-basic-auth",
    )
    assert settings.registry.url == (
        "https://psrc-1oqld6g.ap-northeast-2.aws.confluent.cloud"
    )
    assert settings.job.silver_table == "ktd.silver.sensor_validated"


def test_offline_check_never_creates_session_or_clients(
    deployed_repo, monkeypatch,
):
    _, config = deployed_repo
    create = Mock(side_effect=AssertionError("Spark must not start"))
    resolve = Mock(side_effect=AssertionError("No secret/network access"))
    monkeypatch.setattr(runtime.RuntimeSettings, "resolve_clients", resolve)
    job.main(
        ["--runtime-config", str(config), "--check-config"],
        spark_factory=create,
    )
    create.assert_not_called()
    resolve.assert_not_called()


@pytest.mark.parametrize("failed", [False, True])
def test_launcher_uses_native_session_and_owns_only_query(
    deployed_repo, monkeypatch, failed,
):
    _, config = deployed_repo
    sql = ModuleType("pyspark.sql")
    sql.SparkSession = Mock()
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setattr(
        job, "_connect_session", Mock(side_effect=AssertionError("Connect")),
    )
    query = Mock(isActive=failed)
    if failed:
        query.awaitTermination.side_effect = RuntimeError("batch failed")
    start = Mock(return_value=query)
    monkeypatch.setattr(runtime, "start_configured_validation", start)
    if failed:
        with pytest.raises(RuntimeError, match="batch failed"):
            launcher.main(["--runtime-config", str(config), "--available-now"])
    else:
        launcher.main(["--runtime-config", str(config), "--available-now"])
    spark = sql.SparkSession.builder.getOrCreate.return_value
    start.assert_called_once_with(spark, str(config), available_now=True)
    query.awaitTermination.assert_called_once_with()
    assert query.stop.call_count == int(failed)
    spark.stop.assert_not_called()


def test_launcher_requires_explicit_config():
    with pytest.raises(SystemExit, match="--runtime-config is required"):
        launcher.main([])


def test_offline_check_requires_config():
    with pytest.raises(SystemExit) as error:
        job.main(["--check-config"])
    assert error.value.code == 2


def test_invalid_config_does_not_create_native_session(tmp_path):
    factory = Mock()
    with pytest.raises(runtime.RuntimeConfigError):
        job.main(
            ["--runtime-config", str(tmp_path / "missing.json")],
            spark_factory=factory,
        )
    factory.assert_not_called()
