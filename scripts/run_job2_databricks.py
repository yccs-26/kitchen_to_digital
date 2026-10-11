"""Databricks Python script task에서 repo의 Job 2를 실행한다."""

from pathlib import Path
import sys


def resolve_repo_root() -> Path:
    """실행 코드의 filename을 우선 사용하고 실제 repo 파일 구조를 확인한다."""
    # exec(compile(source, filename, "exec"))도 코드 객체에 filename을 남긴다.
    # 공유 globals의 __file__이나 argv[0]은 실행 wrapper를 가리킬 수 있다.
    candidates = (
        resolve_repo_root.__code__.co_filename,
        globals().get("__file__"),
        sys.argv[0] if sys.argv else None,
    )
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate:
            continue
        path = Path(candidate).resolve()
        if (path.name != "run_job2_databricks.py"
                or path.parent.name != "scripts" or not path.is_file()):
            continue
        root = path.parent.parent
        if (root / "streaming/jobs/sensor_validation_job.py").is_file():
            return root
    raise RuntimeError(
        "Cannot locate Job 2 repository: compile filename or sys.argv[0] "
        "must identify scripts/run_job2_databricks.py in the deployed repo"
    )


REPO_ROOT = resolve_repo_root()


def runtime_session():
    """Databricks Runtime이 제공하는 SparkSession을 사용한다."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def main(argv=None) -> None:
    """CWD와 무관하게 repo import와 상대 runtime config 경로를 해석한다."""
    sys.path.insert(0, str(REPO_ROOT))
    from streaming.jobs.sensor_validation_job import main as run_job

    args = list(sys.argv[1:] if argv is None else argv)
    # 실행용 설정은 명시하게 하여 환경 변수 기반 인증 경로 진입을 막는다.
    for index, argument in enumerate(args):
        if argument == "--runtime-config" and index + 1 < len(args):
            path = Path(args[index + 1])
            if not path.is_absolute():
                args[index + 1] = str(REPO_ROOT / path)
            break
        if argument.startswith("--runtime-config="):
            path = Path(argument.split("=", 1)[1])
            if not path.is_absolute():
                args[index] = f"--runtime-config={REPO_ROOT / path}"
            break
    else:
        if "--help" not in args and "-h" not in args:
            raise SystemExit("--runtime-config is required")
    run_job(args, spark_factory=runtime_session)


if __name__ == "__main__":
    main()
