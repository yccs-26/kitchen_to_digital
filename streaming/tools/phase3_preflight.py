"""클라우드에 접속하지 않는 Phase 3 정적 Preflight CLI."""

import argparse
import json
from pathlib import Path

from streaming.config.phase3_runtime import (
    Phase3ConfigError, load_phase3_runtime,
)


LIVE_CHECKS = (
    "workspace_runtime_build_and_access_mode",
    "kafka_topic_partitions_offsets_retention_and_read_permissions",
    "registry_schema_ids_secrets_and_executor_access",
    "uc_table_provider_schema_and_permissions",
    "checkpoint_exists_isolation_and_read_write_permissions",
    "single_writer_other_jobs_notebooks_and_backfill_exclusion",
    "dbr_stateful_plan_and_foreachbatch_serialization",
    "account_price_credits_and_cost_shutdown_controls",
)


def run_preflight(path: str | Path) -> dict[str, object]:
    """리소스 실존·권한 검사를 실행하지 않으며 설정값도 출력하지 않는다."""
    issues = []
    try:
        settings = load_phase3_runtime(path)
    except Phase3ConfigError as error:
        issues.extend(error.issues)
        settings = None
    blockers = ["UC_GOLD_WRITER_NOT_IMPLEMENTED", "LIVE_E2E_NOT_AUTHORIZED"]
    if settings is None or not settings.gold_format_approved:
        blockers.append("GOLD_FORMAT_APPROVAL_REQUIRED")
    return {
        "mode": "STATIC_ONLY",
        "overall": "BLOCKED",
        "static_validation": "BLOCKED" if issues else "PASS",
        "configuration_issues": issues,
        "blockers": blockers,
        "writer_candidate": {
            "adapter": "uc_managed_delta",
            "implementation": "LOCAL_CANDIDATE_IMPLEMENTED",
            "live_acceptance": "REQUIRES_LIVE_CHECK",
            "legacy_blocker_note": (
                "UC_GOLD_WRITER_NOT_IMPLEMENTED retained for compatibility; "
                "approved live deployment launcher is not implemented"
            ),
        },
        "live_checks": [
            {"check": name, "status": "REQUIRES_LIVE_CHECK"}
            for name in LIVE_CHECKS
        ],
        "runtime_comparison": {
            "local_verified": "PySpark 4.0.0 / Delta 4.0.0 / JDK 21",
            "target_candidate": "DBR 17.3 LTS / Spark 4.0.0 / Standard",
            "actual_target": "REQUIRES_LIVE_CHECK",
        },
        "checkpoint_note": (
            "Format checked only; no filesystem exists/stat or cloud lookup"
        ),
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output")
    args = parser.parse_args(argv)
    report = run_preflight(args.config)
    rendered = json.dumps(report, indent=2)
    if args.output:
        Path(args.output).write_text(rendered + "\n", encoding="utf-8")
    print(rendered)
    return 2 if report["overall"] == "BLOCKED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
