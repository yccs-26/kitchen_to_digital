"""사용자의 별도 승인 기록을 실행 설정에 바인딩한다. 인증 서버/분산 lock은 아니다."""

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import re


class ExecutionNotApproved(RuntimeError):
    """비밀값이나 승인 기록 원문을 노출하지 않는 실행 차단."""


def settings_digest(settings) -> str:
    """참조명만 포함한 검토 설정 fingerprint. Secret 값은 해석하지 않는다."""
    value = json.dumps(asdict(settings), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(value.encode()).hexdigest()


@dataclass(frozen=True)
class ExecutionApproval:
    """별도 검토된 기록. 생성 자체는 실제 리소스 수용이나 사람 승인 증거가 아니다."""

    approval_id: str
    settings_sha256: str
    expires_at: datetime
    job_tag: str
    actions: tuple[str, ...]
    isolated_target: bool
    uc_acceptance_complete: bool
    single_writer_confirmed: bool
    phase2_baseline_reviewed: bool
    cost_controls_reviewed: bool
    fixture_sha256: str = ""
    registry_credential_env: str = ""

    def __post_init__(self):
        if (not all(isinstance(v, str) and v.strip() for v in (
                self.approval_id, self.job_tag,
            ))
                or not isinstance(self.settings_sha256, str)
                or not re.fullmatch(r"[0-9a-f]{64}", self.settings_sha256)
                or not isinstance(self.expires_at, datetime)
                or self.expires_at.utcoffset() is None
                or not isinstance(self.actions, tuple)
                or any(a not in ("launch", "publish_raw", "failure_probe")
                       for a in self.actions)
                or any(type(v) is not bool for v in (
                    self.isolated_target, self.uc_acceptance_complete,
                    self.single_writer_confirmed,
                    self.phase2_baseline_reviewed, self.cost_controls_reviewed,
                ))):
            raise ExecutionNotApproved("Invalid execution approval structure")

    def require(self, settings, action: str, *, now=None) -> None:
        instant = now or datetime.now(timezone.utc)
        if (not self.approval_id.strip() or not self.job_tag.strip()
                or self.expires_at.utcoffset() is None
                or instant >= self.expires_at
                or self.settings_sha256
                != settings_digest(settings)
                or action not in self.actions
                or not all(value is True for value in (
                    self.isolated_target, self.uc_acceptance_complete,
                    self.single_writer_confirmed,
                    self.phase2_baseline_reviewed,
                    self.cost_controls_reviewed,
                ))
                or not settings.gold_format_approved
                or settings.target.format != "delta"):
            raise ExecutionNotApproved("LIVE_EXECUTION_NOT_AUTHORIZED")


def load_execution_approval(path):
    """별도 JSON 기록을 읽는다. 파일/boolean은 사람 승인 증명이 아니다."""
    from pathlib import Path

    try:
        values = json.loads(Path(path).read_text())
        values["expires_at"] = datetime.fromisoformat(values["expires_at"])
        if not isinstance(values["actions"], list):
            raise ValueError("Actions must be an explicit list")
        values["actions"] = tuple(values["actions"])
        return ExecutionApproval(**values)
    except (OSError, ValueError, TypeError, KeyError):
        raise ExecutionNotApproved(
            "Invalid separate execution approval",
        ) from None
