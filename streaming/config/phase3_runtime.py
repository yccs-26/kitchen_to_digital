"""Phase 3의 비밀값 없는 설정을 검사한다. 네트워크나 Spark를 시작하지 않는다."""

from collections.abc import Mapping
from dataclasses import dataclass, field
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

from streaming.jobs.raw_ingestion import quoted_table
from streaming.jobs.runtime_config import SecretReference


WINDOWS = (
    ("1m_tumbling", "1 minute", "1 minute"),
    ("5m_sliding", "5 minutes", "1 minute"),
)
WATERMARK = "10 minutes"


class Phase3ConfigError(ValueError):
    """입력값을 노출하지 않고 실패한 설정 경로만 전달한다."""

    def __init__(self, issues):
        self.issues = tuple(sorted(set(issues)))
        super().__init__("Invalid Phase 3 configuration: " +
                         ", ".join(self.issues))


class Phase3DeploymentBlocked(RuntimeError):
    """미승인 UC 배포를 로컬 path writer로 대체하지 않는다."""


@dataclass(frozen=True)
class GoldTarget:
    """형식 후보와 UC 식별자만 표현하며 저장 API를 제공하지 않는다."""

    format: str
    catalog: str
    schema: str
    table: str

    @property
    def identifier(self) -> str:
        return quoted_table(f"{self.catalog}.{self.schema}.{self.table}")


@dataclass(frozen=True)
class Phase3Runtime:
    """검증된 실행 준비 설정이며 승인이나 실제 리소스 존재를 의미하지 않는다."""

    target: GoldTarget
    workspace_host: str
    checkpoint: str
    bootstrap_servers: str = field(repr=False)
    source_service_credential: str
    starting_offsets: str
    registry_url: str = field(repr=False)
    registry_credentials: SecretReference
    writer_owner: str
    dbr: str
    access_mode: str
    max_runtime_minutes: int
    max_cost_usd: int | float
    gold_format_approved: bool

    def kafka_options(self) -> dict[str, str]:
        """Secret 참조명만 전달하며 조회나 연결은 하지 않는다."""
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": "kitchen.sensor.validated",
            "databricks.serviceCredential": self.source_service_credential,
            "startingOffsets": self.starting_offsets,
            "failOnDataLoss": "true",
            "kafka.allow.auto.create.topics": "false",
        }

    def require_deployment_ready(self) -> None:
        """설정 flag만으로 구현·수용 검증 게이트를 해제하지 않는다."""
        # 기존 소비자용 차단 코드는 유지한다. 후보 구현과 live 승인은 별개다.
        raise Phase3DeploymentBlocked(
            "UC_GOLD_WRITER_NOT_IMPLEMENTED: live deployment launcher remains "
            "blocked; candidate adapter requires separate UC acceptance and "
            "human authorization (LIVE_E2E_NOT_AUTHORIZED)"
        )

    def candidate_writer_target(self, *, adapter: str) -> str:
        """명시적 후보 선택만 검증한다. 연결·승인·writer lock은 제공하지 않는다."""
        if adapter != "uc_managed_delta" or self.target.format != "delta":
            raise Phase3DeploymentBlocked("Unsupported Gold writer candidate")
        # parse_phase3_runtime의 checkpoint/배타 writer 정적 검사를 재사용한다.
        return self.target.identifier


def parse_phase3_runtime(values: Mapping[str, object]) -> Phase3Runtime:
    """JSON 구조와 고정된 실행 계약만 검사한다."""
    import math

    issues = []

    def obj(value, label, names):
        if not isinstance(value, dict):
            issues.append(label)
            return {}
        if set(value) - set(names):
            issues.append(label + ".unknown_field")
        for name in set(names) - set(value):
            issues.append(label + "." + name)
        return value

    def text(value, label):
        if (not isinstance(value, str) or not value.strip()
                or "<" in value or ">" in value):
            issues.append(label)
            return ""
        return value.strip()

    root = obj(values, "runtime", (
        "environment", "workspace_host", "gold", "checkpoint", "kafka",
        "registry", "streaming", "writer", "runtime", "limits",
        "gold_format_approved",
    ))
    if root.get("environment") != "databricks":
        issues.append("environment")
    gold = obj(root.get("gold"), "gold", (
        "format", "catalog", "schema", "table",
    ))
    kind = gold.get("format")
    if kind not in ("delta", "iceberg"):
        issues.append("gold.format")
    identifiers = [text(gold.get(k), "gold." + k)
                   for k in ("catalog", "schema", "table")]
    try:
        quoted_table(".".join(identifiers))
    except ValueError:
        issues.append("gold.identifier")
    checkpoint = text(root.get("checkpoint"), "checkpoint")
    parts = checkpoint.split("/")
    if (len(parts) < 7 or parts[:2] != ["", "Volumes"]
            or "job3" not in parts[5:]
            or any(not re.fullmatch(r"[A-Za-z0-9_-]+", p)
                   for p in parts[2:])):
        issues.append("checkpoint")

    def https_url(value, label):
        value = text(value, label)
        try:
            parsed = urlsplit(value)
            port = parsed.port
            if (port is not None and not 1 <= port <= 65535):
                issues.append(label)
            if (parsed.scheme != "https" or not parsed.hostname
                    or parsed.username or parsed.password or parsed.query
                    or parsed.fragment or parsed.path not in ("", "/")):
                issues.append(label)
        except ValueError:
            issues.append(label)
        return value

    host = https_url(root.get("workspace_host"), "workspace_host")
    kafka = obj(root.get("kafka"), "kafka", (
        "bootstrap_servers", "topic", "auth_mode", "service_credential",
        "starting_offsets", "fail_on_data_loss",
    ))
    bootstrap = text(kafka.get("bootstrap_servers"), "kafka.bootstrap_servers")
    if not bootstrap or any(not re.fullmatch(
        r"[A-Za-z0-9][A-Za-z0-9.-]*:[0-9]{1,5}", server,
    ) or not 1 <= int(server.rsplit(":", 1)[-1]) <= 65535
            for server in bootstrap.split(",")):
        issues.append("kafka.bootstrap_servers")
    if kafka.get("topic") != "kitchen.sensor.validated":
        issues.append("kafka.topic")
    if kafka.get("auth_mode") != "service_credential":
        issues.append("kafka.auth_mode")
    credential = text(kafka.get("service_credential"),
                      "kafka.service_credential")
    if not re.fullmatch(r"[A-Za-z0-9_-]+", credential):
        issues.append("kafka.service_credential")
    offsets = kafka.get("starting_offsets")
    if offsets not in ("earliest", "latest"):
        issues.append("kafka.starting_offsets")
    if kafka.get("fail_on_data_loss") is not True:
        issues.append("kafka.fail_on_data_loss")
    registry = obj(root.get("registry"), "registry", (
        "provider", "url", "auth_mode", "credentials",
    ))
    if registry.get("provider") != "confluent":
        issues.append("registry.provider")
    if registry.get("auth_mode") != "basic_secret_reference":
        issues.append("registry.auth_mode")
    registry_url = https_url(registry.get("url"), "registry.url")
    reference = obj(registry.get("credentials"), "registry.credentials", (
        "scope", "key",
    ))
    for name in ("scope", "key"):
        part = text(reference.get(name), "registry.credentials." + name)
        if not re.fullmatch(r"[A-Za-z0-9_-]+", part):
            issues.append("registry.credentials." + name)
    streaming = obj(root.get("streaming"), "streaming", (
        "watermark", "windows", "output_mode", "timezone",
    ))
    expected = [{"window_type": k, "duration": d, "slide": s}
                for k, d, s in WINDOWS]
    if streaming.get("windows") != expected:
        issues.append("streaming.windows")
    for key, required in (("watermark", WATERMARK),
                          ("output_mode", "append"), ("timezone", "UTC")):
        if streaming.get(key) != required:
            issues.append("streaming." + key)
    writer = obj(root.get("writer"), "writer", (
        "max_concurrent_runs", "exclusive_target", "owner",
    ))
    if (type(writer.get("max_concurrent_runs")) is not int
            or writer.get("max_concurrent_runs") != 1):
        issues.append("writer.max_concurrent_runs")
    if writer.get("exclusive_target") is not True:
        issues.append("writer.exclusive_target")
    owner = text(writer.get("owner"), "writer.owner")
    runtime = obj(root.get("runtime"), "runtime", ("dbr", "access_mode"))
    if runtime.get("dbr") != "17.3 LTS":
        issues.append("runtime.dbr")
    if runtime.get("access_mode") != "STANDARD":
        issues.append("runtime.access_mode")
    limits = obj(root.get("limits"), "limits", (
        "max_runtime_minutes", "max_cost_usd",
    ))
    minutes, cost = (limits.get(k) for k in (
        "max_runtime_minutes", "max_cost_usd",
    ))
    if type(minutes) is not int or minutes <= 0:
        issues.append("limits.max_runtime_minutes")
    if (isinstance(cost, bool) or not isinstance(cost, (int, float))
            or not math.isfinite(cost) or cost <= 0):
        issues.append("limits.max_cost_usd")
    approved = root.get("gold_format_approved")
    if type(approved) is not bool:
        issues.append("gold_format_approved")
    if issues:
        raise Phase3ConfigError(issues)
    return Phase3Runtime(
        GoldTarget(kind, *identifiers), host, checkpoint, bootstrap,
        credential, offsets, registry_url, SecretReference.parse(reference),
        owner, runtime["dbr"], runtime["access_mode"], minutes, cost, approved,
    )


def load_phase3_runtime(path: str | Path) -> Phase3Runtime:
    """로컬 파일만 읽고 Secret 해석·리소스 존재 조회·연결은 하지 않는다."""
    try:
        values = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise Phase3ConfigError(("config.file",)) from None
    return parse_phase3_runtime(values)
