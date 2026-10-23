# Phase 3 Kafka·Databricks E2E 실행 절차

2026-10-23. **준비 문서이며 cloud read/write 명령은 실행하지 않았다.**
UC 후보·범위 승인 launcher·격리 raw publisher·MERGE 경계 probe를 로컬 구현했다.
실제 UC/Kafka/DBR 수용과 별도 실행 승인은 없으므로 실제 실행은 BLOCKED다.
최신 실행 API는 마지막 「실행 코드 연결」을 따르고 이전 미구현 표현은 당시 이력으로 구분한다.
명령 문법과 실행 순서를 정리한 것이 E2E 실행 가능/완료를 의미하지 않는다.
Phase 1/2 운영 table/checkpoint는 공유·변경하지 않는다.

## 0. 로컬 Preflight와 승인

```sh
.venv/bin/python -m streaming.tools.phase3_preflight \
  --config config/phase3_gold.example.json \
  --output /private/tmp/phase3-e2e-preflight.json
# 현재 exit 2, overall=BLOCKED
```

example의 null은 미입력·미승인이다. 실제 값은 추적하지 않는 검토용 JSON에 넣는다.
static_validation=PASS여도 UC_GOLD_WRITER_NOT_IMPLEMENTED와 LIVE_E2E_NOT_AUTHORIZED가 남는다.
후보 구현만으로 live 게이트를 해제하지 않는다. 형식 승인 및 실제 UC 수용 전 아래 live 절차를 시작하지 않는다.
아래 변수는 존재하는 리소스 이름이 아닌 placeholder다. 토큰·설정 전체를 echo하지 않는다.

```sh
export DATABRICKS_CONFIG_PROFILE='<approved-auth-profile>'
export CLUSTER_ID='<approved-cluster-id>'
export JOB_ID='<approved-phase3-job-id>'
export RUN_ID='<record-run-id-after-approved-submission>'
```

읽기 전용 CLI도 실제 접속이므로 이번 실행 범위에서 금지된다.

## 1. Phase 2 validated 및 계약 확인

승인된 notebook에서 설정만 검사하는 예:

```python
from streaming.jobs.runtime_config import load_runtime_config
p2 = load_runtime_config("<approved-job2-config-path>")
assert p2.job.validated_topic == "kitchen.sensor.validated"
```

성공: canonical 불변 payload, sensor_metric_event.avsc의 10필드, equipment_id UTF-8 key,
Confluent framing/schema ID, 실제 등록 장비·metric/unit 확인.
중단: Phase 2 수용·선행 병합 미확인, 미등록 장비/schema ID, validated 직접 우회 발행 계획.

## 2. Kafka/Registry 접근·인증

```python
from streaming.config.phase3_runtime import load_phase3_runtime
settings = load_phase3_runtime("<approved-phase3-config-path>")
settings.require_deployment_ready()  # 별도 ExecutionApproval 없으면 계속 예외
```

승인 후 별도 수용 검사에서 executor의 Registry schema 조회·decode 및 Kafka read 권한,
partition/offset/retention 범위를 확인한다. 현재는 그 검사가 BLOCKED다.
SecretReference는 참조만 보관하고 실제 해석은 승인된 실행 위치에서 한다.
registry_factory에 driver SparkSession/dbutils/native client를 캡처하지 않는다.
Python Kafka package 설치를 Spark connector 또는 IAM 권한 증거로 간주하지 않는다.
기존 simulator의 localhost/비인증 기본값을 운영 인증으로 사용하지 않는다.

## 3. Workspace/Runtime/권한 확인 — 승인된 live read

```sh
databricks clusters get "$CLUSTER_ID" --output json
databricks jobs get "$JOB_ID" --output json
```

성공: 올바른 workspace/profile, DBR 17.3 LTS candidate의 실제 Spark/build/access mode,
승인 task/code/config와 최대 실행 시간 확인. 종료된 cluster 조회는 연결/실행 성공 증거가 아니다.
중단: 다른 workspace/cluster, API 불일치, 미승인 compute·과금 범위.
[DBR 문서](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts),
[CLI clusters](https://docs.databricks.com/aws/en/dev-tools/cli/reference/clusters-commands).

```python
target = settings.target.identifier  # 검증된 3-part 식별자
spark.sql(f"DESCRIBE DETAIL {target}").show(truncate=False)
spark.sql(f"SHOW GRANTS ON TABLE {target}").show(truncate=False)
spark.table(target).printSchema()
```

USE CATALOG/USE SCHEMA, SELECT/MODIFY, 신규 생성 시 CREATE TABLE 권한을 확인한다.
실제 caller와 provider/schema를 확인하며 blanket GRANT는 하지 않는다.
대상이 없으면 별도 생성 승인 전 중단한다.

## 4. Gold 형식·DDL 승인

ADR은 승인 대기다. Managed Delta가 권장 후보지만 자동 확정하지 않는다.
Delta 선택 및 대상 생성 승인 이후에만 다음 DDL을 사용할 수 있다.

```python
if settings.target.format != "delta" or not settings.gold_format_approved:
    raise RuntimeError("Gold format/target approval required")
ddl = f"""CREATE TABLE {settings.target.identifier} (
  window_start TIMESTAMP_LTZ NOT NULL, window_type STRING NOT NULL,
  store_id STRING NOT NULL, equipment_id STRING NOT NULL,
  metric_name STRING NOT NULL, window_end TIMESTAMP_LTZ NOT NULL,
  unit STRING NOT NULL, sample_count BIGINT NOT NULL,
  sum_value DOUBLE NOT NULL, min_value DOUBLE NOT NULL,
  max_value DOUBLE NOT NULL, avg_value DOUBLE NOT NULL
) USING DELTA"""
# spark.sql(ddl)  # 명시적 생성 승인 후에만. 이번 작업에서는 실행 금지.
```

IF NOT EXISTS/CREATE OR REPLACE로 기존 target의 차이를 숨기지 않는다.
Iceberg 선택 시 이 DDL/MERGE를 쓰지 말고 해당 adapter 수용 검증부터 수행한다.
managed table에는 LOCATION/forPath/직접 _delta_log 접근을 사용하지 않는다.
[이름 기반 접근 문서](https://docs.databricks.com/aws/en/delta/tutorial).

## 5. Checkpoint 승인·격리

정적 검사는 /Volumes/<catalog>/<schema>/<volume>/job3/<고정ID> 형식만 검사한다.
실존·소유권·READ VOLUME/WRITE VOLUME 및 다른 writer는 live 확인 사항이다.

```python
entries = dbutils.fs.ls("<approved-checkpoint-volume-parent>")
# 이름/소유 run 대조만 한다. 운영 contents를 변경하지 않는다.
```

초기 테스트 위치와 재시작에 유지할 고정 위치를 승인한다. 단순한 exists/빈 목록을
미사용·권한·안전성 증거로 해석하지 않는다. job1/job2/운영 checkpoint와 공유하면 중단한다.

## 6. 단일 writer 확인

```sh
databricks jobs get "$JOB_ID" --output json
databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json
```

성공: max_concurrent_runs=1, 외부 Job/notebook/backfill 배제, 승인 owner만 MODIFY 권한 보유.
중단: 같은 target/checkpoint의 다른 writer 존재 또는 배제 여부 미확인.
한 Job의 동시 실행 수는 외부 writer까지 잠그지 않는다.

## 7. 소규모 입력의 오프라인 준비

기존 fixture의 과거 시각과 clock-001을 실제 토픽에 그대로 발행하지 않는다.
steps[:3]의 8레코드(동일 ID 재송신 1건 포함), 고유 7이벤트, expected 24행이 기준이다.
다음은 시각과 ID만 변환하는 오프라인 명령이다. 아직 발행 가능한 데이터라는 뜻은 아니다.

```sh
BASE_EVENT_TIME='<approved-UTC-minute>' \
TEST_ID_NAMESPACE='<approved-unique-run-namespace>' \
.venv/bin/python - <<'PY'
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path

fixture_path = Path('tests/fixtures/phase3/sliding_window_events.json')
fixture = json.loads(fixture_path.read_text())
base = datetime.fromisoformat(os.environ['BASE_EVENT_TIME'])
assert base.utcoffset() == timedelta(0) and base.second == base.microsecond == 0
shift = base - datetime(2026, 10, 20, 12, tzinfo=timezone.utc)
namespace = os.environ['TEST_ID_NAMESPACE']
assert namespace and '<' not in namespace
rows = []
for step in fixture['steps'][:3]:
    for event in step['events']:
        row = dict(event)
        row['event_id'] = namespace + ':' + row['event_id']
        original_time = datetime.fromisoformat(row['event_time'])
        row['event_time'] = (original_time + shift).isoformat()
        rows.append(row)
evidence = Path('docs/evidence/phase-3/local-2026-10-22/integrated-final')
paths = list(evidence.glob('test_streaming_final_rows_reac*/expected.json'))
assert len(paths) == 1
expected = json.loads(paths[0].read_text())
assert len(rows) == 8 and len(expected) == 24
for row in expected:
    for key in ('window_start', 'window_end'):
        row[key] = (datetime.fromisoformat(row[key]) + shift).isoformat()
output = Path('/private/tmp')
(output / 'phase3-candidate-events.json').write_text(
    json.dumps(rows, indent=2),
)
(output / 'phase3-candidate-expected.json').write_text(
    json.dumps(expected, indent=2),
)
PY
```

원본 fixture의 장비가 승인되지 않았다면 두 파일에 같은 ID mapping을 적용하고 Phase 2 등록 승인을 받는다.
장비를 임의 등록하거나 기존 장비 이력을 테스트 값으로 오염시키지 않는다.
8건은 ACK/offset과 batch 처리 완료를 관측하며 여러 micro-batch에 보낸다.
한꺼번에 발행한 것만으로 cross-batch 검증을 주장하지 않는다.
최종 window end보다 10분 이상 진행한 합법적인 후속 event-time이 필요해 실제 대기가 포함된다.
미등록 clock이나 미래 시각으로 W를 강제 전진시키지 않는다. 등록된 정상 테스트 이벤트를
실시각으로 기다리거나 격리 synthetic 입력 계약을 별도 승인한다. 시간/예산이 모자라면 중단한다.
랜덤 simulator/main.py --count는 이 24행의 결정적 replay 명령이 아니다.

## 8. Job 실행 — 현재 BLOCKED

승인된 UC writer/entrypoint·Job이 존재하고 앞선 게이트가 닫힌 이후에만:

```sh
databricks jobs run-now "$JOB_ID" --no-wait --output json
# 응답의 run_id를 승인된 RUN_ID로 기록한다.
```

발행은 raw→Phase 2→validated를 지나는 승인된 bounded harness만 사용한다.
현재 publisher는 dry-run CLI와 별도 승인된 Phase2 raw 발행 API를 제공한다.
실제 auth/등록 장비/진행 입력 수용과 승인 없이 발행하지 않는다.
기존 simulator를 대신 실행하거나 validated에 직접 produce해 E2E라고 표시하지 않는다.
실제 데이터·장비·시각·ACK lineage가 승인되지 않으면 Job/producer를 시작하지 않는다.

## 9. Watermark·Window 확정

```python
import json
progress = [json.loads(p.json) for p in query.recentProgress]
# query는 승인된 entrypoint가 반환한 단일 query다.
assert query.isActive or query.exception() is None
```

성공: 실제 W 전진, 4개 state operators, eviction, window 종료 뒤 Append final 관측.
idle만으로 W가 전진하지 않음을 확인하고 같은 query/run의 기록만 대조한다.
시간/예산 상한 또는 오류면 step13에서 취소하고 미확정을 완료로 표시하지 않는다.

## 10. Gold expected/actual 비교

UC 후보의 실제 DBR 수용 후 승인된 notebook에서 수행할 정확 비교 예다.
배경 트래픽이 승인된 장비/시각 범위에 들어올 수 있으면 먼저 중단하고 입력 격리를 해결한다.

```python
from datetime import datetime
import json
from pathlib import Path
from pyspark.sql import functions as F

assert spark.conf.get('spark.sql.session.timeZone') == 'UTC'
expected = json.loads(Path('<approved-expected-json-path>').read_text())
for row in expected:
    for key in ('window_start', 'window_end'):
        row[key] = datetime.fromisoformat(row[key])
stores = sorted({r['store_id'] for r in expected})
equipment = sorted({r['equipment_id'] for r in expected})
low = min(r['window_start'] for r in expected)
high = max(r['window_end'] for r in expected)
frame = spark.table(settings.target.identifier).where(
    F.col('store_id').isin(stores) & F.col('equipment_id').isin(equipment)
    & (F.col('window_start') >= F.lit(low))
    & (F.col('window_end') <= F.lit(high))
)
for key in ('window_start', 'window_end'):
    frame = frame.withColumn(
        key, F.date_format(key, "yyyy-MM-dd'T'HH:mm:ssXXX"),
    )
actual = [r.asDict() for r in frame.collect()]
for row in actual:
    for key in ('window_start', 'window_end'):
        row[key] = datetime.fromisoformat(row[key])


def row_key(row):
    return tuple(row[name] for name in (
        'window_start', 'window_type', 'store_id', 'equipment_id',
        'metric_name',
    ))


assert len(actual) == 24
assert len({row_key(r) for r in actual}) == 24
assert sorted(actual, key=row_key) == sorted(expected, key=row_key)
```

성공: 1분6행+5분18행, 모든 키·unit·경계·count/sum/min/max/avg 일치.
count는 정수, epsilon은 없고 signed zero만 동등하게 취급한다. 1 ULP 차이는 실패로 보존한다.

## 11. Kafka offset과 Spark checkpoint

```sh
databricks jobs get-run "$RUN_ID" --output json
```

progress.sources의 startOffset/endOffset, 발행 ACK의 partition/offset,
checkpoint의 query/run identity를 대조한다. consumer group commit만으로 완료를 판단하지 않는다.
운영 commits/offsets/state를 이동·삭제하지 않는다.

## 12. 동일 batch 재실행·장애 복구

정상 stop→동일 checkpoint 재시작, MERGE 전 예외, MERGE 후 callback 예외를 격리 target에서 수행한다.
FailureProbe(before_merge/after_merge)는 구현했고 로컬 이름 기반 Delta 복구를 검증했다.
실제 DBR callback 및 run 종료/재시작 수용은 여전히 BLOCKED다.
로컬 process-group SIGKILL을 Databricks compute에 그대로 적용하지 않는다.
성공: 같은 batch ID 재실행과 Gold 수치·키 수 불변. CONFLICT는 기존 값 보호 후 중단.
여러 MERGE/table/checkpoint를 하나의 공동 transaction으로 가정하지 않는다.

## 13. 결과·비용·실행 중단

```sh
databricks jobs get-run "$RUN_ID" --output json
databricks jobs cancel-run "$RUN_ID" --timeout 5m --output json
```

cancel은 비동기이므로 끝난 상태를 확인한다. CLI timeout은 비용 상한이나 자동 과금 정지가 아니다.
max_runtime_minutes/max_cost_usd는 선언값이다. 실제 Job timeout·정지 감시·가격 확인이 별도로 필요하다.
DBU/instance/worker 수, MSK/Registry, storage·전송·보존·maintenance 비용 포함 범위를 확인한다.
증빙: 코드 ref/설정 hash, 입력 ACK/offset, 실행 기간, W/state, expected/actual, Delta history,
run 결과, 실측 시간·청구 자료. 가격·크레딧 불명 상태에서 예산 내라고 선언하지 않는다.

## 14. 테스트 리소스 정리 — 별도 승인

writer 중단·증빙 보존 후 owner/전체 식별자/provider/생성 run/보존기한을 다시 확인한다.
삭제 승인 전에는 target과 checkpoint를 보존한다. 운영 prefix·공유 volume은 대상이 아니다.

```python
# 정확한 대상 재확인과 명시적 삭제 승인 이후에만. 이번 작업에서는 실행 금지.
# spark.sql(f"DROP TABLE {approved_test_identifier}")
# dbutils.fs.rm(approved_test_checkpoint, recurse=True)
```

wildcard·catalog/schema 전체 삭제·운영 checkpoint 초기화는 하지 않는다.
managed table DROP은 데이터 삭제를 포함하므로 대상과 승인을 반드시 기록한다.

참고: [CLI Jobs](https://docs.databricks.com/aws/en/dev-tools/cli/reference/jobs-commands),
[foreachBatch](https://docs.databricks.com/aws/en/structured-streaming/foreach),
[checkpoints](https://docs.databricks.com/aws/en/structured-streaming/checkpoints).


## 실행 코드 연결 — 2026-10-23 후속 구현

Phase2 tip은 이번 시작 시 `3d772bd`, merge-base `b8c6ee2`로 후속12개가 현재 HEAD에 없다.
현재 checkout의 Phase2 코드는 바꾸지 않았다. wire/schema/publisher/UTC는 동일하며,
P3 Registry boundary만 최신 40403 구분을 보완한다. Git 계보 정렬은 사용자 검토 작업이다.

### 승인 전: 결정적 dry-run

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m streaming.tools.phase3_e2e_publisher --base-time 2026-10-20T12:00:00Z --namespace offline-demo --output /private/tmp/phase3-e2e-dry-run.json
```

이 시각은 과거 fixture 검증용이며 실제 발행용이 아니다. CLI에는 publish 옵션이 없다.
`e2e_validated_events.json`은 정상/중복/UTC 경계8건 + 실제 진행1건 + late1건이다.
고유 target7개, target Gold24행(1분6 + 5분18)이며 progress 장비 결과는 분리한다.
fixture의 progress 장비는 미승인 표시용이다. `--equipment-map` JSON의 원본 ID별
store_id/equipment_id/equipment_type 전체 mapping으로 등록된 격리 장비를 명시하고
expected/문서 hash를 함께 검토해야 한다. ID 병합·mapping 누락은 거부한다.
이 mapping 없는 dry-run은 실제 publisher wrapper에서 거부한다. 새 예상값은 독립 유한 oracle로 기존24행과 대조했다.

**Phase2 실제 정책 정정:** 유효한 오래된 이벤트는 LATE로 분류되지만 Silver/validated로
전달된다. late를 무조건 Quarantine 처리한다고 가정하지 않는다. future-invalid/도메인
오류와 구별하며 P3에서 실제 W 이후 late drop을 관측한다.

### 별도 승인 기록과 executor 인증

`ExecutionApproval`에는 approval_id, settings_sha256, expires_at(aware ISO8601), job_tag,
actions(launch/publish_raw/failure_probe 중 승인 범위), isolated_target,
uc_acceptance_complete, single_writer_confirmed, phase2_baseline_reviewed,
cost_controls_reviewed, fixture_sha256 및 registry_credential_env를 기록한다.
`settings_digest(settings)`로 검토 설정을 고정하고 입력 문서는 `document_digest`로 고정한다.
실제 기록을 이번 작업에서 생성하거나 승인하지 않았다. JSON/boolean은 인간 승인·권한·lock의
암호학적 증거가 아니다. 검토자가 실제 수용·비용·Git baseline을 확인한 뒤 비공개 기록을 관리한다.
설정 flag만으로는 실행되지 않으며 설정 변경/만료/미승인 action은 query 시작 전에 실패한다.

worker는 `ExecutorRegistryFactory(url, credential_env)`로 처음 decode할 때 client를 만든다.
예: 승인 compute 환경변수의 `PHASE3_REGISTRY_AUTH={{secrets/<approved-scope>/<approved-key>}}`.
실제 credential 값은 코드/closure/설정에 저장하지 않는다. 환경변수 이름을 승인 기록과 대조한다.
Secret 환경변수 방식은 cluster 사용자 노출·log 접근 통제와 실제 worker 전파를 검토해야 한다.
[공식 Secret 참조 지침](https://docs.databricks.com/aws/en/security/secrets/secrets-spark-conf-env-var).
실제 DBR 지원/인증/권한은 미검증이며 이번에 환경변수를 설정하지 않았다.

### 승인된 DBR notebook의 Job3 entrypoint — 이번 실행 금지

```python
from streaming.jobs.metric_aggregation import main
exit_code = main([
    '--config', '<approved-phase3-config-path>',
    '--approval', '<approved-separate-approval-json>',
    '--adapter', 'uc_managed_delta',
    '--registry-credential-env', 'PHASE3_REGISTRY_AUTH',
], spark=spark)
```

기존 승인 compute/session만 사용하며 새 compute/table/CP를 만들거나 초기화하지 않는다.
Live config parser는 기존 /Volumes CP·UTC/두 window/W10분·single writer 정적 조건을 유지한다.
별도 승인이 통과하면 Kafka source→동일 단일 query→Mapper→UC 이름 Writer가 연결된다.
query는 승인 minutes 동안 기다리고 timeout 시 stop한다. Job/compute 과금 종료 보장은 아니며
외부 Job timeout/종료 감시도 필요하다. query 실패는 Job 실패로 전파한다.
`--available-now`는 승인 입력 snapshot용이며 열린 tail window를 강제 확정하지 않는다.

### 승인된 격리 raw→Phase2 publisher API — 이번 실행 금지

```python
from streaming.tools.phase3_e2e_publisher import publish_from_phase2_config
acks = publish_from_phase2_config(
    reviewed_document, approved_batch_index, settings=settings, approval=approval,
    phase2_config='<approved-job2-config-path>', dbutils=dbutils, batch_ready=True,
)
```

`reviewed_document`·settings·approval은 검토된 파일에서 로드한 객체다. factory/serializer는
기존 Phase2 runtime/인증 옵션과 AvroSerializer를 사용한다. auto.register=false,
use.latest=false, topic_subject_name_strategy를 유지한다. producer 연결 전에 topic/bootstrap/
Registry route·fixture hash·Phase2 domain/future 규칙을 검사한다. 직접 validated 발행은 없다.
배경 운영 장비/운영 bootstrap에 쓸 승인이 아니다. 실제 격리 여부는 외부 수용·권한으로 확인한다.
한 batch마다 ACK partition/offset(non-bool int>=0)과 query 처리·W를 확인한 후 다음 batch를
호출한다. batch_ready=true 자체는 관측 증거가 아니다. 여러 ACK는 원자적이지 않으므로 실패 시
확인한 offset·오류를 보존하고 동일 ID로 재시도한다. source event_id를 새로 만들지 않는다.
진행 행은 실시각이 도달한 뒤에만 허용하며 fake future clock으로 비용/시간을 우회하지 않는다.

### 승인된 테스트 query의 실패 probe — 이번 실행 금지

entrypoint에 `--probe-stage before_merge` 또는 `after_merge`와
`--probe-batch-id <approved-test-batch-id>`를 함께 전달한다. 별도 failure_probe action과
job_tag/동일 CP가 일치해야 한다. 해당 batch에서 예외를 일으켜 query 실패를 전파한다.
재시작에서는 probe 옵션을 제거하고 같은 CP를 쓴다. MERGE 후 실패이면 동일 final은 NO-OP다.
probe는 다른 Job의 취소/강제종료 API를 호출하지 않는다. 정상 stop/timeout, callback 예외,
전용 로컬 SIGKILL은 서로 다른 증거다. DBR cancel/restart는 승인된 RUN_ID/Job 소유권을
다시 확인한 뒤 기존 get-run/cancel-run 절차로 수행한다. 실제 강제 종료·DBR 복구는 미검증이다.
