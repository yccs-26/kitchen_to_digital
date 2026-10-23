# Phase 3 UC 후보 실행 명령과 승인 경계

2026-10-23. 로컬 후보 준비용이다. 아래 live 예제는 **실행하지 않았다**.
전체 14단계·중단/정리 기준은 [E2E runbook](phase-3-e2e-execution.md)을 따른다.

## 1. 승인 전 로컬 실행

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/unit -q -p no:cacheprovider
.venv/bin/python -m streaming.tools.phase3_preflight \
  --config config/phase3_gold.example.json
# 현재 exit 2 / BLOCKED는 정상 차단 결과다.
```

네트워크 없이 명시적 후보 선택만 확인한다. 리소스가 존재한다는 뜻이 아니다.

```python
from streaming.jobs.metric_aggregation import prepare_metric_deployment
settings, report = prepare_metric_deployment(
    '<reviewed-local-config.json>', adapter='uc_managed_delta',
)
assert report['static_validation'] == 'PASS'
assert report['overall'] == 'BLOCKED'
```

`start_configured_metric_aggregation`는 현재 항상 배포 게이트에서 실패한다.
`gold_format_approved=true` 또는 명시적 adapter 선택만으로 해제되지 않는다.
기존 `UC_GOLD_WRITER_NOT_IMPLEMENTED` 코드는 이전 소비자와 테스트 호환을 위해
남겼다. 현재 뜻은 '승인된 live launcher 미구현'이며 후보 Writer 미구현이 아니다.
실제 승인을 받은 후 별도 검토 작업에서 launcher 게이트와 UC 수용 절차를 연결해야 한다.
환경변수나 설정 우회, monkeypatch로 게이트를 해제하여 운영 실행하지 않는다.

## 2. Gold 형식·UC 대상·권한 승인 후 수용 검사 — 현재 실행 금지

정확한 catalog/schema/table, 승인 주체, compute/시간/비용/단일 writer 및 격리된
테스트 행이 모두 승인된 경우에만 별도 notebook에서 아래 순서로 수행한다.
테이블이 없거나 형식/metadata가 다르면 중단한다. Writer는 자동 생성하지 않는다.

```python
import json
from streaming.sinks.gold_uc_delta import GoldUCDeltaSink

assert spark.conf.get('spark.sql.session.timeZone') == 'UTC'
assert settings.target.format == 'delta'
name = '.'.join((settings.target.catalog, settings.target.schema,
                 settings.target.table))
metadata = json.loads(spark.sql(
    f'DESCRIBE TABLE EXTENDED {settings.target.identifier} AS JSON'
).first()[0])
assert metadata['type'].upper() == 'MANAGED'
assert metadata['provider'].lower() == 'delta'
assert len(metadata['columns']) == 12
assert all(c['nullable'] is False for c in metadata['columns'])
# 실제 승인과 외부 writer 배제 증거를 먼저 기록한 뒤에만 선언한다.
sink = GoldUCDeltaSink(spark, name, single_writer=True)
# approved_final_rows: 승인된 독립 expected/mapper로 만든 테스트 전용 행.
# sink.merge_rows(approved_final_rows)  # 별도 쓰기 승인 후에만
# 같은 행 재적용 → Gold 값 불변; 다른 final → 오류 + 기존 값/새 행 모두 보호.
```

권한: USE CATALOG, USE SCHEMA, SELECT/MODIFY. 새 테이블이 필요하면 별도
CREATE TABLE 승인과 runbook DDL을 사용한다. blanket GRANT, LOCATION은 금지한다.
출력에 storage 경로/owner 등 계정 metadata가 포함되므로 공개 증빙은 검토·마스킹한다.

## 3. Kafka/Registry 인증 승인 — 현재 실행 금지

Phase 2의 실제 validated wire는 기존 `serialize_validated_event`가 생성한
Confluent magic byte 0 + big-endian schema ID + Avro 10필드다.
Phase 3는 기존 `decode_sensor_event`로 해석한다. 새 adapter 모듈은 만들지 않았다.
`registry_factory`는 executor에서 client를 지연 생성하며 driver SparkSession/dbutils나
client를 캡처하지 않는다. 실제 Secret 해석 방법과 executor 인증/네트워크는 미검증이다.

성공 조건: schema ID/subject 확인, UTF-8 equipment_id key, 불변 event_id/payload,
등록 장비/metric/unit 및 Kafka partition/offset ACK 대사. framing/schema/역직렬화 오류나
Registry transport 오류는 전파하며 임의 skip/offset commit하지 않는다.
동일 ID 상이 payload는 native dedup이 충돌 검출하지 않으므로 Phase 2 보장을 확인한다.

실제 publisher 명령은 아직 없다. 랜덤 simulator나 validated 직접 발행으로 대체하지 않는다.
runbook의 오프라인 후보는 과거 시각·원본 장비이므로 그대로 발행하면 안 된다.

## 4. DBR·Compute 및 checkpoint 승인 — 현재 실행 금지

```sh
export DATABRICKS_CONFIG_PROFILE='<approved-profile>'
export CLUSTER_ID='<approved-cluster-id>'
export JOB_ID='<approved-job-id>'
databricks clusters get "$CLUSTER_ID" --output json
databricks jobs get "$JOB_ID" --output json
databricks jobs list-runs --job-id "$JOB_ID" --active-only --output json
```

DBR 17.3 LTS 실제 build, Standard mode, library/module 배포, executor Registry,
NOT NULL metadata, conflict trap, callback 및 단일 writer를 수용한다.
checkpoint는 `/Volumes/<catalog>/<schema>/<volume>/job3/<approved-fixed-id>`이며
초기 사용·소유권·격리·권한을 승인한다. 재시작에는 같은 위치를 사용한다.
Kafka 시작 옵션은 새 checkpoint에서만 출발 정책이며 기존 state를 초기화하지 않는다.

## 5. 유료 실행·중단·복구 승인 — 현재 실행 금지

live launcher 수용과 결정적 publisher/DBR 장애 hook 준비가 끝난 후에만:

```sh
databricks jobs run-now "$JOB_ID" --no-wait --output json
# 응답에서 실제 run_id를 기록한다.
export RUN_ID='<recorded-run-id>'
databricks jobs get-run "$RUN_ID" --output json
# 오류, 시간/예산 한도 또는 수동 중단 조건이면 취소한다.
databricks jobs cancel-run "$RUN_ID" --timeout 5m --output json
```

성공: expected 24행(1분6 + 5분18), 키/경계/unit와 모든 절대 집계값 정확 일치.
UTC [start,end), 5분 길이/1분 slide. 실제 progress watermark가 마지막 target window
종료까지 지나고 출력 batch가 처리되어야 한다. 마지막 입력 시각이나 idle wall clock만으로
완료를 선언하지 않는다. target과 분리된 합법적인 진행 이벤트/실시각 대기 계획을 승인한다.

실패: 새 checkpoint를 만들거나 운영 state를 지우지 않는다. offset/state/Delta history와
최초 예외를 보존하고 같은 격리 checkpoint에서 재시도한다. MERGE 성공 뒤 checkpoint 실패는
재실행 NO-OP를 확인한다. 상이 final 또는 1 ULP 차이는 CONFLICT이며 값을 덮어쓰지 않는다.
DBR 장애 hook 미구현 상태에서 로컬 killpg/SIGKILL을 compute에 적용하지 않는다.

시간/금액 설정은 자동 과금 차단 기능이 아니다. 실제 가격/크레딧은 미조회이며 비용을
추정하지 않았다. 취소 이후에도 Job/compute 종료와 별도 리소스 비용을 확인한다.

## 6. 정리 승인과 증빙

runbook 14단계의 정확한 테스트 대상·owner·생성 run·보존기한을 재확인하고 별도 삭제 승인을
받는다. DROP/table 데이터 삭제와 checkpoint 삭제는 각각 승인한다. 이 문서는 삭제 승인이 아니다.
스크린샷 후보: Runtime/build/access mode, Catalog provider/schema, Job max concurrency,
query progress/W/state, Gold expected/actual24, 재시작 run/batch와 Gold 불변, 종료 상태.
실제 화면은 아직 없으며 생성하지 않았다. Secret/토큰/계정 metadata는 공개 전에 가린다.

공식 근거:
[UC Managed](https://docs.databricks.com/aws/en/tables/managed),
[Delta tutorial](https://docs.databricks.com/aws/en/delta/tutorial),
[DESCRIBE JSON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table),
[foreachBatch](https://docs.databricks.com/aws/en/structured-streaming/foreach),
[DBR 17.3](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts).
API의 문서상 지원과 실제 Workspace 수용 검증은 구분한다.
