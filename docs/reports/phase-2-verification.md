# Phase 2 검증 초안 — P2-8A

2026-10-08 실행. **P2-8A 미완료: Silver 생성안 승인 및 runtime 설정 확인 대기.**
최종 Phase 2 수용 보고서가 아니며, P2-8B/C는 실행하지 않았다.
크레딧 충전 직후에는 인증 오류가 동일했으나, 이후 사용자 재인증으로 API 접속이 복구됐다.


## B 채택과 로컬 adapter 정리

Silver는 UC Managed Delta로 결정했다. P2의 핵심은 persistent idempotency와
canonical 보존이며, Delta target의 MERGE는 공식 지원이 명확하다. Managed
Iceberg target의 현재 MERGE 지원은 공식적으로 확정하지 못했으므로 이 불확실성으로
P2-8을 지연시키지 않기로 했다. Bronze Managed Iceberg는 변경하지 않는다.

`streaming/sinks/delta_silver.py`의 `DeltaSilverStorage`로 이름과 참조를 정리했다.
SilverStorage 계약, insert-only MERGE, parameter binding, 사후 lookup,
UTC 변환과 중복 행 검출은 유지한다. 테이블 생성·데이터 쓰기·checkpoint 변경은
수행하지 않았다. 실제 Delta runtime 검증과 P2-8 acceptance는 여전히 미완료다.

앞선 읽기 전용 compatibility 확인에서는 DBR 17.3.x-photon-scala2.13,
Spark 4.0.0, range(1) 조회 성공, Bronze의 MANAGED / iceberg provider를 확인했다.
SQL serverless 활성화와 catalog predictive optimization ENABLE도 확인했다.
이는 아래 초기 preflight 시점의 인증/compute 대기 상태를 갱신하는 관측이며,
Silver runtime 성공 증거는 아니다.

## 재인증 후 확인과 생성 제안 — 미실행

- `current-user me` 성공. 기존 compute `ktd-phase1-dev`의 설정 runtime은
  `17.3.x-scala2.13`; 조회 당시 `PENDING / Starting Spark`였다.
  Spark version 응답을 기다리던 로컬 접속 시도는 생성안 제시 단계에서 중단했다.
  range/테이블 메타데이터 SQL은 실행 완료되지 않았고 compute 자체를 중지하지 않았다.
  재인증 후 요약 증빙은 `docs/evidence/phase-2/p2-8a/preflight-after-reauth.json`이다.
- `ktd`의 schema는 `bronze`, `default`, `information_schema`, `ktd_preflight`다.
  `silver` 조회는 NotFound이며 KTD 내 기존 Silver 테이블을 찾지 못했다.
- 기존 Bronze는 `ktd.bronze.sensor_raw`, checkpoint volume은
  `/Volumes/ktd/bronze/checkpoints`다. 그 하위에는 `job1`, `job1-tests`,
  `job1-failure-test`만 확인됐다. Job 2 checkpoint는 생성하지 않았다.
- UC API에서 기존 Bronze의 table_type은 MANAGED, data_source_format은 DELTA로
  표시됐다. 저장소 DDL의 `USING ICEBERG`와 표시가 다르다. 이 API 값만으로
  실제 provider나 Iceberg 지원 여부를 단정하지 않으며 Spark 메타데이터 재확인이 필요하다.
- 원격 프로젝트의 `streaming/jobs/`에는 `raw_ingestion.py`와 `__init__.py`만
  확인됐다. Job 2 원격 배포와 실행 위치의 import 성공은 아직 증명되지 않았다.
- 로컬 Silver setup DDL/notebook은 찾지 못했다. `config/` 및 후보
  `config/validation_rules.yaml`, `config/equipment_registry.yaml`은 없다.
  `load_rules()`는 JSON의 supported_schema_versions/equipment_registry/metric_units를
  읽는다. YAML을 그대로 연결할 수 없다.
- 루트 `.env`는 없고 `infra/docker/.env`는 존재한다. 후자는 PostgreSQL 항목과
  KAFKA_BOOTSTRAP_SERVERS/KAFKA_TOPIC_SENSOR_RAW만 포함한다. 값은 출력하지 않았다.
  Compose에는 로컬 Registry가 있으나 cloud Job 2 Registry/producer 인증 설정은
  발견하지 못했다. `streaming/validation/schema_registry.py`는 0바이트이며 실제
  schema 조회는 `avro_decoder.py`의 주입된 Registry client가 수행한다.

기존 catalog `ktd`를 유지하고 데이터 계층을 나누는 `ktd.silver`를 새로 제안한다.
테이블명은 raw와 대응되는 `ktd.silver.sensor_validated`다. 이전 USING ICEBERG DDL은 실행하지 않은 제안이었으며 B 채택으로 대체했다.
아래는 UC Managed Delta 승인용 DDL이며
실행하지 않았다. LOCATION 없이 UC managed storage를 사용하고 분할은 추가하지 않는다.

```sql
-- 검증된 canonical 이벤트를 보관할 Silver 계층을 만든다.
CREATE SCHEMA ktd.silver;

-- 기존 어댑터가 사용하는 canonical 10개 필드만 저장한다.
CREATE TABLE ktd.silver.sensor_validated (
    event_id STRING NOT NULL,
    event_time TIMESTAMP_LTZ NOT NULL,
    store_id STRING NOT NULL,
    equipment_id STRING NOT NULL,
    equipment_type STRING NOT NULL,
    metric_name STRING NOT NULL,
    metric_value DOUBLE NOT NULL,
    unit STRING NOT NULL,
    schema_version STRING NOT NULL,
    source STRING NOT NULL
)
USING DELTA;
```

모든 필드는 domain validator의 필수 canonical 계약에 맞춰 NOT NULL을 제안한다.
TIMESTAMP_LTZ는 UTC로 정규화된 시점을 저장하며 표시에는 세션 시간대가 적용된다.
event_id UNIQUE/PK 제약은 추가하지 않는다. 따라서 DB constraint가 중복을
보장한다고 주장하지 않으며 기존 단일 writer/MERGE/lookup 경계를 실제 검증해야 한다.
이미 대상이 생겼다면 DDL을 실행하기 전에 다시 조회하고 schema를 대조한다.

공식 문서의 [managed table 생성](https://docs.databricks.com/aws/en/tables/managed),
[NOT NULL 문법](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using),
[TIMESTAMP_LTZ 의미](https://docs.databricks.com/aws/en/sql/language-manual/data-types/timestamp-type)를
확인했다. 문서 확인은 이 workspace의 DDL/MERGE runtime 성공을 뜻하지 않는다.

사용자의 후속 지시에 따라 여기서 생성 전 승인 대기한다. Registry/producer/rules
설정도 새로 만들지 않았다. 아래 표는 최초 인증 실패 시점의 관측 기록이다.

| 순서 | 항목 | 이번 실행 결과 |
|---|---|---|
| 1 | 작업 전 Git | `feat/phase2-validation-silver`, HEAD `fade5d49eb5b359ccda6a3504c95b35fa2b89474`, 작업 트리 clean |
| 2 | 환경 | 현재 셸에 관련 KTD/Kafka/Registry/Databricks/AWS 환경변수 없음. 루트 `.env` 없음. Databricks/AWS 설정 파일 존재. 비밀값 출력 안 함 |
| 3 | Kafka | AWS STS 성공. MSK `ktd-msk-serverless` ACTIVE/SERVERLESS, IAM bootstrap 설정 존재. 이는 관리 API 결과이며 topic 접근·produce/consume 권한 증거가 아님. 기존 `ktd-kafka-client` EC2 stopped. 로컬 실행 중 Docker 컨테이너 없음 |
| 4 | Registry | 실제 Job 2 Registry 설정 미확인. endpoint 접속·SensorMetricEvent lookup·schema ID 모두 미검증 |
| 5 | Databricks/Spark | `.venv-databricks`: databricks-connect 17.3.14, SDK 0.143.0, confluent-kafka 2.15.1. Connect 및 Job 2 로컬 import 성공. 실제 Spark 세션은 refresh token 오류로 실패. 서버 runtime 버전 미확인 |
| 6 | 당시 Silver Iceberg 제안(미실행·대체됨) | Silver 대상 이름 미설정. 테이블 존재·type·schema·SELECT·nullability·constraint 모두 미검증 |
| 7 | 장비 ID | simulator 1개 매장, 6개 장비 ID는 서로 다름. store-002 테스트는 불일치·충돌 검사용. 다중 매장 운영 목록이 없어 전역 유일성은 미확정. key 정책 변경·migration 없음 |
| 8 | 정상 fixture | `493b91c6-1e96-477b-a6c3-babd07f672eb`, UTC `2026-10-08T04:58:50.981456+00:00`, store-001/fridge-001, refrigerator/temperature_celsius, 4.25 celsius, 1.0.0, simulator. **미발행** |
| 9 | raw publish | 미실행. topic 기본값 `kitchen.sensor.raw`는 코드상 기본값이며 실제 존재 확인 아님 |
| 10 | Job 2 처리 | 미실행. batch/query/run ID 없음 |
| 11 | Silver row | 미검증. canonical 입력 대조 불가 |
| 12 | validated | 미검증. 코드 기본값 `kitchen.sensor.validated`. 동일 event_id/key 및 ACK 확인 안 됨 |
| 13 | Quarantine | 미검증. 코드 기본값 `kitchen.sensor.quarantine`. 해당 이벤트 부재를 consume으로 확인하지 않음 |
| 14 | checkpoint | `KTD_VALIDATION_CHECKPOINT` 미설정. 실제 존재·Job 1과 경로 분리·progress 미검증 |
| 15 | lineage | 미발행이므로 raw partition/offset 없음. raw→Silver→validated 관계 증명 안 됨 |
| 16 | MERGE/왕복 | 실제 MERGE·lookup·timestamp/metric_value round-trip 미실행. fake 또는 로컬 Avro 결과로 대체하지 않음 |
| 17 | 문제/수정 | 인증·환경 설정 차단. runtime 코드 버그는 확인하지 못함. 애플리케이션 코드 수정 및 workaround 없음 |
| 18 | 회귀 | 코드 수정 없어 전체 회귀 미실행. 요청의 431 passed를 이번 실행 결과로 재확인하지 않음. fixture의 로컬 Avro 왕복과 domain 검증만 성공 |
| 19 | 증빙 | `docs/evidence/phase-2/p2-8a/preflight-summary.json`, `docs/evidence/phase-2/p2-8a/happy-path-fixture-unpublished.json`. CLI 관측을 요약한 기록이며 E2E 성공 증빙 아님 |
| 20 | 후속 범위 | P2-8A 인증/설정 복구와 정상 E2E 전체가 남음. P2-8B/C의 corrupt/domain invalid/duplicate/conflict/late/future/restart/부분 실패 검증 미실행 |

## 실행 근거와 재개 조건

실행한 읽기 전용 명령:

- `git branch --show-current`, `git status`, `git rev-parse HEAD`
- `databricks auth profiles`, `databricks current-user me --profile ktd`
- `.venv-databricks/bin/python`에서 `DatabricksSession.builder.profile('ktd').getOrCreate()` 및 `range(1).collect()` 시도
- `aws sts get-caller-identity` (출력은 account로 제한)
- `aws kafka list-clusters-v2`, `aws kafka get-bootstrap-brokers` (후자는 응답 key만 출력)
- `aws ec2 describe-instances` (기존 client의 ID/state만 조회)
- `docker ps --format '{{.Names}} {{.Status}}'`

Databricks의 정확한 CLI 오류:

```text
Error: A new access token could not be retrieved because the refresh token is invalid.
```

샌드박스 밖 네트워크 허용 상태에서도 동일 오류였으며, 크레딧 충전 후 CLI와
Spark 세션을 재시도해 같은 원인을 확인했다. 사용자가
`databricks auth login --profile ktd`로 이후 재인증해 해소했다.

재개에 필요한 기존 설정: compute 선택, Kafka broker/인증,
`KTD_SCHEMA_REGISTRY_CONFIG_JSON`, `KTD_PRODUCER_CONFIG_JSON`,
`KTD_SILVER_TABLE`, `KTD_VALIDATION_CHECKPOINT`,
`KTD_PROCESSING_VERSION`, `KTD_VALIDATION_RULES_PATH`.
설정 파일이나 notebook 경로로 확인하며 비밀값을 보고서에 복사하지 않는다.

Silver adapter가 기대하는 것은 canonical 10필드이며 event_time은 TIMESTAMP(LTZ),
metric_value는 DOUBLE, 나머지는 STRING이다. 실제 schema와 nullability를 먼저
대조해야 한다. event_id 유일성을 DB가 보장한다고 가정하지 않는다.
현재 SQL은 event_id 일치 시 보존하고 NOT MATCHED일 때만 INSERT하는 MERGE다.
현재 Silver는 UC Managed Delta를 대상으로 하며 실제 MERGE·재시작 검증은 미완료다.

정상 fixture는 기존 물리 Avro schema로 로컬 왕복 및 명시적 단일 장비 규칙의
domain 검사만 수행했다. Registry framing/schema ID와 운영 규칙 검증은 아니다.
실제 발행 시 새 UUID와 현재 UTC 시각으로 다시 생성하여 시간 정책에 영향을
주지 않도록 한다. 첫 임시 검사 호출은 필수 max_future_skew 인자 누락으로
실패했고 인자를 지정한 재실행은 통과했다. 저장소 코드 결함은 아니다.

이번 실행은 topic/table 생성·변경, EC2 시작, 데이터·checkpoint 삭제,
Git add/commit/push/브랜치 변경을 수행하지 않았다.

## Delta adapter rename/refactor 회귀 결과

이번 작업은 이름·참조·설명 정리이며 새 저장 전략을 구현하지 않았다.
이름과 docstring을 정규화한 AST 비교에서 adapter, SilverStorage/SilverSink,
Job 2의 실행 코드가 HEAD와 동일했다. adapter 테스트 함수 17개를 유지했다.

실행 명령:

```bash
.venv/bin/python -m pytest -q tests/unit/test_delta_silver_adapter.py tests/unit/test_sensor_validation_job.py
.venv/bin/python -m pytest -q tests/unit/test_validation_input.py tests/unit/test_validation_domain.py tests/unit/test_quarantine_record.py tests/unit/test_quarantine_publisher.py tests/unit/test_event_identity.py tests/unit/test_event_time_policy.py tests/unit/test_silver_sink.py tests/unit/test_delta_silver_adapter.py tests/unit/test_validated_publisher.py tests/unit/test_sensor_validation_job.py
```

결과: adapter/Job 113 passed, 전체 Phase 2 431 passed. 각 실행 failed 0,
warning 1(AuthlibDeprecationWarning: httpx 모듈 deprecation). 테스트 삭제·감소 없음.
Python syntax 검사와 import를 포함한 테스트 수집, git diff --check 통과.
이 결과는 실제 Delta MERGE, timestamp 왕복, restart 또는 Kafka E2E 증명이 아니다.
