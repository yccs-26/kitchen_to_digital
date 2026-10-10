> 최신 판단은 아래 **2026-10-09 야간 읽기 전용 discovery 및 E2E readiness**를 따른다. 기존 storage 성공·실패 이력은 보존했다.

# Phase 2 검증 초안 — P2-8A

**Silver 생성 및 storage runtime smoke PASS. P2-8A 전체는 미완료.**

2026-10-10 추가 실측: 현재 Kafka retention / Bronze v3 schema ID inventory와
local Registry writer-schema mapping 완료. 상세 결과는 문서 마지막 절 참조.
최종 Phase 2 수용 보고서가 아니다. 이번 storage 범위의 duplicate/conflict만 검증했으며,
Kafka를 포함한 P2-8B/C 및 restart 검증은 실행하지 않았다.
크레딧 충전 직후에는 인증 오류가 동일했으나, 이후 사용자 재인증으로 API 접속이 복구됐다.

## 실제 UC Managed Delta 생성 및 storage runtime smoke — PASS

사용자가 생성 및 단일 이벤트 검증을 승인한 후 실행했다. 작업 시작 branch는
`feat/phase2-validation-silver`, HEAD는 `dc99afa2b5f1b75dd490e7c7f186fcd4498a92b7`,
작업 트리는 clean이었다. production adapter 코드를 수정하지 않았다.

최초 조회에서 schema/table 모두 없었고 `CREATE SCHEMA ktd.silver`를 실행했다.
첫 시도는 CREATE TABLE 단계에서 `[NO_ACTIVE_SESSION] No active Spark session
found. Please create a new Spark session before running the code.`로 실패했다.
adapter 단계에는 도달하지 않았으며, 관리 API에서 table 부재를 다시 확인했다.
이후 compute가 30분 비활성으로 자동 종료된 상태를 확인했다. 사용자 재개 요청에
따라 기존 compute를 시작하고 RUNNING 확인 후 새 세션으로 재실행했다.
이는 세션/compute 문제였으며 adapter workaround나 설정 변경은 없었다.

재시도에서는 기존 schema를 재사용했고 table 부재를 재확인한 뒤 아래 역사 섹션의
승인된 USING DELTA DDL을 실행했다. LOCATION, DROP, REPLACE, 데이터 삭제는 없다.

| 항목 | 실제 결과 |
|---|---|
| 실행 시각 | 2026-10-08 11:51:49 ~ 11:53:05 UTC |
| Run ID | `5fc182dce0b84c3188750c98f711a51f` |
| Runtime | `17.3.x-photon-scala2.13`, Spark `4.0.0` |
| 대상 | `ktd.silver.sensor_validated` |
| DESCRIBE TABLE EXTENDED | Type=MANAGED, Provider=delta |
| schema | canonical 10개 필드, 모두 nullable=false |
| 타입 | event_time=timestamp(LTZ), metric_value=double, 나머지 8개=string |
| event_id | `p2-8a-storage-5fc182dce0b84c3188750c98f711a51f` |
| 최초 lookup | None |
| 최초 SilverSink.write | INSERTED — 실제 DeltaSilverStorage.insert_if_absent MERGE 및 사후 lookup 성공 |
| timestamp 왕복 | `2026-10-08T11:52:35.123456+00:00` 입력/조회 일치 |
| metric_value 왕복 | 4.25 입력/조회 일치 |
| 동일 이벤트 재처리 | DUPLICATE_NOOP |
| 동일 ID / metric_value=9.5 | CONFLICT, differing_fields=[metric_value] |
| canonical 보존 | 최초 10필드 전체 동일, metric_value=4.25 유지 |
| 최종 행 수 | 해당 event_id 조건으로 count(*)=1 |

실제 SparkSession을 DeltaSilverStorage에 주입하고 SilverSink.write를 호출했다.
duplicate/conflict는 SilverSink의 영속 lookup/비교 경계 결과이며 두 번째 MERGE를
강제로 실행한 실험은 아니다. 단일 writer·동일 실행 세션의 storage smoke이며
프로세스 재시작, 불명확한 commit 결과, 동시 writer 안전성은 검증하지 않았다.

증빙:

- `docs/evidence/phase-2/p2-8a/delta-storage-smoke-07132640d2094e75819f06a5878c25d9.json`: 최초 실패 SQL, 단계, 정확한 세션 오류.
- `docs/evidence/phase-2/p2-8a/delta-storage-smoke-5fc182dce0b84c3188750c98f711a51f.json`: 성공 run, DDL/MERGE SQL, metadata, 입력/조회 payload, 각 결과.

애플리케이션 코드 수정이 없어 unit regression을 다시 실행하지 않았다.
이전 431 passed를 이번 runtime 결과로 대체하지 않는다. Kafka raw publish,
Job 2 전체 실행, checkpoint 변경, Git add/commit/push는 수행하지 않았다.
테스트 canonical 1행은 증빙으로 남겼으며 삭제하지 않았다.

아래 섹션은 생성 전 의사결정과 검증 이력이다. 당시의 미생성·미검증 표현은
위 실제 storage 결과로 갱신되며, Kafka E2E와 P2-8A 전체 완료를 뜻하지 않는다.

## B 채택과 로컬 adapter 정리 (생성 전 이력)

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

## 2026-10-09 야간 읽기 전용 discovery 및 E2E readiness

**결론: P2-8 acceptance 미완료. storage smoke는 PASS이나 actual Kafka → Job 2 →
Silver → validated E2E는 미검증이다.** 신규 runtime 설정의 Spark SASL 옵션 충돌을
실제 DBR에서 확인했다. 코드/config는 수정하지 않았다. 아래 결과가 이 문서의
과거 미생성·431개 회귀·설정 부재 표현보다 최신이며, 과거 기록은 그대로 보존했다.

관측은 2026-10-09 KST(2026-10-08 17:15~17:21 UTC)에 수행했다.
관리 API 조회, 기존 RUNNING compute에서 batch read와 DESCRIBE만 수행했다.
Job 2 streaming query, publish, schema 등록, IAM/secret/topic/table 변경,
checkpoint 생성·삭제·변경, compute 시작·중지 및 Git publication은 하지 않았다.

### 1. 시작 Git 상태와 근거

- branch: `feat/phase2-validation-silver`
- HEAD: `69caada6c3ca0a6545eff35f9836a8d3d68846b5`
- 시작 시 modified: `docs/reports/phase-2-verification.md`
- 시작 시 untracked: `docs/evidence/phase-2/p2-8a/`의
  `delta-storage-smoke-07132640d2094e75819f06a5878c25d9.json`,
  `delta-storage-smoke-5fc182dce0b84c3188750c98f711a51f.json`,
  `kafka-read-preflight.json`.
- 위 사용자 변경과 증빙을 보존했다. 이번 추가 범위는 이 보고서와
  `docs/evidence/phase-2/p2-8a/read-only-discovery-2026-10-09.json`이다.
- 기존 507개 회귀 기록은 `docs/implementation/p2-8a-runtime-config.md`에 있다.
  이번에도 동일한 Phase 2 11개 unit 파일을 실행해 **507 passed, 1 warning,
  failed 0 (0.66s)**를 재확인했다. warning은 Authlib httpx deprecation이다.

### 2. 실제 MSK와 인증 구조

| 항목 | 이번 직접 관측 |
|---|---|
| cluster | `ktd-msk-serverless`, ACTIVE, SERVERLESS |
| 식별자 | `arn:aws:kafka:ap-northeast-2:048715026902:cluster/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2` |
| broker | `boot-aersevd2.c2.kafka-serverless.ap-northeast-2.amazonaws.com:9098` |
| 인증 | Serverless ClientAuthentication.Sasl.Iam.Enabled=true; IAM bootstrap 반환 |
| Spark consumer | UC SERVICE credential `ktd-msk-consumer` → IAM role `ktd-databricks-msk-consumer-role` |
| consumer IAM | cluster Connect, raw DescribeTopic/ReadData, group DescribeGroup/AlterGroup; WriteData 없음 |
| Python producer 방식 | 코드상 SASL_SSL + OAUTHBEARER + AWS IAM signer; 실제 Job 2 callback 미검증 |
| 기존 EC2 client | `i-08e374efb0c31160d`, stopped; instance profile `ktd-msk-producer-role` |
| Databricks | `ktd-phase1-dev` / `0929-113111-cgu7k91c`, RUNNING; config DBR 17.3, PHOTON, 실제 Spark 4.0.0 |

MSK의 VPC 설정을 관리 API로 확인했다. private 연결 구조는 기존 Phase 1 문서와
일치하지만 모든 SG/route를 새로 재감사한 것은 아니다. raw batch read 성공은
이번 compute에서 broker에 도달하고 해당 credential로 읽을 수 있음을 증명한다.
IAM 정책 열람은 producer callback 성공이나 end-to-end 쓰기 권한 증명이 아니다.
MSK cluster policy 조회는 NotFound였으며 이를 topic 부재로 해석하지 않는다.

### 3. topic 상태

다음은 **기존 Job 1 방식(service credential, 명시적 SASL 옵션 없음)**의 최신 결과다.
모든 probe에서 `kafka.allow.auto.create.topics=false`를 사용했다.

| topic | 존재 | partition 수 | 실제 read | write 확인 가능성/현재 근거 |
|---|---|---|---|---|
| kitchen.sensor.raw | 확인 | Phase 1 문서 3; 이번 총수 재확인 불가 | PASS, partition=1 / offset=0 표본 | EC2 producer 정책에 raw WriteData 있음. 이번 publish 미실행 |
| kitchen.sensor.validated | 미확정 | 미확정 | TopicAuthorizationException | 조사한 두 IAM 역할 모두 대상 WriteData 없음; publish 미실행 |
| kitchen.sensor.quarantine | 미확정 | 미확정 | TopicAuthorizationException | 조사한 두 IAM 역할 모두 대상 WriteData 없음; publish 미실행 |

세 topic의 MSK `describe-topic`도 시도했으나 모두
`ServerlessUnsupportedException: Topic APIs are not supported on serverless clusters.`였다.
따라서 관리 API로 존재/partition 수를 보완하지 못했다. 권한 오류를 topic 부재로
간주하지 않는다. 별도 승인된 Describe 권한의 broker metadata 조회가 다음 확인 경로다.

### 4. Registry/provider/auth와 schema 호환성

- 현재 AWS 계정의 `ap-northeast-2`에서 `glue list-registries` 및 `glue list-schemas`
  결과는 각각 빈 목록이다. 타 계정·타 리전·외부 provider의 부재까지 증명하지 않는다.
- Confluent는 `infra/docker/compose.yml`과 `.env.example`의 로컬 설정만 확인했다.
  cloud endpoint/auth는 저장소에서 발견하지 못했다. 다른 provider도 미확정이다.
- Databricks `secrets list-scopes` 결과는 빈 목록이다. 예제의
  `ktd-job2/schema-registry-basic-auth` 참조는 현재 조회 범위에서 사용할 수 없다.
  secret 값을 조회하거나 생성하지 않았다.
- 현재 물리 record는 `ktd.sensor.SensorMetricEvent`이며
  `schemas/avro/sensor_metric_event.avsc`의 10필드 계약이다. payload의 `1.0.0`은
  논리 버전이며 Registry schema ID/subject version이 아니다.
- `avro_decoder.py`는 magic byte 0 + 4-byte big-endian schema ID + Avro 및
  `get_schema(id)`를 요구한다. `runtime_config.py`는 provider `confluent`만 허용한다.
  Glue/기타 provider의 endpoint만 대체하여 호환된다고 할 수 없다.
- 실제 cloud SensorMetricEvent 존재, schema ID/version, raw frame ID의 해당
  Registry 매핑 및 lookup은 모두 미검증이다. Phase 1 fixture exporter는 로컬
  serializer를 사용하므로 cloud Registry에서 과거 raw ID가 같은 schema를
  가리키는지 반드시 확인해야 한다.
- validated serializer는 `auto.register.schemas=False`, `use.latest.version=False`,
  topic subject naming이다. raw decode lookup뿐 아니라
  `kitchen.sensor.validated-value`의 동일 schema lookup도 필요하다.
  Quarantine은 JSON이므로 별도 Avro subject 등록을 가정하지 않는다.

### 5. producer credential discovery

현재 조회된 UC SERVICE credential은 `ktd-msk-consumer` 하나다. 나머지 조회 항목은
STORAGE credential이며 producer용으로 간주하지 않는다.

기존 `ktd-msk-producer-role`은 trust principal이 `ec2.amazonaws.com`이고,
Kafka inline policy는 raw에 대한 CreateTopic/DescribeTopic/WriteData와 cluster
Connect다. 연결된 managed policy는 AmazonSSMManagedInstanceCore다.
따라서 기존 EC2 역할을 Databricks Job 2가 사용할 수 있다고 추정하지 않으며,
validated/quarantine 발행 권한도 발견하지 못했다. consumer 역할에도 WriteData가 없다.

Job 2용 UC credential reference, 그 IAM principal과 trust, 출력 두 topic에 대한
DescribeTopic/WriteData 및 cluster Connect 확인이 필요하다. 실제 유효 권한에는
조직 정책 등 다른 제한이 있을 수 있으므로 이번 role policy 조회만으로 발행 성공을
보장하지 않는다. credential·trust·권한 변경은 이번에 수행하지 않았다.

### 6. runtime config와 실제 환경 대조

| 설정 | 예제/구현과 실제 대조 | 판정 |
|---|---|---|
| bootstrap_servers | 예제는 빈 값, 실제 IAM broker는 위에서 확보 | 채울 값 확인; 파일 미수정 |
| security_protocol / mechanism | SASL_SSL / AWS_MSK_IAM은 producer 경로와 부합. Spark source에는 아래 충돌 | **실제 runtime mismatch** |
| source_service_credential | ktd-msk-consumer 존재, raw read PASS | 확인 |
| producer_service_credential | 예제 빈 값, 적합한 UC SERVICE credential 미발견 | blocker |
| registry provider / URL | 예제 빈 값; cloud 미확정; 구현은 Confluent만 허용 | blocker |
| registry auth / secret scope/key | 예제 Basic + ktd-job2/schema-registry-basic-auth; scopes 빈 목록 | blocker |
| raw topic | kitchen.sensor.raw 실제 read PASS | 확인 |
| validated / quarantine | 이름은 코드와 일치; 실재/partition 미확정, read 권한 실패 | blocker |
| Silver | ktd.silver.sensor_validated; UC MANAGED/DELTA, Spark MANAGED/delta, 10필드 NOT NULL | 확인 |
| checkpoint | 예제 빈 값; /Volumes/ktd/bronze/checkpoints 아래 job1, job1-tests, job1-failure-test만 조회 | Job 2 전용 경로 결정 필요 |
| processing_version | 예제 빈 값; 운영 처리 버전 미정 | 결정 필요 |
| rules_path | JSON 기준 상대 validation_rules.json → config/validation_rules.json | 로컬 일치; 원격 배포 미검증 |
| rules 범위 | store-001의 수치 장비 4개, logical schema 1.0.0 | simulator 한정; 운영 전체 목록 아님 |

**재현된 Spark 옵션 충돌:** `streaming/jobs/runtime_config.py:85`의
`KafkaSettings.spark_options()`는 기존 `databricks.serviceCredential`에
`kafka.security.protocol=SASL_SSL`을 추가한다. 동일 옵션의 batch read가 raw에서
다음 오류로 실패했다(원문):

```text
(org.apache.spark.sql.kafka010.KafkaIllegalStateException) SASL parameters kafka.security.protocol are not allowed in source options when using a service credential. Please remove either the SASL configs or remove databricks.serviceCredential config.
```

명시적 protocol을 넣지 않은 기존 Job 1 방식의 별도 진단에서는 raw read가 성공했다.
이는 source 옵션 문제의 증거이며 Job 2 전체 실행 결과가 아니다. 수정 후보는
MSK IAM source에서 credential이 인증 옵션을 맡도록 하고 Python producer의
SASL_SSL/OAUTHBEARER 설정은 유지하는 것이다. **이번에는 수정하지 않았다.**

추가 환경 제약: `.venv-databricks`에서 Job 2/runtime_config import는 성공했지만
`aws_msk_iam_sasl_signer`와 `botocore` import는 ModuleNotFoundError다.
Connect 17.3.14, SDK 0.143.0, confluent-kafka 2.15.1은 확인했다.
원격 foreachBatch 환경의 패키지·DBUtils·credential API/import는 별도 미검증이다.
프로필만 사용한 Connect 시작은 cluster ID 누락으로 실패해, 이미 RUNNING인
기존 compute ID를 진단 스크립트에 명시했다. 저장소/사용자 프로필은 변경하지 않았다.

### 7. repo consistency audit — 자동 수정 없음

| 대상 | 관측/수정 후보 |
|---|---|
| old adapter | 감사 시작 tracked 파일에서 IcebergSilverStorage, iceberg_silver.py, old import 참조 0건. 현재 DeltaSilverStorage 사용 |
| docs/interview/delivery-and-idempotency.md:11 | "Spark dedup과 Iceberg 멱등 쓰기" 제목을 현 Silver Delta와 구분할 후보 |
| docs/architecture/platform.md:3,24,34 | Job 2 미구현 / Silver 생성·쓰기 미완료 표현은 현 코드·storage smoke보다 오래됨 |
| docs/architecture/diagrams.md:3, docs/architecture/design-decisions.md:18 | Job 2 전체를 후속 미구현/설계로 묶는 표현을 구현과 runtime 미검증으로 분리할 후보 |
| docs/data/storage-design.md:3,18 | Silver 설계 단계 / 실제 생성 전 표현 갱신 후보. 형식 자체는 Delta로 올바름 |
| docs/streaming/delivery-and-idempotency.md:3,21,23 | Silver 전략 보류·MERGE 미확정 표현은 현재 adapter/결정과 불일치 |
| docs/architecture/kafka-topics.md:15 | 오류 topic key 미정 표현은 현재 quarantine_id key 구현과 불일치. raw/validated는 UTF-8 equipment_id, Silver identity는 event_id |
| topic 명칭 | streaming/config/simulator의 raw/validated/quarantine 기본명 일치. 문서의 6/6/3은 목표값이며 실제 partition 확인값이 아님 |
| checkpoint | Job 2 경로에 job2 구성요소 요구, 선택적 bronze_checkpoint 동등 비교. 예제는 bronze_checkpoint 없음; 전체 query 경로 중복을 탐지하지 않으므로 배포 시 실경로 대조 필요 |
| secret 하드코딩 | tracked 1MB 이하 텍스트에 AWS key/private key/Databricks token/민감 literal 할당 패턴 탐지 0건. 완전한 secret 부재 보장은 아님. Docker .env는 key 이름만 확인, 값 비출력 |
| JSON/YAML 및 rules | config JSON 2개 파싱 성공. 실행 경로의 YAML 혼용 없음. 과거 보고서/로컬 계획의 YAML 후보명을 현 배포 경로로 복사하지 말 것 |
| equipment_id 정책 | raw/validated 코드와 계약은 일치. rules 4개 장비만으로 매장 간 전역 유일성은 증명 불가 |
| 구현과 설계 범위 | delivery 문서의 watermark bounded dedup과 현 영속 lookup/시간 분류를 구분해야 함. phase-2 단위 테스트를 Spark watermark state runtime 증거로 표현하지 말 것 |

Bronze의 Iceberg 참조와 역사적 미실행 Iceberg 제안은 잘못된 현 Silver 설명과
구별했다. 역사 기록이나 Bronze 문서를 일괄 치환하지 않았다.

### 8. verification report 반영과 검증 범위

이번에 507개 회귀를 새로 확인했고 실제 storage 증빙 JSON도 읽어 대조했다.
2026-10-08 storage run `5fc182dce0b84c3188750c98f711a51f`의
INSERTED / DUPLICATE_NOOP / CONFLICT, differing_fields=[metric_value],
canonical 전체 보존, 해당 ID 1행, UTC microsecond timestamp round-trip은
기존 runtime 증빙에 의해 확인된다. 오늘은 관리 metadata와 Spark DESCRIBE를
재조회했으며 MERGE나 INSERT를 재실행하지 않았다.

actual Kafka E2E, cloud Registry/provider/auth, producer credential/ACK callback,
Job 2 restart와 failure injection은 미검증이다. unit 507개 통과가 source 옵션의
실제 DBR 거부를 포착하지 못했으므로 fake/unit과 cloud 증거를 분리한다.

### 9. evidence 현황과 screenshot checklist

시작 시 `docs/evidence/phase-2/p2-8a/`에는 JSON 6개가 있었고 p2-8b/c 증빙은 없었다.
기존 6개는 최초 preflight, 재인증 후 preflight, 미발행 fixture,
storage 최초 실패/성공, Kafka read preflight다. 이번 sanitized discovery JSON을
추가했다. 새 screenshot은 생성하지 않았으며 촬영할 화면은 다음과 같다.

| 단계 | 확보된 증빙 | 부족한 증빙 / 다음 촬영 항목 |
|---|---|---|
| p2-8a storage smoke | 성공/실패 JSON, canonical/왕복/상태/1행 | 성공 run의 table provider·10필드·INSERTED/DUPLICATE_NOOP/CONFLICT와 canonical 1행 화면. JSON 성공은 이미 확보되어 재생성 불필요 |
| p2-8a Kafka happy path | raw read, 출력 topic 권한 오류, config 충돌 | Registry ID→schema 및 validated subject lookup, topic metadata, 발행 ACK partition/offset, Job 2 batch/query progress, Silver event_id, validated key/event_id, Quarantine 해당 이벤트 부재 관측 범위 |
| p2-8b invalid/corrupt/duplicate/conflict/late/future | unit 및 storage-only duplicate/conflict | 실제 입력별 기대/실측 표, Quarantine reason·lineage·differing_fields·ACK, canonical 보존, late/future 계수와 출력, 동일 ID 반복에 대한 Silver 1행 |
| p2-8c restart/partial failure | unit 계약만 존재 | 동일 checkpoint 재시작 전후 offset/progress, Silver 성공→publish 실패, ACK 후 checkpoint 실패, Quarantine ACK 실패의 재처리/중복 식별 증빙 |

스크린샷에는 secret/token을 포함하지 않는다. 각 화면에 실행 시각, run/query/event ID,
source topic/partition/offset을 남겨 서로 대조할 수 있게 한다. Job 1 restart 성공
증빙을 Job 2 복구 증거로 재사용하지 않는다.

### 10. 최종 E2E readiness matrix

| 항목 | 상태 | 근거 | blocker | 다음 액션 |
|---|---|---|---|---|
| Silver table | 확인 | UC metadata + Spark DESCRIBE MANAGED/delta | 없음(storage 범위) | 기존 table 보존 |
| Delta MERGE | storage PASS | 기존 실제 INSERTED, duplicate/conflict, canonical/왕복 | restart/동시성 증거 없음 | p2-8c에서 복구 검증 |
| runtime config | 코드/unit 확인, runtime 실패 | 507 unit + 실제 SASL 옵션 거부 | source credential/protocol 충돌, 빈 필수값 | 별도 구현 작업에서 수정·재검증 |
| Kafka raw read | PASS(Job 1 방식) | 이번 partition 1/offset 0 read | 새 config 경로는 충돌 | 옵션 수정 후 같은 probe |
| Kafka validated write | 미검증/차단 | 역할 정책에 대상 WriteData 없음 | UC producer 및 topic metadata 미확정 | 자격/권한/실재 확인 후 승인된 ACK smoke |
| Kafka quarantine write | 미검증/차단 | 위와 같음, read도 권한 실패 | 위와 같음 | 위와 같음 |
| Registry lookup | 미검증/차단 | scoped Glue 목록 빈 값, cloud endpoint 없음 | provider/endpoint/auth | 실제 서비스 결정·확인 |
| schema compatibility | 로컬 계약만 확인 | Confluent framing + SensorMetricEvent | raw ID 매핑 및 validated subject | 실제 Registry에서 조회 |
| checkpoint | 미설정 | volume에 Job 1 세 경로만 확인 | Job 2 전용 경로/처리버전 결정 | 다른 query와 분리해 설정; 실행 시 쓰기는 별도 승인 |
| Job 2 import/runtime | 로컬 import PASS, 원격 미검증 | Connect 환경 import | 로컬 signer/botocore 없음, 원격 callback 환경 불명 | 승인된 환경에 의존성·모듈 배포 확인 |
| producer callback | unit만 PASS | fake IAM 갱신/expiry 및 ACK 테스트 | 실제 UC credential·signer·write 권한 | 실제 실행 위치에서 승인된 callback/ACK 검증 |
| actual E2E | 미실행 | Job 2 query/publish 없음 | config + Registry + producer/topic 준비 | p2-8a 정상 1건부터 |
| restart recovery | 미실행 | storage 동일 세션/단위 테스트만 | E2E 진입 전제 미충족 | p2-8c 동일 checkpoint 복구 실험 |

### 11. actual E2E의 blocker 우선순위

1. 실제 재현된 source SASL 옵션 충돌: 현재 runtime config 경로 그대로는 raw read 시작 불가.
2. cloud Registry/provider/endpoint/auth 및 raw schema ID/validated subject 불명.
3. Job 2 UC producer credential 미발견, 기존 역할의 validated/quarantine 권한 없음.
   두 topic의 실재/partition 수도 아직 미확정.
4. Connect 환경 signer/botocore 누락과 원격 callback/import/DBUtils 미검증.
5. Job 2 checkpoint, processing_version, 배포 config/규칙 경로 미확정.

### 12. 아침에 결정할 다음 3개 액션

1. **source 옵션 충돌 수정만 다음 작은 구현 단위로 지정한다.**
   Spark service credential 경로와 Python producer 설정을 분리하고 실제 raw read까지 확인한다.
2. **cloud Registry와 Job 2 producer의 준비 경로를 결정한다.**
   Registry provider/endpoint/auth와 raw/validated schema 매핑을 확인하고,
   별도 UC producer principal/출력 topic 권한·metadata 확인 범위를 정한다.
   리소스 생성/권한 변경은 별도 명시적 승인 후 진행한다.
3. **준비 조건이 닫히면 p2-8a 정상 1건 E2E만 승인한다.**
   runtime 의존성, 전용 checkpoint·processing_version·rules 배포를 먼저 확인하고
   raw→Silver→validated ACK와 Quarantine 관측까지 증빙한다. p2-8b/c는 후속 단위다.

이번 작업은 여기서 멈춘다. Phase 2 완료·병합 또는 다음 Phase 진입으로 해석하지 않는다.

### 재현 명령과 이번 검증

```bash
.venv/bin/python -m pytest -q tests/unit/test_validation_input.py tests/unit/test_validation_domain.py tests/unit/test_quarantine_record.py tests/unit/test_quarantine_publisher.py tests/unit/test_event_identity.py tests/unit/test_event_time_policy.py tests/unit/test_silver_sink.py tests/unit/test_delta_silver_adapter.py tests/unit/test_validated_publisher.py tests/unit/test_sensor_validation_job.py tests/unit/test_runtime_config.py
```

읽기 전용 discovery 명령은 AWS `kafka list-clusters-v2/get-bootstrap-brokers/get-cluster-policy/describe-topic`,
`glue list-registries/list-schemas`, `sts get-caller-identity`, `ec2 describe-instances`,
IAM `get-role/list-role-policies/get-role-policy/list-attached-role-policies/get-policy/get-policy-version`와
Databricks `clusters list`, `credentials list-credentials`, `secrets list-scopes`,
`tables get`, `fs ls`다. 실행 인자와 batch read 방식은 신규 JSON에 기록했다.
초기 sandbox 네트워크/auth-cache 접근 실패 후 허용된 읽기 전용 접근으로 재시도했다.
인증값·토큰은 출력하지 않았다.


### 사용자 검토 후 commit 안내 — 이번에는 실행하지 않음

이 보고서에는 시작 전 사용자 변경도 포함되어 있다. 기존 untracked 증빙 3개와
이번 discovery JSON을 함께 검토한 후 아래 명시된 파일만 포함할 수 있다.
권장 메시지: `docs(phase2): record read-only E2E readiness audit`.

```bash
git add -- docs/reports/phase-2-verification.md docs/evidence/phase-2/p2-8a/delta-storage-smoke-07132640d2094e75819f06a5878c25d9.json docs/evidence/phase-2/p2-8a/delta-storage-smoke-5fc182dce0b84c3188750c98f711a51f.json docs/evidence/phase-2/p2-8a/kafka-read-preflight.json docs/evidence/phase-2/p2-8a/read-only-discovery-2026-10-09.json
git diff --cached --stat
git commit -m "docs(phase2): record read-only E2E readiness audit"
```

최종 `git diff`, `git diff --check`, `git status` 및 신규 JSON 내용/파싱을 확인했다.
기존 보고서 본문이 그대로 포함되는지 검사했고 시작 전 증빙 3개의 SHA-256도
일치했다. 애플리케이션 코드·config·Git index는 수정하지 않았다.


## 2026-10-10 schema ID inventory 시도 — INCOMPLETE / BLOCKED

증빙: `docs/evidence/phase-2/p2-8a/schema-id-inventory.json`.
HEAD `55e0e50c19bf90fc777d4c5ebf15fbb1f79d3603`에서 읽기 전용으로 조사했다.
2026-10-09 16:36 UTC 조회 시 `ktd-phase1-dev`는 TERMINATED였으며,
로컬 Registry `http://localhost:8081`의 GET은 Connection refused였다.
compute/컨테이너를 시작하거나 secret을 조회하지 않았다.

**현재 MSK raw/Bronze 전수 inventory는 실행되지 않았다.** observed ID 목록,
건수, partition/offset 범위, framing 오류 건수는 UNKNOWN(null)이다.
null을 메시지 0건 또는 ID 부재로 해석하면 안 된다. 과거 raw read 성공은
현재 전체 retention 범위 및 ID 목록 증거를 대체하지 않는다.

| schema_id | observed_in_raw (현재) | writer schema | subject | version | confidence | evidence |
|---|---|---|---|---|---|---|
| 1 | UNKNOWN | 현재 raw 매핑 UNKNOWN; 역사상 SensorMetricEvent 10필드 v1 | 역사상 kitchen.sensor.raw-value | 역사상 1 | HISTORICAL_ONLY | Phase 0 보고서 238~245, 294~296행; 기본 Avro artifact; 로컬 fixture header ID=1 |
| 2 | UNKNOWN | 현재 raw 매핑 UNKNOWN; 역사상 v1 + nullable firmware_version/default null | 역사상 kitchen.sensor.raw-value | 역사상 2 | HISTORICAL_ONLY | Phase 0 보고서; v2 compatibility artifact와 compatibility test 기록 |
| 그 외 | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN | 전수 scan 미실행 |

`tmp/phase1/avro-fixture.json`의 단일 레코드는 길이 161 bytes, magic=0,
schema ID=1, key=fridge-001이다. 파일을 읽어 header만 검사했으며 payload는
새 증빙에 저장하지 않았다. 이 fixture를 현재 MSK 표본으로 표시하지 않았다.
기본 schema와 v2 artifact의 마지막 변경 commit은 모두 `d66a5cfd...`이며,
파일 SHA-256과 전체 schema 정의(이벤트 payload 아님)를 manifest에 보존했다.

Phase 1 보고서에는 partition 1의 offset 0/1 정상 Avro와 offset 2의
길이 2 corrupt 레코드가 Bronze에 원본 보존됐다는 역사 증거가 있다.
현재 retention에서 해당 레코드가 남아 있는지, ID=2/다른 ID가 Bronze에 있는지는
미검증이다. Bronze 계약은 value BINARY 및 key/headers/lineage를 보존하며
별도 schema_id 컬럼은 없다. schema ID는 value header에서 재추출해야 한다.
로컬 Registry의 ID=2 등록 사실과 실제 MSK 발행 여부를 구분한다.

validated는 `schemas/avro/sensor_metric_event.avsc`를 사용하므로 raw v1과
같은 10필드 schema이며 subject만 `kitchen.sensor.validated-value`로 다르다.
raw v2의 11필드와는 다르다. 내부 datetime은 UTC 문자열로 직렬화하며
canonical projection이 firmware_version을 제외한다. raw/validated bytes가
항상 같다고 주장하지 않는다.

manifest는 raw v1/ID=1, raw v2/ID=2, validated v1 순서의 **후보 초안**이다.
source GET export와 현재 raw/Bronze inventory를 대조하기 전에는 실행하지 않는다.
ID=2를 Kafka에서 보지 못했다고 historical schema를 삭제/누락시키지 않는다.
목적지 default context에서 ID 충돌과 subject/삭제 이력을 확인하고 충돌 시 중단한다.
현재 decoder는 get_schema(id)를 사용하므로 custom context 이관을 조용히 적용하지 않는다.

Confluent Cloud 공식 [subject mode API](https://docs.confluent.io/cloud/current/ccloud/update-mode/)는
IMPORT를 지원하고, [schema registration API](https://docs.confluent.io/cloud/current/ccloud/register/)는
명시적 id/version/schema 필드를 제공한다.
[공식 migration 절차](https://docs.confluent.io/platform/current/schema-registry/installation/migrate.html)에
따라 비어 있거나 없는 subject의 IMPORT → 원본 ID/version 등록 → READWRITE 복귀를
후속 승인 작업으로 제안한다. Cloud에 서버 properties 파일을 수정한다고 가정하지 않는다.
목적지에서 실제 실행·권한·충돌 검증은 아직 안 했다. schema 삭제, 강제 덮어쓰기,
USM 배포나 exporter 생성은 이번 최소 경로에 포함하지 않는다.

다음 단계: 사용자가 기존 compute와 local Registry를 volume 초기화 없이 실행한 뒤,
동일한 읽기 전용 작업으로 raw 모든 partition의 earliest~latest 집계,
Bronze header 집계, source Registry schema/version GET을 재실행한다.
framing은 null/5-byte 미만/nonzero magic/유효 header로 나누고 schema ID는 유효
header에서만 추출한다. partition별 최초/최종 offset은 시간 순서가 아닌 offset 경계다.
전체 payload와 민감 key는 저장하지 않는다.

이번 변경은 증빙 JSON 신규 생성과 이 절 추가뿐이다. runtime/unit 테스트는
재실행하지 않았으며 과거 512 passed를 이번 실측 결과로 표현하지 않는다.

## 2026-10-10 schema ID inventory 실측 완료 — 현재 Kafka / Bronze v3

위 INCOMPLETE/BLOCKED 절은 이전 시도 기록이다. 이번 실측으로 현재 retention 및
Bronze version 3의 schema ID inventory와 source writer-schema mapping을 완료했다.
P2-8A 전체 완료나 Confluent Cloud 이관 완료를 뜻하지 않는다.
증빙: `docs/evidence/phase-2/p2-8a/schema-id-inventory.json`.

### 실행 환경과 scan 범위

`ktd-phase1-dev`는 RUNNING(DBR 17.3.x-scala2.13, Spark 4.0.0)이었다.
`ktd-kafka`와 `ktd-schema-registry`도 이미 실행 중이어서 시작·재시작·재생성하지
않았다. 기존 volume과 Registry 데이터를 그대로 사용했다.

Kafka는 2026-10-10 02:33:13~02:33:14 UTC(11:33 KST)에 MSK broker metadata로
전체 partition을 열거하고, 시작/끝 offset을 고정해 assign/seek/poll로 읽었다.
consumer group 없이 `enable_auto_commit=false`, `allow_auto_create_topics=false`,
`auto_offset_reset=none`을 사용했다. 여기서 seek는 이 임시 consumer의 읽기 위치이며
저장된 group offset이나 Spark checkpoint를 변경하지 않는다.

| partition | 시작 offset (포함) | 끝 offset (제외) | 실제 건수 | 최종 consumer position |
|---|---:|---:|---:|---:|
| 0 | 0 | 0 | 0 | 0 |
| 1 | 1 | 3 | 2 | 3 |
| 2 | 0 | 0 | 0 | 0 |

scan 후 broker 경계를 다시 조회해 시작/끝 모두 동일함을 확인했다.
앞서 02:30:35~02:31:04 UTC에 실행한 Spark batch `subscribe=kitchen.sensor.raw`,
`startingOffsets=earliest`, `endingOffsets=latest`, `failOnDataLoss=true`의 집계와도
일치했다. batch에는 LIMIT/streaming query/checkpoint를 사용하지 않았다.
기존 옵션의 `maxOffsetsPerTrigger`는 streaming 옵션이며 이번 batch 범위 제한으로
쓰지 않았다. 별도 bounded consumer 결과로 실제 전체 2건을 확인했다.

Bronze는 `DESCRIBE HISTORY ktd.bronze.sensor_raw LIMIT 1`로 version 3을 확인한 뒤
`versionAsOf=3`의 **전체 행**을 날짜 필터나 LIMIT 없이 집계했다. scan 시간은
02:31:04~02:31:30 UTC 구간이며 `value`의 실제 타입은 BINARY였다.
이전 Bronze version 전체 및 이미 만료된 Kafka 데이터는 이번 범위가 아니다.

### observed IDs, framing, Registry mapping

| 항목 | Kafka | Bronze v3 | partition/offset 예시 |
|---|---:|---:|---|
| schema ID 1 / valid header | 1 | 2 | Kafka p1/o1; Bronze p1/o0, p1/o1 |
| schema ID 2 / valid header | 0 | 0 | 관측 안 됨; Registry 등록과 발행은 구분 |
| payload < 5 bytes | 1 | 1 | 양쪽 모두 p1/o2 |
| 길이 ≥ 5, magic byte != 0 | 0 | 0 | 없음 |
| null value | 0 | 0 | 없음 |
| 총 레코드 | 2 | 3 | Kafka p1/o1~2; Bronze p1/o0~2 |

현재 양쪽 observed IDs는 `[1]`, corrupt framing은 각각 1건이다.
길이가 짧으면 ID를 추출하지 않고 TOO_SHORT로 분류하며, 길이 ≥ 5인 경우에만
magic을 검사한다. null은 별도 분류하고 corrupt 합계에 중복 산입하지 않는다.
길이 5인 header-only 레코드는 0건이었다.

**Bronze에만 존재하는 추가 schema ID는 없다.** 다만 p1/o0의 ID 1 레코드는
현재 Kafka 시작 offset 1보다 앞서며 Bronze에 남아 있다. Kafka의 정상 1건과
Bronze의 정상 2건은 기존 fixture payload와 SHA-256이 일치했다. fixture는 비교
근거일 뿐이며 현재 건수는 실제 Kafka/Bronze scan으로 얻었다. payload는 저장하지 않았다.

local Registry의 subjects/versions는 `deleted=true`를 포함해 조회했다.
`GET /schemas/ids/{id}`, `GET /schemas/ids/{id}/versions?deleted=true`,
`GET /subjects/kitchen.sensor.raw-value/versions/{version}?deleted=true`를 대조했다.

| schema ID | subject | version | 확정 writer schema | 근거 |
|---|---|---:|---|---|
| 1 | kitchen.sensor.raw-value | 1 | `schemas/avro/sensor_metric_event.avsc` — 10필드 v1 | ID/subject/version GET와 artifact JSON 정의 일치 |
| 2 | kitchen.sensor.raw-value | 2 | `schemas/avro/compatibility/sensor_metric_event_v2_compatible.avsc` — nullable firmware_version/default null 추가 | 동일한 삼중 조회와 artifact JSON 정의 일치 |

두 version은 삭제 상태가 아니었으며 schemaType=AVRO, references=[]였다.
source compatibility는 subject의 effective config와 global config 모두 BACKWARD다.
정의 비교는 JSON 객체의 키 순서·공백을 제외한 구조 비교이며, artifact 원본 파일과
Registry schema 문자열 각각의 SHA-256도 JSON에 기록했다.
현재 scan 범위의 **UNKNOWN ID는 `[]`**다. v1/v2 외 ID를 추정한 적은 없으며,
만료된 데이터·삭제된 Bronze 행·미조회 과거 snapshot의 ID 부재까지 주장하지 않는다.

### migration manifest 최종안과 남은 경계

JSON manifest 상태는 `FINAL_SOURCE_VERIFIED_DESTINATION_PREFLIGHT_PENDING`이다.
source schema 원문을 포함한 `proposed_import_request`를 기록했으며 실행하지 않았다.

1. raw subject에 v1을 **ID 1 / version 1**로 보존한다.
2. raw subject에 v2를 **ID 2 / version 2**로 보존한다. 이번 Kafka/Bronze에 없더라도
   source Registry 이력과 과거 replay 호환성을 위해 포함한다.
3. raw 이관 검증 후 별도 `kitchen.sensor.validated-value` subject에 v1 정의를
   등록하는 후속안을 유지한다. 반환 ID/version은 실제 응답으로 확정한다.

목적지는 현재 decoder의 ID lookup과 일치하는 default context를 사용한다.
[Cloud subject mode API](https://docs.confluent.io/cloud/current/ccloud/update-mode/)와
[registration API](https://docs.confluent.io/cloud/current/ccloud/register/),
[공식 ID/version 보존 이관 절차](https://docs.confluent.io/platform/current/schema-registry/installation/migrate.html)를
재확인했다. 후속 작업에서는 목적지 ID/subject/삭제 이력을 먼저 확인하고 충돌 시
중단한다. 승인된 이관은 IMPORT → 명시적 ID/version → READWRITE 복귀 및 BACKWARD
검증 순서이며, 강제 덮어쓰기·schema 삭제는 계획에 포함하지 않는다.

**Cloud 생성 전 source inventory/mapping blocker는 해소됐다.** 대상 Cloud
환경·region·endpoint·인증 준비 및 생성 승인은 별도 작업으로 남는다. 목적지가 생긴
후 ID 충돌·subject 상태·IMPORT 권한을 확인해야 하므로 manifest의
`execution_authorized`와 각 `eligible_for_execution`은 false다. 실제 Cloud import,
validated 등록, executor Registry lookup/decode, Job 2 Kafka E2E는 아직 미검증이다.

### 이번 실행과 변경 검증

실제 실행: `databricks clusters get ... --profile ktd`, `docker ps -a`, local Registry
GET export, Databricks Connect batch 집계 및 version 고정 Bronze 집계,
기존 compute 임시 Python context의 Kafka metadata/bounded read를 수행했다.
공유 compute의 JVM 직접 접근은 `JVM_ATTRIBUTE_NOT_SUPPORTED`여서 사용하지 못했고,
기존 로컬 kafka-python/MSK signer를 임시 ZIP으로 전달해 Python reader로 확인했다.
영구 cluster library 설치 없이 기존 UC service credential을 사용했으며 비밀값은
출력하거나 증빙에 저장하지 않았다.

이번 변경 대상은 inventory JSON과 이 보고서다. 기존 미커밋 보고서 본문은 보존하고
이 결과를 추가했다. JSON 파싱, 전체 건수/offset 경계/두 scan 집계 일치, Registry
정의 및 artifact SHA-256을 검사했다. 애플리케이션 코드는 바꾸지 않았고 unit/E2E
테스트 재실행 결과로 표현하지 않는다. 최종 `git diff`, `git diff --check`,
`git status`와 미추적 JSON 내용도 확인했다.

Confluent Cloud 생성, schema import/register, secret 생성/변경, Kafka publish,
checkpoint 변경, IAM 변경 및 Git staging/commit/push는 수행하지 않았다.
포트폴리오 화면은 이 절의 partition 경계·ID별 집계·Registry mapping 표를 함께
찍으면 실제 scan 범위와 v1/v2 대응을 보여준다. 인증 토큰이나 credential 출력은
촬영 대상에 포함하지 않는다.

검토 후 직접 commit할 경우, 기존 미커밋 보고서 변경도 함께 검토하고 실행한다.

```bash
git diff -- docs/reports/phase-2-verification.md
git add docs/evidence/phase-2/p2-8a/schema-id-inventory.json docs/reports/phase-2-verification.md
git diff --cached --stat
git commit -m "docs(phase2): complete live schema ID inventory"
```
