> 최신 로컬 검증은 문서 마지막 **2026-10-23 P2-8h — Job 2 로컬 신뢰성 검증**을 따른다. 기존 cloud/storage 이력은 보존하며, 로컬 통과를 Cloud E2E 통과로 해석하지 않는다.

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

## 2026-10-10 Cloud Registry 이관 준비 완료 — Cloud 미실행

이번 단위는 source export와 실행 절차 준비다. 기존 실제 inventory는 수정하지
않았고, Confluent Cloud 생성·요청·등록이나 secret 생성/변경은 수행하지 않았다.
P2-8A 전체 및 Cloud smoke 완료를 뜻하지 않는다.

실행 절차: `docs/implementation/p2-8a-registry-migration.md`.
manifest: `docs/evidence/phase-2/p2-8a/registry-migration/manifest.json`.
source export 조회 시각: 2026-10-10 02:41:36 UTC.

local Registry에서 ID 1/2의 GET schema, ID→subject/version,
subject/version→ID/schema 및 effective BACKWARD 설정을 다시 조회했다.
각 schema 정의는 기존 inventory와 저장소 Avro artifact에 일치했다.
source-export.json과 정확한 schema 문자열 `.avsc`, raw ID/version 보존 POST body,
validated canonical v1 등록 body를 준비했다. schema 원문 hash와 JSON 구조 hash를
구분하며, 후자는 field 순서/default를 포함한 내용 변경을 검출한다.

확정 이관 순서는 raw subject IMPORT → ID 1/version 1 → ID 2/version 2 →
READWRITE/BACKWARD 검증 → 별도 validated subject에 10-field v1 등록이다.
validated의 ID/version을 요청에 강제하지 않으며, 동일 default context에서 예상되는
ID 1/version 1을 실제 응답 및 GET으로 확인한다. 이번 actual destination 값은 null이다.

`scripts/registry_migration.py`는 offline/preflight/import-raw/verify-raw/
register-validated/verify-all을 제공한다. source bundle을 검증하고, 생성된 목적지의
subject/삭제 이력 또는 기존 ID 1/2가 있으면 같은 schema라도 자동 진행하지 않는다.
401/403/5xx/임의 404를 부재로 취급하지 않는다. 각 ID의 schema/subject/version/hash를
검사하며 실패 시 즉시 중단하고 force/delete/자동 mutation retry를 하지 않는다.
부분 이관 실패는 IMPORT 상태로 남을 수 있어 GET 대사 후 별도 재개하도록 명시했다.
이번에는 `offline`과 mocked API 테스트만 실행했다.

`scripts/registry_runtime_smoke.py`는 Cloud 이관 후 Databricks driver에서 호출하는
읽기 전용 함수다. get_schema(1/2), 기존 fixture ID 1 decode, 현재 Kafka ID 1 표본
및 corrupt 표본을 검사한다. secret은 기존 RegistrySettings/SecretReference로
실행 위치에서 해석하며 결과에 넣지 않는다. 현재 Kafka 표본이 만료되면 NOT_OBSERVED로
남기고 fixture 성공으로 대체하지 않는다. corrupt의 INVALID_CONFLUENT_FRAME은
Quarantine 후보일 뿐 publish/ACK 성공으로 표시하지 않는다. executor Registry 접근과
Job 2 E2E는 별도 미검증이다.

실제 실행한 검증:

```text
python3 scripts/registry_migration.py offline
OFFLINE_BUNDLE_PASS; no network requests

.venv/bin/python -m pytest -q tests/unit/test_registry_migration.py tests/unit/test_runtime_config.py tests/unit/test_validation_input.py
122 passed, 1 warning (기존 Authlib/httpx deprecation)
```

새 이관 테스트 24개에는 기존/삭제 ID·subject 중단, schema 변조, 부분 import 실패의
무재시도, URL/redirect 보호, 오류 본문의 비밀값 미출력, 명시적 실행 flag,
corrupt frame 후보 경계가 포함된다. 준비된 export schema를 반환하는 오프라인
client로 기존 fixture를 decode하여 ID 1/10필드/UTC-aware event_time을 확인했다.
이 결과는 Cloud Registry 연결 증거가 아니다. Python compile 및 79열/공백 검사,
JSON/Avro parse, manifest/source/request/hash 일치, git diff/check/status 확인도 수행했다.

Cloud 생성 후 남은 사용자 순서는 환경/Registry/API 인증 준비 → destination preflight
→ raw import/검증 → validated 등록/검증 → Databricks secret 참조 연결 → driver smoke다.
endpoint는 생성 후 실제 HTTPS URL로 채우고, provider=confluent, auth_mode=basic,
제안 참조는 scope=ktd-job2/key=schema-registry-basic-auth다. 실제 secret 값이나
API key는 문서·manifest에 포함하지 않았다. 자세한 명령과 실패 시 중단 기준은 위
실행 절차에 있으며, 이번에는 준비 후 멈췄다. Git staging/commit/push는 수행하지 않았다.


## 2026-10-17 P2-8 로컬 복구 — 잘못된 Quarantine ACK 차단

기준 HEAD는 `b8c6ee25ef038c79185d2b0d479bf78781ebf120`, 브랜치는
`feat/phase2-validation-silver`다. 기존 미커밋 보고서 본문은 보존하고 이 절만 추가했다.

기존 `test_sensor_validation_job.py`와 `test_validated_publisher.py`에는
Silver 성공 후 직렬화·발행·ACK 실패, 동일 이벤트 재시도, 일부 배치 재처리,
발행 ACK 후 호출자 실패, 충돌 Quarantine 실패 후 복구가 이미 있다.
따라서 같은 시나리오를 중복 추가하지 않고 잘못된 Quarantine ACK offset의
성공 오판을 이번 한 논리 작업으로 선택했다.

`False < 0`과 `True < 0`, `1.5 < 0`은 거짓이므로 기존 음수 검사만으로는
잘못된 ACK가 정상 완료로 반환된다. None/문자열은 계약 오류 대신 TypeError가 난다.
이는 fake로 확인한 방어 계약 결함이며 실제 Kafka에서 발생한 장애 기록은 아니다.
`streaming/quarantine/quarantine_publisher.py`에서 validated와 동일하게 bool을
제외한 0 이상의 정수만 허용하도록 최소 수정했다. 인계 문서의 테스트 중심 범위에
대해 이번 사용자의 최소 구현 요청을 적용했고 수정 원인과 범위를 먼저 설명했다.

추가 테스트:
`test_conflict_invalid_ack_blocks_batch_and_retry_preserves_canonical`
(`tests/unit/test_sensor_validation_job.py`, 5개 offset 매개변수).

- 정상 이벤트 저장 뒤 같은 event_id의 metric_value 충돌과 후속 정상 행을 입력한다.
- Quarantine ACK offset False/True/None/문자열/실수에서 RuntimeError가 전파된다.
- 실패 시 canonical은 변하지 않고 후속 정상 행은 저장·발행되지 않는다.
- 새 fake producer와 SilverSink를 주입하고 동일 fake 저장소로 재시도한다.
- 정상 ACK offset 0이면 received=2, quarantine=1, conflict=1, validated=1이다.
- 기존 canonical을 보존하고 Quarantine key/value는 첫 시도와 동일하다.

실제 실행 명령과 결과:

```text
# 제품 코드 수정 전 실패 확인
.venv/bin/python -m pytest -q tests/unit/test_sensor_validation_job.py -k conflict_invalid_ack
5 failed, 74 deselected, 1 warning

# 최소 수정 후 관련 회귀
.venv/bin/python -m pytest -q tests/unit/test_sensor_validation_job.py tests/unit/test_silver_sink.py tests/unit/test_delta_silver_adapter.py tests/unit/test_validated_publisher.py tests/unit/test_quarantine_publisher.py
232 passed, 1 warning

# 외부 시스템 호출을 제외한 전체 로컬 회귀
.venv/bin/python -m pytest -q -m 'not integration'
744 passed, 10 deselected, 1 warning
```

warning은 기존 Authlib/httpx deprecation이다. 이번 추가는 한 시나리오의 5개
매개변수 사례이며 과거 739건 기록을 현재 실행 결과로 대체해 인용하지 않았다.
`git diff --check` 통과. 기존 변경 파일의 hash를 대조했고 보고서의 기존 본문도 보존했다.

남은 검증 우선순위:

1. 실제 Delta 저장 성공 후 validated ACK 실패 및 Spark checkpoint 실패의 재시작
   복구. 고정 event_id/동일 10필드 입력에 대해 canonical 1행 유지, validated
   event_id 누락 0건을 대사해야 한다. Kafka 중복은 허용되는 전달 의미다.
2. 새 프로세스/저장소 연결과 재시작 시각 변화까지 포함한 Job 2 복구. 이번 fake
   저장소 재사용은 프로세스 재시작이나 영속 Delta 검증이 아니다.
3. 실제 Quarantine ACK 후 checkpoint 실패 시 동일 source topic/partition/offset의
   quarantine_id 유지와 canonical 불변을 확인해야 한다.

통합 테스트 10건은 명시적으로 제외했다. AWS/Registry/Databricks 호출, Compute 실행,
Avro·운영 Credential·checkpoint 변경 및 Git staging/commit/push/merge는 하지 않았다.
단위 결과는 Spark/Kafka/Delta E2E 또는 Phase 2 완료 증빙이 아니다.
포트폴리오에는 실패 5건과 수정 후 로컬 회귀 744건 화면을 함께 촬영하되
“P2-8 로컬 fake ACK 복구 검증”으로 표시한다.


### 2026-10-17 재개 세션 — 기존 복구 수정 재검증

재개 시 실제 HEAD는 `b8c6ee25ef038c79185d2b0d479bf78781ebf120`, 브랜치는
`feat/phase2-validation-silver`였다. 수정 8개와 미추적 요약 16개가 이미 있었다.
위 Quarantine ACK 수정 및 5개 매개변수 테스트도 시작 시 존재했으므로 이번
재개 세션에서 작성하거나 red→green을 새로 실행한 결과로 주장하지 않는다.

인계 첫 지시의 중복 테스트 금지에 따라 코드·테스트를 추가하지 않았다.
Silver는 매번 저장소의 event_id를 조회하고 INSERTED/DUPLICATE_NOOP 모두
validated 발행을 시도한다. CONFLICT는 canonical을 보존하고 Quarantine ACK를
기다린다. 저장·발행 예외는 배치 밖으로 전파하며 진행 상태는 Spark가 관리한다.
기존 테스트는 아래 경계를 fake 수준에서 검증한다.

| 경계 | 기존 테스트 |
|---|---|
| Silver 성공 후 직렬화/produce/flush 실패와 재처리 | `test_failure_propagates_after_write_and_replay_recovers` |
| validated ACK 오류·timeout 후 중복 재발행 | `test_ack_error_propagates_and_retry_publishes_duplicate`, `test_timeout_is_not_success_and_can_be_retried` |
| ACK 성공 후 호출자 실패와 canonical 1행 보존 | `test_caller_failure_after_ack_allows_duplicate_publish` |
| 배치 일부 성공 후 실패·재처리 | `test_partial_batch_retry_republishes_completed_records` |
| 충돌 Quarantine 실패·잘못된 ACK 차단·복구 | `test_conflict_quarantine_failure_blocks_batch_until_ack`, `test_conflict_invalid_ack_blocks_batch_and_retry_preserves_canonical` |
| 충돌 재처리의 동일 Quarantine identity | `test_conflict_replay_after_checkpoint_failure_has_stable_evidence` |

이번 직접 실행:

```text
.venv/bin/python -m pytest -q tests/unit/test_sensor_validation_job.py tests/unit/test_silver_sink.py tests/unit/test_delta_silver_adapter.py tests/unit/test_validated_publisher.py tests/unit/test_quarantine_publisher.py
232 passed, 1 warning in 0.51s
```

warning은 Authlib/httpx deprecation이다. 전체 744건 및 수정 전 실패 5건은
앞 절의 기존 기록이며 이번 재실행 결과가 아니다.

남은 장애 검증은 다음 순서다. 아래는 실행 명세이며 cloud 실행 승인이 아니다.
입력의 기준은 `tests/unit/test_sensor_validation_job.py`의 `event`/`raw_record`
fixture 10필드와 원본 bytes다. 로컬 고정 event_time은 `2026-10-08T12:00:00+00:00`,
event_id는 `job2-test-1`, metric_value는 4.2, key는 `fridge-001`이다.
실제 Kafka source partition/offset은 발행 결과를 측정해야 하며 fake의 2/42를
실측값으로 사용하지 않는다. 기존 runtime 데이터와 충돌 여부를 먼저 확인해야 한다.

1. **최우선: 실제 Silver 성공→validated ACK 미확정→같은 원본 재처리.**
   기대값은 canonical 10필드 불변·event_id당 1행, 최종 validated 누락 0건,
   정상 이벤트의 Quarantine 0건이다. timeout 후 Kafka 중복은 허용한다.
   실제 Spark 실패 배치의 재실행과 checkpoint 진행 여부까지 함께 측정한다.
2. **ACK 성공→checkpoint 완료 전 장애→새 프로세스로 재개.**
   같은 원본 bytes와 실제 lineage를 재처리해 canonical 1행과 validated 전달을
   대사한다. 새 저장소 연결 및 지연된 재시도에서도 보존돼야 한다.
   고정 fixture는 지연 실행 시 LATE가 될 수 있으나 validated 발행 대상이다.
3. **충돌 Quarantine ACK→checkpoint 완료 전 장애→재개.**
   canonical 4.2를 먼저 보존하고 같은 event_id의 metric_value만 9.5로 바꾼
   원본을 재처리한다. 기대값은 canonical 4.2 유지, 충돌 validated 0건,
   `EVENT_ID_CONFLICT`/`metric_value`와 동일 source lineage·quarantine_id다.
   처리 시각이 바뀌면 Quarantine 전체 payload의 byte 동일성은 요구하지 않는다.

이번 세션은 보고서에 이 절만 추가했다. 제품 코드·테스트·다른 Phase·운영 설정을
수정하지 않았고 외부 서비스 호출, Compute 실행, credential/checkpoint 변경,
Git staging/commit/push/PR/merge를 수행하지 않았다. 실제 프로세스 재시작과
Kafka/Spark/Delta 부분 실패 복구는 여전히 미검증이다.

### 2026-10-17 로컬 수용 보완 — 음수 Quarantine ACK 배치 복구

시작 branch/HEAD는 `feat/phase2-validation-silver` /
`b8c6ee25ef038c79185d2b0d479bf78781ebf120`이다. 기존 수정 8개와 미추적
파일을 보존했다. bool 제외·정수·비음수 ACK 검사와 잘못된 타입 5개 테스트는
시작 시 이미 존재했다. 이번 변경은 기존 배치 복구 테스트에 offset `-1` 한 건과
이 절을 추가한 것이다. publisher의 음수 거부 테스트는 있었지만 배치 중단,
canonical 보존, 후속 행 미처리, 정상 ACK 재시도를 잇는 음수 사례는 없었다.
제품 코드와 Silver/validated 복구 구현을 다시 수정하지 않았다.

두 로컬 계획서와 `docs/plans/handoff/{validated-contract,session-prompts}.md`를
읽었다. 외부 `../ktd-worktrees/handoff/validated-contract.md`는
`Operation not permitted`여서 내부 사본을 사용했다. 외부 최신본과 동일한지는
미확인이다. 계획의 과거 진행 상태 대신 현재 코드와 테스트를 대조했다.

현재 파일을 되돌리지 않고 별도 Python 프로세스에서 HEAD의 publisher 소스를
모듈 namespace에 로드하여 기존 5개 테스트를 실행했다. 실제 재현 명령:

```bash
.venv/bin/python - <<'PYTHON'
import subprocess

import pytest
import streaming.quarantine.quarantine_publisher as publisher

source = subprocess.check_output([
    "git", "show", "HEAD:streaming/quarantine/quarantine_publisher.py",
]).decode()
exec(compile(source, "<HEAD quarantine_publisher.py>", "exec"),
     publisher.__dict__)
raise SystemExit(pytest.main([
    "-q", "tests/unit/test_sensor_validation_job.py",
    "-k", "conflict_invalid_ack", "--basetemp=/tmp/ktd-p2-ack-red",
]))
PYTHON
```

`-1` 추가 전 결과는 **5 failed, 74 deselected, 1 warning**이었다.
False/True/1.5는 예외 없이 잘못 완료하고 None/문자열은 TypeError였다.
이는 이전 HEAD 구현과의 비교이며 이번에 새 제품 수정을 수행한 red→green은
아니다. 위 명령을 최종 테스트 파일로 다시 실행하면 추가된 -1은 통과하므로
5 failed/1 passed가 예상된다.

원인은 이전 구현의 `offset < 0` 단독 검사다. bool은 int의 하위 타입이고
False/True/양의 실수는 음수 비교를 통과한다. 현재 기존 수정은 bool이 아닌
int이며 offset >= 0인 경우만 성공한다. 이번 -1 포함 6개 사례에서 RuntimeError가
process_batch 밖으로 전파되고 canonical과 후속 행을 보호한다. ACK 0 재시도는
conflict=1/quarantine=1/validated=1로 완료하며 canonical과 Quarantine key/value를
유지한다. ACK 실패 시 배치 완료 카운트를 반환하지 않는다.

최종 코드에서 직접 실행한 검증:

```text
.venv/bin/python -m pytest -q tests/unit/test_sensor_validation_job.py tests/unit/test_silver_sink.py tests/unit/test_delta_silver_adapter.py tests/unit/test_validated_publisher.py tests/unit/test_quarantine_publisher.py --basetemp=/tmp/ktd-p2-ack-related
233 passed, 1 warning in 0.42s

.venv/bin/python -m pytest -q tests/unit --basetemp=/tmp/ktd-p2-ack-unit
745 passed, 1 warning in 2.69s

git diff --check
PASS
```

경고는 기존 Authlib/httpx deprecation이다. produce/flush 예외, callback 오류,
callback 부재, 남은 queue, 정상 ACK 0의 기존 테스트도 관련 회귀에 포함된다.
이 결과는 fake/unit 증빙이다. 실제 Spark checkpoint 진행·재시작, 영속 Delta,
Kafka broker ACK를 검증한 결과가 아니다.

다음 수용 검증을 위한 MSK 준비 상태(이번 cloud 조회·변경 없음):

| 항목 | 현재 상태와 다음 확인 |
|---|---|
| validated | 사용자 보고는 UNKNOWN_TOPIC_OR_PART. 이번 metadata 미조회 |
| quarantine | 존재·metadata 미검증 |
| partitions | 최신 사용자 지시로 두 output 모두 3. 설계 문서의 validated 초기 후보 6과 구분 |
| provisioning | 미실행. 관리용 별도 권한, 대상 cluster, RF/min ISR/retention 확인 필요 |
| runtime 권한 | CreateTopic 추가 금지 유지. 기존 raw 읽기와 output 쓰기·metadata 권한 확인 필요 |
| 자동 생성 | 클라이언트 allow.auto.create.topics=False 유지. broker 설정은 이번 미조회 |
| metadata 증빙 | 두 topic의 partition 0/1/2, leader/replica/ISR, topic error와 실제 runtime role의 조회 성공 기록 |
| delivery 증빙 | 별도 승인된 실행에서 callback 성공·non-bool 정수 offset >= 0, topic/partition/offset과 읽기 결과 기록. flush=0만으로 성공 판정 금지 |

로컬에서 남은 공백은 실제 checkpoint·영속 sink를 사용하는 새 프로세스 재개다.
같은 fake 저장소 재사용은 이를 증명하지 않는다. 후속 복구 검증은 다음과 같다.

1. Silver 성공→validated ACK 미확정→같은 원본 재처리: canonical 1행/10필드
   불변, validated 누락 0건, 정상 이벤트 Quarantine 0건. Kafka 중복은 허용한다.
2. validated ACK→checkpoint 완료 전 장애→새 프로세스 재개: canonical 1행과
   최종 validated 전달을 대사한다.
3. Quarantine ACK→checkpoint 완료 전 장애→재개: canonical 불변, 충돌 validated
   0건, 같은 원본 lineage와 quarantine_id를 확인한다.

기존 checkpoint를 유지하고 raw offset이 retention 및 미완료 범위에 있는지
먼저 확인한다. 기존 happy-path raw ACK p1/o3는 과거 증빙이며 현재 재처리
가능성은 미확인이다. payload/처리 시각이 바뀌는 재시도에서는 계약상 요구되는
identity·lineage를 대사하고 전체 payload byte 동일성을 무조건 요구하지 않는다.

다음 논리 작업은 두 output topic provisioning 사양 확정과 읽기 전용 preflight
절차 준비다. 실제 cloud 변경·Compute 실행은 이번 범위에 포함하지 않았다.
Avro 10필드, Bronze Iceberg, Silver Delta, 다른 Phase, 설정, 기존 미추적 파일은
변경하지 않았다. 포트폴리오에는 233/745 통과 화면을 촬영하고
“로컬 fake ACK 검증”으로 표시한다. Git add/commit/push/PR/merge 없이 검토를 기다린다.

## 2026-10-23 P2-8h — Job 2 로컬 신뢰성 검증

**로컬 검증 완료 / Cloud E2E와 Phase 2 전체 DoD는 미완료.**
기준 시각은 KST이며 증빙 JSON에는 UTC를 기록했다. 이번 작업은 과거 P2-8g
승인을 사용하지 않았고 AWS API 조회·변경, EC2, Databricks, 실제 Kafka 발행을
모두 수행하지 않았다. 생산 코드 수정은 Registry 오류 분류 조건 하나다.

### 1. Branch / HEAD / 기존 작업 보존

- 실제 branch: `feat/phase2-validation-silver`.
- 실제 HEAD: `888a59c3acd99989501f487ecd24191a675633af`.
- 시작 시 tracked 수정 6개: `config/job2-runtime.example.json`,
  `docs/implementation/p2-8d-msk-output-topic-provisioning.md`,
  `scripts/phase1_export_avro_fixture.py`, `simulator/producer.py`,
  `tests/unit/test_job2_databricks_entrypoint.py`,
  `tests/unit/test_phase1_export_avro_fixture.py`.
- 기존 미추적 runtime JSON, Registry/happy-path 스크립트·테스트와 cloud 증빙을
  유지했다. 시작 파일 167개의 SHA-256 snapshot과 대조하여 이번 의도된 수정
  4개 외 기존 파일의 내용이 동일함을 확인했다. staged 변경은 없다.
- `AGENTS.md`, 접근 가능한 두 로컬 계획서, P2-8d, platform/data-contract,
  기존 코드·테스트·보고서를 확인했다. 계획서는 Git 제외 상태를 유지한다.
- `docs/architecture/platform.md`의 Job 2 “미구현”과 bounded dedup 표기는
  현재 구현과 다르다. 본 검증에서는 실제 코드를 기준으로 판단했으며,
  이번 범위 밖의 설계 문서 전체를 다시 쓰지 않았다.

### 2. 확인한 Job 2 전체 경로와 영속 경계

경로는 `Kafka raw → foreachBatch → frame/Avro → UTC → domain/time →
canonical lookup/MERGE → validated 또는 Quarantine ACK → batch 반환 →
Spark checkpoint commit`이다. Job 1 Bronze 완료를 기다리지 않는다.

| 단계 / 파일·함수 | 입력 → 출력 및 정상 동작 | 예외 / 영속 상태 / 재처리 |
|---|---|---|
| `streaming/jobs/raw_ingestion.py:kafka_source_options`, `streaming/jobs/sensor_validation_job.py:start_validation` | raw bytes/key/topic/partition/offset, earliest 기본값, failOnDataLoss, auto-create=false, append foreachBatch | source 오류 전파. 전용 checkpoint가 source 진행 상태를 보관하며 query 이름은 offset 저장소가 아님 |
| `sensor_validation_job.py:process_batch/process_record` | topic/partition/offset 정렬 후 순차 iterator, Spark bytearray→bytes | 시스템·sink 오류 시 후속 행 중단, batch 완료 계수 미반환, 재실행 |
| `streaming/validation/avro_decoder.py:decode_sensor_event` | magic byte+schema ID, Registry writer schema, 실제 fastavro decode | 데이터 오류만 SensorDecodeError→Quarantine. 인증/서버/endpoint/설정 오류는 전파. Registry schema cache는 복구 ledger가 아님 |
| `event_time_normalizer.py:normalize_event_time` | aware ISO 문자열→UTC datetime | naive/불량 시각 격리; Avro 10필드 유지 |
| `domain_validator.py:validate_domain_event`, `event_time_policy.py:classify_event_time` | 필수값·버전·finite 수치·장비/metric/unit/key 계약; 처리 기준 시각과 지연/미래 경계 | 미래 허용치 초과 격리. 정상 업무 이상값은 허용. 오래된 valid도 Silver/validated 대상 |
| `streaming/sinks/silver.py:SilverSink.write`, `delta_silver.py:DeltaSilverStorage` | event_id 영속 lookup, 신규 insert-only MERGE 및 사후 확인, 동일 payload DUPLICATE_NOOP | 기존 canonical 보존, 충돌은 CONFLICT. 조회/쓰기/사후 검증 실패 전파. Delta canonical이 유일한 제품 측 dedup ledger |
| `streaming/sinks/validated_publisher.py:deliver_validated_event/publish_validated_event` | 신규·동일 duplicate는 canonical 처리 후 equipment key/10필드 Avro 재발행 | 충돌은 발행 안 함. serialization/produce/flush/callback/ACK 오류 전파, raw replay로 재시도 |
| `streaming/quarantine/quarantine_record.py`, `quarantine_publisher.py` | 원본 lineage/hash/key와 오류를 안정적 quarantine_id JSON으로 발행 | callback과 bool이 아닌 int offset>=0까지 확인. 실패 시 batch 미완료; raw replay 필요 |
| Spark writeStream checkpoint | foreachBatch 정상 반환 뒤 source batch commit | ACK 직후 반환 전 장애는 동일 batch 재실행. Silver/Kafka/checkpoint의 분산 원자성은 없음 |

`allowed_lateness=10분`은 처리 시각 기준 분류다. 실제 Spark watermark,
`withWatermark`, 시간 제한 stateful dedup은 Job 2에 없다. 10분 경계 이전·이후
valid 이벤트 모두 전달하며 영속 Silver의 event_id 비교는 시간 만료가 없다.
운영 checkpoint는 `/Volumes/ktd/silver/checkpoints/job2/sensor-validation`이다.

### 3. 재현한 결함과 원인

기존 decoder는 모든 HTTP 404를 `UNKNOWN_SCHEMA_ID`로 격리했다. 잘못된
Registry endpoint/proxy 404 또는 다른 오류 코드까지 데이터 오류로 처리하면
정상 원본을 Silver/validated에 보내지 않고 Quarantine ACK 후 batch를 완료할
수 있다. 이는 시스템 장애를 데이터 오류로 오인하는 실제 결함이다.

`GET /schemas/ids/{id}`의 schema 부재는 HTTP 404 + error code 40403으로
정의된다. [Confluent 공식 API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html).
404/404, 404/40401, 404/-1을 decoder와 Job 2에 주입한 결과 수정 전
**6 failed / 7 passed**, 수정 후 관련 두 파일 **103 passed**를 확인했다.

### 4. 최소 코드 수정

- `streaming/validation/avro_decoder.py`: HTTP 상태와 오류 코드가 모두
  404/40403일 때만 UNKNOWN_SCHEMA_ID로 변환한다. 나머지는 원래 예외 그대로
  전파하므로 Silver/validated/Quarantine 모두 호출하지 않는다.
- Silver MERGE, validated 복구 순서, Quarantine ACK 로직, Avro 계약,
  Bronze Iceberg/Silver Delta, production checkpoint는 변경하지 않았다.
- 기존 `False` ACK 결함 수정은 이미 HEAD에 있었다. 이번 신규 수정으로
  다시 주장하지 않으며 기존 ACK 실패·retry 테스트를 재실행했다.

### 5. 추가 테스트와 fixture

- `tests/unit/test_validation_input.py`: 기존 Registry 오류 매개변수에 3종의
  비-schema 404를 추가하고 원래 예외의 동일성을 확인한다.
- `tests/unit/test_sensor_validation_job.py`: 같은 3종의 실패를 실제
  process_record에 연결하여 양 publisher가 호출되지 않음을 검증한다.
- `tests/integration/test_job2_local_delta_recovery.py`: opt-in 로컬 통합
  테스트. 임시 input/Delta/checkpoint를 만들고 자식 프로세스 5개를 순차 실행한다.
- `tests/integration/job2_local_recovery_worker.py`: 해당 테스트 전용 실행기.
  실제 production process_batch/SilverSink/DeltaSilverStorage/publisher를
  사용하며 기존 unit의 Avro schema/raw encoder/규칙을 재사용한다.
  Kafka는 fsync journal+delivery callback 대역, Registry는 메모리 schema 응답이다.
- 별도 정적 fixture 파일 대신 임시 합성 fixture를 생성한다. 정상 2 ID,
  동일 이벤트 중복, metric_value 충돌, unit 불량을 포함한다. 입력 시각은
  `2016-10-08T12:00:00.123456+00:00`, 처리 기준은 기존 `NOW` fixture다.
  원본 fixture와 실제 ACK journal은 `local-recovery.json`에 함께 보관했다.
- 로컬 file source와 테스트 foreachBatch를 직접 연결한다. production
  `start_validation`의 Kafka source·클라이언트 생성·UC 경로는 기존 unit wiring
  검증 범위이며 이 통합 테스트가 실제 Kafka start_validation 실행은 아니다.

### 6. 정상 경로 / 데이터 정확성 15개 시나리오

| 요구 시나리오 | 검증 파일 / 결과 |
|---|---|
| 1 정상 Avro | `test_validation_input.py`, `test_sensor_validation_job.py` 실제 fastavro framing/decode PASS |
| 2 framing 오류 | 짧은 값·magic byte·헤더만·불량 body·null 입력, Quarantine 분기 PASS |
| 3 알 수 없는 schema ID | 404/40403 → UNKNOWN_SCHEMA_ID/Quarantine PASS |
| 4 Registry 장애 | 401/5xx/timeout/설정 오류 및 신규 비-schema 404 전파 PASS |
| 5 필수 필드 누락 | `test_validation_domain.py` 필드별 missing/null PASS. 실제 non-null Avro writer가 일부 입력을 먼저 거부할 수 있으므로 dict domain 경계와 구분 |
| 6 타입/버전 | 잘못된 string·numeric/bool·미지원 버전 및 다중 오류 PASS |
| 7 NaN/Infinity | finite 검사 PASS; 정상 수치는 통과 |
| 8 장비/metric/unit | rules·registry·store·key 일치, 독립 오류 수집 PASS |
| 9 UTC/경계 | offset 동치·naive 거부·마이크로초 및 실제 Delta UTC 왕복 PASS; Spark session TZ=Asia/Seoul |
| 10 미래 시각 | +5분까지 허용, 초과 격리·후속 sink 미호출 PASS |
| 11 동일 ID/동일 payload | 신규 뒤 반복·batch replay·새 프로세스·새 checkpoint에서 canonical 1행/ID PASS |
| 12 동일 ID/충돌 payload | canonical 4.2 보존, 충돌 9.5 validated 억제 및 Quarantine PASS |
| 13 지연 경계 이내 | 처리 기준 10분까지 ON_TIME, 전달 PASS. 실제 watermark 검증은 아님 |
| 14 지연 경계 밖 | 11분 및 10년 전 valid 전달·영속 dedup PASS. 실제 watermark 검증은 아님 |
| 15 정상 업무상 이상 값 | `test_high_fridge_temperature_is_valid_contract_data` PASS, 임의 업무 임계값 Quarantine 없음 |

### 7. 실패·부분 실패 A–H

| 경계 | 이번 검증 / 재시도 대상 | 데이터 유실·중복과 필요한 영속 정보 |
|---|---|---|
| A Silver commit 이전 실패 | lookup/insert SQL 예외 unit PASS, publish 미호출 | raw source 위치·payload를 보존하고 같은 batch 재처리. 이미 처리된 앞 행은 재발행 가능 |
| B Silver 성공 / publish 이전 실패 | serializer 실패→canonical 보존→동일 raw 재시도 unit PASS | canonical만 있고 validated가 아직 없을 수 있음. 실패 batch의 raw replay가 필요 |
| C publish 요청 / ACK 미확정 | callback 없음·timeout/비정상 offset unit PASS; 실제 Delta commit 후 테스트 flush TimeoutError PASS | broker 전달 여부는 알 수 없음. 재시도는 중복 가능, 성공으로 간주하지 않음 |
| D ACK / checkpoint 사이 실패 | 실제 local foreachBatch 전체 ACK 뒤 os._exit(23), commit 0 부재, 새 Python/JVM replay PASS | canonical 수렴, validated/Quarantine 재발행. raw+Delta+checkpoint 필요 |
| E validated 성공 / Quarantine 실패 | `test_partial_batch_retry_republishes_completed_records` PASS | batch 재시도 때 앞 validated도 재발행; Quarantine ACK 전 batch 완료 금지 |
| F callback 오류 | 양 publisher KafkaError·잘못된 offset 전파 PASS | 성공 로그/flush()==0만으로 완료하지 않음. 실패 원본부터 batch retry |
| G timeout / 프로세스 종료 | timeout unit 및 별도 ACK 후 강제 프로세스 종료 integration PASS | 실제 broker timeout 직후 kill을 함께 수행한 것은 아님. broker의 in-flight 결과는 미검증 |
| H 재시작 / 동일 batch | 5개 새 프로세스, batch 0 반복 뒤 batch 1, fresh checkpoint replay PASS | canonical 2행, 전달 중복 허용. file source offset 복구이며 Kafka partition offset runtime은 미검증 |

Quarantine의 `False/True/None/"3"/1.5/-1` offset, produce/flush 오류,
callback 오류/없음에서 batch가 실패하고 후속 행을 처리하지 않는 기존 테스트가
PASS다. offset 0 정상 ACK 재시도에서는 canonical·conflict lineage를 보존하고
후속 행을 처리한다. 실제 broker ACK와 구분한다.

### 8. 실제 로컬 재시작 / Delta 결과

환경은 macOS ARM64, Python 3.11.15, OSS Spark 4.0.1, Delta 4.0.0,
Temurin JDK 21.0.13+9, local[2]다. 프로젝트 venv/lockfile은 변경하지 않았다.
기본 venv에는 Spark가 없고 Databricks Connect venv는 remote session만 허용했다.
Docker daemon도 꺼져 있었다. `/tmp/ktd-8h-spark`, `/tmp/ktd-8h-java`,
`/tmp/ktd-8h-jars`에 무료 OSS 의존성을 별도 준비했다.
[Spark 4 요구사항](https://spark.apache.org/docs/4.0.1/)과
[Delta 호환 표](https://docs.delta.io/releases/)를 확인했다.

첫 설치 시 sandbox cache/DNS 제한, 첫 JVM 시도에서 loopback bind 제한이
발생했다. `/tmp` cache와 승인된 로컬 실행 권한으로 해결했다. 안전 설정을
해제하거나 DatabricksSession으로 우회하지 않았다. Hadoop native library
fallback, JDK incubator 경고는 기능 실패가 아니며 원문을 보존했다.

| 순서 | 실제 관측 / assertion |
|---|---|
| 1 새 프로세스, ACK 전 timeout | Delta 실제 신규 행 commit; Spark offsets/0 존재, commits/0 없음, journal 없음. 의도한 TimeoutError 문구 확인 |
| 2 새 프로세스, ACK 후 강제 종료 | valid 3 + Quarantine 2 callback ACK, exit=23; commits/0 여전히 없음 |
| 3 새 프로세스, 동일 checkpoint 재개 | batch_id=0 재실행, duplicate=3/conflict=1; 앞 5개와 같은 ACK payload 5개 재관측, commits/0 생성 |
| 4 동일 checkpoint + 새 file | batch_id=1만 처리, ACK 1개 추가, 기존 batch 재처리 없음 |
| 5 독립 새 checkpoint | 기존 checkpoint 삭제 없이 전체 입력 6개 replay; canonical 불변, ACK 6개 추가 |
| 최종 | canonical 2 ID 각 1행, 값 4.2·10필드·UTC 마이크로초 불변, conflict 9.5 미적재; Delta log 3 versions; 총 journal 17개 |
| query 식별 | 동일 checkpoint의 4번 실행은 query ID 동일, 5개 run ID 모두 다름. 같은 queryName이지만 새 checkpoint의 query ID는 달라짐 |

Spark/Delta는 실제 로컬 runtime이다. UC Managed Delta 권한·Databricks와 동일
실행 환경, Kafka broker/Schema Registry 네트워크·ACK는 증명하지 않는다.
테스트용 journal은 관측 도구이며 제품 outbox·신규 영속 시스템이 아니다.
테스트 후 해당 worker/Spark 프로세스가 남지 않았음을 조회로 확인했다.

### 9. 실행 명령 / 전체 Unit·Integration 결과

저장소 루트에서 실행했다. `test-results.txt`에 실패·통과 원문을 남겼다.

```bash
# 수정 전 baseline: 745 passed, Authlib deprecation 1 warning
.venv/bin/python -m pytest -q tests/unit --basetemp=/tmp/ktd-8h-baseline

# 수정 전 red: 6 failed, 7 passed (새 비-schema 404만 실패)
.venv/bin/python -m pytest -q tests/unit/test_validation_input.py tests/unit/test_sensor_validation_job.py -k 'propagates_registry_failure or registry_system_failure' --basetemp=/tmp/ktd-8h-red

# 수정 후 해당 두 파일: 103 passed
.venv/bin/python -m pytest -q tests/unit/test_validation_input.py tests/unit/test_sensor_validation_job.py --basetemp=/tmp/ktd-8h-green

# 관련 계약·sink·publisher·runtime: 569 passed, 1 warning
.venv/bin/python -m pytest -q tests/unit/test_validation_input.py tests/unit/test_validation_domain.py tests/unit/test_event_time_policy.py tests/unit/test_event_identity.py tests/unit/test_silver_sink.py tests/unit/test_delta_silver_adapter.py tests/unit/test_validated_publisher.py tests/unit/test_quarantine_record.py tests/unit/test_quarantine_publisher.py tests/unit/test_sensor_validation_job.py tests/unit/test_runtime_config.py --basetemp=/tmp/ktd-8h-related

# 최종 전체 unit: 751 passed, 1 warning
.venv/bin/python -m pytest -q tests/unit --basetemp=/tmp/ktd-8h-unit-final

# 기본 integration: 기존 외부 연동 10 + 신규 opt-in 1 = 11 skipped
.venv/bin/python -m pytest -q tests/integration --basetemp=/tmp/ktd-8h-integration-default

# 실제 로컬 통합: 1 passed, 47.59s / 새 프로세스 5회
JAVA_HOME=/tmp/ktd-8h-java/jdk-21.0.13+9/Contents/Home \
KTD_LOCAL_DELTA_JARS=/tmp/ktd-8h-jars/delta-spark.jar,/tmp/ktd-8h-jars/delta-storage.jar \
KTD_RUN_LOCAL_DELTA=1 /tmp/ktd-8h-spark/bin/python -m pytest -q \
  tests/integration/test_job2_local_delta_recovery.py \
  --basetemp=/tmp/ktd-8h-delta-final

# 둘 다 offline config OK; 인증값/토큰을 읽지 않음
.venv/bin/python -m streaming.jobs.sensor_validation_job --runtime-config config/job2-runtime.example.json --check-config
.venv/bin/python -m streaming.jobs.sensor_validation_job --runtime-config config/job2-runtime.json --check-config

git diff --check
git diff --stat
git status --short
```

로컬 재현 의존성 준비는 다음과 같다. JDK 21은 공식 Adoptium의 macOS ARM64
archive를 `/tmp/ktd-8h-java`에 해제했고, Delta JAR 두 개는 Maven Central의
`io/delta/delta-spark_2.13/4.0.0`과 `io/delta/delta-storage/4.0.0`에서 받았다.
기존 local 환경이 있으면 재다운로드가 필요하지 않다.

```bash
UV_CACHE_DIR=/tmp/ktd-8h-uv-cache uv venv /tmp/ktd-8h-spark --python .venv/bin/python
UV_CACHE_DIR=/tmp/ktd-8h-uv-cache uv pip install --python /tmp/ktd-8h-spark/bin/python \
  pyspark==4.0.1 delta-spark==4.0.0 pytest 'confluent-kafka[avro,schemaregistry]' python-dotenv cloudpickle
```

새 테스트는 기본적으로 skip되어 기존 `.venv`에 Spark 설치를 요구하지 않는다.
테스트 재실행은 새 basetemp를 사용하고 원본 증빙 경로를 덮어쓰지 않는다.

### 10. 남은 데이터 유실·중복·복구 위험

1. **at-least-once**: Silver·Kafka·checkpoint를 함께 commit하지 않는다.
   ACK 뒤 checkpoint 실패 시 같은 validated/Quarantine이 다시 전달될 수 있다.
   downstream은 event_id 및 quarantine_id/lineage 기준 중복 대응이 필요하다.
2. **raw retention 의존**: 미완료 batch 원본이 보존되어야 재시도할 수 있다.
   raw가 만료되거나 checkpoint를 잘못 바꾸면 canonical에 남은 정상 이벤트의
   미전달 여부를 자동 식별할 ledger가 없고, invalid 원문은 Silver에 없다.
   `failOnDataLoss`는 실패를 드러내지만 만료 원본을 복원하지 않는다.
3. Silver canonical 자체는 pending publish ledger가 아니다. 독립 backfill/
   대사는 별도 설계가 필요하며 이번 작업에서 outbox나 서비스를 추가하지 않았다.
4. 단일 writer 가정이다. Delta MERGE가 event_id unique constraint를 제공하는
   것은 아니며 동시 다중 writer·동일 checkpoint 병렬 실행은 미검증이다.
5. 실제 Kafka in-flight/ACK 타이밍, executor 종료, broker/network 복구,
   UC checkpoint 권한·Delta commit 불확실성은 cloud/runtime 후속 검증이 필요하다.
6. 처리 시각 기준 late 판정은 재시작 시 달라질 수 있다. canonical identity는
   원 payload 기반이다. 실제 watermark·bounded state/scale 성능은 미구현·미검증이다.

### 11. Cloud E2E 준비 상태 / 차단 요소

이번에는 저장소 증빙만 읽었다. 현재 클라우드 실측으로 표현하지 않는다.

| 항목 | 확인한 저장소 상태 / 남은 조건 |
|---|---|
| output topics | P2-8g `20261022T051154Z/topics-final.json`에 두 topic 생성·3 partitions·delete·604800000ms·bytes=-1, RF=3/min ISR=2 기록. 현 시점 broker 재조회는 미실행 |
| 종료/비용 | 같은 실행 cleanup.json에 EC2 stopped, 임시 IAM NoSuchEntity, cleanup_errors=[] 기록. 이번에는 변경/유료 실행 0건 |
| runtime 일치 | 실제·example config 모두 같은 broker:9098, ap-northeast-2, raw/validated/quarantine, Silver Delta, Job 2 전용 checkpoint. offline check-config 둘 다 PASS |
| 인증 참조 | source=`ktd-msk-consumer`, producer=`ktd-msk-job2-credential`, Registry scope/key=`ktd-job2/schema-registry-basic-auth`. 값은 읽거나 출력하지 않음 |
| 인증 구현 | Spark source service credential과 Python OAUTHBEARER signer 분리, Producer acks=all/auto-create=false, worker credential/token 갱신 코드 존재. 실제 만료·갱신 및 양 topic metadata/ACK는 미검증 |
| 읽기 대사 권한 | P2-8d 기록의 source consumer role은 raw ReadData만 있음. 기존 `scripts/phase2_verify_happy_path.py`는 같은 source credential로 validated도 읽으므로 실행 전 output ReadData 가능한 별도 검증 주체/권한을 확정해야 함. 권한을 임의 수정하지 않음 |
| Registry | Confluent Cloud endpoint/basic secret 참조 및 등록 schema 조회 설정 존재. 실제 HTTP 및 과거 schema ID replay 가능성 재확인 필요 |
| fixture | 기존 p2-8b fixture는 2026-10-11 준비/raw p1/o3 ACK의 과거 증거. 현재 retention 내 존재·미래/late 분류·Job 2 처리 증거가 아님. 신규 승인 E2E에는 새 event_id/시각과 등록 schema로 준비·발행 필요 |
| 처리 버전 | 실제 runtime JSON git:b8c6ee2 / example git:79163ad는 현재 HEAD와 다름. 사용자 기존 파일은 보존. E2E 실행 증빙에는 실제 배포 SHA와 processing_version 대응 확인 필요 |
| 테이블/checkpoint | 과거 UC Managed Delta storage smoke 있음. 이번 실제 Delta는 임시 local table로 분리. 운영 table/schema/Volume grants 및 기존 offset는 실행 전 읽기 대사 필요 |
| 비용 | 로컬만 실행, 신규 AWS/Databricks 비용 없음. 후속 Cloud는 DBU/VM, 기존 MSK capacity/partition/storage/IO/전송, Registry plan 비용 요인. 크레딧 적용·단가는 이번 미조회로 확정하지 않음 |

### 12. 문서·증빙 경로와 역할

아래 모든 상대 경로의 절대 경로 prefix는
`/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/`이다.
새 파일을 중복 보고서로 만들지 않고 기존 보고서에 결과를 추가했다.

| 전체 상대 경로 | 역할 / 추가 이유 |
|---|---|
| `docs/reports/phase-2-verification.md` | 기존 보고서에 현재 결과·실패 경계·cloud gate 통합 |
| `tests/integration/test_job2_local_delta_recovery.py` | 기존 unit으로 증명하지 못했던 새 프로세스/실제 Delta/checkpoint 수렴 검증 |
| `tests/integration/job2_local_recovery_worker.py` | 실제 Python/JVM 재시작 및 강제 종료를 격리할 자식 실행기 |
| `docs/evidence/phase-2/p2-8h-local-reliability/run-summary.json` | HEAD/환경·통과 수·소스 hash·검증 한계 |
| `docs/evidence/phase-2/p2-8h-local-reliability/local-recovery.json` | 실제 synthetic 입력·17 ACK journal·2 canonical·query/batch·checkpoint·Delta log 원문 |
| `docs/evidence/phase-2/p2-8h-local-reliability/test-results.txt` | red/full/related/skip/local integration 명령별 pytest 원문 |
| `docs/evidence/phase-2/p2-8h-local-reliability/worker-traces.txt` | 의도한 timeout·강제 종료와 새 프로세스 실행 로그 |

스크린샷은 직접 촬영하지 않았다. 사용자는 `test-results.txt`의 6 failed →
751 passed와 local 1 passed, `local-recovery.json`의 canonical 2행/3 Delta
versions/같은 query ID·다른 run ID를 열어 캡처하면 된다. “실제 로컬 Spark/Delta,
Kafka/Registry 대역”이라고 표시하고 broker ACK 화면으로 표현하지 않는다.

### 13. 이번 commit 후보 파일 (실행하지 않음)

다음 목록에는 기존 사용자 미커밋 변경을 섞지 않았다. 검토 뒤 사용자가 실행한다.

```bash
git add -- streaming/validation/avro_decoder.py \
  tests/unit/test_validation_input.py tests/unit/test_sensor_validation_job.py \
  tests/integration/test_job2_local_delta_recovery.py \
  tests/integration/job2_local_recovery_worker.py \
  docs/reports/phase-2-verification.md \
  docs/evidence/phase-2/p2-8h-local-reliability/run-summary.json \
  docs/evidence/phase-2/p2-8h-local-reliability/local-recovery.json \
  docs/evidence/phase-2/p2-8h-local-reliability/test-results.txt \
  docs/evidence/phase-2/p2-8h-local-reliability/worker-traces.txt
# 권장 메시지: fix: preserve Registry failures and verify Job 2 local recovery
```

### 14. 별도 승인이 필요한 Cloud 작업

실행 전 fixture·검증 principal·actual deployed SHA·기존 checkpoint와 raw retention을
확정해야 한다. 사용자 승인 범위는 Databricks 유료 Compute 시작/실행/종료,
실제 raw 테스트 발행, Job 2 두 output ACK, 필요한 읽기 권한/자격 증명,
격리된 failure injection 및 재시작·대사까지 구체화해야 한다. 기존 runtime role에
CreateTopic을 추가하거나 checkpoint/offset를 초기화할 이유는 없다.
실패 시 streaming query 종료→해당 compute 종료 확인→변경한 임시 권한만 회수→
잔여 비용/원본·checkpoint 보존을 기록하는 별도 승인 절차가 필요하다.

### 15. Phase 2 DoD 중 미완료와 다음 논리 작업

- actual MSK raw→Job 2→UC Delta/validated/Quarantine 원본·key·lineage 대사.
- 두 output의 실제 producer credential metadata/callback ACK 및 읽기 검증 주체.
- Databricks Kafka source checkpoint offset 복구와 실제 broker 부분 실패 재처리.
- raw retention/Job 1 Bronze와 Job 2 독립 소비 lag·누락 대사.
- 단일 writer 운영 제약·동시 실행 방지와 필요 시 별도 성능/다중 writer 검증.
- 계획이 실제 Spark watermark를 요구한다면 처리 시각 late 분류와의 차이를
  명시적으로 닫아야 한다. 이번 작업은 미구현 watermark를 구현했다고 하지 않는다.

다음 논리 작업은 **output 읽기 대사 principal과 새 E2E fixture/승인 범위를
확정하는 실행 전 검토**다. 이번 로컬 수정·검증을 사용자 검토에 넘기고 멈춘다.
