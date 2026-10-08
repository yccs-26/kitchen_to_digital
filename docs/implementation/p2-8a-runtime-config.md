# P2-8A Job 2 runtime 설정 연결

2026-10-09 기준 설정 코드와 offline unit 검증이다. 실제 secret 생성·조회,
Registry 요청, Kafka publish, Job 2 query 실행, checkpoint 변경은 수행하지 않았다.
기존 Delta storage smoke 증빙과 이번 인증 wiring의 runtime 검증은 별개다.

## 기존 설정 조사

- `.env.example`: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_SENSOR_RAW`,
  `SCHEMA_REGISTRY_URL`. 루트 `.env`는 없다. Docker `.env`는 존재하지만
  Job 2 Registry/auth 설정은 없으며 비밀값은 출력하지 않았다.
- `infra/docker/compose.yml`: 로컬 PLAINTEXT Kafka 및 Confluent Schema Registry.
  `simulator/producer.py`는 해당 Registry와 Confluent AvroSerializer를 사용한다.
  simulator producer에는 cloud SASL/auth 연결이 없다.
- `scripts/phase1_msk_fixture_producer.py`: MSK SASL_SSL/OAUTHBEARER 및
  AWS IAM signer. 기존 ambient IAM 자격 증명 방식은 Job 2에 암묵적으로 이식하지 않았다.
- `streaming/jobs/raw_ingestion.py`: `KTD_KAFKA_BOOTSTRAP_SERVERS`,
  `KTD_KAFKA_SERVICE_CREDENTIAL`, `KTD_BRONZE_CHECKPOINT`와
  Spark `databricks.serviceCredential` 경계가 있다. Job 1은 변경하지 않았다.
- 기존 Job 2는 `KTD_VALIDATION_RULES_PATH`, `KTD_SCHEMA_REGISTRY_CONFIG_JSON`,
  `KTD_PRODUCER_CONFIG_JSON` 등 환경 변수 기반이다. 호환 경로는 유지한다.
  새 경로는 인증 값이 들어가는 환경 변수 JSON 대신 secret 참조를 사용한다.
- 기존 Job 2용 JSON/YAML rules, secret scope/key 참조, dbutils secret 로더,
  notebook widget 기반 설정은 없었다. `schema_registry.py`는 빈 파일이었다.

## Registry 결정 경계

로컬 provider는 Confluent지만 **cloud provider·endpoint·auth는 미확정**이다.
`avro_decoder.py`는 magic byte 0 + 4-byte schema ID + Avro 및 `get_schema(id)`를
사용한다. 이는 [Confluent framing](https://docs.confluent.io/platform/8.0/schema-registry/fundamentals/serdes-develop/index.html)과 일치한다.
[AWS Glue Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-works.html)의
schema version ID/serde를 endpoint만 바꾸어 연결할 수 있다고 가정하지 않는다.

현재 최소 구현은 명시적 `provider: "confluent"`만 허용한다. 다른 provider면
query 시작 전 거부한다. 예제의 provider와 URL은 비워 두었다. 실제 provider가
다르면 decoder/client 계약을 별도 결정해야 하며 이 구현으로 연결 완료라고 하지 않는다.
Registry 인증은 HTTPS Basic 또는 명시적 `none`만 지원한다. 다른 인증은 미구현이다.

## 파일과 설정 계약

- `streaming/jobs/runtime_config.py`: JSON 파싱, fail-fast, secret 참조,
  Kafka API별 옵션 변환, 실행 위치에서 client 설정 생성.
- `streaming/jobs/sensor_validation_job.py`: 기존 처리 흐름에 client factory와
  source options 주입; `--runtime-config` 진입점; JSON rules 구조 검사.
- `config/job2-runtime.example.json`: 비밀값 없는 불완전 예제.
- `config/validation_rules.json`: 기존 JSON 구조의 명시적 simulator 검증용 규칙.
- `tests/unit/test_runtime_config.py`: fake 인증/클라이언트 경계 검증.

JSON 최상위 필드는 `kafka`, `registry`, `topics`, `silver_table`, `checkpoint`,
`processing_version`, `rules_path`다. 선택 필드는 `starting_offsets`,
`max_offsets_per_trigger`, `bronze_checkpoint`다. rules 경로는 JSON 파일 기준 상대
경로 또는 절대 경로다. 알 수 없는 runtime key와 inline password는 거부한다.
누락/빈 필수 값, 잘못된 파일/규칙, secret 참조 누락은 시작 전 실패한다.
`bronze_checkpoint`를 제공하면 같은 경로를 거부하며 기존 Job 2 checkpoint 경로
검사도 유지한다. 실제 모든 query의 checkpoint 중복 여부를 자동 탐색하지는 않는다.

`validation_rules.json`에 equipment registry를 포함하므로 별도
`config/equipment_registry.json`은 만들지 않았다. 기존 simulator의 numeric 장비
fridge/freezer/fryer/hood 네 개와 schema version 1.0.0만 포함한다. door/dishwasher의
문자열 지표는 numeric Avro 계약 대상이 아니다. 전체 운영 장비 목록·전역 ID 유일성
증빙으로 해석하지 않는다. rules를 runtime에서 simulator로부터 자동 생성하지 않는다.

## 인증과 Kafka API 변환

| 논리 설정 | Spark source | Python confluent producer |
|---|---|---|
| broker | `kafka.bootstrap.servers` | `bootstrap.servers` |
| SASL_SSL | `kafka.security.protocol` | `security.protocol` |
| MSK IAM | `databricks.serviceCredential` | `sasl.mechanism=OAUTHBEARER`, `oauth_cb` |
| 자동 topic 생성 | `kafka.allow.auto.create.topics=false` | `allow.auto.create.topics=false` |
| ACK | 기존 Spark source 계약 | `acks=all`, 성공 delivery callback 활성화 |

같은 broker/security를 공유하지만 source와 producer service credential 이름은
각각 지정한다. 기존 `ktd-msk-consumer`에 발행 권한이 있다고 추정하지 않는다.
MSK producer는 [Databricks service credential API](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/use-service-credentials)의
botocore session을 signer CredentialProvider로 감싼다.
[AWS IAM signer](https://github.com/aws/aws-msk-iam-sasl-signer-python)의 expiry
milliseconds를 confluent callback용 epoch seconds로 변환한다. 매 callback에서
자격 증명을 읽으며 토큰을 설정에 보관하지 않는다. 기존 `msk` dependency group의
signer와 botocore가 callback 실행 환경에도 설치되어야 한다.

명시적 `NONE` + PLAINTEXT/SSL은 인증 없는 로컬/서버 TLS용 경로다.
SCRAM/PLAIN/일반 OAUTH, mTLS의 인증서 연결은 구현하지 않았으며 자동 전환하지 않는다.

Registry Basic을 채택한다면 제안 참조는 다음 하나다. **실제 생성하지 않았다.**

| Scope | Key | Secret의 의미 |
|---|---|---|
| `ktd-job2` | `schema-registry-basic-auth` | Registry `username:password` 또는 `API key:API secret` |

[`dbutils.secrets.get(scope=..., key=...)`](https://docs.databricks.com/aws/en/security/secrets)으로
값을 해석한다. scope/key는 변경 가능하다. Kafka IAM에는 이 secret을 재사용하지
않으며 AWS access key를 JSON에 저장하지 않는다. producer UC service credential은
Databricks secret scope/key와 다른 리소스다.

query 시작 전에 secret 접근과 producer credential provider 생성을 검사한다.
이 과정은 broker/Registry 연결, IAM 권한, 토큰 서명 성공을 증명하지 않는다.
foreachBatch에는 값이 아닌 설정 참조를 넘기고 batch 실행 위치에서 다시 해석한다.
실제 DBUtils/service credential 사용 가능 여부와 Spark Connect callback 배포는
runtime 미검증이다. client 설정 객체의 repr에는 인증 dict를 숨기며 자체 secret/token
오류는 외부 예외 내용을 제거한다. 설정 dict나 token을 직접 출력하지 않아야 한다.

## 이후 실행 경로와 남은 준비

아래는 준비 완료 후 사용할 API 예시이며 이번 작업에서는 실행하지 않았다.
기존 notebook의 실제 SparkSession/dbutils를 주입한다.

```python
from streaming.jobs.runtime_config import start_configured_validation

# 실제 실행은 provider와 권한, topic 및 checkpoint 확인 후 진행한다.
query = start_configured_validation(
    spark,
    "/Workspace/<runtime-config-path>/job2-runtime.json",
    dbutils=dbutils,
    available_now=True,
)
```

로컬 Databricks Connect CLI 경로는
`python -m streaming.jobs.sensor_validation_job --runtime-config <path> --available-now`다.
이 경로에서도 callback 실행 위치의 모듈·dbutils 접근 검증이 남아 있다.

실제 E2E 전에는 다음을 준비·확인해야 한다.

- cloud Registry provider, endpoint, 인증 방식과 필요한 secret 참조 접근 권한.
- raw/validated subject의 SensorEvent 호환 schema. 자동 schema 등록은 하지 않는다.
- MSK broker 주소, region, source/producer UC credentials와 read/write 권한.
- validated/quarantine topic 존재와 접근 권한. 이전 read preflight의 실패는
  `TopicAuthorizationException`이며 topic 부재의 증거로 단정할 수 없다.
- Job 2 전용 checkpoint, Job 1의 실제 checkpoint와 비교, processing_version.
- callback 환경의 패키지/규칙 전달, secret/credential API 및 Registry/Kafka 연결.

SilverStorage, insert-only MERGE, publisher ACK, quarantine 처리와 checkpoint 진행
의미론은 변경하지 않았다. Bronze Managed Iceberg도 변경하지 않았다.

## 검증

- runtime 설정 + Job 2: **149 passed**.
- 기존 Phase 2 10개 unit 파일 + 신규 runtime 설정: **507 passed**
  (기존 431개 유지 + 신규 76개), 실패 0.
- Authlib 의존성의 httpx deprecation warning 1개. 실제 cloud 인증 검증과 무관하다.
- 범위: 파싱/필수값/규칙 구조, fake secret 조회와 노출 방지, API별 변환,
  IAM callback 갱신·expiry 단위, no mutation, fail-fast 및 batch factory 연결.
- 실제 Kafka→Job 2→Silver→validated E2E, 인증/권한과 callback runtime은 미검증이다.
