# P2-8A Confluent Cloud Registry 이관 준비

**준비 완료 / Cloud 실행 미실행**. 기존 local Registry를 GET으로
재조회하여 source export를 만들었다. Cloud resource·schema·secret을 생성하거나
변경하지 않았다. 이 문서의 Cloud 명령은 생성·이관을 별도로 진행할 때 사용한다.
현재 Kafka/Bronze inventory 결과는
`docs/evidence/phase-2/p2-8a/schema-id-inventory.json`에 보존한다.

최신 사용자 제공 상태: Cloud Registry와 API Key/Secret이 생성됐다.
AWS ap-northeast-2의 실제 endpoint를 반영한 **검토용 curl 순서는 7절**에 있다.
아래 2절의 생성 전 안내는 이전 준비 기록이며, 이번 실제 Cloud 요청은 미실행이다.

## 1. 준비된 파일과 source 확인

bundle 디렉터리는 `docs/evidence/phase-2/p2-8a/registry-migration/`다.
아래 파일명은 모두 이 디렉터리 기준이다.

| 파일 | 용도 |
|---|---|
| `source-export.json` | local Registry의 실제 ID/schema/subject/version/compatibility GET 응답과 조회 시각 |
| `manifest.json` | raw ID/version, 파일·구조 hash, validated 계약, 기존 fixture hash |
| `raw-v1.avsc`, `raw-v2.avsc` | Registry가 반환한 정확한 schema 문자열의 UTF-8 export |
| `raw-v1.import.json`, `raw-v2.import.json` | 명시적 `id`, `version`, `schemaType`, `references`, `schema`를 포함한 POST body |
| `validated-v1.avsc` | canonical 10-field v1. raw-v1과 동일한 schema 원문 |
| `validated-v1.register.json` | validated 등록 body. `id`와 `version`은 의도적으로 제외 |
| `registry-runtime.example.json` | provider/auth_mode/secret 참조. 실제 endpoint는 빈 값 |

실행 도구는 `scripts/registry_migration.py`, Databricks smoke 함수는
`scripts/registry_runtime_smoke.py`다. 도구를 import하는 것만으로 요청하지 않는다.

| ID | subject | version | schema | 현재 Kafka / Bronze v3 건수 |
|---|---|---:|---|---|
| 1 | kitchen.sensor.raw-value | 1 | SensorMetricEvent v1, 10필드 | 1 / 2 |
| 2 | kitchen.sensor.raw-value | 2 | v1 + nullable firmware_version, default null | 0 / 0 |

source는 AVRO, references=[], effective compatibility=BACKWARD다.
ID 2는 현재 raw에서 관측되지 않았지만 source Registry 이력 보존을 위해 포함한다.
Source GET schema, ID→subject/version, subject/version→ID/schema 및 기존
`schemas/avro/sensor_metric_event.avsc`,
`schemas/avro/compatibility/sensor_metric_event_v2_compatible.avsc`의 정의를 대조했다.

정확한 source 문자열 SHA-256:

```text
raw-v1 / validated-v1:
f1ee995b812fe2e1d7e78d0a001d545b0506330124a8e2e22ab7cf315d702938
raw-v2:
648dbf02f037e3412d1b1780841716c5072877c3d3376f15bf34e1079459a521
```

파일 끝에 추가 개행을 붙이지 않아 source schema 문자열 hash와 export 파일 hash가
같다. 이 hash는 보기 좋게 들여쓴 기존 `.avsc` 파일 hash와 다를 수 있다.
목적지 응답의 공백·객체 키 순서는 바뀔 수 있으므로 post-import 검사는 manifest의
`json_structure_sha256`으로 수행한다. 이는 JSON을 `sort_keys=True`,
`separators=(',', ':')`, `ensure_ascii=False`로 직렬화한 UTF-8 SHA-256이며
**Avro Parsing Canonical Form이 아니다**. 필드 배열 순서, nullable/default 및 모든
schema 내용은 보존해 비교한다. 값이 다르면 동등하다고 추정하지 않고 중단한다.

## 2. Cloud 생성 후 사용자가 진행할 순서

1. 사용할 Confluent Cloud 환경/region과 Registry를 확정·생성한다. 새 default
   context를 이관 대상으로 사용하고, 완료 전 다른 schema writer를 실행하지 않는다.
2. Registry HTTPS endpoint와 **Schema Registry용** API key/secret을 확보한다.
   Kafka broker API key로 대체하지 않는다. 관리용 자격 증명에는 subject mode,
   config, schema 등록과 조회 권한이 필요하다. 이번 준비는 그 권한을 검증하지 않았다.
3. 아래 offline → preflight → import-raw → verify-raw를 순서대로 실행한다.
4. raw 검증 성공 후 register-validated → verify-all을 실행한다.
5. Databricks에 조회용 credential을 저장하고 non-secret Registry 설정을 채운다.
6. Databricks driver에서 아래 smoke를 실행한다. PASS와 미관측/실패를 구분해 저장한다.
7. 이 smoke까지 검토한 뒤 별도 Job 2 E2E 단위로 진행한다. smoke 자체는 Job 2,
   producer, Silver sink, Quarantine publisher, streaming query를 시작하지 않는다.

명령은 저장소 루트에서 실행한다. 아래 URL은 프롬프트로 **실제 생성된 endpoint**를
입력한다. API key/secret은 도구의 숨김 입력에만 제공한다. 커맨드 인자·환경 변수·
문서·JSON·shell history에 실제 API key/secret을 넣지 않는다.

```bash
python3 scripts/registry_migration.py offline
read -r KTD_REGISTRY_URL
export KTD_REGISTRY_URL
python3 scripts/registry_migration.py preflight
python3 scripts/registry_migration.py import-raw --execute
python3 scripts/registry_migration.py verify-raw
python3 scripts/registry_migration.py register-validated --execute
python3 scripts/registry_migration.py verify-all
unset KTD_REGISTRY_URL
```

`read`가 대기하면 HTTPS base URL만 붙여 넣는다. 각 명령은 **직전 명령 exit 0과
PASS 확인 후 한 줄씩 실행**한다. `--execute`는 준비된 도구의 변경 동작을 명시적으로
선택하는 플래그이며, 이번 준비 작업에서 실제 실행하지 않았다. preflight만으로
향후 PUT/POST 권한까지 입증되지는 않는다.

## 3. Exact REST API sequence

base URL은 실제 Registry endpoint, context는 `.`(default)다. 모든 요청은
HTTPS Basic auth, `Accept`와 `Content-Type`은
`application/vnd.schemaregistry.v1+json`이다. 인증은 메모리에서 구성하고 출력하지
않는다. redirect, HTTP 오류, 잘못된 JSON이면 중단하며 mutation 재시도는 없다.

### A. 목적지 충돌 preflight — 전부 GET

| 순서 | 요청 | 반드시 확인할 결과 |
|---|---|---|
| A1 | `GET /mode` | `mode=READWRITE` |
| A2 | `GET /subjects?deleted=true` | `[]`. 삭제된 subject도 없어야 함 |
| A3 | `GET /subjects/kitchen.sensor.raw-value/versions?deleted=true` | HTTP 404 **및** error_code=40401 |
| A4 | `GET /subjects/kitchen.sensor.validated-value/versions?deleted=true` | HTTP 404 **및** error_code=40401 |
| A5 | `GET /schemas/ids/1` | HTTP 404 **및** error_code=40403 |
| A6 | `GET /schemas/ids/2` | HTTP 404 **및** error_code=40403 |

401/403/5xx/timeout/임의 404는 부재의 증거가 아니다. **어떤 기존 subject나 ID 1/2도
자동 진행하지 않는다.** 같은 ID에 다른 schema면 충돌, 같은 schema여도 기존 작업이나
부분 이관 상태일 수 있으므로 검토 후 재개한다. 이 도구는 새 목적지용이며 기존
Registry에 병합하거나 자동 복구하는 도구가 아니다. `DELETE`, `force=true`, 전역
IMPORT, source READONLY 변경은 사용하지 않는다. 새 목적지에 대한 단일 writer
운영은 사용자 책임이며, REST preflight는 원자적 잠금이 아니다.

### B. raw subject IMPORT — `import-raw --execute`

A1~A6을 다시 수행한 뒤 다음 요청을 실행한다.

| 순서 | 요청 | body / 검증 |
|---|---|---|
| B1 | `PUT /mode/kitchen.sensor.raw-value` | `{"mode":"IMPORT"}` → 응답 mode=IMPORT |
| B2 | `GET /mode/kitchen.sensor.raw-value` | mode=IMPORT |
| B3 | `GET /schemas/ids/1` | 여전히 404/40403이어야 함 |
| B4 | `POST /subjects/kitchen.sensor.raw-value/versions` | `raw-v1.import.json` 그대로 → 반환 id=1 |
| B5 | 아래 V 요청, raw/version 1/ID 1 | schema/ID/version/hash 모두 일치 |
| B6 | `GET /schemas/ids/2` | 여전히 404/40403이어야 함 |
| B7 | `POST /subjects/kitchen.sensor.raw-value/versions` | `raw-v2.import.json` 그대로 → 반환 id=2 |
| B8 | 아래 V 요청, raw/version 2/ID 2 | 모두 일치 |
| B9 | `PUT /mode/kitchen.sensor.raw-value` | `{"mode":"READWRITE"}` |
| B10 | `PUT /config/kitchen.sensor.raw-value` | `{"compatibility":"BACKWARD"}` |
| B11 | 아래 C 전체 | raw post-import 검증 |

schema body를 손으로 복사하거나 재직렬화하지 말고 준비된 JSON 파일을 사용한다.
IMPORT 중 version별 조회까지 성공한 뒤 READWRITE로 돌아간다. config의 PUT body는
`compatibility`, GET 응답은 `compatibilityLevel`이다.

**V: 개별 version 검증 요청** (`s`, `v`, `i`는 위에서 확정한 subject/version/ID):

```text
GET /subjects/{s}/versions/{v}
    subject=s, version=v, id=i, schemaType=AVRO, references=[]
    schema JSON structure SHA-256 = manifest 기대값
GET /schemas/ids/{i}
    같은 schema type/references/content/hash
GET /schemas/ids/{i}/versions?deleted=true
    {"subject": s, "version": v} 항목 포함
```

ID lookup 응답에 schemaType/references가 생략되면 API의 AVRO/[] 기본값으로 검사한다.
예상하지 않은 schema rules나 삭제 상태도 거부한다. 단순 ID 숫자 일치만으로 PASS하지 않는다.

### C. raw post-import verification — `verify-raw`

```text
GET /subjects
    kitchen.sensor.raw-value 포함
GET /subjects/kitchen.sensor.raw-value/versions
    [1,2]
GET /subjects/kitchen.sensor.raw-value/versions?deleted=true
    [1,2]
V(raw, 1, 1), V(raw, 2, 2)
GET /mode/kitchen.sensor.raw-value
    READWRITE
GET /config/kitchen.sensor.raw-value
    compatibilityLevel=BACKWARD
```

### D. validated 등록 — `register-validated --execute`

C 전체를 다시 확인한 뒤 실행한다. validated는 raw v2가 아닌 10필드 v1이다.

| 순서 | 요청 | body / 검증 |
|---|---|---|
| D1 | `GET /subjects/kitchen.sensor.validated-value/versions?deleted=true` | 404/40401 |
| D2 | `GET /mode` | READWRITE |
| D3 | `GET /mode/kitchen.sensor.validated-value?defaultToGlobal=true` | effective READWRITE |
| D4 | `PUT /config/kitchen.sensor.validated-value` | `{"compatibility":"BACKWARD"}` |
| D5 | `POST /subjects/kitchen.sensor.validated-value/versions` | `validated-v1.register.json`; id/version 강제 지정 없음 |
| D6 | 아래 E 전체 | 실제 반환 ID와 version 기록 |

동일 default context의 동일 schema이므로 예상 반환 ID는 1이다. 그러나 반환값을
실제로 확인하며 1이 아니면 중단한다. 새 validated subject의 예상 version은 1이다.
manifest의 actual_destination_id/version은 준비 시 null이며 실행 응답으로 입증한다.

### E. 전체 post-import verification — `verify-all`

```text
C 전체
GET /subjects
    kitchen.sensor.validated-value 포함
GET /subjects/kitchen.sensor.validated-value/versions?deleted=true
    [1]
GET /subjects/kitchen.sensor.validated-value/versions/1
    실제 id=1 / version=1 / subject 일치
V(validated, 1, 실제 반환 ID)
GET /mode/kitchen.sensor.validated-value?defaultToGlobal=true
    READWRITE
GET /config/kitchen.sensor.validated-value
    compatibilityLevel=BACKWARD
```

여기서 compatibility 검증은 **설정값과 schema 정의 보존** 확인이다. 실제 v1/v2
producer-consumer schema evolution E2E 성공을 뜻하지 않는다.

### 부분 실패와 재개

어느 단계든 실패하면 즉시 중단한다. timeout은 성공 여부 불명이므로 POST를
맹목적으로 반복하지 않는다. 특히 ID 1 이후 ID 2에서 실패하면 raw가 IMPORT로
남을 수 있다. mode/subjects/versions/ID/schema를 GET으로 대사한 후 남은 요청만
별도 검토한다. 자동 rollback·삭제·force·전체 import 재실행은 하지 않는다.
`verify-raw`는 미복구 IMPORT를 실패로 표시한다. validated 실패 후에도 같은 방식으로
조회해 실제 등록 여부를 확인한다. 기존 schema가 보이면 일반 preflight는 중단하는 것이
정상이며, 이미 완료됐는지는 `verify-raw`/`verify-all`로 확인한다.

## 4. Databricks non-secret 설정

`registry-runtime.example.json`은 **Registry 조각 설정**이며 전체 Job 2 runtime
파일이 아니다. 별도 local/workspace 복사본의 url을 실제 HTTPS endpoint로 채운다.
Job 2 연결 시에는 같은 `registry` 객체만 `config/job2-runtime.example.json` 기반의
실제 runtime 설정에 반영한다. 이번에는 Kafka/producer/checkpoint 설정을 바꾸지 않는다.

```json
{
  "registry": {
    "provider": "confluent",
    "url": "",
    "auth_mode": "basic",
    "credentials": {
      "scope": "ktd-job2",
      "key": "schema-registry-basic-auth"
    }
  }
}
```

| 값 | 의미 / 현재 상태 |
|---|---|
| registry.url | 생성 후 받은 HTTPS Registry base endpoint. 현재 미정, URL에 인증값 없음 |
| registry.provider | `confluent` 확정 |
| registry.auth_mode | `basic` 확정 |
| credentials.scope / key | `ktd-job2` / `schema-registry-basic-auth` 제안 참조. 이번 존재/권한/값 확인·생성 안 함 |
| secret 값의 형식 | `API_KEY:API_SECRET` 한 문자열. 실제 값은 Databricks secret 저장소에만 입력 |
| raw / validated subjects | 위 manifest의 두 subject, default context |
| MSK source credential | 기존 `ktd-msk-consumer`; Registry API credential과 별개 |

Databricks의 승인된 secret 입력 UI/보안 절차로 값을 저장하고 실행 주체의 읽기 권한을
확인한다. 관리용 API key와 runtime 조회용 key는 필요한 권한에 맞춰 구분할 수 있다.
값을 notebook 셀·문서·커맨드 인자·Git에 붙여 넣지 않는다. 기존
`RegistrySettings.resolve(dbutils)`가 `dbutils.secrets.get(scope=..., key=...)` 후
`basic.auth.user.info`를 메모리에서 구성한다. 이 설정 객체는 출력하지 않는다.

## 5. Cloud 이관 직후 Databricks smoke

전제: compute RUNNING, driver에서 저장소와 bundle 접근 가능, 기존 fixture
`tmp/phase1/avro-fixture.json`을 driver가 읽을 수 있는 경로로 준비, 필요한
`confluent-kafka[avro,schemaregistry]`, `fastavro`와 Spark 사용 가능. fixture는 이미
존재하는 파일을 복사하며 새 publish로 만들지 않는다. manifest의 payload SHA-256과
일치해야 한다. 파일이 없으면 원본을 확보할 때까지 fixture 단계를 미검증으로 둔다.

아래 코드는 Databricks notebook에서 실행한다. 경로와 broker는 실제 환경의
**non-secret 값**으로 바꾼다. root를 sys.path에 추가해 기존 decoder를 재사용한다.

```python
import json
import sys

repo_root = "/Workspace/실제/저장소/경로"
sys.path.insert(0, repo_root)
from scripts.registry_runtime_smoke import run_smoke

try:
    result = run_smoke(
        spark,
        dbutils,
        registry_config_path="/Workspace/실제/registry-runtime.json",
        bundle_directory=(
            repo_root + "/docs/evidence/phase-2/p2-8a/registry-migration"
        ),
        fixture_path="/Workspace/실제/avro-fixture.json",
        bootstrap_servers="실제 MSK IAM bootstrap broker 주소",
        service_credential="ktd-msk-consumer",
    )
except Exception as error:
    # 인증 라이브러리 예외 원문이나 traceback을 증빙에 출력하지 않는다.
    print(json.dumps({"status": "FAILED", "error_type": type(error).__name__}))
else:
    print(json.dumps(result, indent=2))
```

실행 결과를 별도 증빙 `docs/evidence/phase-2/p2-8a/registry-cloud-smoke.json`에
저장한다. 이 파일은 이번 준비 중 생성하지 않는다.

| smoke | 동작 / 기대 결과 |
|---|---|
| get_schema(1) | Cloud GET 후 manifest v1 content/hash 대조 |
| get_schema(2) | Cloud GET 후 manifest v2 content/hash 대조 |
| 기존 raw fixture ID=1 | payload hash 확인 → 기존 decode_sensor_event → 10필드/UTC-aware event_time |
| 현재 Kafka raw ID=1 | batch earliest~latest 중 실제 ID 1 표본 최대 1건 decode, topic/partition/offset 기록 |
| 현재 corrupt 표본 | null/짧은 payload/nonzero magic 최대 1건 → SensorDecodeError 코드 INVALID_CONFLUENT_FRAME, quarantine_candidate=true |

Kafka source는 service credential만 지정하며 충돌하는 `kafka.security.protocol`을
함께 넣지 않는다. auto topic creation=false, failOnDataLoss=true, batch read로
checkpoint·group commit·publish 없이 실행한다. 전체 retention 재-inventory가 아닌
**smoke 표본 조회**이며 정상/오류 표본은 서로 다른 batch action 시점일 수 있다.
현재 표본이 만료됐으면 `NOT_OBSERVED`, 전체는
`INCOMPLETE_CURRENT_SAMPLE_MISSING`이다. fixture/Bronze 결과를 현재 Kafka 성공으로
대체하거나 새 raw를 발행하지 않는다. 별도 승인을 받은 후속 범위에서 표본을 확보한다.

corrupt의 decode 실패는 Quarantine **후보 확인**이다. record build/publish/ACK 또는
Job 2 E2E는 수행하지 않는다. Registry 401/403/timeout은 데이터 오류로 분류하지 않는다.
이 smoke는 Databricks **driver**의 Registry 접근/decode를 검증한다. Spark executor의
Registry 접근, Job 2 foreachBatch 실행 위치, 전체 sink 전달/재시작 보장은 별도 검증이다.

## 6. 준비 검증과 commit 경계

- local Registry GET export 및 source artifact 대조: 실제 실행.
- `python3 scripts/registry_migration.py offline`: 실제 실행, 네트워크 없음.
- `tests/unit/test_registry_migration.py`: 충돌/오류/부분 실패 및 decode 후보 경계의
  오프라인 테스트. Cloud 권한/API/runtime smoke 성공 증거가 아님.
- Cloud resource 생성·preflight·IMPORT·register·Databricks Cloud smoke: 미실행.

변경 파일은 이 문서, bundle의 9개 파일, `scripts/registry_migration.py`,
`scripts/registry_runtime_smoke.py`, `tests/unit/test_registry_migration.py`,
`docs/reports/phase-2-verification.md`다. 기존 inventory는 수정하지 않았다.
검토한 뒤 직접 staging/commit한다. 보고서에는 기존 미커밋 변경도 포함되어 있으므로
함께 확인한다.

```bash
git diff -- docs/reports/phase-2-verification.md
git add docs/implementation/p2-8a-registry-migration.md docs/evidence/phase-2/p2-8a/registry-migration scripts/registry_migration.py scripts/registry_runtime_smoke.py tests/unit/test_registry_migration.py docs/reports/phase-2-verification.md
git diff --cached --stat
git commit -m "chore(phase2): prepare Registry migration and smoke"
```

Cloud 실행 후에는 verify-all의 실제 ID/version 및 smoke의 status/lineage 요약을
촬영하면 이관과 decode 사실을 보여준다. secret 입력 화면·API key·인증 설정 출력은
촬영하지 않는다.

## 공식 근거

- [Cloud subject mode API](https://docs.confluent.io/cloud/current/ccloud/update-mode/): subject IMPORT/READWRITE.
- [Cloud schema registration API](https://docs.confluent.io/cloud/current/ccloud/register/): 명시적 ID/version, 동일 schema의 ID 재사용.
- [Schema Registry API](https://docs.confluent.io/platform/current/schema-registry/develop/api.html): GET ID/version/config, deleted 조회 및 오류 코드.
- [ID/version 보존 이관](https://docs.confluent.io/platform/current/schema-registry/installation/migrate.html): 비어 있거나 없는 subject에 대한 import와 충돌 경계.

Confluent Cloud의 실제 권한·응답은 아직 검증하지 않았다. 서버 properties의
`mode.mutability` 설정을 Cloud에 적용한다고 가정하지 않는다.

## 7. 생성된 Cloud endpoint 대상 검토용 exact curl sequence

2026-10-10 추가 준비. 이번 중심 문서는 로컬 참고 자료
`docs/plans/ktd-forward-execution-plan-2026-10-06.md`이며 해당 계획서는 수정하거나
추적 파일로 복사하지 않았다. 이 절은 앞 절의 “Cloud 생성 전” 상태를 갱신한다.

- Cloud provider: **AWS**, region: **ap-northeast-2**.
- endpoint: `https://psrc-1oqld6g.ap-northeast-2.aws.confluent.cloud`.
- Registry 및 API Key/Secret 생성 완료는 사용자가 알려준 상태다.
- 이번 작업에서는 해당 endpoint에 GET/PUT/POST를 **하나도 실행하지 않았다**.
- AWS는 호스팅 provider이며 Job 2의 `registry.provider`는 **confluent**다.
- 기존 구현·테스트·source bundle은 변경하지 않았다. 아래는 검토용 명령이다.

### 7.1 공식 문서 재확인 결과

2026-10-10 확인한 [Cloud subject mode API](https://docs.confluent.io/cloud/current/ccloud/update-mode/)는
`PUT /mode/{subject}`의 mode enum에 IMPORT를 제공한다.
[Cloud registration API](https://docs.confluent.io/cloud/current/ccloud/register/)는
`POST /subjects/{subject}/versions` body에 정수 `id`와 `version`을 제공한다.
[이관 가이드](https://docs.confluent.io/platform/current/schema-registry/installation/migrate.html)는
빈 subject에 IMPORT를 적용해 ID/version을 보존하고 READWRITE로 복귀하는 절차를 설명한다.

따라서 **문서상 ID 1/2 보존이 가능**하다. 목적지 충돌·권한·실제 API 응답은 아직
검증하지 않았다. 일반 READWRITE 등록으로 먼저 ID를 발급받은 뒤 번호가 우연히
일치하기를 기대하는 절차는 사용하지 않는다. Cloud에 서버 properties 파일 변경이나
`mode.mutability=true` 설정을 요구하지 않는다.

기존 source-export/manifest/Avro artifact를 다시 대조했다. raw v1은 순서대로
`event_id`, `event_time`, `store_id`, `equipment_id`, `equipment_type`, `metric_name`,
`metric_value`, `unit`, `schema_version`, `source`의 10필드다. `metric_value`만 double,
나머지는 string이며 v2는 마지막에 `firmware_version: ["null", "string"]`,
`default: null`을 추가한다. validated는 raw v1과 동일하다. namespace/name은
`ktd.sensor.SensorMetricEvent`이며 ID/version은 `1/1`, `2/2`로 고정한다.
원문 및 구조 SHA-256은 1절과 manifest에 있고, 이번 offline bundle 검사를 통과했다.

### 7.2 검토용 Bash/curl 본문 — 아직 실행하지 않음

저장소 루트에서 **Bash**로 실행할 수 있도록 준비했다. 필요한 도구는 curl,
python3, jq다. 아래 전체 본문은 순차 실행되며 첫 오류에서 중단한다. 검토 후 별도로
이관을 진행하기로 했을 때만 사용한다. 이번에는 네트워크 호출 없이 `bash -n` 구문
검사만 수행했다. 기존 `scripts/registry_migration.py`의 schema/hash 검사 함수를
읽기 전용으로 재사용한다.

API key/secret은 숨김 입력으로 환경변수에만 보관한다. 인증 헤더는 curl의 stdin
config로 전달하므로 실제 값을 파일이나 curl 인자에 쓰지 않는다. `set -x`, curl
verbose/trace, 환경변수 전체 출력 또는 터미널 입력 녹화는 사용하지 않는다.

```bash
set +x
set -euo pipefail
cd /Users/uchan/Downloads/DE/02_project/kitchen_to_digital

SR_URL='https://psrc-1oqld6g.ap-northeast-2.aws.confluent.cloud'
BUNDLE='docs/evidence/phase-2/p2-8a/registry-migration'
RAW='kitchen.sensor.raw-value'
VALIDATED='kitchen.sensor.validated-value'
command -v curl >/dev/null
command -v python3 >/dev/null
command -v jq >/dev/null
python3 scripts/registry_migration.py offline

read -rs -p 'Schema Registry API key: ' SR_API_KEY
printf '\n'
read -rs -p 'Schema Registry API secret: ' SR_API_SECRET
printf '\n'
export SR_API_KEY SR_API_SECRET
trap 'unset SR_API_KEY SR_API_SECRET' EXIT
: "${SR_API_KEY:?API key required}"
: "${SR_API_SECRET:?API secret required}"

stop() {
    printf 'STOP: %s\n' "$1" >&2
    exit 1
}

# HTTP body와 status를 분리한다. body/인증 헤더는 로그에 출력하지 않는다.
sr() {
    local method="$1" path="$2" response
    shift 2
    response=$(
        python3 -c '
import base64
import os
value = os.environ["SR_API_KEY"] + ":" + os.environ["SR_API_SECRET"]
token = base64.b64encode(value.encode()).decode()
print("header = \"Authorization: Basic " + token + "\"")
' | curl -q --config - --silent --show-error \
            --proto '=https' --connect-timeout 10 --max-time 30 \
            --max-redirs 0 --request "$method" \
            --header 'Accept: application/vnd.schemaregistry.v1+json' \
            --header 'Content-Type: application/vnd.schemaregistry.v1+json' \
            --write-out '\n%{http_code}' \
            "$SR_URL$path" "$@"
    ) || stop 'Transport failure; do not retry mutation blindly'
    SR_STATUS=${response##*$'\n'}
    SR_BODY=${response%$'\n'*}
    printf '%s %s -> HTTP %s\n' "$method" "$path" "$SR_STATUS"
}

ok() {
    sr "$@"
    [[ "$SR_STATUS" == 200 ]] || stop 'Expected HTTP 200'
}

check() {
    printf '%s' "$SR_BODY" | jq -e "$@" >/dev/null \
        || stop 'Response mismatch'
}

absent() {
    sr GET "$1"
    [[ "$SR_STATUS" == 404 ]] || stop 'Existing resource or unexpected HTTP'
    check --argjson code "$2" '.error_code == $code'
}

schema_hash() {
    # $1은 manifest의 raw version 1 또는 2이며 validated는 1을 사용한다.
    printf '%s' "$SR_BODY" | python3 -c '
import json
import sys
from scripts.registry_migration import check_definition, load_bundle
manifest = load_bundle(sys.argv[1])
entry = manifest["raw"][int(sys.argv[2]) - 1]
check_definition(json.load(sys.stdin), entry)
' "$BUNDLE" "$1" || stop 'Schema type/references/content/hash mismatch'
}

verify_version() {
    local subject="$1" version="$2" id="$3" source_version="$4"
    ok GET "/subjects/$subject/versions/$version"
    check --arg s "$subject" --argjson v "$version" --argjson i "$id" \
        '.subject == $s and .version == $v and .id == $i'
    schema_hash "$source_version"
    ok GET "/schemas/ids/$id"
    schema_hash "$source_version"
    ok GET "/schemas/ids/$id/versions?deleted=true"
    check --arg s "$subject" --argjson v "$version" \
        'any(.[]; .subject == $s and .version == $v)'
}

verify_raw() {
    ok GET '/subjects'
    check --arg s "$RAW" 'index($s) != null'
    ok GET "/subjects/$RAW/versions"
    check '. == [1,2]'
    ok GET "/subjects/$RAW/versions?deleted=true"
    check '. == [1,2]'
    verify_version "$RAW" 1 1 1
    verify_version "$RAW" 2 2 2
    ok GET "/mode/$RAW"
    check '.mode == "READWRITE"'
    ok GET "/config/$RAW"
    check '.compatibilityLevel == "BACKWARD"'
}

# A. Collision preflight.200인 ID는 같은 schema라도 검토 없이 진행하지 않는다.
ok GET '/mode'
check '.mode == "READWRITE"'
ok GET '/subjects?deleted=true'
check '. == []'
absent "/subjects/$RAW/versions?deleted=true" 40401
absent "/subjects/$VALIDATED/versions?deleted=true" 40401
absent '/schemas/ids/1' 40403
absent '/schemas/ids/2' 40403

# B. Raw subject-level IMPORT. 이하 PUT/POST는 실제 변경 명령이다.
ok PUT "/mode/$RAW" --data-binary '{"mode":"IMPORT"}'
check '.mode == "IMPORT"'
ok GET "/mode/$RAW"
check '.mode == "IMPORT"'

absent '/schemas/ids/1' 40403
ok POST "/subjects/$RAW/versions" \
    --data-binary "@$BUNDLE/raw-v1.import.json"
check '.id == 1'
verify_version "$RAW" 1 1 1

absent '/schemas/ids/2' 40403
ok POST "/subjects/$RAW/versions" \
    --data-binary "@$BUNDLE/raw-v2.import.json"
check '.id == 2'
verify_version "$RAW" 2 2 2
ok GET "/subjects/$RAW/versions?deleted=true"
check '. == [1,2]'

ok PUT "/mode/$RAW" --data-binary '{"mode":"READWRITE"}'
check '.mode == "READWRITE"'
ok PUT "/config/$RAW" --data-binary '{"compatibility":"BACKWARD"}'
verify_raw

# C. Validated의 canonical v1 등록. 등록 body에는 id/version이 없다.
absent "/subjects/$VALIDATED/versions?deleted=true" 40401
ok GET '/mode'
check '.mode == "READWRITE"'
ok GET "/mode/$VALIDATED?defaultToGlobal=true"
check '.mode == "READWRITE"'
ok PUT "/config/$VALIDATED" \
    --data-binary '{"compatibility":"BACKWARD"}'
ok POST "/subjects/$VALIDATED/versions" \
    --data-binary "@$BUNDLE/validated-v1.register.json"
check '.id == 1'

# D. Post-import/registration 최종 GET 검증.
verify_raw
ok GET '/subjects'
check --arg s "$VALIDATED" 'index($s) != null'
ok GET "/subjects/$VALIDATED/versions"
check '. == [1]'
ok GET "/subjects/$VALIDATED/versions?deleted=true"
check '. == [1]'
verify_version "$VALIDATED" 1 1 1
ok GET "/mode/$VALIDATED?defaultToGlobal=true"
check '.mode == "READWRITE"'
ok GET "/config/$VALIDATED"
check '.compatibilityLevel == "BACKWARD"'
printf 'PASS: raw IDs/versions 1/1, 2/2; validated actual ID/version 1/1\n'
unset SR_API_KEY SR_API_SECRET SR_BODY
```

### 7.3 중단 기준과 미확인 사항

- ID 1/2 또는 raw subject의 HTTP 200은 **중단**이다. ID에 다른 schema가 있으면
  충돌이고, 같아도 이미 실행됐거나 부분 이관된 상태일 수 있다. 임의 overwrite/
  삭제/새 번호 할당으로 우회하지 않는다. validated subject도 같은 원칙을 적용한다.
- ID 부재는 404+40403, subject 부재는 404+40401만 인정한다. 401/403/5xx,
  HTML 응답, redirect, timeout을 부재로 처리하지 않는다.
- 새 목적지 전용 절차이므로 unrelated/deleted subject가 있어도 중단한다.
  preflight는 잠금이 아니다. 검토/실행 사이 및 이관 중 다른 schema writer가
  등록하지 않게 운영해야 한다. Cloud API가 실제 ID 점유 충돌을 거부하는지도 확인한다.
- IMPORT PUT의 실제 허용 여부, API key의 mode/config/register 권한은 아직 미확인이다.
  문서상 지원과 이 endpoint의 실측 성공을 구분한다. 403/42205면 권한/subject 상태를
  확인하며 `force=true`로 재시도하지 않는다.
- mutation timeout/부분 실패 시 전체 본문을 다시 실행하지 않는다. GET으로 mode,
  versions, IDs, schema를 대사해 다음 요청을 결정한다. raw가 IMPORT에 남을 수 있다.
  이 문서에는 자동 rollback이나 강제 삭제가 없다.
- validated는 같은 schema/default context에서 ID 1 재사용을 예상하지만 실제 POST
  응답 및 subject version GET으로 확인한다. 다른 ID이면 즉시 중단한다.
- schema 검증은 JSON 구조 hash를 사용한다. compatibility 검증은 BACKWARD 설정
  확인이며 schema evolution E2E를 대신하지 않는다. Databricks decode/smoke는
  이 REST 검토 단계 이후의 별도 작업이다.
- 현재 endpoint 인증·collision·import·post-import·runtime smoke는 전부 미실행이다.
  API key/secret은 받거나 저장하지 않았으며 기존 테스트 결과를 새 Cloud 성공으로
  표시하지 않았다. 이번에는 offline bundle 대조와 curl 본문의 정적 구문만 확인했다.
