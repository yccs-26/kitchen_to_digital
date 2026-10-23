# 최신 UC 후보 준비 상태 — 2026-10-23

UC 이름 기반 후보 Writer 및 Job 선택 경로를 추가했다. **실제 UC/Kafka/DBR E2E는 BLOCKED**다.
아래 이전 준비 기록은 이력이며 최신 판정은 문서 마지막 **실행 경로 통합**을 따른다.

# Phase 3 Kafka·Databricks E2E 실행 준비 판정

2026-10-23. Worktree `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows`,
branch `feat/phase-3-gold-windows`, 시작 HEAD `12024e7`.
기존 미커밋 작업이 존재했으며 보존했다. Git 조회/diff만 수행하고 쓰기 명령은 실행하지 않았다.
클라우드·Kafka·Registry 연결, 운영 table 생성·checkpoint 변경·실제 데이터 발행은 하지 않았다.

## 판정

**로컬 실행 준비 패키지는 작성했지만 live E2E 실행은 BLOCKED다.**
Gold 형식 승인, UC writer 구현/수용, 승인된 bounded raw publisher 및 DBR 장애 hook,
실제 환경/권한/비용 확인이 남아 있다. 문서의 명령을 실행 가능 배포 구현으로 과장하지 않는다.
Stage별 판정은 A 의사결정 자료 완료(승인 대기), B 공통 설정 완료(쓰기 차단),
C 정적 검사 완료, D 실행 절차 완료(일부 단계 차단), E 승인표 완료,
F 최종 로컬 회귀 완료다.

## 이번 배포 준비 기능

- `streaming/config/phase3_runtime.py`: 비밀값 없는 immutable 설정, GoldTarget 식별자,
  Kafka read 옵션, namespace/UTC/10분 W/두 window/Append/single writer 검증.
- `streaming/tools/phase3_preflight.py`: 파일만 읽는 CLI, 안전한 오류 경로, 명시적 BLOCKED와
  REQUIRES_LIVE_CHECK. 잘 구성된 파일도 UC writer 미구현 게이트를 해제하지 못한다.
- `config/phase3_gold.example.json`: 실제 이름·계정·Secret 값·비용 기본값 없이 null 템플릿.
  기존 프로젝트 JSON 방식과 Phase 2 SecretReference/식별자 검증을 재사용했다.
  YAML parser 의존성이 없어 YAML 후보 대신 JSON을 선택했다. 프로젝트 의존성은 변경하지 않았다.
- 기존 Job은 이미 명시적인 Kafka options/registry_factory/sink 주입을 받는다.
  새 설정을 자동 소비해 미승인 target에 쓰는 연결이나 UC writer는 추가하지 않았다.
- Preflight는 checkpoint **형식만** 검사한다. 존재/비어 있음/권한은 조회하지 않는다.
  socket·SDK·Spark 실행, Secret 읽기, cloud filesystem stat은 하지 않는다.
- 하드코딩 방지는 schema 밖의 password/token/JAAS 필드와 URL 내 인증값 등 알려진 구조를
  거부하는 범위다. 임의 문자열이 비밀인지 의미까지 증명하는 DLP 기능은 아니다.

## Gold 추천안과 승인 경계

권장 후보는 UC Managed Delta Gold다. 현재 Silver의 Delta 운영 지식과 로컬 불변 MERGE/
복구 증거를 재사용하는 비용 이점이 있다. ADR은 **승인 대기**이며 자동 채택하지 않았다.
Bronze Iceberg와 Silver Delta의 기존 구현·운영 저장 형식은 바꾸지 않는다.

로컬 path/forPath/_delta_log JSON adapter와 UC 3-part name/forName/catalog metadata는 다르다.
Managed table에는 이름으로 접근해야 하며 path 변경만으로 현재 adapter를 배포할 수 없다.
[Databricks 공식 tutorial](https://docs.databricks.com/aws/en/delta/tutorial)
[managed tables](https://docs.databricks.com/aws/en/tables/managed)

DBR 17.3 LTS는 문서상 Spark 4.0.0을 포함한다. 실제 build/access mode·UC API·직렬화·
Kafka connector·executor Registry 접근은 별도 live 수용 사항이다.
로컬 Delta 4.0.0이 DBR 내장 Delta와 같은 버전이라고 추론하지 않는다.
[공식 runtime](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)
[foreachBatch 제약](https://docs.databricks.com/aws/en/structured-streaming/foreach)

Iceberg 대안의 지원 API/constraint/abort·retry는 해당 runtime에서 별도로 검증한다.
어느 선택에서도 자연키·12컬럼·정확 비교·기존 값 보존 정책을 약화하지 않는다.

## 정적 Preflight 결과와 해석

```sh
.venv/bin/python -m streaming.tools.phase3_preflight \
  --config config/phase3_gold.example.json \
  --output /private/tmp/ktd-phase3-readiness-20261023/template-preflight.json
```

템플릿: static_validation=BLOCKED, overall=BLOCKED, 종료 코드 2.
이는 미입력 템플릿에 대한 의도된 안전 결과이며 테스트 실패가 아니다.
합성 완성 후보: static_validation=PASS, overall=BLOCKED.
승인 flag=true여도 UC_GOLD_WRITER_NOT_IMPLEMENTED/LIVE_E2E_NOT_AUTHORIZED는 남는다.
설정 flag를 인간의 승인이나 실제 지원 검증으로 사용하지 않는다.

live 항목은 workspace/runtime, Kafka topic/partition/offset/retention/권한,
Registry schema/Secret/executor, UC provider/schema/권한, checkpoint 실존/격리/권한,
외부 writer 통제, 실제 DBR plan/serialization, 계정 가격/크레딧/정지 통제다.

## 사용자 승인표 — 값 미입력, 실행 금지

| 승인 항목 | 현재 상태 | 제출할 증거/값 |
|---|---|---|
| Gold Delta 또는 Iceberg | PENDING | ADR 선택과 불변 저장 정책 확인 |
| Workspace | PENDING | 실제 host/profile, 실행 주체, 인증 경로 |
| UC Catalog/Schema/Table | PENDING | 정확한 3-part name/provider/owner, 신규 test target 범위 |
| UC 권한 | REQUIRES_LIVE_CHECK | USE CATALOG/SCHEMA, SELECT/MODIFY, 필요 시 CREATE TABLE |
| Kafka·Registry | PENDING | 승인 bootstrap/topic/schema ID/subject·네트워크·읽기/발행 주체 |
| Secret/Service Credential | PENDING | 참조명만, 실제 scope/key 권한과 executor 접근 |
| Checkpoint | PENDING | 운영과 분리된 승인 고정 경로, READ/WRITE VOLUME, 보존 정책 |
| DBR·Compute | PENDING | build/access mode, node/worker 수, timeout/동시 실행 설정 |
| 시간·비용 상한 | PENDING | 사용자 지정 시간·금액·포함 비용·독립 중단 감시 |
| 단일 writer | PENDING | Job concurrency=1과 타 Job/notebook/backfill 배제·권한 통제 |
| 입력 데이터 | PENDING | 등록 test 장비, event ID namespace/시각, ACK/offset, 실제 진행 이벤트 |
| 종료·정리 | PENDING | run 취소/종료 확인, 증빙 보존, 삭제 대상별 별도 승인 |

설정의 max_cost_usd는 금액 상한 선언이지 결제/과금 자동 차단 기능이 아니다.
가격·잔여 크레딧을 조회하지 않았고 추정 금액도 쓰지 않았다.
DBU와 compute instance, MSK/Registry, storage/전송/보존, maintenance의 포함 범위를
실제 승인 시 정한다. CLI timeout만으로 run이나 별도 compute 과금이 끝나지 않는다.

## 입력과 24행 재현 위험

오프라인 증빙에는 clock-001을 제외한 8레코드/고유7이벤트와 expected 24행을 준비했다.
과거 fixture timestamp/원본 장비 ID를 유지한 **발행 불가 후보**이며 manifest에 명시했다.
실제 발행 전 UTC 시각·등록 장비·ID namespace와 expected를 함께 변환·승인해야 한다.
현재 repo의 simulator는 랜덤/현재시각이며 정확한 24행 인증 replay publisher가 아니다.
후속 raw publisher는 raw→Phase 2→validated를 지나도록 별도 승인·구현한다.

Phase 2의 late/future 제한을 깨는 과거/미래 clock 발행을 금지한다.
최종 window 종료 +10분 이상 실제 event-time 진행이 필요하므로 합법적인 후속 입력과
실제 대기 시간/compute 수명을 승인한다. 부족한 시간·예산을 watermark 변경으로 우회하지 않는다.
공유 토픽의 배경 이벤트가 같은 장비·시각에 들어오면 expected가 달라지므로
등록된 격리 test 장비/통제된 구간과 ACK/offset 대사를 먼저 승인한다.

## Git 관계와 권장 commit 분리

로컬 main HEAD는 `7006ae6`(Phase 1)이고 main tree에 sensor_validation_job.py가 없다.
원격 fetch/접속은 하지 않아 원격 Phase 2 병합 여부는 미확인이다.
Phase 2 선행 병합과 UC/E2E 수용 완료 전 main merge는 유보한다.
Push 대상은 사용자 지정 `origin/feat/phase-3-gold-windows`이며 실제 push는 하지 않았다.

1. `feat(phase3): add secret-reference runtime config and static preflight`
   — runtime 모듈, JSON 템플릿, Preflight CLI, 한정된 설정 테스트.
2. `docs(phase3): prepare gated Kafka Databricks E2E execution package`
   — ADR 추천안, E2E Runbook, 이 readiness 보고서와 신규 증빙.

기존 미커밋 파일은 이번 변경과 분리해 검토한다. 자동 staging/commit/push/PR/merge는 없다.

## 남은 Phase 3 전체 DoD

Gold 형식 승인과 UC writer 구현/수용, 실제 Kafka·Registry·DBR 연결,
승인된 bounded 입력/ACK 대사, 실제 target 24행·late/idle·retry/복구,
single writer 권한 통제, 실제 비용·증빙 및 Phase 2 병합 관계 확인이 남아 있다.
로컬 설정 PASS와 mock/offline 검사로 이를 완료 처리하지 않는다.

## 최종 로컬 검증 결과와 재현

기존 환경을 재사용했다: Python 3.11.15, 임시 PySpark 4.0.0/Delta 4.0.0,
Temurin JDK 21.0.13+9, local[2], UTC. 일반 Spark와 Delta는 별도 프로세스로 실행했다.
원본 fixture/assertion·Phase 2·기존 Mapper/집계·의존성·과거 증빙은 변경하지 않았다.
로컬 테스트가 생성하는 임시 Delta/checkpoint만 사용했고 운영 target은 건드리지 않았다.

| 실행 | pass | fail/error | skip |
|---|---:|---:|---:|
| 신규 설정·Preflight | 30 | 0 | 0 |
| 전체 unit(신규 30 포함) | 990 | 0 | 0 |
| 기존 Streaming 관련 | 10 | 0 | 0 |
| 기존 Gold 통합·복구 + 확장 검사 | 15(13+2) | 0 | 0 |

전체 unit은 기존 Authlib warning 1건, 2.69s였다. Streaming은 46.20s,
Delta/복구는 131.48s였다. 기존 10건 중 하나는 수동 표 Python 검사로 실제 query가 아니다.
Preflight 30건은 전체 unit에 포함되므로 별도 합산하지 않는다.
구문·공백·PEP 8 및 git diff --check도 통과했다.

```sh
.venv/bin/python -m pytest tests/unit/phase3/test_phase3_preflight.py \
  -q -p no:cacheprovider \
  --junitxml=/private/tmp/ktd-phase3-readiness-20261023/preflight.xml
.venv/bin/python -m pytest tests/unit -q -p no:cacheprovider \
  --junitxml=/private/tmp/ktd-phase3-readiness-20261023/unit.xml
```

각 Spark 명령은 다음 env prefix로 시작했다.

```sh
env JAVA_HOME='/private/tmp/ktd-phase3-integrated/jdk/jdk-21.0.13+9/Contents/Home' \
  PYSPARK_PYTHON=/private/tmp/ktd-phase3-integrated/venv/bin/python \
  KTD_LOCAL_IVY=/private/tmp/ktd-phase3-integrated/ivy \
  PYTHONDONTWRITEBYTECODE=1 \
  /private/tmp/ktd-phase3-integrated/venv/bin/python -m pytest \
  tests/integration/test_phase3_streaming.py \
  tests/integration/test_phase3_sliding_window.py -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-readiness-20261023/streaming \
  --junitxml=/private/tmp/ktd-phase3-readiness-20261023/streaming.xml
```

동일 prefix를 사용하되 별도 프로세스에서 다음 pytest 인자를 실행했다.

```sh
  tests/integration/test_phase3_gold_delta.py \
  tests/integration/test_phase3_metric_aggregation.py \
  tests/integration/test_phase3_streaming_gold.py \
  tests/integration/test_phase3_checkpoint_recovery.py \
  tests/integration/test_phase3_recovery_extended.py -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-readiness-20261023/delta \
  --junitxml=/private/tmp/ktd-phase3-readiness-20261023/delta.xml
```

재현 시 이전 증빙이 지워지지 않게 새 임시 basetemp를 선택한다.
새 source/doc 경로는 아래와 같고 증빙 경로는 별도 폴더다.

- `streaming/config/phase3_runtime.py`
- `streaming/tools/phase3_preflight.py`
- `config/phase3_gold.example.json`
- `tests/unit/phase3/test_phase3_preflight.py`
- `docs/adr/phase-3-gold-table-format.md` (기존 초안에 추천 근거 추가)
- `docs/runbooks/phase-3-e2e-execution.md`
- `docs/reports/phase-3-e2e-readiness.md`
- `docs/evidence/phase-3/e2e-readiness-2026-10-23/`

정확한 전체 경로·변경 목록·hash·Git 현황, 로그/JUnit, 정적 BLOCKED 결과,
발행 불가 candidate/expected는 위 증빙의 README와 manifest에 연결한다.
실제 Kafka/Databricks E2E 증빙이나 비용 실측 자료는 없다.

## 최종 UC 후보 준비 부록 — 2026-10-23

### Stage 판정

| Stage | 판정 | 완료 내용 / 남은 게이트 |
|---|---|---|
| A | 로컬 후보 완료 / 실제 UC 미실행 | 이름 기반 Writer, 기존 계약 재사용, 로컬 Delta 9건 |
| B | 연결 준비 완료 / live launcher BLOCKED | 명시적 adapter 선택, Preflight 재사용, callback은 table 문자열만 캡처 |
| C | 오프라인 완료 / 실제 연결 미실행 | 기존 Phase 2 serializer/decoder·Avro schema 재사용, 12건 |
| D | 최종 로컬 회귀 완료 | Unit 1019, 기존 Streaming10, 기존 Gold15, 신규 이름 기반9 |
| E | 준비 문서 완료 / 실행 승인 대기 | 명령 참조·증빙·Git 분리안. 실제 E2E는 시작하지 않음 |

Phase 3 전체 DoD는 미충족이다. 실제 UC/DBR/Kafka와 checkpoint 복구, 형식 승인,
권한·single writer 통제 수용, Phase 2 선행 병합 확인을 완료로 바꾸지 않았다.

### 실제 구현과 계약

- `streaming/sinks/gold_uc_delta.py::GoldUCDeltaSink(spark, table, single_writer=True)`.
  table은 `catalog.schema.table`만 허용하며 기존 `quoted_table`로 검증한다.
  `merge_rows(rows)` 반환은 처리한 입력 행 수다. INSERT 행 수가 아니다.
- `Catalog.getTable`로 MANAGED/비임시 속성 확인, `DeltaTable.forName`으로 Delta 확인,
  read schema의 12개 이름/물리 타입 확인 후 MERGE한다. 자동 DDL/GRANT/LOCATION,
  path/직접 log 접근, schema evolution은 없다. 빈 입력은 전량 소비 후 0을 반환한다.
- 기존 `GoldDeltaSink`의 source 검증과 MERGE 표현식을 `prepare_gold_rows`,
  `merge_final_rows`로 추출했다. 자연키5/불변값7·mapper·signed zero·정확 비교는 같다.
  source 동일 키 중복은 쓰기 전 실패, 신규 INSERT, 동일 final NO-OP,
  상이 final은 raise_error로 MERGE 전체 실패다. 누적 합산은 없다.
- `gold_batch_callback`은 UC 후보의 table 문자열만 캡처하고 `batch.sparkSession`으로
  Writer를 재구성한다. 기존 local path callback의 freevars=(path,)도 유지됐다.
  기존 두 window branch/10분 W/Append/단일 checkpoint 구조는 바꾸지 않았다.
- 명시적 `adapter='uc_managed_delta'` 인자와 검증된 설정으로
  `prepare_metric_deployment`가 후보를 선택한다. 로컬 path로 암묵 전환하지 않는다.
  `start_configured_metric_aggregation`는 live gate에서 항상 중단한다.
  설정 flag가 실제 승인·실존·권한·배타 lock을 뜻하지 않는다.
- Preflight의 과거 `UC_GOLD_WRITER_NOT_IMPLEMENTED` 차단 코드는 기존 소비자/30개 테스트
  호환을 위해 유지했다. `writer_candidate`에 후보 구현 완료/실제 수용 필요를 별도 표시한다.
  이 코드는 현재 승인된 live launcher가 없다는 뜻이며 후보 모듈 부재를 뜻하지 않는다.

### NOT NULL metadata API 실험과 정확성 한계

추가 시도에서 OSS Spark4 Delta V2는 DESCRIBE TABLE EXTENDED AS JSON에
`NOT_SUPPORTED_COMMAND_FOR_V2_TABLE`을 반환했다. 시도 결과는 4 passed/5 failed였고
`named-metadata.xml/log`에 보존했다. 이를 무시하거나 path log로 우회하지 않았다.
최종 공통 Writer에서 미지원 API 의존을 제거하고 DBR 수용 검사로 분리했다.
최종 동일 9건은 모두 통과했다. 기대값과 기존 Assertion은 변경하지 않았다.

Writer 입력의 NOT NULL은 생산 Mapper가 검증한다. **대상 read schema 타입 검사는
NOT NULL DDL을 입증하지 않는다.** 로컬 fixture의 실제 NOT NULL은 잘못된 NULL append의
Delta 거부로 검증했다. 실제 UC의 12 NOT NULL DDL·기존 NULL/중복키 부재는 승인된
수용 절차에서 확인해야 하며 현재 미검증이다. nullable target이 타입 검사만 통과할 수 있으므로
이 수용 게이트 전에 live 실행하면 안 된다. 동시 DDL/외부 writer도 배제해야 한다.

[DBR DESCRIBE JSON](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-syntax-aux-describe-table)은
16.2 이상 지원으로 문서화되어 있다. 문서상 API와 OSS/실제 UC 동작을 동일시하지 않는다.
[forName](https://docs.delta.io/api/latest/python/spark/index.html)은 이름 기반 Delta 접근을 제공하며
로컬 4.0.0 설치에서 실제 실행했다. 이 링크의 latest 버전 자체가 로컬 설치 버전 증거는 아니다.
[UC managed](https://docs.databricks.com/aws/en/tables/managed),
[foreachBatch](https://docs.databricks.com/aws/en/structured-streaming/foreach),
[DBR17.3](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)를 대조했다.

단일 MERGE의 원자성과 checkpoint 완료는 별개다. 기존 local SIGKILL/재시작 증거를
실제 UC/Kafka 복구로 확대하지 않는다. 자연키 UNIQUE/분산 lock은 없고 단일 writer 선언만 있다.
드라이버 전량 materialization은 초기 소규모 final 출력용이며 대규모 성능 검증은 하지 않았다.
부동소수점 epsilon은 없고 1 ULP/누적 순서 차이는 여전히 CONFLICT다.
동일 event_id 상이 payload는 native dedup이 탐지하지 않으며 upstream 불변 보장에 의존한다.

### 최종 실행 결과와 실패 이력

환경: Python3.11.15, Temurin21.0.13+9, PySpark4.0.0, delta-spark4.0.0.
기존 임시 검증 환경을 재사용했으며 프로젝트 의존성/lockfile/Java 기본값을 바꾸지 않았다.

| 범위 | 최종 결과 | 증거 |
|---|---|---|
| 전체 Unit | 1019 passed, 기존 Authlib warning1 | unit-verified.xml/log |
| Phase3 Unit | 305 passed, warning1 (위 전체에 포함) | phase3-verified.xml/log |
| 신규 Unit | Writer12 + 선택5 + 입력12 = 29 passed (위에 포함) | JUnit testcase 경로 |
| 기존 Preflight | 30 passed 유지 (Unit에 포함) | JUnit test_phase3_preflight |
| 기존 Streaming | 10 passed | streaming.xml/log |
| 기존 Gold 통합/복구 | 15 passed, SIGKILL 포함 | delta.xml/log, recovery JSON |
| 신규 이름 기반 Delta | 9 passed | named-final.xml/log |
| UC/Kafka/DBR | 미실행 / BLOCKED | 접속/생성/발행/compute 실행 안 함 |

신규 9건: INSERT/NO-OP replay1, conflict 전체 중단3(unit/min/max), empty/duplicate/실제NULL1,
테이블 없음/자동 생성 없음1, schema 불일치1, callback pickle1, 실제 Streaming24행1.
이름 기반 Streaming은 1분6행+5분18행이 모든 컬럼에서 expected와 정확 일치했다.
실제 W는 epoch→2026-10-20T12:15:30Z, state operators는 두 batch 모두4개다.
최종 append는 W가 지나고 후속 batch를 처리한 후 생성됐다.
기존 idle/late/multi-batch 검증은 Streaming10으로 별도 유지했다.
SIGKILL 기존 회귀에서는 같은 checkpoint의 batch1을 재실행하고 Gold24행이 불변이었다.

초기 sandbox 실행9건은 localhost bind 거부로 JVM 시작 전에 error였다. 로컬 JVM 권한으로
재실행했다. 새 offline serializer 테스트 초기 실패는 테스트 stub에 기존 Phase2와 같은
`topic_subject_name_strategy`를 명시하지 않은 것이 원인이었다. 테스트 설정만 맞추고
기존 생산 serializer/decoder·입력·기대값은 보존했다. 실패를 skip/xfail로 바꾸지 않았다.
최종 구문/PEP8/공백 검사와 기존 파일 hash 보호 검사 결과는 evidence checks에 기록한다.

정확한 실행 명령은 증빙 `commands.txt`에 보존한다. 일반 Spark와 Delta는 별도 pytest/JVM,
고유한 임시 source/table/checkpoint를 사용했다. 실존 cloud 리소스는 조회하지 않았다.

### E2E 시작 순서와 승인 항목

1. Gold 형식 선택(ADR은 승인 대기), Phase2 선행 병합 확인.
2. Workspace·DBR/compute·시간·금액·종료 감시와 UC 정확한 target/권한/DDL 승인.
3. 운영과 격리된 고정 checkpoint·소유권·보존 및 single writer 배제 수단 승인.
4. Kafka/Registry 인증·executor 접근·schema/등록 장비/offset 범위 승인 후 live read 수용.
5. UC 후보의 실제 metadata/NULL 제약·MERGE conflict abort·callback 수용.
6. 별도 검토로 live launcher 게이트 연결, 결정적 인증 raw publisher와 DBR 장애 hook 준비.
7. 승인된 입력/시각/expected, 실제 합법적인 W 진전으로 24행 대사, 같은 CP 재시작/장애 복구.
8. 오류/상한이면 중단, 결과·청구·종료 증거 수집, 정확한 테스트 대상별 삭제는 재승인.

실제 비용/크레딧은 미조회다. 선언한 금액·시간이 자동 결제 차단이나 compute 종료를 보장하지 않는다.
실제 실행 명령·성공/중단 기준은 runbook과 새 command reference를 따른다.

### 변경 파일·Git·포트폴리오 증빙

이번 14개 source/doc 변경 및 이전 미커밋 파일은
`docs/evidence/phase-3/uc-e2e-preparation/changes.json`, `source-changes.diff`,
`git-status-before.txt`, `git-status-after.txt`로 구분한다. 기존 테스트 Assertion/fixture,
Phase2, 의존성 파일은 수정하지 않았다. 기존 다른 증빙은 보존했다.

권장 commit 분리는 같은 폴더 `git-plan.md`의 정확한 파일별 명령을 따른다.
**명령은 제안만 했으며 staging/commit/push/PR/merge는 실행하지 않았다.**
로컬 main은 7006ae6(Phase1)이며 Job2 파일이 없다. 원격 fetch 없이 원격 Phase2 병합은
확인할 수 없다. push 대상은 origin/feat/phase-3-gold-windows이며 main merge는 전체
DoD와 실제 UC/Kafka/DBR/같은 checkpoint 복구 확인 이후에만 검토한다.

현재 포트폴리오 증빙은 local named Delta, expected/actual24, W/state4, local SIGKILL,
Unit/JUnit·정적 BLOCKED 결과다. 실제 UC 화면·Kafka ACK·DBR run/청구 자료는 없다.
촬영할 승인 후 화면과 로그는 command reference 마지막 절에 지정했다.


## 승인 전 최종 기술 점검 — 2026-10-23

**기존 로컬 구현 DoD는 실행 증거 기준 충족, 전체 Phase3 DoD/E2E는 BLOCKED다.**
최신 현재 Git/placeholder 통합 승인표·명령 분류·실행/중단 순서는
[승인 체크리스트](phase-3-e2e-approval-checklist.md)에 모았다. 실제 승인값은 채우지 않았다.

- 현재 branch `feat/phase-3-gold-windows`, HEAD `ad0f999`; 로컬 upstream은
  `origin/feat/phase-3-gold-windows`의 저장된 ref `12024e7`이며 ahead2/behind0이다.
  이전 구현·문서는 이미 `0246de9`/`ad0f999` commit에 포함되어 있다.
  작업 시작 시 tracked 수정/staging은 없고 기존 증빙155개만 미추적이었다.
- local main 및 origin/main은 `7006ae6`(Phase1), main은 HEAD의 조상이지만
  현재 Phase2 tip `42dd5c9`는 main/HEAD 어느 쪽에도 포함되지 않는다.
  Phase2/HEAD 공통 조상은 `b8c6ee2`, Phase2 후속5개가 현재 Phase3에 없다.
  Registry 404 분류·Quarantine ACK 검증 보강, Phase2 E2E 도구 등이 포함된다.
  따라서 main의 포함과 Phase2 병합 후 기준 정렬 완료를 구별한다. 원격 최신 상태는 UNKNOWN이다.
- Gold 권장 후보는 UC Managed Delta이며 미승인이다. Iceberg 대안은 새 Gold writer/API/
  제약·복구 검증과 metadata 유지 요구를 추가 검토해야 한다. 상대적인 구현 노력만 비교했으며
  cloud 비용 우열이나 가격·크레딧을 추정하지 않았다. Bronze/Silver 데이터·형식은 변경하지 않는다.
- `start_configured_metric_aggregation`는 항상 deployment gate에서 실패한다.
  `gold_format_approved=true`도 해제하지 못한다. low-level 실행 API는 있으나 승인 통제가
  자체 내장된 경로가 아니므로 이를 호출해 우회하지 않는다. 실제 승인 후에도 별도 launcher 검토가 필요하다.
- 실제 executor Registry 인증 factory, 결정적 raw replay publisher, DBR 실패/재시작 hook과
  관측 연결은 준비가 남았다. CLI run-now/DDL 예제는 완성된 배포 도구가 아니다.
  config load/정적 검사, 승인 후 cloud read/write, 미구현 도구를 체크리스트에서 구분했다.
- 기존 fixture 마지막 target window는 base B+10분에 끝난다. W가 이를 지나려면
  B+20분 이후의 합법적인 실제 event-time과 후속 출력 처리가 필요하다. 마지막 target 입력이나
  idle wall clock만으로24행 완료를 선언하지 않는다. 시간·예산 부족 시 미확정으로 중단한다.
- 이번 fresh 검증은 기존 Preflight30+후보 선택5 **35 passed**, 템플릿 Preflight **exit2/BLOCKED**,
  기존 UC 후보 증빙 manifest26개 **누락/불일치0**이다. 전체 Unit1019/Phase3 Unit305/
  Streaming10/Gold15/이름 기반9는 기존 증거를 대조했으며 이번에 재실행한 수치가 아니다.
- 로컬 설치 Python3.11.15/Temurin21.0.13+9/PySpark4.0.0/Delta4.0.0을 재조회했다.
  실제 Workspace/DBR/UC/Kafka/비용은 조회하지 않았다. 기존 증빙과 생산 코드는 보존했다.

이번 변경은 이 readiness와 신규 승인 체크리스트2개뿐이다. 기존 runbook 명령을 실제
승인된 runnable E2E로 표시하지 않았으며 새 기능/테스트/인프라를 추가하지 않았다.
권장 문서 commit 및 기존 미추적 증빙의 별도 검토·push 제안은 체크리스트 §7을 따른다.
Git add/commit/push/merge/rebase, cloud/Kafka 접속·발행·테이블 생성·유료 compute 실행은 하지 않았다.

## 실행 경로 통합 — 2026-10-23 후속 구현

### 결론과 Git baseline

**실행 코드와 로컬 회귀는 보완했으나 실제 UC/Kafka/DBR E2E 및 Phase3 전체 DoD는 미완료다.**
시작 HEAD는 `ad0f999`, branch는 `feat/phase-3-gold-windows`였다.
시작 시 readiness 수정과 승인 체크리스트·기존 증빙이 미추적 상태로 있었으며 보존했다.
로컬 main은 `7006ae6`, 저장된 Phase3 upstream은 `12024e7`(ahead2/behind0)이었다.
fetch나 Git 쓰기는 하지 않았다. 원격 최신 상태는 UNKNOWN이다.

이번 시작 시 Phase2 로컬 tip은 `3d772bd`로 진행되어 기존 보고5개 대신 **후속12개**가 없다.
merge-base는 `b8c6ee2`다. commit별 파일/역할은 새 증빙 `phase2-history.txt`에 기록했다.

| 누락 commit | 역할 |
|---|---|
| 8392b77 | Quarantine ACK non-bool 정수 경계 보강 |
| f2fa1f2 / 888a59c | output topic provisioning 준비 문서 |
| e266a54 | Registry40403 구분과 Job2 로컬 복구 증거 |
| 42dd5c9 | 격리된 Phase2 cloud E2E 도구 |
| 59476f4 | 로컬 runtime/credential ignore |
| 9772caa | Registry migration/초기 validation artifacts와 도구 |
| 3d7083e | producer credential 참조·routing test 정렬 |
| faa0682 / f2ea39e | provisioning/cleanup·로컬 검증 증거 보존 |
| 0c0b9dc | Phase2 입력 시간창/준비 차단 검증 |
| 3d772bd | 누적 Git 감사·commit 경계 문서 |

Phase2 변경 경로와 이번 P3 변경 경로는 직접 겹치지 않았다. 실제 merge는 시도하지 않아
충돌 없음이나 의미적 통합 완료를 보증하지 않는다. .gitignore·새 Phase2 도구/테스트·실제
producer credential 참조·공유 Registry 예외 분류는 merge 후 재검토한다.
현재 main이 HEAD의 조상인 것과 Phase2 선행 main 병합 완료는 다르다.

### Phase2 계약 대조와 실제 입력

- 최신 로컬 Phase2와 현재 HEAD의 sensor_metric_event.avsc, validated_publisher.py,
  UTC normalizer에는 차이가 없다. topic `kitchen.sensor.validated`, UTF-8 equipment_id key,
  Confluent magic0/schema ID/Avro10필드, event_id·schema_version·metric/unit 계약을 유지했다.
  기존 serializer→P3 decoder12건도 전체 회귀에 포함한다.
- Silver/Kafka는 공동 원자성이 아니고 동일 ID/payload가 재발행될 수 있다. offset/checkpoint는
  Spark source가 관리하며 새 adapter에서 manual commit하지 않는다.
- 기존 Phase2 decoder는 HTTP404 전체를 데이터 오류로 분류하지만 후속 Phase2는40403만
  unknown-schema로 분류한다. P2 파일을 바꾸지 않고 P3 `_RegistryBoundary`에서40403 외404를
  runtime 서비스 오류로 분리했다. 40403은 기존 SensorDecodeError, 다른 Registry 장애는 전파한다.
  wire 호환과 Git 계보 불일치/오류 분류 차이를 구분한다.
- **정정:** 현재 Phase2의 유효한 late 이벤트는 LATE 분류 후 Silver/validated로 전달된다.
  late를 항상 Quarantine한다고 한 과거 가정은 적용하지 않는다. 새 fixture는 실제 W 이후
  P3 drop을 기대하며 future-invalid/도메인 오류와 구분한다.
- `streaming/sources/validated_kafka.py`로 기존 reader의 UDF 변환을 추출했다. 실제 Kafka
  source와 local binary DataFrame이 같은 변환을 쓴다. 필수10필드·UTC·key 검사를 유지한다.
  local worker가 native Registry client/dbutils/credential을 캡처하지 않는 경로를 검증했다.

### 범위 승인 launcher / Publisher / Probe

`ExecutionApproval`은 별도 검토 기록으로 설정 digest, UTC 만료, job tag, 허용 action,
격리/UC 수용/single writer/P2 baseline/비용 통제 확인과 입력 hash·worker 환경변수 이름을
명시한다. 설정 flag 단독, 만료, 범위/설정 불일치는 실행되지 않는다. 기록 파일이나 boolean이
실제 사람 승인·권한·리소스 실존을 증명하는 보안 서비스라고 주장하지 않는다.
실제 승인 기록/리소스 값은 이번 작업에서 만들지 않았다.
Preflight는 승인 파일을 읽지 않는 STATIC_ONLY 도구이므로 overall=BLOCKED를 유지한다.
launcher는 그 static_validation과 별도 ExecutionApproval을 각각 검증한다. 기존 legacy 차단
코드는 호환용으로 남겼으며 실제 UC 수용이 자동으로 완료됐다는 신호로 바꾸지 않았다.

`start_configured_metric_aggregation`와 `main`은 범위 승인 후 실제 Kafka→기존 plan→
Mapper→UC 이름 Writer→같은 CP로 연결한다. 기존 local Delta API도 보존했다.
main은 기존 승인 DBR session만 사용하고 제한 시간 await/정상 stop/오류 전파를 제공한다.
반드시 외부 Job timeout·종료/비용 감시도 필요하며 compute 과금 hard cap은 아니다.
단일 query·두 branch/W10분/Append와 checkpoint 구조를 바꾸지 않았다.

worker Registry 인증은 승인 compute에 Secret 참조로 주입한 환경변수를 지연 해석한다.
closure에는 URL/환경변수명만 저장하고 credential은 넣지 않는다. 실제 환경변수 전파/Secret
scope-key binding·cluster 사용자 노출 통제·executor network는 별도 DBR 수용 대상이다.
로컬 Spark venv에 없는 fastavro/confluent_kafka는 기존 프로젝트 site-packages를 PYTHONPATH로
재사용했다. 설치·의존성/lockfile 변경은 없다. 실제 Kafka connector는 미검증이다.

Publisher CLI는 항상 dry-run이다. scope 승인된 API는 validated 직접 쓰기 대신 **격리 raw→P2**를
사용하고 기존 P2 인증/serializer를 재사용한다. 전체 장비 mapping·fixture hash·topic/bootstrap/
Registry route·domain/future 규칙을 client 생성 전에 검사한다. batch별 ACK offset·partition을
검증하며 누적 발행은 원자적이지 않다. 실제 진행 관측과 장비/시각·offset 승인은 외부 책임이다.
정상/중복/경계8건(고유7), 진행1, late1과 target24행을 준비했다. dry-run 과거 시각/미등록
progress 장비를 실제 발행하지 않는다. `--equipment-map`으로 모든 장비와 expected를 함께
이동하고 승인한 후에만 발행 API를 사용할 수 있다.

FailureProbe는 승인된 job tag/CP/batch의 before_merge 또는 after_merge에 예외를 주입한다.
외부 Job 취소·강제종료 API를 호출하지 않는다. main timeout 정상 stop, callback 실패,
기존 전용 local SIGKILL은 구분한다. 재시작 시 probe를 빼고 같은 CP로 replay한다.
DBR get-run/cancel-run의 실제 Job identity 확인·종료/재시작과 강제 장애 수용은 미실행이다.

### 실제 테스트와 실패 이력

| 범위 | 최종 결과 | 근거 |
|---|---|---|
| 전체 Unit | 1043 passed, 기존 Authlib warning1 | unit-verified.xml/log |
| Phase3 Unit | 329 passed (전체에 포함) | phase3-verified.xml/log |
| 신규 Unit | 24 passed (위에 포함) | 실행17 + source runtime7 |
| 기존 Streaming | 10 passed | streaming.xml/log |
| 기존 Gold/복구/이름 기반 | 24 passed (15+9) | delta.xml/log, SIGKILL 유지 |
| 새 binary worker/Streaming source | 5 passed | source-verified.xml/log, wire-streaming-24.json |
| 새 실제 Delta probe 복구 | 2 passed | probe.xml/log, before/after recovery JSON |
| dry-run | 10 입력 / target expected24, 발행0 | dry-run.json / summary |
| actual Kafka/UC/DBR | 미실행 | mock/local 증거로 대체하지 않음 |

새 wire Streaming은 각 입력 batch 후 실제 progress를 기록했다. 진행 행 후 no-data batch7에서는
1분6행만 나왔고 다음 batch8에서 sliding18행이 추가되어24행이 됐다. 해당 late 입력은 두
native dedup operator에서 drop됐다. W는12:15:30Z, stateful operators4였다.
새 테스트 최초 실패는 'progress batch 뒤 즉시24행'이라는 관측 시점 가정이었다.
생산 로직/W/기대 수치를 바꾸지 않고 후속 실제 처리까지 관측했다. 두 번째 차이는 UTC의
Z/+00:00 표현이어서 같은 instant로만 정규화해 비교했다. 기대 fixture24행은 유지했다.
probe before 실패는 Gold0→재시도24, after 실패는24→재시도24 불변이며 CP commits/1을 확인했다.
최종 모든 실행은 fail/error/skip0이다. 이전 진단 실패와 최신 결과는 별도 파일로 보존한다.

실행 명령·code diff·Git 시작/종료 상태·commit별 파일·실패 로그·expected/actual·hash는
`docs/evidence/phase-3/e2e-integration-preparation/`에 있다. 이전 증빙은 변경하지 않았다.
일반 Spark/source와 Delta는 별도 pytest/JVM으로 실행했다. 기존 Assertion/fixture/P2 코드는
바꾸지 않았다. AST/PEP8/공백·기존 파일 보호 검사는 checks.json을 따른다.

### Git 정렬과 실제 E2E 착수

1. 현재 작업/기존 사용자 문서를 검토·commit하는 것은 사용자 작업이며 이번엔 수행하지 않는다.
2. Phase2의 최신 검토 범위를 main에 선행 병합하고 실제 원격 결과를 확인한다.
3. clean한 P3에서 사용자 승인 후 `git fetch origin`, `git merge origin/main`을 검토한다.
   지금 실행하지 않으며 rebase/cherry-pick으로12개를 임의 복사하지 않는다.
   squash merge이면 tip ancestry만으로 판정하지 말고 PR·변경 tree/계약을 함께 대조한다.
4. merge 후 공유 decoder·Quarantine ACK·runtime 참조·새 테스트와 전체 회귀를 재검증한다.
5. Gold 형식(Delta 권장/미승인), UC target/DDL/권한, Workspace/실제 DBR·compute,
   Kafka/Registry/Secret·인증 전파, 격리 CP·single writer, fixture·실제 W 진전·시간/비용/
   종료·정리 범위를 승인한다. 그 뒤에만 별도 승인 기록으로 live 실행한다.

권장 commit은 (1) source·범위 승인 launcher와 단위/source 통합 테스트,
(2) fixture/publisher/probe와 복구 테스트, (3) 문서·공개 검토 증빙이다.
파일 단위 의존성상 처음 두 묶음을 하나의 구현 commit으로 합쳐도 된다. 정확한 제안은
새 증빙 git-plan.md를 따른다. push 대상은 origin/feat/phase-3-gold-windows다.
main merge는 실제 UC/Kafka/DBR E2E·동일CP 복구 및 전체 DoD/Phase2 선행 통합 이후에만 검토한다.
