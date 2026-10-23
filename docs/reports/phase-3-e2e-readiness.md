# 최신 UC 후보 준비 상태 — 2026-10-23

UC 이름 기반 후보 Writer 및 Job 선택 경로를 추가했다. **실제 UC/Kafka/DBR E2E는 BLOCKED**다.
아래 이전 준비 기록은 이력이며 최종 판정은 문서 마지막 UC 후보 부록을 따른다.

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
