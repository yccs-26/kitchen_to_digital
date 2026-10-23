# Phase 3 Gold Aggregation 통합 구현 및 로컬 검증

최신 로컬 검증은 문서 마지막 UC 후보 준비 부록을 따른다.
UC 이름 기반 후보 Writer를 추가했으며 실제 UC/DBR/Kafka E2E와 형식 승인은 미완료다.
아래 960/990건 등 과거 수치는 당시 실행 이력이다.
아래 2026-10-22 본문과 초기 실패 기록은 당시 이력으로 보존한다.

확인일: 2026-10-22 (Asia/Seoul).
Worktree: `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows`.
Branch: `feat/phase-3-gold-windows` (`.git`가 가리키는 worktree HEAD 파일로 확인).

**로컬 검증: 전체 Python unit 958 passed / 기존 Streaming 관련 10 passed /
신규 Spark·Delta 통합 12 passed + 별도 프로세스 재시작 1 passed. 실패·skip은 각각 0건이다.**
전체 unit의 기존 AuthlibDeprecationWarning 1건은 유지된다.
실제 Kafka/MSK/Registry/Databricks E2E가 없으므로 Phase 3 전체 DoD 완료나 main merge를 권고하지 않는다.

## 기준과 변경 경계

- 실제 생산 Gold Mapper와 기존 매핑 105건, Gold 참조 모델 25건,
  기존 집계/window/streaming Python 98건이 있었다. 이번에는 그 코드를 보존했다.
- `metric_aggregation.py`, 생산 비교 모듈과 Delta Sink는 없어서 지정 경로에 생성했다.
- 계획서는 이 worktree에 없어 원본 checkout의 사용자 관리 로컬 계획서 두 파일을 읽었다.
  계획서 사본이나 본문을 추적 산출물로 옮기지 않았다.
- 원본 AGENTS.md의 '한 논리 단위 뒤 정지'보다 최신 사용자의 Stage A–F 연속 실행 지시를 따랐다.
- 사용자의 Git 명령 실행 금지에 따라 읽기 전용 Git 명령도 실행하지 않았다.
  branch는 metadata로 확인했고 파일 변경은 시작 SHA-256 manifest와 직접 대조했다.
  따라서 git status/diff 실행 결과 또는 clean checkout을 확인했다고 주장하지 않는다.
- Phase 2 Avro 10필드/validated 발행·검증 코드, 기존 순수 집계,
  기존 fixture/assertion/테스트와 프로젝트 의존성·lockfile은 변경하지 않았다.

## Stage별 판정

| Stage | 판정 | 구현 및 실제 관측 |
|---|---|---|
| A | 완료 | 순수 비교 8건, INSERT/NO-OP/CONFLICT·키 불일치·정확 비교 |
| B | 로컬 완료 | 실제 Delta 7건: DDL/NOT NULL, replay, 충돌 batch 원자성, source 중복, 스키마 거부 |
| C | 로컬 완료 / Kafka runtime 미검증 | 생산 단일 query의 두 window, late/eviction 2건. Avro 입력 어댑터 unit 8건 |
| D | 로컬 완료 | 생산 Streaming→Mapper→실제 Delta 1건, 대상 24행 일치 |
| E | 로컬 완료 | MERGE 전/후 실패 복구 2건 + 별도 Python/JVM 프로세스 재시작 1건 |
| F | 완료 | 실제 progress 53개에서 지표 수집·원본 보존. 성능 벤치마크는 아님 |

Stage A 최초 관련 회귀는 236 passed, Kafka 입력 unit 추가 후 Phase 3는 244 passed다.
244 = 기존 228 + 비교 8 + 입력 어댑터 8.
기존 228 = Mapper 파일 105 + Gold 참조 25 + 원래 Phase 3 Python 98.
Mapper 파일의 실제 생산 함수 호출 수 100건은 이전 계측이며 이번에 재계측하지 않았다.
최종 unit JUnit에서도 Mapper 105 / Gold 참조 25 / 기존 Phase 3 98건 통과를 확인했다.

기존 Streaming 관련 10건은 normal/cross_batch_duplicate/payload_conflict/utc_boundary/
watermark_late/idle/batch_replay 7건과 sliding 관련 3건이다.
후자의 하나는 수동 기대값과 기존 Python 집계를 대조하는 검사이고 실제 query 실행 사례는 아니다.
나머지 9건과 신규 통합 및 프로세스 복구 13건은 실제 로컬 Spark 런타임을 사용했다.

## 주요 생산 함수와 계약

| 파일 | 인터페이스 | 역할 |
|---|---|---|
| streaming/aggregations/gold_row_comparison.py | gold_natural_key, compare_gold_rows | 검증된 물리 행의 키와 7개 불변 속성 비교 |
| streaming/sinks/gold_delta.py | GoldDeltaSink.create_table, merge_rows, gold_schema | 로컬 12컬럼 NOT NULL Delta 생성·검증 및 단일 MERGE |
| streaming/aggregations/windows.py | spark_window_spec | 기존 순수 1분 함수를 바꾸지 않고 1분/5분 Spark 길이·slide 제공 |
| streaming/jobs/metric_aggregation.py | validated_schema, decode_validated_record, read_validated_kafka | 기존 Avro frame/schema ID decoder 재사용, 명시적 Registry factory와 Kafka 옵션 |
| streaming/jobs/metric_aggregation.py | build_metric_aggregation, gold_batch_callback, start_metric_query, start_metric_aggregation | bounded dedup, 두 window, Append-final, Mapper와 Sink 연결 |

자연키는 `(window_start, window_type, store_id, equipment_id, metric_name)`이다.
window_end와 unit은 값이며, 수치와 함께 정확히 비교한다.
기존 12컬럼·BIGINT count·유한 DOUBLE·signed zero·avg=sum/count 계약을 유지했다.
생산 비교 함수는 이미 Mapper로 검증된 행을 받는다. 저장소 조회는 수행하지 않는다.

Sink는 모든 source 행을 재검증하고 자연키 중복을 거부한 뒤 단일 MERGE를 실행한다.
동일 값은 matched action 없이 유지하고 신규 키만 절대값 INSERT한다.
상이한 확정값은 MERGE 안의 raise_error로 실패한다. WHEN MATCHED UPDATE의 표현식이
오류를 발생시키므로 정상적인 UPDATE나 누적 합산이 일어나는 경로는 없다.
신규 키와 충돌 키를 같은 MERGE에 넣었을 때 전체 commit이 없고 기존 행도 변하지 않았다.
Delta MERGE가 자동으로 source 중복이나 논리 충돌을 판정한다고 가정하지 않는다.

Delta는 자연키 UNIQUE 제약을 제공하지 않으므로 이 adapter는 단일 writer 전제다.
자연키 비교와 MERGE 사이의 별도 pre-read에 충돌 안전성을 의존하지 않는다.
외부 동시 writer, 손상되거나 중복 키를 이미 가진 target을 자동 수복하지 않는다.

## 단일 query와 window 관측

공통 dedup DataFrame에서 1분 tumbling과 5분/1분 sliding을 각각 집계해 union한다.
최종 query/checkpoint는 하나이며 새로운 영속 event_id 원장은 없다.
물리 실행에서는 dedup 2개 + aggregation 2개, stateful operator 총 4개가 관측됐다.
공통 변수 선언만으로 dedup state가 공유된다고 보고하지 않는다.

분기별 Watermark→dedup→window가 Spark 4.0.0 correctness check를 켠 상태에서 시작·처리됐다.
설계의 1분 단일 branch를 두 해상도로 확장한 상태 비용이다. 다른 query/checkpoint를
추가하거나 1분 출력이 5분 종료까지 기다리는 대안은 사용하지 않았다.
동일 checkpoint의 branch 종류·순서·stateful 구조를 바꾸면 안 된다.

| 단계 | 최대 관측 watermark | 대상 1분 행 | 대상 5분 행 |
|---|---|---:|---:|
| initial | 11:51:00Z | 0 | 0 |
| cross_batch | 11:54:59Z | 0 | 0 |
| end_boundary | 11:55:00Z | 0 | 0 |
| idle | 11:55:00Z | 0 | 0 |
| partial_close | 12:06:30Z | 6 | 11 |
| all_close | 12:15:30Z | 6 | 18 |

시각은 2026-10-20 UTC다. 대상은 기존 fixture의 장비 집합으로 제한했고
watermark 진행용 별도 clock 장비의 결과를 대상 기대값에 섞지 않았다.
이 clock 이벤트는 기존 로컬 fixture의 장치이며 운영 heartbeat 정책을 추가한 것이 아니다.

5분 기대값 18행은 기존 수동 표, 1분 6행은 기존 순수 Python 집계로 독립 계산했다.
각 행의 모든 자연키·unit·경계·count/sum/min/max/avg를 실제 Mapper/Delta 결과와 비교했다.
12:00 시작 5분 행의 count=4, sum=30, avg=7.5이고 12:05:00 이벤트는 포함되지 않는다.
한 이벤트의 겹치는 window 기여, UTC [start,end), cross-batch 중복, metric/unit 분리가 유지됐다.
각 branch의 동일 중복 이벤트 제거가 관측되어 해당 fixture의 duplicate 지표 합은 2였다.
원본 고유 중복 수가 2개라는 뜻은 아니다.

별도 생산 plan late 검사에서 실제 W 전진 및 dedup eviction을 먼저 관측한 뒤
오래된 중복과 오래된 신규 ID를 재전달했다. 확정 출력은 변하지 않았고 late drop이 관측됐다.
대상 1분 1행과 5분 5행은 모두 count=2, sum=6, avg=3을 보존했다.
idle에서 벽시계만으로 W가 진전하거나 final이 새로 출력되지는 않았다.

## 장애 경계와 복구

| 주입 위치 | 실패 직후 Gold | checkpoint | 동일 checkpoint 재시작 |
|---|---|---|---|
| MERGE 전 | 기존 상태 그대로 | offset log가 commit log보다 앞섬 | 미완료 batch 처리, 중복 없이 기대 상태 도달 |
| MERGE 성공 후 callback 반환 전 | 신규 17행이 이미 저장됨 | offset log가 commit log보다 앞섬 | 같은 final을 NO-OP으로 처리, 17행과 값 불변 |
| 기존 target와 다른 final + 신규 행 | 기존 행 그대로 | 단일 Delta commit 실패 | 정상 batch 재전달 시 두 키로 수렴 |

실제 동일 checkpoint를 유지했고 삭제·초기화·새 checkpoint 전환으로 성공시키지 않았다.
그 후 idle 재시작과 같은 원본 이벤트를 새 source 파일에 재전달해 Gold 불변도 확인했다.
정상 query 중단/재시작은 AvailableNow 완료 뒤 같은 checkpoint 재사용으로 검증했다.
실패 후 출력 재계산이 같은 canonical 값을 만든 이 fixture 범위에서 수렴한 것이다.

Delta MERGE는 행 prefix를 부분 커밋하지 않으므로 메모리 참조 모델의 prefix 성공을
실제 Delta의 정상 동작이라고 표현하지 않는다. 충돌과 NOT NULL 실패에서 commit되지 않음을
검증했다. 네트워크 단절·disk full·JVM 강제 종료·강제 프로세스 종료 직후 복구는 미검증이다.
별도 Python/JVM 두 프로세스도 순차 실행했다. 첫 프로세스는 입력/state/offset을
저장하고 종료했고, 두 번째는 동일 checkpoint에서 이를 복구하여 나머지 입력으로
대상 24행을 확정했다. 프로세스별 원본 Spark 로그와 progress를 보존했다.
Delta transaction과 Spark checkpoint가 공동 원자적이라는 보장은 없다.
1 ULP 재계산 차이가 생기면 fail-closed CONFLICT가 되며 무조건 재시도 성공을 보장하지 않는다.

## 실행 환경과 명령

macOS Darwin ARM64 / CPython 3.11.15 / Temurin JDK 21.0.13+9 /
PySpark 4.0.0 / Delta Lake 4.0.0 / pytest 9.1.1, local[2], UTC, shuffle=2.
기존 임시 Spark 환경은 삭제되어 새 `/private/tmp/ktd-phase3-integrated/`에 복구했다.
JDK archive SHA-256은 공식 API metadata와 일치했다.
`bf036b35b071bfb2f54f2c0af8ee8d88afa95ee6eb3dce02c0f94cfc2524d04a`.

[공식 Delta 호환표](https://docs.delta.io/releases/)의 4.0.0↔Spark 4.0.x와
[공식 Quick Start](https://docs.delta.io/quick-start/)의 extension/catalog 설정을 확인했다.
[MERGE 문서](https://docs.delta.io/delta-update/)와
[Spark 4.0 Streaming 문서](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html)를
참조하고 실제 pinned runtime으로 검증했다. DBR 호환성을 이 결과로 치환하지 않는다.

초기 sandbox DNS 오류는 임시 다운로드 명령만 네트워크 허용 실행으로 재시도해 해결했다.
프로젝트 .venv·pyproject.toml·uv.lock은 변경하지 않았다. 관리자 설치나 전역 Java 교체도 없다.

```sh
uv --cache-dir /private/tmp/ktd-phase3-integrated/uv-cache venv \
  --python .venv/bin/python /private/tmp/ktd-phase3-integrated/venv
uv --cache-dir /private/tmp/ktd-phase3-integrated/uv-cache pip install \
  --python /private/tmp/ktd-phase3-integrated/venv/bin/python \
  'pyspark==4.0.0' 'delta-spark==4.0.0' 'pytest==9.1.1' pycodestyle

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/unit -q \
  -p no:cacheprovider --basetemp=/private/tmp/ktd-phase3-integrated/all-unit \
  --junitxml=/private/tmp/ktd-phase3-integrated/all-unit.xml
# 958 passed, 1 warning in 2.84s
```

Spark 실행에는 다음 프로세스 한정 환경을 적용했다.

```sh
export JAVA_HOME='/private/tmp/ktd-phase3-integrated/jdk/jdk-21.0.13+9/Contents/Home'
export PYSPARK_PYTHON=/private/tmp/ktd-phase3-integrated/venv/bin/python
export KTD_LOCAL_IVY=/private/tmp/ktd-phase3-integrated/ivy
export PYTHONDONTWRITEBYTECODE=1
/private/tmp/ktd-phase3-integrated/venv/bin/python -m pytest \
  tests/integration/test_phase3_streaming.py \
  tests/integration/test_phase3_sliding_window.py -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-integrated/existing-streaming \
  --junitxml=/private/tmp/ktd-phase3-integrated/existing-streaming.xml
# 10 passed in 36.54s

/private/tmp/ktd-phase3-integrated/venv/bin/python -m pytest \
  tests/integration/test_phase3_gold_delta.py \
  tests/integration/test_phase3_metric_aggregation.py \
  tests/integration/test_phase3_streaming_gold.py \
  tests/integration/test_phase3_checkpoint_recovery.py -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-integrated/integrated-final \
  --junitxml=/private/tmp/ktd-phase3-integrated/integrated-final.xml
# 12 passed in 64.86s

/private/tmp/ktd-phase3-integrated/venv/bin/python -m pytest \
  tests/integration/test_phase3_checkpoint_recovery.py::test_separate_process_reuses_checkpoint \
  -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-integrated/process-restart \
  --junitxml=/private/tmp/ktd-phase3-integrated/process-restart.xml
# 1 passed in 24.27s
```

실제 실행은 export로 사용자 shell을 바꾸지 않고 위 변수들을 `env ...`로 개별 명령에 전달했다.
재현 시 기존 테스트 디렉터리를 재사용하지 말고 새 임시 basetemp를 선택한다.

## 운영 지표: 관측 범위

생산 plan/Delta/복구의 progress 53개를 공식 progress.json 표현으로 저장했다.
`numInputRows`, `inputRowsPerSecond`, `processedRowsPerSecond`, eventTime.watermark,
stateOperators의 numRowsTotal/Updated/Removed, memoryUsedBytes, durationMs가 원본에 있다.
최대 동시 operator 수=4, 한 batch의 전체 operator state 합계 최대=46행,
state memory 합계 최대=18,176 bytes, triggerExecution 관측 범위=0~4,328ms였다.
0ms는 입력 없는 progress의 기록값을 그대로 포함한다. 근거 없는 처리량·p95는 제시하지 않는다.
이 값은 작은 fixture의 관측이며 production 용량/성능 목표의 증거가 아니다.
branch별 입력/duplicate 지표를 원본 Kafka 고유 이벤트 수로 합산 해석하지 않는다.
부하·파티션 튜닝은 Phase 9로 유보한다.

## 검증 한계와 다음 작업

- Kafka broker/Schema Registry, MSK IAM/권한/connector jar, Databricks Runtime,
  UC/운영 catalog의 Delta/Iceberg 지원과 실제 배포는 미검증이다.
- Kafka read→decoder→aggregation→sink 연결 함수는 있지만 실제 Kafka source 실행은 없다.
  로컬 Avro decode/key/UTC 검사는 broker/executor Registry 연결의 증거가 아니다.
- 단일 writer 외 자연키 유일성, 재계산 수치 결정성, checkpoint 유실/backfill,
  driver 강제 종료와 복구, log-retention 후 테이블 검사, 대량 final batch는 미검증이다.
- 현재 Sink는 작은 final batch를 driver에 materialize한다. 큰 상태나 큰 출력에 대한
  운영 보장은 없으며 대규모 batch를 임의로 수용할 메모리 용량을 주장하지 않는다.
- Phase 2 main 병합 관계는 Git 조회를 하지 않았으므로 확인하지 않았다. merge 권고도 없다.

다음 최소 검증은 별도 프로세스의 강제 종료 경계 복구다. 정상 프로세스 재시작은 이번에 통과했다.
그다음 명시적으로 승인된 broker/Registry 및 DBR 환경에서의 source/권한 수용 검증이 필요하다.
AWS/Databricks 유료 실행·운영 데이터/checkpoint 변경·새 원장은 수행하지 않았다.

## 변경 파일과 권장 commit 묶음

아래는 권장 메시지일 뿐 staging/commit/push/PR/merge 명령은 실행하지 않았다.
Push 후보는 사용자가 지정한 `origin/feat/phase-3-gold-windows`이며 원격 상태는 조회하지 않았다.

| Stage | 파일 | 권장 commit 메시지 |
|---|---|---|
| A | streaming/aggregations/gold_row_comparison.py; tests/unit/phase3/test_gold_row_comparison.py | feat(phase3): add exact Gold row comparison |
| B | streaming/sinks/gold_delta.py; tests/integration/phase3_local_runtime.py; tests/integration/test_phase3_gold_delta.py | feat(phase3): add immutable local Delta Gold sink |
| C | streaming/aggregations/windows.py; streaming/jobs/metric_aggregation.py; tests/unit/phase3/test_metric_input.py; tests/integration/test_phase3_metric_aggregation.py | feat(phase3): build single-query Gold window aggregation |
| D | tests/integration/test_phase3_streaming_gold.py | test(phase3): verify streaming finals reach Delta Gold |
| E | tests/integration/test_phase3_checkpoint_recovery.py | test(phase3): verify checkpoint recovery around Gold commits |
| F | streaming/aggregations/phase3-streaming-design.md; docs/reports/phase-3-verification.md; docs/evidence/phase-3/local-2026-10-22/ | docs(phase3): record local Gold integration evidence |

증빙 전체 파일명과 SHA-256은 evidence-manifest.json, 소스 변경 목록은 source-changes.json에 있다.
Git 제외 설정은 변경하지 않았고 기존 계획서를 강제 추가하지 않았다.

## 최종 정적 검사 및 파일 보호

새 Python 모듈과 확장한 windows.py에 pycodestyle(기본 79컬럼)을 실행해 오류 0건이었다.
변경 Python 전체 compile 기반 구문 검사와 trailing whitespace 검사도 통과했다.
별도 lint/type-check 설정이나 CI workflow는 이 checkout에서 발견하지 못했다.
정적 타입 검사기 실행은 별도 검증으로 주장하지 않는다.
시작 manifest 대조에서 기존 파일의 변경은 windows.py와 설계 문서 두 개뿐이며,
그 밖의 새 소스/테스트/보고서를 포함한 변경 경로는 13개다. 증빙 폴더는 별도로 기록했다.
Git diff/check 명령 대신 Python unified diff와 공백 검사를 사용했다.
원본 Mapper·명세 테스트·Phase 2·fixture·의존성 파일은 hash가 그대로였다.

## 운영 연결 전 감사·최종 로컬 검증 — 2026-10-23

이 부록은 2026-10-22 구현 기록을 보존하고 그다음 감사 결과를 추가한다.
시작 branch `feat/phase-3-gold-windows`, HEAD `12024e7`을 Git 읽기 명령으로 확인했다.
시작 상태는 기존 Phase 3 미커밋 작업 다수이며 clean이 아니었다. 그 파일을 보존했다.
현재 요청에서 Git 상태/diff 조회는 허용됐지만 staging/commit/push/PR/merge는 하지 않았다.
외부 Kafka/Registry/AWS/Databricks API·compute·catalog에는 접속하지 않았다.
공식 공개 문서 조회와 기존 임시 로컬 runtime 재사용만 수행했다.

### A — 생산 경로 감사와 최소 수정

실제 순서는 `validated → UTC timestamp → withWatermark(10분) → event_id dedup →
branch별 window → union → Append → Mapper → Delta MERGE`다.
요청의 개략도에서 dedup보다 뒤에 적힌 watermark는 API에서 **dedup 전에** 정의돼야 하며
현재 구현이 그 순서를 따른다. processing-time은 집계 key나 window closure에 사용하지 않는다.

| 감사 항목 | 확인 결과 |
|---|---|
| Event-time | 기존 decoder UTC 정규화 후 native timestamp, UTC session 필수 |
| W/state eviction | 실제 W 전진·dedup 제거 후 late 입력 검사가 생산 plan을 호출 |
| micro-batch dedup | 각 branch의 bounded state에 event_id 저장, checkpoint 복원 |
| 상이 payload | native dedup은 탐지하지 않음. Phase 2 canonical 불변 계약에 의존 |
| Window | 1m/1m tumbling, 5m/1m sliding, UTC [start,end), 같은 label 설정 고정 |
| open window 저장 | 생산 builder+Append 경로는 final만 출력. Mapper/Sink 직접 호출은 closure 증명 기능이 없음 |
| metric/unit | groupBy는 둘 다 포함, 자연키는 unit 제외. 동일 키의 단위 차이는 거부/CONFLICT |
| 12컬럼 | 기존 Mapper 재사용, NULL/nonfinite/손실 변환 거부와 signed zero/avg 검산 유지 |
| 저장 | 절대값 insert, identical no-op, differing final fail-closed. 자동 UPDATE 없음 |
| foreachBatch | 입력 전량 소비, 같은 batch_id를 건너뛰지 않음, sink 오류 전파 |
| Query/checkpoint | 하나씩 유지. stateful operator는 4개 |
| 생산 함수 호출 | native 실험과 생산 plan 테스트를 구분. Mapper 105건 중 100건 실제 진입 재계측 |

중요한 사전 결함은 callback이 SparkSession을 보유한 GoldDeltaSink를 캡처한 것이다.
로컬 classic 실행 성공은 그 closure의 직렬화 성공을 보장하지 않았다.
`gold_batch_callback`의 실제 Delta 경로는 이제 **path 문자열만 캡처**하고
`batch.sparkSession`으로 sink를 구성한다. 테스트는 원래 sink 객체 pickle이
CONTEXT_ONLY_VALID_ON_DRIVER로 실패하는 경계와 새 callback pickle round-trip을 확인한다.
Standard/DBR에서 실행했다는 뜻은 아니다. 주입형 로컬 테스트 sink의 캡처 경로는 별도로 유지했다.

추가로 callback에서 timezone 없는 window 문자열을 호스트 timezone으로 해석할 수 있었던
경로를 명시적인 ValueError로 막았다. 생산 plan이 만드는 UTC 문자열 기대값은 변경하지 않았다.
Registry transport 오류 전파를 오프라인 unit으로 추가했다.
Phase 2·Mapper·수치 정책·watermark·자연키·query 수는 변경하지 않았다.

### B — Iceberg/Delta 아키텍처 감사: 운영 채택 차단

현재 파일을 직접 확인한 결과 **Bronze=Managed Iceberg, Silver=Managed Delta 구현**이다.
`DeltaSilverStorage`는 Job 2가 주입하는 실제 adapter이며 Phase 2 보고서에는
Silver Delta 채택과 storage smoke가 기록돼 있다. 오래된 IcebergSilverStorage 메모리를
현 코드로 사용하지 않았다. 이번에 해당 cloud target의 provider를 새로 조회하지는 않았다.

Silver의 결정을 Gold에 자동 확대하지 않는다. Phase 3 설계 §7은 Delta Gold를
물리 형식 변경 제안/검토 대상으로 명시한다. 이전 사용자의 로컬 Delta 구현 요청은
운영 UC Gold 형식의 최종 채택·이전 승인으로 취급하지 않았다.

`docs/adr/phase-3-gold-table-format.md`에 Managed Iceberg 유지와 Delta 채택의
정확성·복구·구현 비용·기존 코드 영향을 비교했다. **상태는 승인 대기 초안**이다.
현 GoldDeltaSink는 로컬 path/forPath/log JSON용이어서 UC managed table 이름 기반
adapter를 대체하지 않는다. 이 배포 차이는 문서만으로 해결됐다고 보고하지 않는다.

[DBR 17.3 LTS 문서](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)는
Spark 4.0.0을 포함하지만 로컬 OSS Delta 버전과 DBR 내장 Delta가 같다는 뜻은 아니다.
[MERGE reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into)는
Delta target 전용으로 명시한다. managed Iceberg의 동일 MERGE 지원을 추론하지 않았다.
[UC 요구사항](https://docs.databricks.com/aws/en/data-governance/unity-catalog/requirements)상
Delta/Iceberg 공존 가능성과 특정 adapter의 DBR/UC 실행 성공을 구별한다.
Bronze/Phase 2 데이터·catalog·운영 checkpoint 변경은 없다.

### C — 장애 복구 확대

기존 13건의 Gold 통합·복구 검사를 재사용하고 별도 파일에 두 검사만 추가했다.

1. 실제 SparkSession을 가진 sink의 직렬화 실패와 수정 callback의 round-trip.
2. MERGE 성공을 실제 조회·fsync한 marker로 관측 → 전용 Python/JVM process group에
   SIGKILL → 동일 checkpoint·동일 source/Gold로 새 프로세스 복원.

강제 종료는 `start_new_session=True`로 생성한 PGID만 대상으로 한다.
checkpoint의 offsets/N 존재·commits/N 부재를 먼저 확인하고 종료한다.
재시작 뒤 **같은 N의 callback 재실행**, commits/N 생성, Gold 행/수치 불변,
대상 24개 expected/actual 일치를 검사한다. checkpoint를 삭제하거나 이동하지 않는다.
marker는 임시 파일에 기록·fsync한 뒤 rename하여 불완전한 JSON을 읽는 경합도 피한다.
프로세스별 원본 Spark 로그를 보존한다.

단일 MERGE의 transaction abort와 여러 MERGE/여러 batch 사이의 부분 완료는 별개다.
앞선 MERGE가 성공한 뒤 다음 MERGE가 실패할 수 있고 전체 query/checkpoint와 공동 원자성은 없다.
이번 kill 경계는 MERGE 후 callback 미완료다. MERGE 내부 임의 지점의 SIGKILL,
OS 전원 손실·disk full·네트워크 분할까지 검증한 것은 아니다.

### D — 기존 53개 progress 분석

기존 증빙은 수정하지 않고 별도 `analyze_progress.py`로 분석했다.
query_id/run_id/batch_id로 대조한 결과 **53개 고유 표본, 중복 0개**였다.
operator index+name별 관측 범위를 보존하고 missing은 0이 아니라 N/A로 처리했다.

| index / name | 최대 rows total | 최대 rows updated | 최대 rows removed | 최대 memory bytes | 최대 late dropped |
|---|---:|---:|---:|---:|---:|
| 0 / stateStoreSave | 7 | 6 | 6 | 3,392 | 0 |
| 1 / dedupeWithinWatermark | 9 | 7 | 7 | 3,032 | 3 |
| 2 / stateStoreSave | 23 | 18 | 18 | 9,184 | 0 |
| 3 / dedupeWithinWatermark | 9 | 7 | 7 | 3,032 | 3 |

두 dedup은 branch별 유효 horizon의 고유 ID를 저장한다.
두 aggregation은 살아 있는 window×store×equipment×metric×unit 그룹을 저장한다.
5분/1분 sliding은 이벤트가 최대 5개 window에 기여하여 cardinality가 더 커질 수 있다.
각 열의 최대가 같은 batch라는 뜻은 아니며 이를 더해 동시 최대라고 해석하지 않는다.
index별 window 길이와 state store provider는 progress에 직접 없어 N/A로 기록했다.
코드의 union branch 순서와 실제 provider 확인은 운영 preflight의 별도 항목이다.
customMetrics의 state-store 지표도 operator-analysis.json에 기록했다.

53개는 고정 synthetic fixture이며 운영 workload나 장시간 state 안정성 자료가 아니다.
기존 자료에 W 진전·eviction·idle·late·작은 cardinality 변화가 있어 추가 확대 부하 시험은
하지 않았다. tuning은 Phase 9로 유보하며 throughput 목표나 p95를 만들어내지 않았다.

### E/F — 운영 준비와 Runbook

`docs/runbook.md`는 Phase 1용이므로 보존하고
`docs/runbooks/phase-3-gold-streaming.md`를 추가했다.
DBR/Standard callback, UC 대상·권한·DDL, checkpoint, Kafka/Registry 비밀 주입,
max_concurrent_runs=1 및 외부 writer 통제, 재시작/CONFLICT/1 ULP/state 증가 대응,
운영 수용 테스트를 포함한다. 모든 미정 리소스는 placeholder이며 존재를 단정하지 않는다.

실제 broker/DBR 연결 없이 기존 framing/schema ID/UTC/key/error 경로를 unit으로 검사했다.
Spark Kafka connector·executor Registry 접근·IAM·UC table adapter는 여전히 미검증이다.
Kafka 시작 offset은 첫 query 설정이고 재시작 offset은 checkpoint가 결정한다.
현재 adapter가 source options를 전달하므로 배포 시 failOnDataLoss/startingOffsets를
명시적으로 확인해야 하며, 인증정보를 파일이나 closure에 무분별하게 저장하지 않는다.

로컬 main은 `7006ae6` Phase 1이고 main tree에는 sensor_validation_job.py가 없다.
원격 fetch는 하지 않아 원격 main 상태를 단정하지 않는다. Phase 2 병합 확인도 main merge 게이트다.

### 최초 실패 기록과 환경 격리

실패 결과는 보존했다. 최초 확장 검사는 **1 failed / 1 passed**였고,
실패 원인은 임시 환경에 독립 cloudpickle package가 없었던 것이다.
테스트가 실제 Spark에서 쓰는 `pyspark.cloudpickle`을 사용하도록 수정했다.
프로젝트 의존성 파일이나 기존 .venv는 변경하지 않았다.

기존 일반 Spark 10건을 먼저 실행한 동일 JVM에 Delta 테스트를 이어 붙였던 시도는
**14 passed / 4 failed / 7 errors**였다. DeltaCatalog/DeltaSparkSessionExtension을
찾지 못했다. builder의 spark.jars.packages는 이미 시작된 JVM classpath를 소급 변경하지 않는다.
이는 aggregate 기대값이나 stateful API 오류가 아니라 검증 환경의 클래스 로딩 문제다.
일반 Spark와 Delta 집합을 별도 pytest 프로세스로 실행하는 기존 재현 패턴으로 분리했다.
실패를 skip/xfail로 바꾸거나 assertion을 완화하지 않았다.

### 최종 Stage 판정과 회귀 결과

| Stage | 판정 | 실제 변경/산출물 | 남은 사항 |
|---|---|---|---|
| A | 로컬 완료 | metric_aggregation.py callback 수정, test_metric_input.py +2건 | 실제 DBR callback 수용 |
| B | 감사 완료 / 배포 차단 | 승인 대기 ADR | Gold 형식 결정·UC adapter·provider/권한 검증 |
| C | 로컬 완료 | test_phase3_recovery_extended.py 2건 | MERGE 내부 kill/전원·disk 장애는 미검증 |
| D | 완료(고정 fixture 한정) | operator-analysis.json, 분석 script | 운영 부하/장시간 state는 Phase 9 |
| E | 준비 문서 완료 / 운영 미검증 | Runbook·설정 placeholder·오프라인 오류 검사 | 실제 broker/Registry/DBR 연결 |
| F | 로컬 완료 / 전체 DoD 미충족 | 회귀·구문·공백·증빙/변경 목록 | 형식 승인·운영 E2E·Phase 2 병합 |

| 최종 실행 집합 | pass | fail/error | skip | 관측 |
|---|---:|---:|---:|---|
| 전체 unit | 960 | 0 | 0 | 1 existing Authlib deprecation warning |
| 기존 Streaming 관련 | 10 | 0 | 0 | 7 native multibatch + 2 sliding query + 1 수동 표 Python 검사 |
| 기존 Gold 통합·복구 | 13 | 0 | 0 | 기존 기대값 유지, Delta·24행·정상/실패 재시작 |
| 새 확장 검사 | 2 | 0 | 0 | callback 직렬화·SIGKILL 같은 batch/CP 복구 |
| Mapper 별도 호출 계측 | 105 | 0 | 0 | 생산 Mapper 실제 진입 100건, Phase 2 validator 5건 |

마지막 두 integration 행은 **15 passed in 127.45s**인 동일 Delta 집합의 분류다.
기존 Streaming은 **10 passed in 46.20s**, unit은 **960 passed in 2.47s**다.
별도 Mapper 105건은 전체 unit에 이미 포함되므로 총 테스트 수에 중복 합산하지 않는다.
코드/검증 증거는 새 `docs/evidence/phase-3/predeploy-2026-10-23/`에 있고 기존 증빙은 보존했다.

### 실제 재현 명령

cwd는 worktree 루트다. 기존 `/private/tmp/ktd-phase3-integrated/venv`와 JDK 21.0.13+9를
재사용했다. 프로젝트 의존성 설치·변경 없이 PySpark 4.0.0/Delta 4.0.0을 실행했다.
전체 unit은 기존 `.venv`의 Python 3.11.15를 사용했다.
아래 통합 집합은 반드시 서로 다른 pytest 프로세스에서 시작한다.

```sh
.venv/bin/python -m pytest tests/unit -q -p no:cacheprovider \
  --junitxml=/private/tmp/ktd-phase3-predeploy-20261023/unit.xml
```

각 Spark 명령 앞에는 다음 `env` prefix를 붙였다. 사용자 shell 설정은 바꾸지 않았다.

```sh
env JAVA_HOME='/private/tmp/ktd-phase3-integrated/jdk/jdk-21.0.13+9/Contents/Home' \
  PYSPARK_PYTHON=/private/tmp/ktd-phase3-integrated/venv/bin/python \
  KTD_LOCAL_IVY=/private/tmp/ktd-phase3-integrated/ivy \
  PYTHONDONTWRITEBYTECODE=1 \
  /private/tmp/ktd-phase3-integrated/venv/bin/python -m pytest \
  tests/integration/test_phase3_streaming.py \
  tests/integration/test_phase3_sliding_window.py -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-predeploy-20261023/stream-final \
  --junitxml=/private/tmp/ktd-phase3-predeploy-20261023/stream-final.xml
```

위와 동일한 env prefix로 별도 프로세스에서 다음 pytest 인자를 실행했다.

```sh
  tests/integration/test_phase3_gold_delta.py \
  tests/integration/test_phase3_metric_aggregation.py \
  tests/integration/test_phase3_streaming_gold.py \
  tests/integration/test_phase3_checkpoint_recovery.py \
  tests/integration/test_phase3_recovery_extended.py -v -p no:cacheprovider \
  --basetemp=/private/tmp/ktd-phase3-predeploy-20261023/delta-final \
  --junitxml=/private/tmp/ktd-phase3-predeploy-20261023/delta-final.xml
```

재실행 시 위 basetemp는 재사용하지 말고 새 디렉터리를 선택하여 이전 장애 증거를 보존한다.
pycodestyle은 변경 Python과 분석 script에 대해 실행했고 compile/공백 검사와 git diff --check도
통과했다. 정적 type checker 실행이나 cloud runtime 검증을 주장하지 않는다.

### 운영 착수 승인 항목 및 merge 차단

- 승인 필요: Gold 형식(ADR), 실제 UC target·provider·DDL, 해당 형식 adapter 구현 범위,
  Kafka/Registry/DBR 인증·네트워크·권한, 고정 checkpoint, 단일 writer 소유권,
  유료 실행 예산·시간 상한·정지/보존 절차.
- 로컬 DoD: 현재 검증된 단일 query/불변 final/실패 재시도/SIGKILL 경계는 충족.
  운영 장시간 상태·실제 external sink/source 보장까지 포함하지 않는다.
- 전체 Phase 3 DoD: **미충족**. 실제 Kafka/DBR E2E, 형식 승인과 UC adapter,
  single writer 권한 통제 수용, Phase 2 main 병합 관계 확인이 필요하다.
- main merge: 위 미결 항목과 운영 수용 증거 확보 전 유보한다. 로컬 main의 Phase 1 상태만
  조회했으며 remote 상태를 갱신하지 않았다.

### 이번 논리 작업별 권장 commit 구성

1. `fix(phase3): serialize Gold callbacks without SparkSession capture`
   — streaming/jobs/metric_aggregation.py, tests/unit/phase3/test_metric_input.py.
2. `test(phase3): verify SIGKILL recovery after a committed Gold merge`
   — tests/integration/test_phase3_recovery_extended.py.
3. `docs(phase3): gate Gold format adoption and prepare deployment runbook`
   — ADR 초안, Phase 3 Runbook, 본 보고서, 신규 감사 증빙.

이 목록은 이전 미커밋 Stage A–F 파일 목록과 구분한다. 기존 변경은 이번 commit에 자동으로
포함시키지 않으며 어떠한 Git 쓰기 명령도 실행하지 않았다.
변경 전체 경로·SHA-256·작업 시작 대비 diff와 스크린샷 후보는 신규 증빙 README에 연결했다.

## UC 후보 Writer 및 E2E 최종 준비 — 2026-10-23

최종 fresh 결과: **전체 Unit1019 / Phase3 Unit305(전체에 포함) / 기존 Streaming10 /
기존 Gold 통합·복구15 / 신규 로컬 이름 기반 Delta9 passed**. Unit의 기존 Authlib warning1.
신규 단위29건(Writer12·선택5·기존 validated adapter 연결12)을 포함한다.
기존 Preflight30, Mapper/비교/Gold 명세 및 기존 테스트 기대값은 유지했다.

`GoldUCDeltaSink`는 기존 3-part 검증기와 공통 source 검증/MERGE를 재사용한다.
테이블·권한을 자동 생성하지 않으며 local path Sink와 별개다. Job callback은 table 문자열만
캡처하고 batch 세션으로 재구성한다. `prepare_metric_deployment(adapter='uc_managed_delta')`는
정적 후보 선택이며 현재 configured launcher는 계속 BLOCKED다. 승인 flag로 해제하지 않는다.

실제 local catalog MERGE와 단일 query24행(expected=actual, 1분6+5분18)을 확인했다.
W epoch→12:15:30Z, state operators4. 기존 SIGKILL 회귀는 같은 checkpoint batch1 재실행 후24행 불변.
신규 NOT NULL metadata 실험5fail/4pass는 OSS V2의 DESCRIBE AS JSON 미지원이었다.
해당 API를 공통 Writer에서 제외하고 DBR 수용 단계로 분리한 최종9건은 통과했다.
대상 NOT NULL DDL은 Writer read schema 타입 검사만으로 증명되지 않는다.
로컬 NULL append 거부 테스트와 실제 UC DDL/권한 수용을 구분한다.

순수 offline Kafka fixture는 Phase2 serializer/decoder와 기존 schema를 재사용했다.
별도 adapter 모듈/새 topic/원장/추가 운영 query/Phase2 변경은 없다.
실제 Kafka/Registry/UC/DBR 접근, cloud table/checkpoint 변경, 데이터 발행, 유료 실행은 없었다.
Gold 형식은 승인 대기이며 UC 이름 기반 **후보 구현**을 UC 배포 완료로 표시하지 않는다.
비용/권한/실제 복구·동시 writer 통제와 Phase2 main 병합은 미확인/미완료다.

세부 Stage/승인/정확성 한계·실행 순서:
[최신 readiness 부록](phase-3-e2e-readiness.md#최종-uc-후보-준비-부록--2026-10-23),
[명령 참조](../runbooks/phase-3-e2e-command-reference.md).
로그/JUnit·expected/actual·실패 이력·현재 변경 diff·Git 제안:
`docs/evidence/phase-3/uc-e2e-preparation/`.
과거 실행 증빙을 보존했고 Git 쓰기는 하지 않았다. Phase3 전체 DoD와 main merge 준비는 미완료다.

## Phase2 계약·E2E 실행 코드 보완 — 2026-10-23

최종 fresh: 전체 Unit1043 / Phase3 Unit329(전체에 포함), 기존 Streaming10,
기존 Gold·복구·이름 기반24(15+9), 새 binary source5, 새 probe 실제 Delta 복구2 passed.
Unit 기존 Authlib warning1. 최종 fail/error/skip0이며 초기 source 타이밍/UTC 표현 차이
진단 실패는 별도 보존했다. 기대 수치/기존 Assertion과 기존 P2 코드는 유지했다.

Phase2 현재 로컬 tip `3d772bd`와 merge-base `b8c6ee2` 사이12개가 HEAD에 없다.
wire/schema/publisher/UTC는 같고 P3 Registry boundary에서40403만 data error로 구분한다.
유효한 late는 P2에서 분류 후 validated로 전달되며 무조건 Quarantine하지 않는 것을 확인했다.
Git 계보 정렬은 미실행이고 최신 원격 상태는 UNKNOWN이다.

추가: worker Secret 참조 factory·공유 binary source 변환, 범위 승인된 DBR entrypoint,
격리 raw→P2 publisher(dry-run 기본, 실제 발행0), MERGE 전/후 callback failure probe.
실제 source Streaming target24행과 state4/W/late drop, probe0→24 및24→24/동일CP를 관측했다.
단일 query·window/W/Append·CP 구조와 local path Sink를 보존했다.

실제 UC/DBR/Kafka/Registry·Secret 전파·IAM·비용/권한·강제 종료 수용은 미실행이다.
Gold 형식은 여전히 승인 대기다. typed 승인 객체/JSON을 실제 승인/권한/lock의 증거로
해석하지 않는다. 전체 Phase3 DoD 완료나 main merge를 권고하지 않는다.
자세한 API/실행 조건은 E2E runbook 마지막 절, 결과/Git 제안은 readiness의 최신 절,
증거는 `docs/evidence/phase-3/e2e-integration-preparation/`를 따른다.
