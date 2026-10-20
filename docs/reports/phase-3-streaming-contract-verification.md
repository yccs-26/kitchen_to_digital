# Phase 3 Multi-batch dedup/window 계약 검증

확인일: 2026-10-17 (Asia/Seoul). 기준 HEAD: `92b5620`.
브랜치: `feat/phase-3-gold-windows`. 시작 시 working tree clean.
최신 확인: 2026-10-20. progress 공식 JSON 저장으로 최소 수정한 뒤 기존 Streaming
**7 passed / 0 failed / 0 skipped**, Phase 3 Python 회귀 **98 passed**를 확인했다.
직전 실행의 7건 FAILED는 progress UUID 직렬화 오류이며 역사 기록으로 보존한다.
추가로 5분/1분 sliding의 신규 3건과 기존 Streaming 7건을 함께 실행해 **10 passed**를
확인했다. 신규 3건은 Python 독립 대조 1건과 실제 Spark 실행 2건이다.
최신 명령·관측값·변경 범위는 문서 끝 「5분 Sliding Window 실제 검증」을 참조한다.
아래 초기 구현 기록의 PySpark 부재/skip은 이전 실행 결과이며 최신 실행과 구분한다.

## 구현 범위

기존 `minute_aggregation.py`, `windows.py`, validated Avro/publisher, 공통 설정·의존성을
변경하지 않았다. Gold sink·운영 Spark Job을 만들지 않았다. AWS/Databricks 실행 없음.
기준 계획 실행 묶음 C와 `streaming/aggregations/phase3-streaming-design.md`를 직접 확인했다.

추가 파일:

- `tests/fixtures/phase3/multi_batch_events.json`
- `tests/unit/phase3/test_streaming_contract.py`
- `tests/integration/test_phase3_streaming.py`
- 이 보고서 `docs/reports/phase-3-streaming-contract-verification.md`

## 환경과 실제 API 확인

| 항목 | 현재 확인 결과 |
|---|---|
| 로컬 Python | `.venv` Python 3.11.15 |
| PySpark / Delta | 둘 다 설치되어 있지 않음. pyproject/uv.lock 변경·설치 없음 |
| 로컬 Java | `java -version` 및 `java_home -V`: Oracle JDK 24 하나만 발견 |
| DBR | 저장소 기록의 목표는 17.3 LTS / Spark 4.0.0. 현재 원격 runtime은 조회하지 않음 |
| 새 테스트 결과 | Python 계약 19 passed, Streaming 7 skipped |
| 전체 unit | 812 passed, 1 warning (2.35초) |

Spark 4.0.0은 Python 3.9+, Java 17/21을 명시한다. Python 버전은 충족하지만 로컬 classic
실행에는 PySpark 4.0.0과 지원 JDK 준비가 필요하다. JDK 24로 실행 실패를 관측한 것은 아니다.
[공식 Spark 4.0 환경](https://spark.apache.org/docs/4.0.0/),
[PySpark 설치·기본 의존성](https://spark.apache.org/docs/4.0.0/api/python/getting_started/install.html).
실제 설치 전 보고 사항: 후속 승인 시 별도 로컬 환경에 `pyspark==4.0.0`과 동반 py4j,
JDK 17 또는 21을 준비하는 안이 필요하다. 이번 테스트는 Delta를 사용하지 않는다.
현재 환경과 lock을 임의 변경하거나 Databricks Connect로 대체 실행하지 않았다.

`dropDuplicatesWithinWatermark([event_id])`는 Spark 3.5부터 존재하며 watermark가 설정된
streaming DataFrame이 필요하다. ID에 따른 bounded dedup이며 9필드 payload 충돌 검사는 없다.
[API 문서](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html)

dedup과 window 집계는 둘 다 checkpoint state를 요구한다. Update/Complete는 다중 stateful
연결에 제약이 있으며 이번 테스트는 설계대로 Append와 correctness check 활성화를 유지한다.
Append 조합의 실제 plan 지원·watermark 전파는 환경 부재로 확정하지 못했다.
[Structured Streaming 지원 제약](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations)
공식 DBR 17.3도 Spark 4.0.0을 포함하지만 로컬 테스트가 DBR/Standard/Connect 동작까지
입증하지 않는다. [DBR release note](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)

## Fixture와 기대값

모든 이벤트는 기존 Avro의 필수 10필드를 가진다. batch 순서·실패 지점·기대 결정은 wire payload
외부 envelope다. 아래 기대값은 `fridge-001` 대상이며 시간 진행용 `clock-001`은 필터하여
비교한다. 시간 진행 행은 로컬 테스트용 정상 numeric 행으로 운영 heartbeat가 아니다.

| 시나리오 | 유한 Python oracle | Streaming 기대값 | 이번 실제 검사 |
|---|---|---|---|
| normal | a=2,b=4,c=6 → count 3/sum 12/avg 4/min 2/max 6 | W 진행 후 동일 final 1행 | unit 통과, Spark skipped |
| cross_batch_duplicate | 첫 batch a,b / 다음 a,c → 전체 입력에서 3/12 | 살아 있는 ID state로 a 제거, final 3/12 | unit 통과, Spark skipped |
| payload_conflict | a=2 뒤 a=99 → EventConflictError, differing_fields=metric_value | native ID dedup은 두 번째를 숨겨 첫 값 1/2. 이는 충돌 검증 성공이 아님 | unit 통과, Spark skipped |
| utc_boundary | 00:00:59.999999Z와 09:01:00+09:00 → UTC 두 minute, 각각 1/2,1/4 | W 진행 후 두 final | unit 통과, Spark skipped |
| watermark_late | 전체 유한 입력은 late-new까지 3/12 | W·ID eviction 관측 뒤 old a와 late-new 제거, 이미 확정한 2/6 유지 | unit 통과, Spark skipped |
| idle | Python은 유한 입력 2/6을 즉시 반환 | 입력 중단 시 final 없음, OPEN_PENDING_EVENT_TIME | unit 통과, Spark skipped |
| batch_replay | 동일 입력을 다시 합쳐도 2/6 | 출력 관측 후 callback 실패, 같은 batch/checkpoint로 같은 결과 재실행 | unit 통과, Spark skipped |

유한 oracle는 늦은 신규 ID도 포함하고 tail window도 반환한다. Python 성공으로 watermark/drop/
window final화를 검증했다고 표현하지 않는다. 각 fixture의 expected_decisions도 테스트 명세이며
Spark state를 흉내 낸 Python 구현의 관측 결과가 아니다.
독립 Python 호출의 count를 batch별로 합치면 중복 사례에서 4가 되는 반면 전체 호출은 3이라는
회귀도 추가했다. foreachBatch 안에서 기존 Python 함수를 매번 부르는 것의 한계를 확인한다.

## 로컬 Streaming 테스트의 내용과 한계

- PySpark 없으면 각 시나리오가 사유와 함께 skip된다. PySpark 4.0.0 및 JDK 17/21 선택을
  검사한다. 준비 후 API/plan 예외가 발생하면 실패시키며 broad skip/xfail로 숨기지 않는다.
- 로컬 classic `local[2]`, UTC, shuffle 2, correctness check 활성화. 원격 Spark 환경변수를
  제거하고 Databricks runtime에서는 skip한다. Kafka/Registry/Delta/cloud에 연결하지 않는다.
- JSON 파일 source에 batch마다 파일 하나를 원자적으로 추가한다. 같은 checkpoint의
  AvailableNow query를 각 단계에서 재시작하고 각 실행은 45초 timeout으로 제한한다.
- 현재 harness는 같은 로컬 Spark process에서 query를 재시작한다. 별도 process kill/복구나
  장기 연속 query의 모든 스케줄링을 검증하는 테스트는 아니다.
- callback은 작은 fixture 결과를 전량 collect하여 로컬 관측 journal에 기록한다.
  Gold sink/Delta MERGE를 구현하지 않으며 운영 collect 패턴을 권장하는 것이 아니다.
- replay는 출력 관측 뒤, Spark batch 완료 전에 한 번 예외를 주입한다. 같은 batch ID에서
  같은 결과가 재관측되는지 검사한다. journal의 같은 batch 결과를 비교하기 위한 정규화는
  테스트 관측용이며 실제 Gold sink 멱등성이나 두 시스템의 원자성 증거가 아니다.
- 늦은 입력 전에 실제 W가 임계치를 넘었고 dedup state 제거가 관측됐는지 검사한다.
  이후 dropped 지표와 final 결과를 확인한다. drop 지표를 정확한 전체 고유 late 수로 간주하지 않는다.
- idle은 event-time 진행 없이 final이 없고 W가 유지되는지 확인한다. AvailableNow만으로 모든
  마지막 window가 닫힌다고 가정하지 않는다.
- UTC 출력은 Spark session의 date_format 후 UTC instant로 비교하여 호스트 시간대 영향을 피한다.
- `progress.json`과 callback journal은 pytest tmp_path 아래에 남긴다. 실패 시 재현 자료로 쓰며
  이번 실행은 skip이므로 Spark progress 증거가 생성되지 않았다.

## 실행 결과

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-phase3-contract-current \
  tests/unit/phase3/test_streaming_contract.py \
  tests/integration/test_phase3_streaming.py -rs
# 19 passed, 7 skipped

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-phase3-unit-current tests/unit
# 812 passed, 1 warning
```

warning은 기존 Authlib/httpx deprecation이다. 기존 793개와 새 19개의 합계가 812개다.
최종 Phase 3 전체 unit + 새 Streaming 파일 검사는 `98 passed, 7 skipped in 0.08s`다.
Python 구문·79열·네 신규 파일 공백 검사 통과, 기존 tracked 파일 diff는 0이다.
구문 검사가 실제 PySpark API 실행을 대신하지 않는다.

## 설계 판정과 다음 논리 단위

현재 설계를 강제로 변경할 runtime 실패는 아직 관측하지 않았다. **구현 가능 확정도 아니다.**
문서상 API는 존재하지만 실제 단일 Append query의 동작은 PySpark 부재로 미검증이다.
충돌 fixture는 설계에 이미 명시된 upstream canonical 불변 전제를 드러내며, native dedup이
충돌을 거부한다고 설명하면 안 된다. 기존 설계 파일은 변경하지 않았다.

다음 논리 단위는 **승인된 별도 로컬 PySpark 4.0.0/JDK 17·21 환경에서 이 7개 Streaming
시나리오를 실행**하는 것이다. 운영 Job/Gold sink 구현보다 이 검증을 먼저 완료한다.
실제 chaining 제약이 발생하면 예외·plan·환경을 보존하고, 원장→별도 window query 또는
전용 dedup/window state 구현을 대안으로 보고한다. correctness check 비활성화나 Update 전환,
원장 추가로 설계를 임의 변경하지 않는다.

검토 후 staging 대상은 위 네 신규 파일만이다. staging/commit은 실행하지 않았다.
제안 commit 메시지: `test(phase3): add multi-batch streaming contract fixtures`.

## 로컬 Spark 실제 실행 — 2026-10-17

### 환경과 격리

실행 기준 HEAD는 `22ecebe`이며 브랜치는 `feat/phase-3-gold-windows`, 시작 시 Git clean이었다.
macOS 26.6.2 / Darwin 25.6.0 / ARM64 환경이다. 기존 의존성 관리는 uv/pyproject/uv.lock이며
프로젝트 `.venv`는 Python 3.11.15, PySpark 미설치 상태를 유지했다.

별도 검증 루트는 `/private/tmp/ktd-spark4-sFWBFypd`다.

| 항목 | 실제 사용 값 |
|---|---|
| Python | CPython 3.11.15 ARM64, 별도 `venv/bin/python` |
| PySpark / Py4J | 4.0.0 / 0.10.9.9 |
| pytest | 9.1.1 |
| JDK | Eclipse Temurin OpenJDK 21.0.12.1+1-LTS ARM64 |
| Spark master | 기존 fixture의 `local[2]` |
| SQL | session timezone UTC, shuffle 2, correctness check true |
| Source / sink | 로컬 JSON 파일 / 기존 foreachBatch 관측 journal |

초기 시스템에는 Oracle Java 24만 있었고 JAVA_HOME은 unset이었다.
Temurin의 공식 macOS aarch64 tar.gz를 임시 디렉터리에 풀었으며 관리자 설치·전역 Java 교체·
쉘 설정 변경을 하지 않았다. JAVA_HOME은 pytest 프로세스에만 전달했다.
JDK 다운로드 SHA-256은 공식 API metadata와 일치했다:
`3623232f33a9c3baadf304480b2535f9a3cba8a58d42ecbb438ba267315d9998`.

준비 명령은 임시 cache/venv 대상으로만 실행했다.

```sh
uv --cache-dir /private/tmp/ktd-spark4-sFWBFypd/uv-cache venv \
  --python .venv/bin/python /private/tmp/ktd-spark4-sFWBFypd/venv
uv --cache-dir /private/tmp/ktd-spark4-sFWBFypd/uv-cache pip install \
  --python /private/tmp/ktd-spark4-sFWBFypd/venv/bin/python \
  'pyspark==4.0.0' 'pytest==9.1.1'
```

이 환경은 [Spark 4.0의 Java 17/21·Python 3.9+ 요구](https://spark.apache.org/docs/4.0.0/)를
충족한다. JDK는 [공식 archive 방식](https://adoptium.net/installation/archives/)으로 준비했다.
AWS/Databricks/유료 클라우드·Delta·외부 Kafka는 실행하지 않았다.

### 실행 명령과 테스트 결과

기존 테스트·fixture·집계 로직을 수정하지 않은 첫 실행:

```sh
env JAVA_HOME='/private/tmp/ktd-spark4-sFWBFypd/jdk/jdk-21.0.12.1+1/Contents/Home' \
  PYSPARK_PYTHON=/private/tmp/ktd-spark4-sFWBFypd/venv/bin/python \
  PYTHONDONTWRITEBYTECODE=1 \
  /private/tmp/ktd-spark4-sFWBFypd/venv/bin/python -m pytest \
  -vv -s -p no:cacheprovider \
  --basetemp /private/tmp/ktd-spark4-sFWBFypd/pytest \
  --junitxml /private/tmp/ktd-spark4-sFWBFypd/streaming-junit.xml \
  tests/integration/test_phase3_streaming.py \
  > /private/tmp/ktd-spark4-sFWBFypd/streaming.log 2>&1
```

결과: **0 passed / 7 failed / 0 skipped, 33.53초, exit code 1**.
7건 모두 `test_phase3_streaming.py:237`의 `json.dumps(progress, indent=2)`에서 동일 예외:
`TypeError: Object of type UUID is not JSON serializable`.

| 기존 시나리오 | 최종 판정 | 오류 전 실제 관측 |
|---|---|---|
| normal | FAIL: UUID JSON 저장 | Append final count=3,sum=12,avg=4,min=2,max=6 |
| cross_batch_duplicate | FAIL: 동일 원인 | 다른 source batch의 a 재전달 제거, final 3/12 |
| payload_conflict | FAIL: 동일 원인 | 두 번째 a=99는 ID dedup에 숨겨짐, 최초 a=2만 final 1/2 |
| utc_boundary | FAIL: 동일 원인 | UTC 두 window에 각각 count/sum=1/2,1/4 |
| watermark_late | FAIL: 동일 원인 | ID eviction 뒤 old a/late-new 2행 late drop, final 2/6 유지 |
| idle | FAIL: 동일 원인 | final 없음, W 정지, 열린 집계 state 유지 |
| batch_replay | FAIL: 동일 원인 | 의도한 callback 실패 후 batch ID 3에서 동일 final 2/6 두 번 관측 |

모든 시나리오가 결과 비교·개별 metric assertion을 지나 마지막 로그 저장에서 실패했다.
따라서 시나리오의 관측 결과가 기대와 일치했다는 사실과 pytest PASS 여부를 분리한다.
`PHASE3_LOCAL_OUTPUT_BEFORE_CHECKPOINT` 예외는 batch_replay의 의도한 오류 주입이며,
최종 실패의 최초 원인인 UUID JSON 오류와 다르다.

별도 로컬 회귀는 기존 프로젝트 `.venv`에서 실행했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-spark4-sFWBFypd/unit tests/unit/phase3
# 98 passed in 0.07s
```

이번에 전체 unit 812개를 다시 실행한 것은 아니다. 전체 812개 결과는 위의 이전 실행 기록이다.

### 로그 확보와 실제 watermark/state 관측

원래 테스트는 마지막 JSON 저장에 실패하므로 progress.json 증거도 정상 생성되지 않았다.
기존 코드 수정 없이 정확한 진단을 위해 임시 pytest hook `ktd_progress_capture.py`를 추가한
관측 재실행을 한 번 수행했다. 이 hook은 실패 프레임의 `progress`, `final`, `attempts_list`를
읽고 progress의 공식 `.json` 표현을 보존할 뿐 assertion·query·skip·실패 판정을 바꾸지 않는다.
새 query builder나 새 기능을 추가하지 않았고 기존 7개를 그대로 재실행했다.
관측 재실행도 **0 passed / 7 failed / 0 skipped, 29.52초, exit code 1**이었다.

재실행 명령은 위 명령의 로그/basetemp/JUnit을 각각 `streaming-observed.log`, `pytest-observed`,
`streaming-observed-junit.xml`로 분리하고 다음 값만 추가했다.

```text
PYTHONPATH=/private/tmp/ktd-spark4-sFWBFypd
-p ktd_progress_capture
```

이 실행의 공식 progress JSON에서 query UUID/runId·batchId·W·state operator들을 보존했다.
관측값은 fixture 순서로 추측하지 않고 이 자료와 callback journal에서 추출했다.

- 두 state operator 이름은 `stateStoreSave`와 `dedupeWithinWatermark`다.
  로컬 Spark 4.0.0은 correctness check를 켠 상태에서 이 dedup→window Append 조합의
  query 시작·실제 batch 처리를 허용했다. unsupported operator/AnalysisException은 없었다.
- `normal`, `cross_batch_duplicate`: W가 `2026-10-15T23:50:20Z` → `23:50:30Z` →
  `2026-10-16T00:15:00Z` → `00:16:00Z`로 진행했다. batch 5, W=00:15:00Z에서
  `[00:00,00:01)` final이 관측됐다. 집계 state 1개, ID state 3개 제거가 기록됐다.
- 중복 사례 batch 2의 `customMetrics.numDroppedDuplicateRows=1`, final count=3/sum=12.
  입력 batch마다 query를 같은 checkpoint로 다시 시작한 뒤에도 이 결과가 유지됐다.
- late 사례: batch 3, W=00:15:00Z에서 dedup `numRowsRemoved=2`가 먼저 기록됐다.
  이후 batch 6, W=00:16:00Z에서 `numRowsDroppedByWatermark=2`가 기록됐다.
  집계 operator의 late drop은 0이고 dedup operator가 두 입력을 제외했다.
  이는 이 fixture의 두 행 관측값이며 전체 운영 고유 late 이벤트 수로 확대하지 않는다.
- idle 사례: 입력 처리 뒤 W는 `2026-10-15T23:50:20Z`로 유지됐다.
  집계 state 최대 1행, ID state 최대 2행이 남고 제거 수는 각각 0, final은 없었다.
- replay 사례: 실패 전 관측 journal과 재실행 journal 모두 batch ID 3의 count=2/sum=6을 담는다.
  이는 동일 결과 재실행 증거이며 Delta/Gold commit의 멱등성을 검증한 것은 아니다.
- 충돌 사례는 Spark가 payload 충돌을 탐지하지 않는 기존 한계와 일치한다.
  충돌을 정상 검증했다고 판정하지 않는다.

### 최초 원인·최소 재현·분류

분류는 **테스트 관측 코드의 Spark 4.0 progress API 직렬화 호환성 오류**다.
환경/의존성 오류, AnalysisException, query 시작 실패, watermark 기대값 불일치,
수치 집계 assertion 실패는 이번 7건의 최종 원인이 아니다.

설치된 `pyspark/sql/streaming/listener.py`에서 StreamingQueryProgress는 Spark 4.0부터
`dict` subclass이고 내부 id/runId를 UUID로 저장한다. `progress['id']`는 호환성 때문에
문자열로 반환하지만 `json.dumps`가 순회하는 내부 dict 값은 UUID다.
공식 `.json` property는 JVM 또는 원본 JSON 표현을 사용한다.

새 Spark query 없이 실제 progress JSON 하나를 재구성해 아래 최소 오류도 재현했다.

```python
progress = StreamingQueryProgress.fromJson(actual_progress_json)
json.dumps([progress])  # TypeError: Object of type UUID ...
json.loads(progress.json)  # 정상 JSON 객체
```

후속 최소 수정 후보는 테스트 마지막의 progress 저장만
`[json.loads(p.json) for p in progress]`로 정규화하는 것이다.
이번에는 적용하지 않았다. generic default=str로 모든 타입 오류를 숨기는 수정은 제안하지 않는다.
해당 직렬화만 수정한 별도 작업에서 7개를 다시 실행해 PASS를 확보해야 한다.

### 증거 파일

아래는 모두 Git 비추적 임시 검증 루트의 산출물이다. `/private/tmp` 정리 시 없어질 수 있다.
보고서의 수치 요약은 보존하지만 로그 영구 보관이 필요하면 별도 승인된 위치로 복사해야 한다.

- [실행 환경](/private/tmp/ktd-spark4-sFWBFypd/environment.txt)
- [첫 실행 Spark/pytest 로그](/private/tmp/ktd-spark4-sFWBFypd/streaming.log)
- [첫 실행 JUnit](/private/tmp/ktd-spark4-sFWBFypd/streaming-junit.xml)
- [관측 재실행 로그](/private/tmp/ktd-spark4-sFWBFypd/streaming-observed.log)
- [관측 재실행 JUnit](/private/tmp/ktd-spark4-sFWBFypd/streaming-observed-junit.xml)
- [실제 progress·결과 요약](/private/tmp/ktd-spark4-sFWBFypd/observations-summary.json)
- [late 원본 progress](/private/tmp/ktd-spark4-sFWBFypd/diagnostics/watermark_late.json)
- [idle 원본 progress](/private/tmp/ktd-spark4-sFWBFypd/diagnostics/idle.json)
- [replay 원본 progress/journal](/private/tmp/ktd-spark4-sFWBFypd/diagnostics/batch_replay.json)
- [Python 회귀 로그](/private/tmp/ktd-spark4-sFWBFypd/python-contract.log)
- [UUID 최소 재현 결과](/private/tmp/ktd-spark4-sFWBFypd/uuid-reproduction.log)

### 범위·변경·다음 작업

기존 7개는 1분 tumbling만 구성한다. **5분 sliding 결과는 미검증**이다.
새 query 추가 금지 범위를 지켜 임의 sliding query를 만들지 않았다.
별도 process kill, 장기 연속 query, Gold Delta MERGE/UC/Kafka/DBR도 미검증이다.

Git 변경은 이 보고서 한 파일의 최신 상태 설명과 실행 기록 추가뿐이다.
테스트·fixture·설계·Python 집계·Phase 2·pyproject·uv.lock은 변경하지 않았다.
시스템 Java 기본값과 기존 `.venv`도 보존했다. commit/push/PR/merge는 수행하지 않았다.
다음 최소 작업은 **progress JSON 저장 호환성만 수정한 뒤 기존 7건 재실행**이다.
집계 설계 재작성이나 Gold 구현에 앞서 이 테스트 실패를 해소해야 한다.

## Progress 직렬화 수정 및 재검증 — 2026-10-20

### 범위와 정확한 원인

기준 HEAD `22ecebe`, branch `feat/phase-3-gold-windows`를 재확인했다.
시작 시 이 보고서에는 이전 실행의 미커밋 변경이 있었으며 이를 그대로 보존하고 최신 기록을 추가했다.
코드 변경은 `tests/integration/test_phase3_streaming.py`의 마지막 JSON 저장 부분뿐이다.
fixture·assertion·기대 집계값·입력 순서·watermark·집계·validated 계약은 변경하지 않았다.

기존 별도 환경 `/private/tmp/ktd-spark4-sFWBFypd/venv`와 Temurin JDK를 재사용했다.
실제 버전은 Python 3.11.15 / PySpark 4.0.0 / Py4J 0.10.9.9 / Temurin Java 21.0.12.1+1-LTS다.
프로젝트 `.venv`, pyproject, uv.lock, 시스템 Java 기본값을 변경하지 않았다.

실제 타입은 `StreamingQueryProgress`이며 Spark 4.0의 dict subclass다.
설치된 PySpark 소스와 이전 실제 progress를 재구성하여 내부 id가 UUID임을 다시 확인했다.
`json.dumps([progress])`는 동일 TypeError로 재현됐고,
`json.loads(progress.json)`은 원본 progress JSON 전체와 같았다.
UUID 문자열 변환 외의 중첩 구조·통계 의미를 바꾸는 일반 변환은 필요하지 않았다.
현재 `recentProgress` 경로는 plain dict가 아니므로 불필요한 dict fallback을 추가하지 않았다.

변경 전:

```python
(tmp_path / "progress.json").write_text(json.dumps(progress, indent=2))
```

변경 후:

```python
serialized_progress = [json.loads(item.json) for item in progress]
(tmp_path / "progress.json").write_text(
    json.dumps(serialized_progress, indent=2)
)
```

공식 `.json`으로 UUID·중첩 state/source/sink 통계의 표현을 보존한 뒤 파일에 저장한다.
`default=str`, 오류 무시, 예외 catch, assertion 삭제, skip/xfail 추가는 하지 않았다.
새 helper/query/운영 기능도 추가하지 않았다.

### 실제 실행 명령과 결과

새 로그·checkpoint·관측 루트는 `/private/tmp/ktd-progressfix-rifh4F8Z`다.
기존 진단 hook 없이 수정된 원래 테스트만 실행했다.

```sh
env JAVA_HOME='/private/tmp/ktd-spark4-sFWBFypd/jdk/jdk-21.0.12.1+1/Contents/Home' \
  PYSPARK_PYTHON=/private/tmp/ktd-spark4-sFWBFypd/venv/bin/python \
  PYTHONDONTWRITEBYTECODE=1 \
  /private/tmp/ktd-spark4-sFWBFypd/venv/bin/python -m pytest \
  -vv -s -p no:cacheprovider \
  --basetemp /private/tmp/ktd-progressfix-rifh4F8Z/pytest \
  --junitxml /private/tmp/ktd-progressfix-rifh4F8Z/streaming-junit.xml \
  tests/integration/test_phase3_streaming.py \
  > /private/tmp/ktd-progressfix-rifh4F8Z/streaming.log 2>&1
# 7 passed in 36.78s, exit code 0

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-progressfix-rifh4F8Z/python-unit tests/unit/phase3
# 98 passed in 0.05s
```

| 시나리오 | 최종 결과 | 기대값과 실제값 / 관측 |
|---|---|---|
| normal | PASS | count=3,sum=12,avg=4,min=2,max=6 일치 |
| cross_batch_duplicate | PASS | duplicate drop 1건, final count=3/sum=12 일치 |
| payload_conflict | PASS | 두 번째 충돌 payload는 ID dedup에 숨겨짐, 최초 값 count=1/sum=2 일치. 충돌 탐지 성공은 아님 |
| utc_boundary | PASS | UTC 인접 두 window의 count/sum 각각 1/2,1/4 일치 |
| watermark_late | PASS | ID state 2개 제거, 이후 late drop 2건, final count=2/sum=6 유지 |
| idle | PASS | W가 2026-10-15T23:50:20Z에 유지, final 없음 |
| batch_replay | PASS | 의도한 callback 실패 뒤 동일 batch ID 3의 같은 count=2/sum=6 재관측 |

모두 실제 `local[2]` query 시작·micro-batch 처리를 거쳐 기존 모든 assertion과 JSON 저장을 완료했다.
correctness check를 켠 dedup→1분 window→Append 연결에서 AnalysisException은 없었다.
`batch_replay` 로그의 `PHASE3_LOCAL_OUTPUT_BEFORE_CHECKPOINT`는 pytest가 기대한 오류 주입이며
새 미처리 실패가 아니다. 환경/unsupported/query 시작/집계/기대값 불일치도 새로 발생하지 않았다.

### 저장된 JSON과 실제 관측 증거

각 테스트의 `progress.json` 7개를 다시 파싱했다. 각각 기록 수는 fixture 순서대로
8/8/7/8/9/3/5개이며 id/runId는 UUID로 역해석 가능한 문자열이었다.
`durationMs`는 dict, `stateOperators`는 list, `customMetrics`는 dict,
`batchId`는 숫자로 유지됐다. callback journal의 실제 결과도 fixture 기대값과 일치했다.

정상/중복 사례는 적용 W가 23:50:20Z → 23:50:30Z → 다음 날 00:15:00Z → 00:16:00Z로
실제 진행했고, batch 5에서 `[00:00,00:01)` final을 출력했다.
late 사례의 state 제거·late drop은 `dedupeWithinWatermark`의 실제 metric이다.
idle은 벽시계 진행과 무관하게 W·미확정 window를 유지했다.
원본 nested progress와 callback journal은 다음 위치에서 확인할 수 있다.

- [Spark/pytest 실행 로그](/private/tmp/ktd-progressfix-rifh4F8Z/streaming.log)
- [JUnit 7건 결과](/private/tmp/ktd-progressfix-rifh4F8Z/streaming-junit.xml)
- [7개 progress·결과·state metric 요약](/private/tmp/ktd-progressfix-rifh4F8Z/summary.json)
- [normal progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac0/progress.json)
- [중복 progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac1/progress.json)
- [충돌 한계 progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac2/progress.json)
- [UTC 경계 progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac3/progress.json)
- [late progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac4/progress.json)
- [idle progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac5/progress.json)
- [replay progress](/private/tmp/ktd-progressfix-rifh4F8Z/pytest/test_native_multibatch_contrac6/progress.json)
- [Python 회귀 로그](/private/tmp/ktd-progressfix-rifh4F8Z/python-unit.log)

증거는 Git 비추적 임시 디렉터리에 있고 OS 정리 시 삭제될 수 있다. 결과와 주요 관측값은
이 보고서에 보존한다. 이번에 전체 unit 812개를 다시 실행한 것은 아니다.

### 변경 검증·남은 범위·다음 최소 작업

허용한 두 파일만 변경했으며 보고서의 이전 미커밋 기록도 보존했다.
테스트 diff는 저장 부분 1줄을 공식 JSON 정규화·저장 4줄로 바꾼 것뿐이다.
구문·79열·공백 검사를 통과했다. 작업 전 추적 파일 128개 hash 대조에서 허용한 두 파일만
변경됐고, 모든 기존 assert 노드는 AST 비교로 동일함을 확인했다.
Git staging/commit/push/PR/merge, AWS/Databricks 실행은 하지 않았다.

검증 범위는 local classic 파일 source, 같은 Spark process 내 동일 checkpoint query restart,
1분 tumbling, 테스트 관측 callback이다. 5분 sliding, 별도 process kill, Delta MERGE,
UC/Kafka/DBR, 임의 충돌 payload의 영속 검증은 여전히 미검증이다.
다음 최소 논리 작업은 **5분 sliding/1분 slide의 UTC 경계·중첩 window·샘플 가중 평균을
별도 fixture와 로컬 테스트로 검증**하는 것이다. 이번에는 해당 기능을 구현하지 않았다.

## 5분 Sliding Window 실제 검증 — 2026-10-20

### 사전 확인과 범위

branch `feat/phase-3-gold-windows`, HEAD `e76531e`, 시작 시 working tree clean을 확인했다.
`streaming/jobs/metric_aggregation.py`는 없으며 `windows.py`는 1분 tumbling만 구현한다.
설계의 5분 sliding은 미래 계획으로, 생산 구현이 존재하지 않는 점을 테스트 작성 전에 보고했다.
이번 검증은 새 테스트 안의 Spark native 식으로만 수행했다. 생산 Job/builder, 별도 운영 query,
원장, Kafka/Delta/클라우드는 추가하지 않았다. 기존 생산 코드·1분 테스트·fixture는 보존했다.

추가 파일은 다음 두 개이며 기존 보고서인 이 파일만 갱신했다.

- `tests/fixtures/phase3/sliding_window_events.json`: 수동 기대 행과 batch/시간 진행 입력.
- `tests/integration/test_phase3_sliding_window.py`: 독립 oracle 대조와 로컬 sliding 검증.

기존 `local_spark` fixture와 `run_available` helper를 재사용했다. 별도 테스트 격리를 위한
checkpoint만 사용하며 query는 같은 checkpoint로 AvailableNow를 순차 재시작하는 기존 패턴이다.
실시간 sleep은 사용하지 않는다.

### 실제 API·환경·실행 명령

환경은 기존 임시 venv의 Python 3.11.15 / PySpark 4.0.0 / Temurin Java 21.0.12.1이다.
`local[2]`, session timezone UTC, correctness check true를 그대로 사용했다.
API는 `withWatermark(event_time, "10 minutes")`,
`dropDuplicatesWithinWatermark(["event_id"])`,
`window(event_time, "5 minutes", "1 minute")`, Append + foreachBatch 관측이다.
집계는 count/sum/min/max 후 `avg = sum / count`다. window 평균의 평균을 사용하지 않는다.
unit은 분리 속성이며 자연키는
`(window_start, window_type, store_id, equipment_id, metric_name)`이고 type은 `5m_sliding`이다.

```sh
env JAVA_HOME='/private/tmp/ktd-spark4-sFWBFypd/jdk/jdk-21.0.12.1+1/Contents/Home' \
  PYSPARK_PYTHON=/private/tmp/ktd-spark4-sFWBFypd/venv/bin/python \
  PYTHONDONTWRITEBYTECODE=1 \
  /private/tmp/ktd-spark4-sFWBFypd/venv/bin/python -m pytest \
  -vv -s -p no:cacheprovider \
  --basetemp /private/tmp/ktd-sliding-0XSyzMri/pytest \
  --junitxml /private/tmp/ktd-sliding-0XSyzMri/junit.xml \
  tests/integration/test_phase3_sliding_window.py \
  tests/integration/test_phase3_streaming.py \
  > /private/tmp/ktd-sliding-0XSyzMri/spark.log 2>&1
# 10 passed in 37.03s, exit code 0

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-sliding-0XSyzMri/python-unit tests/unit/phase3
# 98 passed in 0.06s
```

| 신규 테스트 | 결과 | 검증 |
|---|---|---|
| test_manual_sliding_rows_against_existing_python_contract | PASS | 수동 18행·고유 ID membership·기존 Python 수치/unit 계약 대조 |
| test_sliding_multibatch_overlap_dedup_and_append | PASS | 실제 query, 겹침·경계·batch 합산·중복·metric/unit·단계별 final |
| test_sliding_idle_does_not_finalize_tail | PASS | 별도 격리 query에서 미래 진행 입력 없이 W/미확정 유지 |

기존 normal/cross_batch_duplicate/payload_conflict/utc_boundary/watermark_late/idle/batch_replay도
7건 모두 PASS였다. 기존 Python 회귀는 98 PASS다. 전체 unit을 추가로 재실행하지는 않았다.
API/AnalysisException/query 시작/수치/기대값 불일치 실패는 없었다.
기대값·watermark 임계값을 실행 후 바꾸지 않았다.

### 수동 기대값과 실제 결과

유효한 기존 metric/unit인 refrigerator temperature_celsius/celsius와 hood fan_rpm/rpm을 썼다.
온도 입력은 a=2@12:00:00, b=4@12:00:59, c=8@12:01:00,
d=16@12:04:59, e=32@12:05:00Z다. a는 다음 source batch에서 한 번 더 전달한다.
rpm은 100@12:01:00, 300@12:04:59Z다. 모든 이벤트는 기존 wire 10필드를 유지한다.

수동 표는 Spark 출력에서 생성하지 않았다. 각 window에 시간 범위로 선택한 입력을 기존
aggregate_minutes에 넘기고 minute 결과의 sum/count/min/max를 조합해 독립 대조했다.
기존 함수에는 sliding 기능을 추가하지 않았다. 고유 이벤트 7개 각각이 5개 window에 포함돼
전체 결과의 sample_count 합은 35다. 기대/실제 18행의 전체 필드 비교도 정확히 일치했다.
테스트 수치 assertion은 기존 설계처럼 rel/abs tolerance 1e-12를 사용한다.

아래 온도 행은 날짜 2026-10-20 UTC, store-001/fridge-001/temperature_celsius/celsius,
window_type=5m_sliding이다. 각 종료는 시작+5분이며 전체 자연키·end는 fixture에 명시했다.

| 시작 | count | sum | min | max | avg | 실제 |
|---|---:|---:|---:|---:|---:|---|
| 11:56 | 2 | 6 | 2 | 4 | 3 | 일치 |
| 11:57 | 3 | 14 | 2 | 8 | 14/3 | 일치 |
| 11:58 | 3 | 14 | 2 | 8 | 14/3 | 일치 |
| 11:59 | 3 | 14 | 2 | 8 | 14/3 | 일치 |
| 12:00 | 4 | 30 | 2 | 16 | 7.5 | 일치 |
| 12:01 | 3 | 56 | 8 | 32 | 56/3 | 일치 |
| 12:02 | 2 | 48 | 16 | 32 | 24 | 일치 |
| 12:03 | 2 | 48 | 16 | 32 | 24 | 일치 |
| 12:04 | 2 | 48 | 16 | 32 | 24 | 일치 |
| 12:05 | 1 | 32 | 32 | 32 | 32 | 일치 |

hood-001/fan_rpm/rpm의 8행도 일치했다. 시작 11:57~11:59는 count=1/sum=100/min=max=avg=100,
12:00~12:01은 count=2/sum=400/min=100/max=300/avg=200,
12:02~12:04는 count=1/sum=300/min=max=avg=300이다.
기존 고정 단위 계약을 유지했고 같은 metric의 상이한 단위 허용·변환을 검증한 것은 아니다.

`[12:00,12:05)`는 a,b,c,d를 포함하고 정확히 end의 e를 제외한다.
`[12:05,12:10)`는 정확히 start의 e를 포함하고 d는 제외한다.
`[11:56,12:01)`은 c를 제외한다. 따라서 start 포함/end 제외와 겹침을 모두 검증한다.
온도 12:00 window의 avg는 30/4=7.5다. minute 평균 3,8,16을 단순 평균한 9와 다르다.
중복 a는 dedup operator의 numDroppedDuplicateRows=1로 관측됐고 수치에는 한 번만 반영됐다.

### 실제 watermark와 Append 시점

시간 진행 입력은 별도 `clock-001` 장비이며 검증 대상 행에서 제외한다. 운영 heartbeat가 아니다.
적용 W는 progress에서 읽었으며 각 final 행을 내보낸 batch의 W가 그 window_end보다 큰지
확인했다. 등호의 구현 차이를 피하도록 진행 입력은 30초 경계를 사용했다.

| 단계 | 실제 W (2026-10-20 UTC) | 누적 대상 final 행 |
|---|---|---:|
| initial | 11:51:00 | 0 |
| cross_batch | 11:54:59 | 0 |
| end_boundary | 11:55:00 | 0 |
| idle | 11:55:00 | 0 |
| partial_close | 12:06:30 | 11 |
| all_close | 12:15:30 | 18 |

batch 7에서 11행, batch 9에서 나머지 7행이 출력됐다. 처음부터 final을 즉시 내보내거나
마지막 입력의 wall-clock age로 닫지 않았다. 별도 idle 테스트에서도 동일 W 유지와
state 제거 0을 확인했다. 파일 순서만으로 W 진전을 추정하지 않았다.

### 실제 실행 증거와 변경 확인

- [Spark/pytest 로그](/private/tmp/ktd-sliding-0XSyzMri/spark.log)
- [JUnit 10건 결과](/private/tmp/ktd-sliding-0XSyzMri/junit.xml)
- [18행 expected/actual·W·출력 batch 요약](/private/tmp/ktd-sliding-0XSyzMri/summary.json)
- [sliding 원본 progress](/private/tmp/ktd-sliding-0XSyzMri/pytest/test_sliding_multibatch_overla0/progress.json)
- [단계별 W와 출력 수](/private/tmp/ktd-sliding-0XSyzMri/pytest/test_sliding_multibatch_overla0/stages.json)
- [callback 실제 행](/private/tmp/ktd-sliding-0XSyzMri/pytest/test_sliding_multibatch_overla0/attempts.json)
- [독립 idle progress](/private/tmp/ktd-sliding-0XSyzMri/pytest/test_sliding_idle_does_not_fin0/progress.json)
- [Python 회귀 로그](/private/tmp/ktd-sliding-0XSyzMri/python-unit.log)

증거는 Git에 추가하지 않은 임시 파일이며 OS 정리 시 없어질 수 있다.
변경은 신규 테스트·fixture와 이 보고서뿐이다. 작업 전 hash 대조에서 기존 streaming test·
fixture·생산 Python 코드·validated·의존성 파일의 변경이 없음을 확인했다.
새 Python의 79열·구문, fixture JSON 파싱, 기존/신규 파일 공백 검사도 통과했다.

### 미검증 범위와 다음 최소 논리 작업

이번 결과는 native sliding 식의 실제 로컬 실행 증거이며 생산용 5분 구현 완료가 아니다.
생산 metric_aggregation Job은 여전히 미구현이다. 장기 연속 query/별도 process kill,
Gold Delta MERGE/UC/Kafka/DBR/영속 payload 충돌 판정은 미검증이다.
다음 최소 작업은 **Gold 자연키의 절대 집계값 쓰기 계약: 신규 INSERT, 동일 final no-op,
상이 final 실패와 재시도 수렴을 순수 테스트로 고정**하는 것이다. 이번에는 구현하지 않았다.
Git staging/commit/push/PR/merge, 유료 클라우드 실행은 수행하지 않았다.
