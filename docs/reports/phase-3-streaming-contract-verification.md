# Phase 3 Multi-batch dedup/window 계약 검증

확인일: 2026-10-17 (Asia/Seoul). 기준 HEAD: `92b5620`.
브랜치: `feat/phase-3-gold-windows`. 시작 시 working tree clean.
상태: fixture·Python 계약 테스트 완료, 실제 Spark 실행 미검증.

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
