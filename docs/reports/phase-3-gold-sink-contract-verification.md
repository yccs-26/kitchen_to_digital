# Phase 3 Gold 자연키 멱등 저장 명세 검증

확인일: 2026-10-21 (Asia/Seoul).
Worktree: `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows`.
Branch: `feat/phase-3-gold-windows`, 기준 HEAD `c0b28d4`, 시작 시 working tree clean.

**결과: 테스트 전용 메모리 참조 모델 25 passed, 기존 Phase 3 Python 회귀 98 passed.**
후속 물리 매핑·수치 동일성 명세는 **48 passed**, 기존 회귀는 **123 passed(25+98)**다.
최신 결과: **생산 Mapper 연결 매핑 테스트 105 passed**, 기존 회귀 **123 passed**.
생산 함수 호출은 100건이며 아래 「생산 Gold Row Mapper 구현」을 참조한다.
앞부분의 매핑/허용오차 미결정 기록은 이전 저장 참조 모델 단계의 이력으로 보존한다.
생산 Row Mapper는 구현했지만 Gold Sink는 없으므로 저장 계약 구현의 검증 완료가 아니다.
실제 Delta MERGE·트랜잭션·별도 프로세스 재시작·Kafka E2E는 미검증이다.
이번에는 순수 Python 저장 명세의 기대 동작을 고정했다.

## 기존 계약과 사전 확인

직접 확인한 기준은 `phase3-streaming-design.md` §1·§5·§6, `windows.py`,
`minute_aggregation.py`, 기존 unit/fixture, 기존 Streaming 검증 보고서다.
`streaming/jobs/metric_aggregation.py`와 `streaming/sinks/gold_metric_sink.py`는 없다.
설계상 새 생산 sink나 helper를 만들지 않고 다음 기준을 테스트 전에 명시했다.

| 구분 | 기존 설계/실제 모델 | 이번 적용 |
|---|---|---|
| 자연키 | `(window_start, window_type, store_id, equipment_id, metric_name)` | `AggregationKey._fields`를 재사용 |
| 키가 아닌 속성 | window_end, unit, count, sum, avg, min, max | `MinuteAggregate`의 key 외 dataclass 필드 재사용 |
| unit | 키에 넣지 않고 고정 metric 규칙과 일치해야 함 | 같은 키의 unit 변경은 CONFLICT, 별도 행 INSERT 아님 |
| window_end | 값의 일관성 조건 | 같은 키의 end 변경은 CONFLICT |
| 해상도 | 1m_tumbling / 5m_sliding | 같은 시작 시각도 type이 달라 별도 키 |
| closed | upstream이 확정한 Append-final 입력 | closed 플래그·현재 시각 필드 새로 추가하지 않음 |
| 평균 | sum/count, 절대 집계값 | 재실행 때 평균·sum·count를 누적하지 않음 |

기존 논리 Gold 문서에는 `sample_count`, `avg_value` 등이 있지만 현재 모델은 `count`, `avg`다.
이번에는 현재 모델을 사용하고 물리 컬럼 매핑을 새로 결정하지 않았다.
`equipment_type`도 현재 MinuteAggregate에는 없으므로 임의 추가하지 않았다.

요청의 unit/window 경계 구분은 기존 설계에 맞춰 키와 불변 속성으로 나누어 검증했다.
unit을 키로 추가하면 같은 metric의 단위 변경이 CONFLICT 대신 INSERT로 우회될 수 있으므로
기존 자연키를 유지했다. 다른 metric+unit 조합은 서로 다른 키로 저장한다.

유효한 1분/5분 window는 같은 시작을 공유해도 end가 다르다. 둘 다 같은 start/end인 행은
정상 1분·5분 집계가 동시에 될 수 없다. 유효 행의 분리 저장과, 경계가 같은 가상 dict의
window_type 키 투영을 별도 테스트했다. 후자는 잘못된 길이의 행을 저장하는 테스트가 아니다.

## 참조 모델의 범위와 정책

새 로직은 `tests/unit/phase3/test_gold_sink_contract.py` 안에만 있는 MemoryGoldReference다.
실제 sink를 대체하거나 생산 코드에서 가져다 쓸 모듈을 만들지 않았다.

- New key → INSERT: 전체 확정 집계값을 복사해 저장.
- Same key + 같은 속성 → NO-OP: 성공 쓰기 수와 저장값 불변.
- Same key + 상이한 속성 → CONFLICT: 예외를 전파하고 canonical 값을 보존.
- source batch의 같은 자연키 중복은 동일/상이 payload 모두 쓰기 전 거부.
  배치 안의 임의 first/last winner를 선택하지 않음.
- 순차 처리의 성공 prefix 뒤에만 테스트 예외를 주입. 이미 성공한 행을 rollback하지 않음.
- 재시도는 이미 있는 같은 값은 NO-OP, 나머지만 INSERT하여 기대 상태로 수렴.
- batch ID 원장·서비스·DB·실제 트랜잭션은 없음. `insert_count`는 테스트 관측용 카운터다.

1분 입력은 기존 multi_batch_events fixture의 정상 값으로 실제 aggregate_minutes를 호출해
MinuteAggregate를 만든 뒤 필드를 펼친다. 5분 입력은 기존 sliding_window_events의 12:00Z
fridge 행(count=4,sum=30,avg=7.5)을 재사용한다. 기존 fixture는 수정하지 않았고 새 fixture도 불필요했다.
차원 변경 행은 저장 키의 분리 명세를 검사하는 합성 행이다. upstream 장비/metric 등록 규칙을
변경하거나 해당 조합의 domain validation 통과를 입증하는 입력은 아니다.

기존 설계는 근접 수치의 허용 비교와 policy version을 언급하지만 최종 sink 허용오차와
물리 version 필드는 확정하지 않았다. 이 점을 테스트 전에 명시했다.
참조 모델의 exact 비교는 본 fixture의 정확히 같은 재실행·명백히 다른 값 사례를 모델링한다.
이를 모든 실제 DOUBLE 근접 차이의 생산 판정 정책으로 채택한 것은 아니다.
policy version 필드·새 메타데이터를 추가하거나 근접 허용오차를 임의 선택하지 않았다.

## 테스트별 결과

| 테스트 그룹 | 수 | 결과·관측 |
|---|---:|---|
| 기존 키/집계 필드 재사용 | 1 | PASS: unit/end 제외 5필드 키, count=3/sum=12/avg=4 |
| 신규 INSERT·동일 행 NO-OP | 1 | PASS: 한 번만 쓰고 상태 불변 |
| count/sum, sum/avg/max, min, max, unit, end 변경 | 6 | PASS: differing_fields와 CONFLICT, 기존 행 불변 |
| store/equipment/metric/start 분리 | 4 | PASS: 키별 별도 INSERT |
| 다른 metric+unit | 1 | PASS: 두 행으로 분리, 단위 혼합 없음 |
| 유효한 1분/5분 해상도 | 1 | PASS: 공유 start, 다른 type/end, 두 키 저장·역순 retry NO-OP |
| 동일 경계의 type 키 투영 | 1 | PASS: type 자체가 namespace를 구분, 가상 행은 저장 안 함 |
| 동일 batch 전체·역순 재실행 | 1 | PASS: 3행 값·sum/count/avg·쓰기 수 불변 |
| 실패 지점 0/1/2/3 후 retry | 4 | PASS: prefix 보존·나머지 INSERT, 최종 3행·쓰기 3회 |
| source batch 동일/상이 중복 키 | 2 | PASS: 쓰기 전 거부, 기존 상태 보호 |
| 성공 prefix 뒤 영구 CONFLICT | 1 | PASS: prefix는 남고 canonical 보호, 재시도도 충돌 유지 |
| 입력/snapshot 외부 변조 방어 | 1 | PASS: canonical 복사본 보호 |
| 빈 batch | 1 | PASS: NO-OP, 빈 상태 |
| 합계 | 25 | 모두 PASS |

CONFLICT는 입력을 고치지 않으면 반복 실패하는 영구 데이터 문제다.
부분 실패 retry의 수렴은 동일하고 유효한 batch의 일시적 실패에 대한 기대값이며,
충돌까지 무조건 성공으로 만드는 복구 정책이 아니다.

## 부분 실패·재시도의 명시적 관측

3행 batch에서 처리 완료한 행 수를 기준으로 실패를 주입했다.

| fail_after | 실패 뒤 남은 행 / 성공 INSERT 수 | 재시도 결정 | 최종 행 / 누적 INSERT 수 |
|---:|---|---|---|
| 0 | 0 / 0 | INSERT, INSERT, INSERT | 3 / 3 |
| 1 | 1 / 1 | NO-OP, INSERT, INSERT | 3 / 3 |
| 2 | 2 / 2 | NO-OP, NO-OP, INSERT | 3 / 3 |
| 3 | 3 / 3 | NO-OP, NO-OP, NO-OP | 3 / 3 |

fail_after=3은 모든 메모리 쓰기 성공 후 응답 전 실패를 나타내며 실제 checkpoint를 구현하지 않는다.
모든 실패 지점에서 기존 prefix 값이 보존되고 retry 후 세 행의 절대값이 기대값과 같았다.
성공 후 다시 전체 batch를 적용해도 NO-OP이고 쓰기 수는 3이었다.

별도 충돌 테스트는 신규 행 하나가 저장된 뒤 다른 키의 canonical과 충돌해도 신규 행을
rollback하지 않는다. 이는 명시적으로 원자성을 가정하지 않는 모델이다.
source 중복을 쓰기 전 검사하는 것은 Python preflight 동작이지 Delta transaction 증거가 아니다.

## 실제 실행 명령

새 의존성 설치 없이 기존 프로젝트 `.venv` Python 3.11.15에서 실행했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -vv -p no:cacheprovider \
  --basetemp /private/tmp/ktd-gold-contract-JzTHew1B/pytest \
  --junitxml /private/tmp/ktd-gold-contract-JzTHew1B/gold-junit.xml \
  tests/unit/phase3/test_gold_sink_contract.py \
  > /private/tmp/ktd-gold-contract-JzTHew1B/gold.log 2>&1
# 25 passed in 0.04s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-gold-contract-JzTHew1B/legacy \
  tests/unit/phase3 --ignore=tests/unit/phase3/test_gold_sink_contract.py \
  > /private/tmp/ktd-gold-contract-JzTHew1B/legacy.log 2>&1
# 98 passed in 0.05s
```

- [25건 상세 로그](/private/tmp/ktd-gold-contract-JzTHew1B/gold.log)
- [25건 JUnit 결과](/private/tmp/ktd-gold-contract-JzTHew1B/gold-junit.xml)
- [기존 98건 회귀 로그](/private/tmp/ktd-gold-contract-JzTHew1B/legacy.log)

로그는 임시 디렉터리로 Git에 추가하지 않았다. OS 정리 시 없어질 수 있다.
기존 Streaming 10건은 이번에 재실행하지 않았다. 이 보고서의 결과를 Spark 회귀로 표현하지 않는다.

## 한계·미결정·다음 최소 작업

저장 동작을 수행하는 생산 테스트 대상이 없으므로 위 25건은 참조 모델의 자기 검증을 포함하는
실행 가능한 명세다. 기존 생산 코드와 연결된 부분은 자연키/MinuteAggregate 필드와
1분 aggregate_minutes의 입력 결과뿐이다. 생산 Gold sink의 계약 검증 완료로 간주하지 않는다.

dict의 키 유일성은 모델의 구조적 성질이며 물리 table의 중복 키/PK enforcement를 입증하지 않는다.
메모리 예외는 Delta MERGE의 원자성·격리·동시 writer·네트워크 ACK·프로세스 장애 복구를 입증하지 않는다.
이 모델은 upstream의 closed·schema·numeric 검증을 신뢰하며 window geometry/확정 시각과
모든 데이터 품질 규칙을 검증하는 새 sink validator도 아니다.

미결정은 DOUBLE 동일성 허용오차, policy/rules version 필드, 논리/물리 컬럼 매핑이다.
5분 slide 간격을 바꿀 때의 namespace/새 table 정책도 이번에 확장하지 않았다.
다음 최소 논리 작업은 **Gold 최종 행의 물리 필드 매핑과 수치 동일성 비교 정책을 확정**하는 것이다.
이후 실제 adapter/로컬 Delta 검증에서 같은 명세를 다시 검증해야 한다.

이 보고서는 Spark runtime 실행 기록과 책임을 구분하기 위해 허용된 별도 Gold 계약 보고서 경로를 썼다.
변경 파일은 신규 테스트와 이 보고서 두 개뿐이다. hash 대조에서 기존 추적 파일 130개가
변경되지 않았음을 확인했다. 새 Python 구문/79열·두 신규 파일 공백 검사도 통과했다.
Git staging/commit/push/PR/merge,
유료 클라우드 실행과 의존성 변경은 수행하지 않았다.

## Gold 물리 행 매핑·수치 동일성 확정안 — 2026-10-21

### 확인 기준과 결정 수준

이번 작업 기준 HEAD는 `ff6cc63`, branch `feat/phase-3-gold-windows`, 시작 시 Git clean이다.
설계, windows.py, MinuteAggregate, Gold 참조 모델 25건과 본 보고서를 다시 읽었다.
`metric_aggregation.py`, 생산 Gold Mapper/Sink, 실제 Gold DDL은 여전히 없다.
따라서 아래는 **저장 명세의 확정안이며 실제 테이블 생성·생산 Mapper 구현 증거가 아니다.**
생산 코드·기존 기대값·fixture·의존성은 변경하지 않는다.

### 논리 필드 → 물리 컬럼

현재 Python의 실제 명명과 타입을 입력으로 유지한다. 기존 논리 모델 문서의
sample_count/avg_value/min_value/max_value를 물리 별칭으로 채택한다.
sum_value는 기존 sum을 같은 `_value` 계열로 저장하기 위해 이번에 정한 물리 별칭이다.
새 측정값·계산식·version 컬럼을 추가한 것이 아니다.

| Python 논리 필드 | 현재 Python 타입 | 물리 컬럼 | 저장 타입 | NULL 허용 | 역할 |
|---|---|---|---|---|---|
| key.window_start | aware datetime | window_start | TIMESTAMP_LTZ | 아니오 | 자연키, UTC instant |
| key.window_type | 현재 Literal 1m_tumbling | window_type | STRING | 아니오 | 자연키, 아래 namespace |
| key.store_id | str | store_id | STRING | 아니오 | 자연키, 원문 유지 |
| key.equipment_id | str | equipment_id | STRING | 아니오 | 자연키, 원문 유지 |
| key.metric_name | str | metric_name | STRING | 아니오 | 자연키, 원문 유지 |
| window_end | aware datetime | window_end | TIMESTAMP_LTZ | 아니오 | 설정으로 결정되는 불변 경계 |
| unit | str | unit | STRING | 아니오 | 키 아님, 불변 속성 |
| count | int | sample_count | BIGINT | 아니오 | 고유 이벤트 수, 양의 정수 |
| sum | float | sum_value | DOUBLE | 아니오 | 절대 합계 |
| min | float (입력이 int면 실제 int일 수도 있음) | min_value | DOUBLE | 아니오 | 절대 최솟값 |
| max | float (입력이 int면 실제 int일 수도 있음) | max_value | DOUBLE | 아니오 | 절대 최댓값 |
| avg | float | avg_value | DOUBLE | 아니오 | sum_value/sample_count |

Python dataclass의 타입 힌트 자체가 NULL을 강제 차단하는 것은 아니다. NOT NULL은 이번
저장 명세이며 실제 SQL provider enforcement는 미검증이다. 문자열은 nonblank, 시각은 aware
UTC instant/microsecond 의미로 보존하고 session timezone UTC를 요구한다. NTZ나 문자열로
시각 의미를 바꾸지 않는다. 이름을 lower/strip하여 다른 키를 합치지도 않는다.

count는 `1 <= count <= 2**63-1`이고 bool·실수는 거부한다. count를 DOUBLE로 바꾸어
비교하지 않는다. 빈 window는 기존 계약대로 만들지 않으므로 count=0 행은 수용하지 않는다.
집계 수치는 유한 DOUBLE이어야 한다. Python int 수치도 DOUBLE로 손실 없이 표현될 때만
변환한다. 표현 불가능한 큰 정수를 조용히 반올림하지 않는다.

추가 필드 현황:

- equipment_type: 논리 Gold 문서에만 있고 현재 MinuteAggregate에는 없다. 초기 12컬럼
  계약에서는 자동 추가/NULL 채움하지 않는다. dimension 매핑 방식과 포함 여부는 미결정이다.
- policy_version/rules_version/schema version: 현재 최종 집계 행에 실제 필드가 없다.
  validated의 schema_version을 Gold 정책 버전으로 복사하지 않는다. 컬럼명·타입·값 모두 미정이다.
- batch_id, query epoch, is_final 등 새 설정/완료 컬럼을 추가하지 않는다.

Spark의 BIGINT/DOUBLE/TIMESTAMP_LTZ 의미는
[공식 Spark 4.0 타입 문서](https://spark.apache.org/docs/4.0.0/sql-ref-datatypes.html)를 확인했다.
실제 DDL·카탈로그 스키마를 조회하거나 실행하지 않았다.

### 자연키와 Window 불변성

자연키는 기존 5개 그대로다:
`(window_start, window_type, store_id, equipment_id, metric_name)`.
unit/window_end/sample_count/집계 수치는 키에 추가하지 않는다.
동일 키의 유효한 확정값에서 이 비교 속성이 바뀌면 canonical을 보호하고 CONFLICT다.

초기 Gold namespace의 의미를 다음과 같이 고정하는 안으로 정한다.

| window_type | 길이 | slide | anchor | 종료 |
|---|---|---|---|---|
| 1m_tumbling | 1분 | 1분 | UTC epoch, 분 경계 | start+1분 |
| 5m_sliding | 5분 | 1분 | UTC epoch, 분 경계 | start+5분 |

현재 생산 AggregationKey의 타입은 1m만 지원한다. 5m은 기존 수동 fixture 및 native Spark
테스트로만 확인한 표현이며 생산 타입을 이번에 확장하지 않는다.
기존 설계의 sliding interval 실험 후보는 같은 Gold namespace의 설정을 임의 변경해도
된다는 뜻으로 해석하지 않는다. 운영 시 5m_sliding에 다른 slide/anchor를 재사용하지 않는다.

이 조건에서 type이 길이/slide를 결정하고 end도 결정되므로 현재 자연키는 유효하다.
단, **행 자체에는 slide/anchor/version이 없어 실제 producer 설정을 증명할 수 없다.**
예를 들어 5분/30초와 5분/1분은 분 경계 start와 같은 end를 공유할 수 있다.
같은 5m_sliding label을 재사용하면 키가 충돌하거나 서로 다른 정책의 결과가 혼재할 수 있다.
다른 설정 도입은 별도 namespace/table·이관 검토가 필요하며 이번에 키를 자동 변경하지 않는다.
이는 배포 설정의 불변 조건이지 실제 Job의 검증 완료가 아니다.

window_end가 설정과 다르면 올바른 신규 확정 행으로 수용하면 안 된다. 기존 키에 잘못된
end를 재전달하는 사례는 이전 참조 모델과 동일하게 CONFLICT로 기존 값을 보호한다.
이번 매핑 참조 함수는 완전한 window geometry/closure validator가 아니다.

### 비교 대상과 수치 동일성 결정

비교 대상은 키 일치 후 다음 7개 물리 속성이다:
`window_end, unit, sample_count, sum_value, min_value, max_value, avg_value`.
미정 version 컬럼이나 실행 시각은 비교 대상에 넣지 않는다.

| 대상 | 결정 |
|---|---|
| sample_count | BIGINT 범위의 정확한 정수 비교 |
| unit | 문자열 정확 비교, 단위 변환 없음 |
| window_end | UTC instant 정확 비교 |
| sum/min/max | 유한 binary64 수치의 정확 비교. 반올림·epsilon 없음 |
| avg | canonical sum/count로 계산된 값인지 정확 검증 후 비교 |
| 0.0 / -0.0 | 둘 다 +0.0으로 정규화하고 NO-OP 허용 |
| NULL/NaN/Infinity | 비교 전에 입력 거부. 결측값을 0으로 채우지 않음 |

avg가 sum/count와 다르면 유효한 확정 행이 아니므로 입력 거부다. 서로 다른 sum/count를
가진 유효 행은 CONFLICT다. 별도로 수집한 avg를 기존 값에 더하거나, 검산에 실패한 avg를
조용히 덮어써서 정상화하지 않는다.

이 정책은 기존 INSERT/NO-OP/CONFLICT에서 '다른 확정값은 자동 갱신하지 않음'을
보수적으로 유지한다. signed zero만 정규화하고, 서로 다른 유한 수치를 같은 결과로 합치지 않는다.

이전 설계의 rel/abs tolerance 1e-12는 Python fsum과 Spark 수치 검산용 시작값이었다.
이를 sink identity 비교에 자동 적용하지 않는다. 테스트에서 12와 그 다음 representable
DOUBLE은 그 tolerance로 isclose지만 exact 정책은 CONFLICT였다. 허용오차는 실제 데이터
변경과 누산 오차를 구분할 근거가 없고 작은 변경을 NO-OP으로 숨길 수 있으므로 채택하지 않는다.

Python float는 이 환경에서 53-bit mantissa/8-byte binary64이고 Spark DOUBLE도 8-byte다.
표현 폭이 같아도 계산 알고리즘·누산 순서가 같다는 뜻은 아니다.
Python 일반 sum([0.1,0.2,0.3])과 math.fsum은 서로 다른 수치가 되었고 같은 키에서 충돌했다.
이는 로컬 Python 수치 실험이며 Spark의 모든 재계산 순서 차이를 실측한 것은 아니다.
[Python 부동소수점 설명](https://docs.python.org/3.11/tutorial/floatingpoint.html)

따라서 NO-OP 보장 범위는 **동일한 canonical final 값의 재전달**이다.
Spark가 원본 입력에서 다시 집계해 1 ULP 달라졌을 때도 NO-OP이 된다는 보장은 하지 않는다.
그 경우 canonical을 보존하고 실패시키므로 데이터 덮어쓰기는 막지만 가용성은 떨어질 수 있다.
재계산 결정성/확정 결과 materialization/다른 누산 방식 중 구현 해법은 후속 검증 사항이다.
엄밀한 값 보호와 무조건 retry 성공을 동시에 입증했다고 보고하지 않는다.

### 입력 방어와 JSON

기존 Phase 2 `validate_domain_event`의 실제 NON_FINITE_METRIC_VALUE/INVALID_METRIC_VALUE/
NULL_FIELD 경로를 재사용해 NaN/±Infinity/bool/NULL 입력 거부를 직접 확인했다.
기존 aggregate_minutes도 finite·bool 방어와 math.fsum을 사용한다. 생산 검증 코드를 수정하지 않았다.
유한 입력만 있어도 누산 범위 초과는 별개이므로 최종 집계의 모든 DOUBLE도 다시 유한성 검사가 필요하다.

JSON은 수치 표기 문자열을 identity로 삼지 않는다. Python dumps/loads 왕복에서 binary64
값이 보존되는지 확인했고 0.3과 3e-1은 같은 숫자였다. 반면 0.1+0.2와 0.3의 계산 차이를
JSON이 제거해 주지는 않는다. allow_nan=False를 사용하고 NaN/Infinity의 비표준 JSON을 허용하지 않는다.
BIGINT count는 2**53+1도 정수로 왕복해야 하며 DOUBLE/JavaScript Number 등을 거치면 안 된다.
다른 외부 serializer의 동작까지 이번 Python 왕복으로 보장하지 않는다.

### 최소 명세 테스트와 회귀

신규 파일은 `tests/unit/phase3/test_gold_row_mapping.py` 하나다.
기존 Gold 테스트의 실제 MinuteAggregate fixture와 5m 수동 행, MemoryGoldReference를 재사용한다.
새 mapping 함수는 테스트 내부의 physical_row_spec일 뿐 생산 Mapper가 아니다.
기존 25개 테스트의 기대값과 참조 모델을 변경하지 않았다.

| 그룹 | 수 | 결과 |
|---|---:|---|
| 12필드 매핑/키/정수 의미 | 1 | PASS |
| window type→길이/slide/end 및 충돌 조건 | 1 | PASS |
| 동일값·JSON 왕복·수치 비트 보존 | 1 | PASS |
| 다른 count/sum, unit, end | 4 | PASS: CONFLICT |
| signed zero 정규화 | 1 | PASS: +0.0 보존, NO-OP |
| 1 ULP·누산 순서 차이 | 2 | PASS: 차이를 숨기지 않는 CONFLICT |
| avg 파생 관계 | 1 | PASS: 잘못된 avg 거부 |
| 잘못된 count | 6 | PASS: NULL/bool/0/음수/float/BIGINT 초과 거부 |
| BIGINT와 손실 없는 DOUBLE 변환 | 2 | PASS: 큰 count 정확 비교, 손실 있는 정수 수치 변환 거부 |
| 12컬럼별 NULL | 12 | PASS: 모두 거부 |
| 4수치별 NaN/±Infinity | 12 | PASS: 비교 전 거부 |
| 실제 Phase 2 비정상 입력 방어 | 5 | PASS |
| 신규 합계 | 48 | 모두 PASS |

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -vv -p no:cacheprovider \
  --basetemp /private/tmp/ktd-gold-mapping-Rwdxfitc/final \
  --junitxml /private/tmp/ktd-gold-mapping-Rwdxfitc/mapping-final-junit.xml \
  tests/unit/phase3/test_gold_row_mapping.py \
  > /private/tmp/ktd-gold-mapping-Rwdxfitc/mapping-final.log 2>&1
# 48 passed in 0.07s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-gold-mapping-Rwdxfitc/regression \
  --junitxml /private/tmp/ktd-gold-mapping-Rwdxfitc/regression-junit.xml \
  tests/unit/phase3 --ignore=tests/unit/phase3/test_gold_row_mapping.py \
  > /private/tmp/ktd-gold-mapping-Rwdxfitc/regression.log 2>&1
# 123 passed in 0.10s = Gold 25 + 기존 Phase 3 98
```

- [신규 48건 로그](/private/tmp/ktd-gold-mapping-Rwdxfitc/mapping-final.log)
- [신규 JUnit](/private/tmp/ktd-gold-mapping-Rwdxfitc/mapping-final-junit.xml)
- [기존 123건 회귀 로그](/private/tmp/ktd-gold-mapping-Rwdxfitc/regression.log)
- [기존 회귀 JUnit](/private/tmp/ktd-gold-mapping-Rwdxfitc/regression-junit.xml)

임시 로그는 Git에 추가하지 않았으며 OS 정리 시 없어질 수 있다.
Spark Streaming 10 passed는 이전 작업 결과다. 이번에는 Spark/Delta/Kafka/Databricks를 실행하지 않았다.

### 실제 구현과 남은 결정

확정한 것은 12컬럼 매핑·NOT NULL·BIGINT/DOUBLE/TIMESTAMP_LTZ 명세와 정확 비교 정책이다.
DDL, 실제 mapper, Delta NULL/타입/동시성 enforcement, end/slide 설정을 강제하는 생산 validator는 없다.
참조 함수는 타입/정규화/avg 파생 검산의 명세 테스트이며 생산 구현의 증거가 아니다.

미정은 equipment_type의 포함/소스, policy/rules/schema version 필드·타입·수명,
실제 Gold DDL 적용과 재계산 결정성/완전한 restart 수렴 방식이다.
같은 window_type을 다른 slide로 재사용하는 충돌을 자동 키 변경으로 해결하지 않았다.
초기 namespace에서는 위 설정을 고정하고 변경은 별도 검토한다.

다음 최소 논리 작업은 **이 명세의 순수 Gold row Mapper를 생산 모듈로 구현하고 같은 테스트를
실제 대상에 연결**하는 것이다. Delta 저장은 그 다음 단계로 남긴다.
이번 변경은 신규 명세 테스트와 기존 보고서뿐이다. Git staging/commit/push/PR/merge,
의존성 변경·유료 클라우드 실행을 하지 않았다.

최종 구문·79열·공백 검사를 통과했다. 작업 전 추적 파일 132개 hash 대조에서 이 보고서만
변경됐으며 기존 Gold 25건·Phase 3 코드/테스트·fixture·설계·의존성 파일은 그대로다.


## 생산 Gold Row Mapper 구현 — 2026-10-22

### 범위와 인터페이스

시작 HEAD `89a4b50fce5effa3569fb48d740523960518fea7`, working tree clean,
branch `feat/phase-3-gold-windows`를 직접 확인했다. 위의 과거 명세 단계 기록은 보존한다.
동등한 Mapper와 `streaming/jobs/metric_aggregation.py`는 없었다.

생산 모듈 `streaming/aggregations/gold_row_mapper.py`:

```python
map_gold_row(logical: Mapping[str, object] | MinuteAggregate) -> GoldPhysicalRow
```

`GoldPhysicalRow`는 12컬럼 TypedDict이며 런타임에는 새 dict를 반환한다.
기존 frozen MinuteAggregate와 평탄한 12필드 논리 Mapping을 받는다.
생산 5분 집계 모델은 추가하지 않았고 5m_sliding 행은 Mapping으로 입력한다.
잘못된 입력은 `GoldRowMappingError(ValueError)`로 거부한다.
입력 변경, 저장소 조회, INSERT/NO-OP/CONFLICT 판정은 수행하지 않는다.

| 논리 필드 | 물리 컬럼 | Python 반환 타입 / 저장 타입 계약 |
|---|---|---|
| window_start, window_end | 동명 | UTC aware datetime / TIMESTAMP_LTZ |
| window_type, store_id, equipment_id, metric_name, unit | 동명 | str / STRING |
| count | sample_count | int / BIGINT |
| sum | sum_value | float / DOUBLE |
| min | min_value | float / DOUBLE |
| max | max_value | float / DOUBLE |
| avg | avg_value | float / DOUBLE |

이는 Python 변환 계약이다. SQL 타입의 실제 DDL 적용을 입증하지 않는다.
자연키 5필드는 그대로이고 unit/window_end는 속성이다.

### 검증과 정규화

- 정확한 12필드 집합, 누락/추가 필드 및 모든 NULL 거부.
- 문자열은 비어 있거나 공백뿐이면 거부하며 원문을 보존한다.
- naive datetime과 문자열 timestamp 거부. aware datetime은 같은 instant의 UTC로 변환.
- 시작은 UTC 분 경계, end는 정확히 start+기간이어야 한다.
  1m_tumbling=1분/1분, 5m_sliding=5분/1분의 기존 namespace만 허용한다.
- count는 bool을 제외한 정확한 int, `1 <= count <= 2**63-1`이며 float로 변환하지 않는다.
- sum/min/max/avg는 bool을 제외한 int/float만 수용하고 유한 binary64로 변환한다.
  정수의 손실 있는 DOUBLE 변환과 범위 초과는 명시적으로 거부한다.
- 모든 signed zero는 +0.0으로 정규화하고 avg는 sum/count와 정확히 일치해야 한다.
- 기존 명세에 없는 min/max와 sum의 추가 관계식, epsilon, 반올림, 단위 변환은 추가하지 않았다.

닫힌 window라는 사실과 producer의 실제 slide 설정은 행만으로 증명할 수 없다.
확정 집계 전달은 호출자의 책임이며 closure 검증이나 새 streaming query는 없다.

### 기존 assertion과 테스트 연결

기존 `physical_row_spec`의 참조 구현을 제거하고 실제 `map_gold_row` 호출 어댑터로 바꿨다.
테스트의 독립 FIELD_MAPPING/수동 수치 기대값은 생산 상수에서 가져오지 않는다.
기존 assertion AST가 모두 남아 있음을 HEAD와 비교해 확인했다.
기존 48개 시나리오와 parameter 값/기대값을 변경하지 않았다.

유일한 경계 차이는 잘못된 1분 window_end 사례다. 생산 Mapper는 이를 입력 오류로 거부한다.
테스트 어댑터는 명시적인 raises assertion으로 그 거부를 확인한 다음, 원래 입력을 독립
MemoryGoldReference에서 비교하여 기존 CONFLICT assertion도 보존한다.
거부된 행이 Mapper를 통과했다거나 실제 저장되었다는 의미가 아니다.
유효한 unit/집계값 변경은 Mapper를 통과한 후 참조 저장소에서 CONFLICT로 비교된다.

임시 pytest profile hook으로 실제 생산 함수 진입을 계측했다. 저장소에 계측 코드는 추가하지 않았다.

| 구분 | 결과 | 실제 생산 Mapper 호출 테스트 수 |
|---|---|---|
| 기존 매핑 명세 시나리오 | 48 passed | 43 |
| 추가 입력/타입/UTC/모델 경계 시나리오 | 57 passed | 57 |
| 매핑 파일 합계 | 105 passed | 100 |
| 기존 Gold Sink 참조 모델 | 25 passed | 0 |
| 기존 Phase 3 Python | 98 passed | 0 |
| 전체 Phase 3 unit | 228 passed, 0 failed, 0 skipped | 100 |

Mapper를 호출하지 않는 매핑 파일의 5건은 기존 Phase 2 validator의 NULL/bool/NaN/±Infinity
입력 거부 검사다. 이를 생산 Mapper 테스트 수에 포함하지 않았다.
추가 57건은 물리 수치/타입 2, 기존 dataclass 1, 누락 필드 12, schema/window 경계 9,
sliding 길이 1, UTC/입력 보존 1, 문자열 차원 15, 수치 타입/overflow 12,
BIGINT 상한 1, 비집계 입력 3이다.

### 실행 근거

Python 3.11.15와 기존 `.venv`를 재사용했고 의존성 변경은 없다.
worktree 루트에서 실행했다.

```sh
.venv/bin/python -m pytest tests/unit/phase3/test_gold_row_mapping.py -q \
  -o cache_dir=/private/tmp/ktd-gold-mapper-verification/pytest-cache \
  --basetemp=/private/tmp/ktd-gold-mapper-verification/mapping-tmp
# 105 passed in 0.12s

PYTHONPATH=/private/tmp/ktd-gold-mapper-verification \
.venv/bin/python -m pytest tests/unit/phase3 -v -p mapper_calls \
  -o cache_dir=/private/tmp/ktd-gold-mapper-verification/pytest-cache \
  --basetemp=/private/tmp/ktd-gold-mapper-verification/regression-tmp \
  --junitxml=/private/tmp/ktd-gold-mapper-verification/regression.xml
# 228 passed in 0.16s

git diff --check
# PASS
```

`/private/tmp/ktd-gold-mapper-verification/`에 regression.log, regression.xml,
mapper_calls.py, mapper-calls.json을 보존했다. 임시 파일은 Git 대상이 아니다.
변경 Python 파일은 compile 기반 구문 검사와 79컬럼/공백 검사를 통과했다.

### 한계와 다음 단위

정확 비교와 1 ULP/누산 순서 차이에 대한 참조 Sink의 CONFLICT assertion을 유지했다.
Mapper는 그 작은 차이를 없애지 않으며 충돌 판단 자체는 후속 Sink 책임이다.
동일 canonical 값의 재전달만으로부터 재집계 결정성이나 무조건 retry 성공을 추론하지 않는다.

Spark Streaming 10 passed는 이전 검증 이력이며 이번에는 재실행하지 않았다.
Delta DDL/MERGE/트랜잭션, checkpoint/프로세스 재시작, Kafka/Databricks E2E는 미검증이다.
유료 클라우드 실행, 자동 staging/commit/push/PR/merge는 수행하지 않았다.

변경 파일은 정확히 다음 3개다.

- `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows/streaming/aggregations/gold_row_mapper.py`
- `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows/tests/unit/phase3/test_gold_row_mapping.py`
- `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows/docs/reports/phase-3-gold-sink-contract-verification.md`

다음 최소 단위는 이 물리 행을 입력으로 하는 순수한 자연키/속성 비교 함수의 계약 검토다.
별도 승인 후에만 후속 구현을 진행하며 실제 Delta Sink와 분리해 검증할 수 있다.
