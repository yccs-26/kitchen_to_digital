# Phase 3 Gold 자연키 멱등 저장 명세 검증

확인일: 2026-10-21 (Asia/Seoul).
Worktree: `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows`.
Branch: `feat/phase-3-gold-windows`, 기준 HEAD `c0b28d4`, 시작 시 working tree clean.

**결과: 테스트 전용 메모리 참조 모델 25 passed, 기존 Phase 3 Python 회귀 98 passed.**
생산 Gold sink/helper는 없으므로 생산용 저장 계약 구현의 검증 완료가 아니다.
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
