# Phase 4 검증 기록 — P4-1 순수 operational FSM

검증일: 2026-10-17 (Asia/Seoul). 브랜치 `feat/phase-4-twin-state`,
시작 HEAD `9600ee9aabc47d1c85a329ba5a4900940465e835`.
`b8c6ee2`의 후속 커밋임을 확인했다. 시작 Git status는 clean이었다.
기존 신호 타입·형태 테스트·synthetic fixture를 재사용했다.

## 구현한 한 단위

| 파일 | 변경 |
|---|---|
| `streaming/state/contracts.py` | 명시적 청소 정상 완료 payload 추가, 기존 후보 유지 |
| `streaming/state/fsm.py` | immutable snapshot, 순수 전이, 판정 이유, 전이 출력 의도·안정 ID |
| `streaming/state/README.md` | 신호 제안·전이표·시간/중복/재실행 정책·통합 한계 |
| `tests/fixtures/phase4/reference_cycle.json` | synthetic 4단계 정상 순환 |
| `tests/unit/phase4/test_fsm.py` | 정상/금지/중복/충돌/시각/identity/결정성 검증 |
| `docs/reports/phase-4-verification.md` | 이번 증거와 미검증 경계 |

순수 함수는 이전 상태, synthetic 명시적 신호, aware 기준 시각을 받는다.
IDLE→PREHEATING은 예열 모드 보고, PREHEATING→COOKING은 조리 모드 보고,
COOKING→CLEANING은 청소 활성, CLEANING→IDLE은 명시적 청소 정상 완료로만
전이한다. 전체 허용/금지 표와 no-op 조건은 `streaming/state/README.md`에 있다.
청소 비활성이나 IDLE 보고만으로 청소 완료를 판단하지 않는다.

동일 ID·동일 전체 입력은 DUPLICATE, 동일 ID·다른 내용은 ID_CONFLICT다.
store_id/equipment_id 불일치, 미래 사건, 역순 사건, distinct ID 동시각,
마지막 소비 시각 이전 기준 시각을 거부한다. 시간 검사 통과 후 금지 전이도
원장에 남겨 같은 입력을 다른 상태에서 새 전이로 재해석하지 않는다.
원장과 상태는 호출자가 전달하며 전역 cache·시계·외부 I/O가 없다.

Health는 operational state와 분리해서 그대로 보존한다. FaultSignal은
HEALTH_DEFERRED이며 health 발생/해제 구현 완료가 아니다. 전이 차단 interlock,
다중 장애, STALE/freshness, last_seen timer는 미구현이다.

## 확인한 입력과 기준 문서

현재 10필드 Avro·domain rules·validated publisher를 직접 확인했다.
기존 sensor event_id, equipment_id key, UTC event_time 계약을 변경하지 않았다.
새 synthetic signal_id는 별도 신호의 identity로 기존 event_id의 대체물이 아니다.
실제 validated numeric 이벤트를 이 FSM으로 연결하는 adapter는 없다.
CLEANING/FAULT를 수치로 추정하거나 metric_name으로 몰래 추가하지 않았다.

지정 실행 계획은 원본 checkout의
`docs/plans/ktd-forward-execution-plan-2026-10-06.md`에서 읽었다.
기존 상태 설계도 함께 확인했다. 계획서 사본을 추적 파일로 복사하지 않았다.
`ktd-worktrees/handoff/validated-contract.md`는 일반 읽기 및 권한 확장 읽기
모두 OS의 `Operation not permitted`로 실패했다. 따라서 handoff 원문과의 최신
대조는 **미검증**이다. 코드 확인을 handoff 본문 확인으로 대신 표기하지 않는다.

## 실제 실행한 테스트

저장소 루트에서 실행했다. 모든 명령은 bytecode/cache 기록을 끄고 이 작업
전용 임시 디렉터리를 사용한다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p41-unit tests/unit/phase4
# 131 passed in 0.12s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p41-regression tests/unit
# 845 passed, 1 warning in 5.18s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p41-full -m 'not integration'
# 845 passed, 10 deselected, 1 warning in 3.05s
```

마지막 PEP 8 줄 길이 정리 후 같은 전체 로컬 회귀를
`--basetemp /private/tmp/ktd-p41-final`로 다시 실행했다.
결과: `845 passed, 10 deselected, 1 warning in 3.19s`.
테스트 인자 줄바꿈을 마무리한 뒤 `--basetemp /private/tmp/ktd-p41-style`로
Phase 4 단위를 재확인했다: `131 passed in 0.08s`.
경고는 설치된 Authlib httpx 모듈 deprecation 1건이다.

검증 시나리오: 4상태 × 6개 operational payload의 전체 조합, 모든 health
초기값에서 정상 순환, 모든 상태/health 조합에서 fault 입력의 분리,
과거 ID 재입력·변조·금지 입력 재입력, 장비/매장 불일치, 미래→정상 재시도,
역순/동시각/기준 시각 역전, UTC 동등성, 동일 snapshot 재계산·처리 지연 후
동일 출력, 잘못된 시간·numeric payload·불일치 원장 거부.

## 보장 한계와 후속 결정

- synthetic fixture 기반 순수 Python 증거다. 실제 센서 신호·Kafka/MSK·Spark·
  Databricks·DynamoDB·Iceberg E2E 증거가 아니다. 외부 실행은 하지 않았다.
- 제어기 source 권한, 실제 장비 대응, 별도 신호 wire 계약/전송 경로,
  event_id 대응, 실제 청소 완료 의미는 통합 전 확정해야 한다.
- 이번 정책은 명시적 관측 보고를 즉시 적용한다. 수치 threshold·hysteresis·
  지속시간·장비별 정책은 구현하지 않았다. 운영/안전 제어 규칙으로 검증하지 않았다.
- 동시각은 먼저 소비한 신호만 인정하며 입력 순서를 바꾼 결과 수렴은 보장하지
  않는다. timestamp만으로 제어기 순서를 복원하지 않는다. 재정렬·sequence는 후속이다.
- 전체 원문 tuple 원장은 로컬 참조용이다. 무제한 크기·선형 검색/복사 비용이
  있으며 운영 dedup 보존/복구 정책과 checkpoint 직렬화는 미검증이다.
- state_change_id와 출력 의도는 결정적이지만 전달 ACK가 아니다. 다음 snapshot
  확정 후 replay는 DUPLICATE로 출력이 없다. 실제 I/O 연결 전에 outbox/부분 실패
  복구·확정 순서를 설계해야 한다. 분산 exactly-once를 보장하지 않는다.
- `config/state_machine.yaml`은 만들지 않았다. 이 단위는 버전이 고정된 명시적
  보고 정책이며 설정 로더 I/O와 미사용 설정은 추가하지 않았다.
- Phase 2/3·Avro는 수정하지 않았고 Job/producer/sink를 추가하지 않았다.
  P4 전체 DoD 완료 또는 main 병합 준비 완료가 아니다.

## Git 및 사용자 검토

작업 브랜치와 HEAD를 유지하고 staging/commit/push/merge를 수행하지 않았다.
권장 commit 범위는 위 6개 파일만이며 메시지 제안은
`feat(phase4): add deterministic synthetic reference FSM`이다.
원문 handoff 대조 한계, 청소 완료의 분리, 시간·중복 정책과 health 유보 범위를
검토한 뒤 사용자가 직접 commit한다. 다음 구현 단위는 자동으로 진행하지 않는다.
테스트 화면을 증빙으로 남긴다면 'P4-1 synthetic 순수 FSM 로컬 단위 검증'으로
표시하고 명령·결과와 연결한다.

## P4-2 — 순서 제어 및 Hysteresis 범위 검증

검증일: 2026-10-17 (Asia/Seoul). 시작 HEAD
`52473133fa934208639555dd0215e61cc1677da5`, 브랜치
`feat/phase-4-twin-state`. 시작 Git status는 clean이었다.
현재 `contracts.py`, `fsm.py`, 기존 Phase 4 테스트·fixture·README와
상태 설계를 직접 확인했다. 아래 기록은 P4-1의 이전 검증 기록에 추가한다.

### 변경 범위와 결정

| 파일 | 역할 |
|---|---|
| `streaming/state/README.md` | 순서 우선순위, Hysteresis 적용/미적용 이유, 수치 qualifier 정책, Health/timer 인계 |
| `tests/fixtures/phase4/rapid_reports.json` | 10 ms 간격의 16개 명시적 보고 입력과 기대 판정/상태 |
| `tests/unit/phase4/test_ordering.py` | 새 순서·반복·재실행 테스트 20개 |
| `docs/reports/phase-4-verification.md` | P4-2 실행 결과와 미검증 경계 추가 |

FSM 함수·계약·정책 버전은 변경하지 않았다. 기존 Avro 10필드,
Phase 2/3도 유지했다. 새 runtime/연결·설정 파일은 추가하지 않았다.

순서 정책은 identity → 저장된 ID의 중복/충돌 → 기준 시각 역전 → 미래 →
역순/동시각 → 전이 판정 순이다. 금지/no-op/health 유보도 소비 cursor에
기록하지만 시간·identity 거부는 snapshot을 유지한다. DUPLICATE/ID_CONFLICT는
시각 검사보다 우선한다. 오래된 사건이 현재 상태를 되돌리거나 전이 의도를
새로 만들지 못한다. 동일 시각 distinct ID는 도착 순서에 따라 결과가 다를 수
있다. 같은 순서 재실행의 결정성을 순열 불변성으로 설명하지 않는다.

명시적 mode에는 Hysteresis/지속시간을 추가하지 않는다. 확정된 제어기 관측
보고를 즉시 반영하는 기존 의미를 보존한다. 허용된 전체 순환이 빠르게
반복되어도 억제하지 않는다. 테스트는 이 사실을 고정하며 물리적 속도나
anti-chatter 성능을 검증하지 않는다.

수치 qualifier는 별도 LOW/HIGH 판정으로 정의했다. `enter > exit`의 임계값,
각 방향 양수 지속시간, 최대 관측 공백, deadband에서 확정 구간 유지·후보 취소,
조건 이탈/공백 시 후보 초기화, 새 유효 샘플에서만 확정을 요구한다.
중복/시각 거부 또는 벽시계 경과를 지속시간 근거로 추가하지 않는다.
실제 threshold/기간·numeric 판정 함수·테스트는 **미구현·미검증**이다.
HIGH를 COOKING/FAULT로 매핑하지 않고 CLEANING 완료의 명시적 신호를 유지한다.
상세 정책은 `streaming/state/README.md`의 P4-2 절에 있다.

### 새 테스트 증거

- 반복 보고 fixture를 3개 Health 초기값에서 실행: 기대 상태/판정, 6개 출력
  의도, 두 번째 예열의 새 ID, 같은 snapshot 재계산·처리 지연 후 ID 동일성 확인.
- 정상 4개 전이 각각에 동시각·1 μs 과거·1초 과거의 새 ID를 입력하는
  12개 사례: 모든 경우 snapshot 원문·원장·current와 빈 출력을 유지.
- no-op/금지/health 유보 3개 사례: cursor 전진과 이후 ID replay·내용 충돌,
  시각 검사 이전 중복 판정 확인.
- 동시각 distinct ID 2개 도착 순서: 예열 먼저와 IDLE 먼저가 서로 다른
  결과를 낸다는 현재 정책의 한계를 명시적으로 확인.

### 실행 명령과 결과

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p42-unit tests/unit/phase4
# 151 passed in 0.13s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p42-regression -m 'not integration'
# 865 passed, 10 deselected, 1 warning in 3.15s
```

기존 Authlib httpx deprecation warning 1건이다. 외부 서비스가 필요한
integration 10개를 명시적으로 제외했다. 단위 테스트의 숫자를 실제 센서,
Kafka/MSK, Spark, DynamoDB 또는 cloud E2E 증거로 사용하지 않는다.
Python 구문·79열·fixture JSON 검사를 수행하며 종료 시 `git diff --check`와
변경 범위를 확인했다. 테스트 호출 인자 줄바꿈 후
`--basetemp /private/tmp/ktd-p42-final`로 단위를 재확인했다:
`151 passed in 0.11s`.

### 다음 Health/timer의 경계

operational cursor와 별도의 freshness 근거·fault 집합을 가진 snapshot으로
진행한다. 평가 시각의 단조성, 유효 관측의 last_seen 갱신 기준, timeout 등호
경계와 관측 세대가 바뀐 timer의 무효화를 다음 단위에서 검증해야 한다.
금지/중복/시각 거부를 수신했다는 사실만으로 freshness를 연장하지 않는다.
실제 허용 source·payload, timeout 숫자, FAULT/STALE 동시 조건 표시 우선순위와
초기 unknown·복귀는 구현 전에 확정한다. timer와 fault lifecycle은 이번에
구현하지 않았다. 원장 영속성/크기 제한, outbox/출력 확정 순서 역시 미검증이다.

이번 권장 commit 범위는 위 4개 파일만이다. 메시지 제안:
`test(phase4): verify ordering and define hysteresis scope`.
Staging/commit/push/merge 또는 다음 Health 구현을 자동 실행하지 않는다.
