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

## 장비별 Operational State 계약 검토 — 문서 설계 단위

검토일: 2026-10-17 (Asia/Seoul). 시작 HEAD
`9c3dabbd1340d3324267d6624a803012482b1ac5`, 브랜치
`feat/phase-4-twin-state`, 시작 Git status clean.

이번 변경은 [장비별 계약·확장 설계](../streaming/equipment-state-contracts.md),
`streaming/state/README.md`의 연결, 이 보고서의 기록 세 문서다.
Simulator·equipment registry·metric 규칙·Avro·기존 FSM·Phase 2/3 코드를
직접 읽었으며 수정하지 않았다. 새 장비 profile/registry·신호·Health/timer는
구현하지 않았다.

### 확인 결과

- Simulator 설정은 6항목·5유형·5개 metric 이름이다. numeric main 경로와
  현재 validation_rules에 일치하는 것은 4항목·4유형·3개 metric 이름이다.
  이는 설정/코드 정합성 증거이며 실시간 Kafka 데이터 목록이 아니다.
- fridge-door-001의 door_open과 dishwasher-001의 state는 boolean/categorical
  생성 후보가 있지만 main 발행 및 현재 numeric Avro/validated 검증 경로는
  지원하지 않는다. door의 parent 장비 관계도 등록되어 있지 않다.
- 냉장·냉동고 온도와 fryer 유온은 각각 냉각 동작/조리·청소 의도를 확정하지
  못한다. Hood RPM은 회전 관측 파생 후보이나 정책·정지/기동 fixture가 없다.
- 냉각 STANDBY/COOLING, hood STOPPED/ROTATING, dishwasher cycle 상태와
  장비별 허용/금지 표는 **설계 제안**이다. 실제 제어기 신호 또는 장비 지원이
  확인된 구현 상태가 아니다. 기존 synthetic 조리 reference는 그대로 유지한다.
- 장비별 binding과 profile/version registry가 기존 reference 함수를 adapter로
  호출하도록 제안했다. 새 장비를 조리 enum에 강제 매핑하거나 미설정 장비를
  조리 정책으로 fallback하지 않는다. 실제 typed 입력·source·cycle·완료·
  정책 migration은 별도 선행 계약이다.

### 실제 검증과 한계

오프라인 inventory 스크립트로 EQUIPMENTS와 validation_rules의 장비·metric·unit
일치 여부를 대조했다. simulator main 또는 producer를 실행하지 않았다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-device-design-phase4 tests/unit/phase4
# 151 passed in 0.11s
```

P4-1/P4-2 기존 테스트 호환성을 확인했다. 문서만 변경하여 전체 회귀를 이번에
재실행하지 않았으며 기존 전체 회귀 숫자를 이번 실행 결과로 재사용하지 않는다.
새 profile·정책 선택·numeric qualifier·실제 입력 E2E는 미검증이다.
AWS/Databricks 실행, Spark/Kafka/DynamoDB 연결을 수행하지 않았다.

다음 권장 한 단위는 로컬 Policy Registry 선택 계약과 reference adapter다.
장비별 입력이 확보되기 전 기본 정책 자동 적용을 막는 것이 완료 기준이다.
세 문서만 대상으로 `docs(phase4): define equipment state contracts and registry design`
메시지를 권장한다. Git staging/commit/push/merge 없이 사용자 검토를 기다린다.

## Equipment FSM Registry — 순수 선택 계층 구현

검증일: 2026-10-17 (Asia/Seoul). 시작 HEAD
`897d745d4443d069824334ceb541eefc74da8d93`, 브랜치
`feat/phase-4-twin-state`, 시작 Git status clean.

### 실제 변경 파일

| 파일 | 변경 |
|---|---|
| `streaming/state/bindings.py` | ProfileKey·입력 종류·명시적 장비 binding |
| `streaming/state/adapters.py` | 기존 Fryer Reference transition 위임 |
| `streaming/state/registry.py` | tuple catalog·lookup·snapshot key 검사·선택 오류 |
| `tests/unit/phase4/test_registry.py` | 새 테스트 51개 |
| `docs/streaming/equipment-state-contracts.md` | 구현 상태·API·오류 우선순위·후속 범위 갱신 |
| `docs/reports/phase-4-verification.md` | 이번 검증 증거 |

현재 validation_rules의 실제 identity는 store-001의 fridge-001, freezer-001,
fryer-001, hood-001이다. equipment_id는 key이며 store/type를 함께 대조한다.
계약을 직접 확인했고 설정이나 기존 10필드 Avro를 변경하지 않았다.

### Registry 계약과 검증

ProfileKey는 equipment_type/profile_id/version을 분리한 exact-match key다.
장비별 InputKind까지 포함하는 binding은 호출자가 명시적으로 주입한다.
기본 장비 등록·설정 파일 읽기·최신 버전 선택·다른 장비 profile fallback은 없다.
등록된 실제 장비라는 사실만으로 명시적 mode/청소 신호를 사용할 수 있다고
간주하지 않는다. 테스트의 fryer-001 identity는 실제 설정을 사용하되
SYNTHETIC_SIGNAL 능력은 테스트 가정이다. NUMERIC_TELEMETRY binding은
INPUT_CONTRACT_UNAVAILABLE로 거부한다.

장비 없음, store/type 충돌, snapshot/request profile 불일치, 미등록 profile,
알려진 profile의 handler 미구현을 구별했다. 같은 profile에 handler를 중복 등록하거나
다른 profile/version에 Reference handler를 연결하는 구성도 거부한다.
RegistryError는 FSM Decision과 별도이며 거부된 입력은 snapshot을 바꾸지 않는다.

FryerReferenceAdapter 하나만 계산 가능하다. 기존 FSM·순서·중복·Health 분리와
`state_change_id` 알고리즘을 그대로 사용한다. ProfileSnapshot이 key를 보존하므로
다른 binding/profile/version으로 옮겨진 snapshot은 선택 단계에서 거부한다.
refrigerator/freezer/hood/dishwasher는 catalog entry만 있고 handler=None이다.
다른 장비 snapshot·Operational State·Health/timer는 미리 구현하지 않았다.

테스트는 실제 fryer identity의 정상 binding과 4단계 전이·3개 Health 초기값,
동일 snapshot/처리 지연 재실행 ID·직접 FSM 결과 동일성·다음 snapshot 중복을
검증한다. 미등록 identity·store/type 충돌·profile/version 불일치·미등록 profile·
미구현 handler·numeric/잘못된 입력·다른 identity snapshot·잘못된 handler key·
중복 구성과 frozen 입력 보존도 검증했다.

### 실제 실행 명령

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-registry-unit tests/unit/phase4
# 202 passed in 0.16s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-registry-regression -m 'not integration'
# 916 passed, 10 deselected, 1 warning in 3.33s
```

외부 서비스 integration 10개를 제외했다. 경고는 기존 Authlib httpx deprecation
1건이다. 새 Python 구문·79열 검사와 종료 시 git diff --check를 확인했다.
기존 FSM·계약·Simulator·Avro·Phase 2/3 코드의 diff가 없는지도 확인했다.
미사용 테스트 import 정리 후 `--basetemp /private/tmp/ktd-registry-final`로
Phase 4 단위를 재확인했다: `202 passed in 0.14s`.

### 미검증과 다음 경계

실제 Fryer 제어기 입력·source 권한·새 wire/validated 연결은 미구현이다.
새 profile의 실제 장비 동작, 수치 Hysteresis, Health/timer, profile migration,
영속 snapshot/원장, outbox/sink 복구·E2E는 이번 검증 범위가 아니다.
AWS/Databricks·Spark·Kafka·DynamoDB를 실행하거나 연결하지 않았다.

다음 권장 단위는 Hood RPM qualifier의 순수 계산·시계열 fixture 검토다.
사용자 검토 전 자동으로 다음 구현을 시작하지 않는다. 위 여섯 파일만 대상으로
`feat(phase4): add equipment FSM registry and reference adapter` 메시지를
권장한다. git add/commit/push/merge는 수행하지 않았다.

## Hood RPM Qualifier — 순수 Python 구현

검증일: 2026-10-17 (Asia/Seoul). 시작 HEAD
`2d4c81868d82ec152922d9592a183df1039ae65b`, 브랜치
`feat/phase-4-twin-state`, 시작 Git status clean.
실행 계획의 Phase 4·Health 분리 경계, 현재 Simulator·validation_rules·Avro·
validated publisher·event_identity·기존 P4-1/P4-2·Registry 코드를 직접 확인했다.

### 이번 변경 파일

- `streaming/state/hood.py`: validated 10필드 입력 모델, Hood 전용 enum, 명시적
  immutable 정책·snapshot, 순수 qualify_rpm, 초기화/후보/전이 결과와 결정적 출력 ID.
- `tests/unit/phase4/test_hood.py`: 새 테스트 69개.
- `tests/fixtures/phase4/hood_rpm.json`: 정지 초기화→경계 노이즈→회전→정지 시계열.
- `docs/streaming/equipment-state-contracts.md`: 8절에 입력·수치·시간·identity·한계 기록.
- `docs/reports/phase-4-verification.md`: 이번 실행 증거.

### 계약과 구현 판단

실제 설정은 store-001/hood-001, equipment_type=hood, fan_rpm/rpm이며 Simulator
생성 범위는 800~1800이다. 실제 정상 운전 사양이 아니다. fixture는 별도 초기
테스트 파라미터 enter=600 rpm/3초, exit=200 rpm/2초, max gap=2초를 주입한다.
정지/기동 fixture는 synthetic이며 Simulator나 실제 Kafka에서 캡처한 것이 아니다.
모든 fixture 입력은 기존 domain validator·등록 장비·metric/unit·버전 검사를
통과하는지 테스트했다. Phase 2 파일은 수정하지 않았다.

임계값·기간·공백의 등호 경계를 포함한다. deadband/조건 이탈은 후보를 취소하고
확정 상태를 유지한다. 공백 초과는 후보를 새로 시작한다. None에서 최초 확정은
INITIALIZED/빈 전이 의도이며, 이후 STOPPED→ROTATING→STOPPED만 전이 출력한다.
중복/충돌·역순/동시각·미래·기준 역전·identity 불일치는 후보와 원장을 보존한다.
새 시간순의 음수·잘못된 type/metric/unit/version은 current를 보존하고 후보를
취소하며 INVALID_SAMPLE로 소비한다. 구조 오류는 모델 생성 예외이며 caller의
오류 인계·stream 연결은 미구현이다. Health 계산은 포함하지 않는다.

state_change_id는 정책 버전·Hood profile·모든 정책값·장비 identity·원본 event_id·
UTC event_time·전후 상태·후보 시작 시각의 SHA-256이다. 처리 지연과 무관하고
같은 순서 재실행은 동일 결과를 만든다. 다음 snapshot에 replay하면 재출력하지
않는다. 초기화는 가상의 이전 상태나 state_change_id를 만들지 않는다.

기존 Registry·bindings·adapters·Fryer FSM·contracts는 수정하지 않았다.
HOOD_PROFILE은 계속 handler=None이며 Registry 호출은 기존처럼
HANDLER_NOT_IMPLEMENTED다. 새 계산을 기존 Registry에 연결했다고 주장하지 않는다.

### 테스트 우선 실행과 실제 결과

먼저 테스트·fixture만 작성하고 실행했다:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-red tests/unit/phase4/test_hood.py
# ModuleNotFoundError: streaming.state.hood (collection error 1건)
```

구현 후 63개가 통과했고 fixture의 전이 trigger 기대 ID 오기 1건을 수정했다.
시계열 자체의 기대 전이·상태는 모두 통과했으며 3초/2초 충족 시각인
rpm-10/rpm-17을 출력 trigger로 확인했다. 추가 replay/Health 분리 사례를 포함한
최종 기능 코드에서 다음을 실행했다:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-final-phase4 tests/unit/phase4
# 271 passed in 0.19s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-regression -m 'not integration'
# 985 passed, 10 deselected, 1 warning in 3.14s
```

경고는 기존 Authlib httpx deprecation 1건이다. 외부 integration 10개는 제외했다.
경계 duration 직전/정확한 충족, 최대 공백 등호/초과, 초기 방향 변경, deadband·
확정 구간 복귀, 긴 공백과 처리 지연, 과거 ID replay·전체 payload 충돌·UTC 및
int/float 동등성, 정책값의 출력 ID 반영을 확인했다. 기존 Registry 51개를 포함한
Phase 4 회귀가 통과했다. Python 구문·79열과 git diff --check도 최종 확인한다.

### 한계와 다음 작업

순수 Python/fixture 증거이며 실장비, Spark·Kafka·DynamoDB·AWS/Databricks·E2E
실행 증거가 아니다. 해당 연결/실행은 하지 않았다. 현재 정책은 샘플 사이 조건
유지를 가정하며 source 권한·물리적 운전 성능을 검증하지 않는다. 원장은 무제한
로컬 tuple이며 영속 복구·보존 정책·outbox와 출력 ACK는 미구현이다.
Health/timer·정책 migration·실제 validated consumer도 미구현이다.

다음 제안은 Hood용 Registry adapter/snapshot/result 확장 계약을 별도 단위로
검토하는 것이다. 이번에는 자동 등록하거나 기존 Registry 계약을 바꾸지 않았다.
위 5개 파일만 권장 staging 대상이며 메시지는
`feat(phase4): add deterministic hood RPM qualifier`이다.
Git add/commit/push/merge 없이 이 논리 단위를 마치고 사용자 검토를 기다린다.

## Hood Registry Adapter — 순수 연결 계층 구현

검증일: 2026-10-17 (Asia/Seoul). 시작 브랜치 `feat/phase-4-twin-state`, HEAD
`2d4c81868d82ec152922d9592a183df1039ae65b`.
시작 시 이전 Hood Qualifier 작업의 문서 2개가 수정 상태였고 Hood 코드·테스트·
fixture 3개가 untracked였다. 이 기존 작업을 보존하며 이번 연결 계층을 추가했다.
현재 소스·Registry 테스트·실행 계획 Phase 4와 장비 계약을 직접 대조했다.

### 변경과 공통 인터페이스

이번 단위의 파일은 아래 6개다.

- `streaming/state/adapters.py`: HoodRpmAdapter, 공통 profile/input_kind/
  snapshot_type/accepts/evaluate 표면, 허용 adapter 두 타입 union.
- `streaming/state/registry.py`: 두 snapshot/result/input 타입 지원, Hood v1 명시
  catalog 등록, 잘못된 입력·snapshot 타입의 실행 전 거부.
- `tests/unit/phase4/test_registry.py`: 지원 handler 목록에 Hood 반영, 미구현 목록의
  Hood 사례 1개 제거. 나머지 Fryer 테스트와 기대 동작은 보존.
- `tests/unit/phase4/test_hood_registry.py`: 새 연결 계층 테스트 25개.
- `docs/streaming/equipment-state-contracts.md`: 최신 지원 목록과 9절 계약.
- `docs/reports/phase-4-verification.md`: 이번 검증 기록.

기존 미추적 선행 파일 `streaming/state/hood.py`, `tests/unit/phase4/test_hood.py`,
`tests/fixtures/phase4/hood_rpm.json`은 이번 시작 시 SHA-256과 종료 시 값을 비교하여
내용 보존을 확인했다. 기존 FSM·contracts·bindings도 같은 방식으로 보존 확인했다.
FryerReferenceAdapter의 evaluate 위임 함수 본문은 그대로다.

상태 공통화나 전이 재구현 없이 `ProfileSnapshot.state`에 TwinState/HoodSnapshot을
보존하고 `RegistryResult.fsm`에 원본 FsmResult/HoodResult를 반환한다. snapshot의
policy·후보 방향·시작 시각·원장·cursor를 버리지 않는다. Hood profile/version은
`(hood, rpm-observation, "1")`, 입력은 NUMERIC_TELEMETRY/HoodRpmEvent다.
장비 binding은 caller가 명시하며 파일 읽기·기본 장비·fallback은 없다.

정상 등록 시 실제 지원하는 계산은 Fryer synthetic Reference와 Hood RPM 두 개다.
refrigerator/freezer/dishwasher는 알려진 catalog만 있고 handler는 없다.
Hood 정책은 snapshot에서 그대로 가져오며 기본 숫자를 새로 주입하지 않는다.
PENDING/NO_CHANGE/INITIALIZED는 전이 출력과 state_change_id를 만들지 않는다.
실제 전이 의도와 SHA-256은 기존 Qualifier 결과 그대로다.

Registry는 잘못된 type/metric/unit/schema version·입력 교차를 계산 전에 거부한다.
이때 snapshot 전체는 유지된다. 직접 Qualifier에서 잘못된 metric/unit/version을
INVALID_SAMPLE로 소비·후보 취소하는 동작과 구분한다. 음수 RPM은 Qualifier의 기존
INVALID_SAMPLE 처리에 위임한다. 실제 소비 경로에서 계약 거부를 무시한 구간의
연속성을 보장하지 않으며 오류 인계/후보 단절 절차는 후속이다.

### Fixture 및 검증

새 fixture를 복제하지 않고 기존 hood_rpm.json을 그대로 사용했다. 단계마다 direct
qualify_rpm과 Registry 결과가 동일한지 대조했다. 초기화·노이즈·후보 유지·양방향
전이, 같은 snapshot의 재계산·처리 지연, 확정 snapshot replay, 원문 ID 중복·충돌,
동시각·역순·음수 입력, profile/version·장비 identity·snapshot 타입·InputKind와
잘못된 metric/unit/schema version을 확인했다. 선택 거부 시 두 Adapter의 evaluate를
호출하지 않는 것도 monkeypatch로 검증했다. 외부 I/O mock 성공을 E2E로 해석하지 않는다.

테스트를 먼저 작성해 HoodRpmAdapter import 부재로 실패하는 것을 확인한 뒤 구현했다.
기존 지원 목록 테스트를 새 기능에 맞춰 갱신했다. 이전 271개에서 미구현 Hood 기대
사례 1개를 제거하고 새 25개를 추가하여 현재 Phase 4는 295개다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-adapter-red tests/unit/phase4/test_hood_registry.py
# ImportError: HoodRpmAdapter 미구현, collection error 1건

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-adapter-focused tests/unit/phase4/test_hood_registry.py
# 25 passed in 0.05s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-adapter-regression -m 'not integration'
# 1009 passed, 10 deselected, 1 warning in 3.34s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-hood-adapter-final tests/unit/phase4
# 295 passed in 0.29s
```

마지막 Phase 4 재실행은 테스트 줄 길이 정리 후 수행했다. 전체 회귀 이후 기능 코드
변경은 없다. 경고는 기존 Authlib httpx deprecation 1건이며 외부 integration 10개를
제외했다. 새/수정 Python의 AST 구문·79열 검사와 최종 git diff --check를 확인했다.

### 미검증·Git·다음 단위

Health/Alert·freshness/timer, 정책 migration, 승인 source 권한, Avro decode·validated
consumer·Kafka key 검사, 영속 checkpoint/원장 보존, outbox·sink ACK·분산 복구는
미구현·미검증이다. 실제 하드웨어나 cloud E2E 증거가 아니다. Avro·Simulator·
Phase 2/3 변경, AWS/Databricks 실행, Spark/Kafka/DynamoDB 연결은 수행하지 않았다.

최종 확인 중 HEAD가 `9a90b3d55b79464edbfd7f4a9dc611939d8e6c04`로 바뀌었고,
선행 Hood 5개 파일이 `feat(phase4): add deterministic hood RPM qualifier` commit에
포함된 것을 git log/show로 확인했다. 이 작업에서 Git 변경 명령은 실행하지 않았다.
선행 Hood/FSM/contracts/bindings의 내용 hash는 시작 시점과 같으며 기능 코드도
회귀 실행 때와 같다. 최종 git status 기준 정확한 staging 대상은 위 연결 계층
6개 파일뿐이다. 메시지 제안은
`feat(phase4): integrate hood RPM qualifier with registry`다. git add/commit/push/merge는
수행하지 않았다.

다음 논리 작업 후보는 Operational State와 독립적인 Health freshness/timer 계약이다.
별도 last_seen·평가 시각 단조성·timeout 경계·관측 갱신과 timer 경합을 먼저 정한다.
이번 연결 단위 완료 후 사용자 검토를 기다린다.
