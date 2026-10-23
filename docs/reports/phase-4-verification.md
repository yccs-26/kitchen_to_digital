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

## Health Freshness / Timer — 계약 및 테스트 시나리오 설계

검토일: 2026-10-17 (Asia/Seoul). 브랜치 `feat/phase-4-twin-state`, 시작 HEAD
`8bed0f7b41f4a2d10c7064137ddf0bea410bb9c8`, 시작 git status clean.
이번 작업은 **문서 설계만**이며 Health FSM·Scheduler·Job 4·sink를 구현하지 않았다.

### 확인 근거와 문서 배치

현재 adapters/registry/bindings/contracts/FSM/Hood/README, 기존 Ordering·Dedup·
Hood 관련 테스트와 장비 계약·이 보고서, config의 장비/metric/unit/버전·Job 2 설정을
직접 확인했다. 원본 checkout의 기준 실행 계획 Phase 4, 기존 ADR 두 개와
architecture/design-decisions.md·planning-update.md·state-and-alerts.md도 대조했다.
Health 전용 ADR 및 실제 freshness timeout/profile 설정은 발견되지 않았다.
worktree에는 AGENTS.md가 없어 제공된 PEP 8 지침과 원본 checkout의 AGENTS.md를
읽었다. 계획서 사본이나 새 AGENTS.md를 생성하지 않았다.

기존 장비 계약·README에는 Health 분리와 후속 방향만 있어 상세 규범은 별도
`docs/streaming/equipment-health-contracts.md`에 모았다. 기존 장비 계약은 10절 링크와
요약만 추가해 상세 내용을 중복 관리하지 않는다. 수정/신규 대상은 이 세 문서뿐이다.

### 논리적으로 결정한 정책

- HEALTHY는 승인 관측의 freshness, STALE은 timeout 만료, FAULT는 승인된 명시적
  고장이다. Operational State와 Phase 5 Alert를 변경하지 않는다. active fault
  우선 표시를 정의하되 현재 numeric 장비의 fault 입력은 미지원으로 유지한다.
- last_seen은 수용한 유효 관측의 UTC event_time이며 수신/ingestion/processing
  timestamp와 다르다. 장비별 실제 metric을 증거로 삼고 새 heartbeat를 가정하지 않는다.
- timeout T는 미정이다. Health 전용 exact-match profile에 명시적 양수 T·source
  allowlist가 있어야 활성화한다. Hood max_sample_gap을 freshness timeout으로 쓰지 않는다.
- 관측 age<T만 freshness 수용한다. 이미 만료된 지연 이벤트는 upstream에서 valid여도
  복구/연장을 하지 않는다. dedup/conflict/역순/동시각·부적합 입력은 last_seen을
  연장하지 않고 canonical snapshot을 보존한다.
- 별도 주입 tick의 평가 시각 >= deadline부터 만료한다. 명시적 활성화 epoch/start로
  최초 무입력도 다루며 재시작 때 start를 현재 시각으로 리셋하지 않는다.
- 관측 세대+deadline+profile+epoch token이 다른 timer는 폐기한다. 평가 시각의 단조성과
  직렬화된 관측/tick 순서를 보존한다. 경합 순서를 바꾼 이력의 수렴은 보장하지 않는다.
- Health snapshot·관측 원장·revision과 별도 health_change_id를 정의한다. 원본
  Operational state_change_id 규칙을 바꾸지 않는다. 같은 기록 명령/시각의 replay와
  실제 현재 시각으로 재평가하는 동작을 구별한다. 출력 ACK/outbox는 미구현이다.

### 테스트 설계와 실행한 회귀의 구분

Health 계약 8절의 표에 최초 관측, timeout 직전/등호/직후·무입력·초기 무관측,
복구·만료된 지연 입력·중복/충돌·동시각·부적합 입력 혼재, timer 반복·snapshot
복원·신규 관측 경합 양쪽 순서·과거 token·평가 시각 역전·fault 미지원·profile
비호환의 입력, 초기 snapshot, 평가 시각, 기대 상태/출력을 명시했다.
T는 기호이며 실제 설정값이나 runtime fixture를 추가하지 않았다.
**모든 Health 시나리오는 미구현·미실행**이다. 가상 fault profile 사례도 설계뿐이다.

기존 코드의 회귀 확인으로 아래만 실행했다:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-contract-phase4 tests/unit/phase4
# 295 passed in 0.22s
```

이 숫자는 기존 Operational FSM/Registry/Hood 테스트 결과이며 신규 Health 검증이
아니다. 문서 변경만 있어 전체 로컬 회귀는 재실행하지 않았고 이전 1009 passed를
이번 실행으로 재사용하지 않는다. 문서 링크·시나리오 표의 구조, 최종 git diff --check,
보호 대상 state 코드/Phase 4 테스트·fixture/config의 시작/종료 SHA-256 동일성을
확인한다. 실행/설정 파일 변경은 없다.

### 미결정 사항과 완료 경계

장비별 T·source 권한, tick runtime·주기/시계 동기화, 원장 TTL/영속 저장·CAS,
fault source/code/latch/reset, Health wire schema·outbox/sink 복구는 선택지·권장안·
근거를 별도로 기록했다. timeout/source·runtime 선택이 닫히기 전 운영 Health
활성화를 허용하지 않는다. 새 계약의 단위 테스트와 Health 동작은 미검증이다.
AWS/Databricks/Confluent 실행, Spark/Kafka/DynamoDB 연결, Alert 구현은 하지 않았다.

이번 정확한 git add 대상은 아래 세 문서다:

```sh
git add -- docs/streaming/equipment-health-contracts.md docs/streaming/equipment-state-contracts.md docs/reports/phase-4-verification.md
```

권장 메시지: `docs(phase4): define health freshness and timer contracts`.
위 명령과 commit/push/merge는 실행하지 않았다. 이 설계 단위 완료 후 멈추며
사용자 검토 없이 다음 구현으로 진행하지 않는다.

## Health Core Reducer — 순수 관측/tick 계산 구현

검증일: 2026-10-20 (Asia/Seoul). 브랜치 `feat/phase-4-twin-state`, 시작 HEAD
`f03203fe81613dcf2194d55c5b191686a30c8158`, 시작 git status clean.
최근 Phase 4 commit은 Health 계약, Hood Registry 연결, Hood Qualifier, Registry다.
현재 계약 원문·코드·Ordering/Dedup/Hood 테스트·config를 직접 확인했다.
worktree에 AGENTS.md가 없어 제공된 지침과 원본 checkout의 AGENTS.md를 확인했다.
기준 실행 계획의 Phase 4·Health/Alert 경계도 대조했다.

### 변경 파일과 구조

- `streaming/state/health.py`: HealthPolicy/Observation/Snapshot, timer token/tick,
  initialize_health/reduce_health, HealthResult/Change/Decision. 단일 파일의 순수 코어다.
- `tests/unit/phase4/test_health.py`: 관측·원장·정책/형식 호환성·JSON 테스트 재구성.
- `tests/unit/phase4/test_health_timer.py`: tick·경합·기록 순서 replay·변화 ID chain.
- `docs/streaming/equipment-health-contracts.md`: 현재 구현 API/필드 매핑과 26개 설계
  시나리오의 실제 테스트 대응을 10절에 추가했다. 최초 설계표와 최신 상태를 구분한다.
- `docs/streaming/equipment-state-contracts.md`: Health 절의 미구현 표기를 최신 상세
  계약으로 연결하도록 최소 갱신했다. Operational 계약은 변경하지 않았다.
- `docs/reports/phase-4-verification.md`: 이번 실행 기록.

기존 Fryer FSM/Hood qualifier/Registry/adapters/bindings/contracts와 기존 테스트·fixture,
config는 시작 시 SHA-256과 종료 시 값을 비교해 보존 확인한다. 외부 I/O나 scheduler를
구현하지 않았고 별도 health_timer.py도 필요하지 않아 만들지 않았다.

### 정책 분류와 입력/출력

확정된 last_seen=UTC event_time, 초기 None, 관측 age<timeout, deadline 등호부터
STALE, monotonic evaluation, 중복/충돌·세대 token·revision·SHA-256 정책을 구현했다.
실제 timeout/source·fault lifecycle·운영 scheduler/storage는 미정으로 유지한다.
테스트는 10초 timeout과 fixture-simulator source를 명시적으로 주입한다. 이는
실장비 운전 사양 또는 승인된 실 source가 아니다.

HealthPolicy는 caller가 제공한 numeric EquipmentBinding, binding/입력 계약 revision,
별도 Health ProfileKey, 단일 metric/unit·schema/source 목록·timeout을 보존한다.
정책 fingerprint는 정수 μs·정렬 allowlist·장비 identity·Operational dependency까지
포함한다. snapshot은 정책 본문·epoch/start·관측/평가 시각·deadline·원장·표시·revision을
저장하며 fingerprint/generation은 재구성한다. HealthStatus enum 값만 재사용하고
Operational State 객체와 cursor/원장은 분리한다.

reduce_health(previous, item, policy=..., reference_time=...)는 immutable snapshot과
HealthObservation 또는 HealthTick(token)을 받고 HealthResult(snapshot, decision,
changes)를 반환한다. 원본 입력/snapshot은 변경하지 않으며 현재 시각을 읽지 않는다.
불일치 policy/profile/dependency·표현/계약 version은 INCOMPATIBLE_SNAPSHOT이다.
부적합 primitive는 INVALID_OBSERVATION, 구조/API 오류는 생성 시 예외다.

최초 유효 관측은 HEALTHY 초기화 1개, 최초 무입력 만료는 STALE 초기화 1개다.
health_revision은 표시 변화에만 증가하고 수용 no-op은 snapshot_revision만 증가한다.
새 관측은 last_seen/deadline을 갱신하며 이미 만료된 관측·중복·충돌·역순·동시각·잘못된
입력은 snapshot 전체를 보존한다. tick이 먼저 만료되면 STALE 뒤 복구 HEALTHY,
관측이 먼저 수용되면 옛 token은 무효다. 과거 deadline 경과만으로 미처리 이력을
합성하지 않는다. 옛 token은 평가 cursor도 전진시키지 못한다.

Health 변화는 기존 state_change_id와 별도 health_change_id를 사용한다. 장비·profile/
정책·epoch·Health revision/이전 change chain·전후 표시·초기화/전이 종류·관측 또는
세대/deadline cause의 canonical SHA-256이며 현재 시각/random UUID는 없다. 같은
snapshot·입력·평가 시각 replay는 동일 결과이며 늦은 같은 만료의 출력 ID도 동일하다.
이미 확정된 snapshot의 반복 tick/관측은 변화 이벤트를 재생성하지 않는다.

### 테스트 우선 실행과 실제 검증

테스트 두 파일을 먼저 작성해 health 모듈 부재로 collection error 2건을 확인한 뒤
코어를 구현했다. 최초 90개 통과 후 신규 복구·평가 시각 API·snapshot 형식/계약 version·
처리 지연 ID·boot/event 세대 충돌 방어를 추가하여 최종 98개가 통과했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-core-red tests/unit/phase4/test_health.py tests/unit/phase4/test_health_timer.py
# ModuleNotFoundError: streaming.state.health, collection error 2건

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-core-unit tests/unit/phase4/test_health.py tests/unit/phase4/test_health_timer.py
# 98 passed in 0.10s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-core-phase4 tests/unit/phase4
# 393 passed in 0.28s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-core-regression -m 'not integration'
# 1107 passed, 10 deselected, 1 warning in 3.52s
```

경고는 기존 Authlib httpx deprecation 1건이다. 외부 integration 10개는 제외했다.
AST 구문·79열·문서 링크/표·git diff --check와 보호 파일 hash를 최종 확인한다.

26개 설계 시나리오 중 25개를 로컬 코어 테스트에 대응했다. 20번은 승인된 fault
code/source·latch/reset·순서 계약이 없으므로 후속 fault 코어 담당으로 보류했다.
현재 코어는 fault_supported=True 정책·비어 있지 않은 active_faults·FAULT snapshot
복원을 거부하며 미지원 FaultSignal 입력으로 Health를 바꾸지 않는다. FAULT enum은
존재하지만 numeric 고장 추론·발생/해제를 임의 구현하지 않았다.
10/19번은 테스트 helper가 모든 필드를 JSON으로 재구성한 모델 복원 검증이다.
production codec·영속 원장/snapshot 원자적 재적재·실제 checkpoint 재시작 검증이 아니다.
기록된 입력 순서 replay는 매 단계 모델을 재구성해 전체 snapshot/result와 초기화,
STALE, 복구의 ID/revision chain을 비교했다. 순서를 바꾼 이력의 수렴은 주장하지 않는다.

### 계약 차이·미구현과 Git

기존 계약과 Operational 구현을 바꿔야 하는 충돌은 발견하지 않았다. 미정인 항목을
기본값으로 채우지 않고 명시 정책 주입으로 분리했다. 논리 snapshot 필드 중 정책
fingerprint·timer generation은 저장한 정책/원문 ID에서 파생하고 FAULT 관련 활성화는
보류했다. 기본 지원은 단일 채널의 네 numeric 유형과 schema 1.0.0으로 제한한다.

원장은 무제한 tuple이며 source 실제 권한/등록 로더, 다중 필수 metric, 원장 TTL,
저장 codec·policy migration·CAS, scheduler·tick liveness, Registry/Job 4 연결, History/
Kafka/DynamoDB sink·wire envelope·outbox/ACK·분산 복구는 미구현·미검증이다.
AWS/Databricks/Confluent나 Spark/Kafka/DynamoDB를 실행·연결하지 않았다.
이번 단위는 순수 Python Health 계산 검증이며 Phase 4 전체 완료·E2E 증거가 아니다.

정확한 staging 대상은 위 6개 파일뿐이다. 권장 메시지:
`feat(phase4): add deterministic health freshness core`.
Git add/commit/push/merge는 수행하지 않았고 사용자 검토를 기다린다.

## Health Snapshot Codec — 로컬 직렬화·손상·복원 검증

검증일: 2026-10-21 (Asia/Seoul). 시작 브랜치 `feat/phase-4-twin-state`, HEAD
`721fb4502965d441ebc7aecb3a73d85398469964`, 시작 git status clean.
직전 Health Core commit 반영을 git log와 실제 소스로 확인했다. Health/장비 계약,
이 보고서·Core·adapters/registry·관련 테스트를 읽었다. 기존 직렬화/checkpoint 패턴을
탐색했으며 동등한 Codec은 없고 test_health.py에 테스트 helper만 있었다.
worktree에는 AGENTS.md가 없어 제공된 지침과 원본 checkout의 AGENTS.md를 확인했다.

### 변경 파일

- `streaming/state/health_codec.py`: 명시 schema의 canonical UTF-8 JSON bytes encode/
  decode, codec version, 정책/무결성/타입/상태 검증, HealthCodecError.
- `tests/unit/phase4/test_health_codec.py`: 전체 필드·bytes 결정성·비호환/손상 거부.
- `tests/unit/phase4/test_health_recovery.py`: tmp_path 파일 저장/새 객체 복원 이후
  연속 실행과 결과/ID/원장/token 비교, 손상 파일 거부.
- `docs/streaming/equipment-health-contracts.md`: 최신 상태와 11절 wire/API/한계.
- `docs/reports/phase-4-verification.md`: 이번 기록.

health.py 수정은 필요하지 않았다. 기존 모델·타입은 재사용하고 policy payload를
새 정책으로 생성/활성화하지 않는다. 기존 Core·Operational FSM·Qualifier·Registry·
관련 테스트·fixture·config 23개 파일은 시작/종료 SHA-256으로 보존 확인한다.

### Codec 계약과 복원 검증

encode_health_snapshot(snapshot)는 canonical JSON bytes, decode_health_snapshot(
payload, policy=...)는 검증된 새 HealthSnapshot을 반환한다. 순수 함수이며 I/O를
수행하지 않는다. 파일 쓰기/읽기는 테스트에서만 수행한다.

codec_version="1", snapshot_schema_version="1", health_contract_version=
"health-freshness-v1"을 별도로 관리한다. root에 장비 identity, Health profile,
policy fingerprint·명시 정책 본문, 전체 snapshot 상태, content_sha256을 보존한다.
정확한 필드 목록과 nullable/타입 규칙은 Health 계약 11절을 따른다.

객체 키 정렬·고정 구분자·UTF-8·6자리 UTC 시각·정수 μs timeout을 사용한다.
원장은 원래 strict 시간순 전체 배열이며 정렬/중복 제거로 복구하지 않는다.
동일 구체 snapshot은 동일 bytes이고 int/float 원문 표현은 보존한다.
revision의 bool/float/string coercion, 비유한 원장 수치, 누락/미지 필드·중복 JSON
key·naive/부정확 timestamp·FAULT/active fault 복원은 거부한다. Profile/version·
정책/fingerprint·의존 binding 변경도 거부하며 자동 migration은 없다.

기존 HealthSnapshot 불변식에 더해 revision/수용 원장 길이의 도달 가능 상한/하한을
Codec 경계에서 검사한다. content_sha256은 canonical 본문 digest로, 정상 수치 한 자리
변경 같은 우발적 손상을 검출한다. 인증 서명이 아니며 본문/digest를 함께 다시 쓴
일관된 변조·원장 유실·과거 change ID 진위까지 증명하지 않는다.

전체 dataclass 필드 round-trip, 초기·복수 원장·STALE·초과 2^53 정수 revision,
120000일+1μs timeout·Unicode·int/float 표현을 확인했다. 정책/버전·identity,
누락·잘못된 타입·원장 순서/중복/cursor·last_seen/deadline/Health 모순은 복원 거부했다.

tmp_path에서 관측→encode→파일→decode→동일 후속 입력을 적용해 연속 실행과 매 단계
결과를 대조했다. 중복/충돌·역순 거부, 옛 timer 무효·새 timer 만료·STALE 복구,
같은 시각 관측/tick 순서별 이력, 원장/revision/health_change_id chain이 동일했다.
잘린 JSON·수치 한 자리 변경·잘못된 UTF-8 파일의 복원 거부도 확인했다.

### 실제 실행 결과

먼저 테스트 두 파일을 작성하고 모듈 부재로 collection error 2건을 확인한 뒤
구현했다. 초기 80개 통과 후 Unicode·중첩 필드 누락·정책 타입·revision/원장
도달 가능성 사례를 보강하여 신규 테스트는 89개다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-codec-red tests/unit/phase4/test_health_codec.py tests/unit/phase4/test_health_recovery.py
# ModuleNotFoundError: streaming.state.health_codec, collection error 2건

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-codec-unit tests/unit/phase4/test_health_codec.py tests/unit/phase4/test_health_recovery.py
# 89 passed in 0.13s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-codec-phase4 tests/unit/phase4
# 482 passed in 0.41s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-codec-regression -m 'not integration'
# 1196 passed, 10 deselected, 1 warning in 3.53s
```

최종 Phase 4/전체 회귀는 79열 정리 후 실행했다. 경고는 기존 Authlib httpx
 deprecation 1건이고 외부 integration 10개는 제외했다. AST·79열·문서 링크/표와
최종 git diff --check 및 보호 대상 hash를 확인한다.

### 범위 한계와 Git

로컬 Codec/파일 재적재만 검증했다. 실제 저장소, 파일 원자적 교체/fsync,
Spark checkpoint·DynamoDB 내구성·scheduler 복구, snapshot/원장 원자성,
원장 TTL·migration, 출력 outbox/ACK·exactly-once·외부 E2E는 미구현·미검증이다.
AWS/Databricks/Confluent 실행, Spark Job 4, Kafka/DynamoDB/History Sink·Alert 구현은
하지 않았다. Health 상태 판정/ID 규칙·기존 Operational 코드는 바꾸지 않았다.

정확한 staging 대상은 위 5개 파일뿐이다. 권장 메시지:
`feat(phase4): add health snapshot codec and local recovery tests`.
Git add/commit/push/merge는 수행하지 않았다. 이 단위를 완료한 뒤 사용자 검토를 기다린다.

## Health Timer Coordinator — 순수 조정 및 로컬 재시작 검증

검증일: 2026-10-21 (Asia/Seoul). 브랜치 `feat/phase-4-twin-state`, 시작 HEAD
`a564beba1fd6006687c81a02f9eff2464de09e20`, 시작 git status clean.
최근 5개 commit과 Codec commit 반영을 확인하고 실제 Core·Codec·Registry·Health/장비
계약·이 보고서 및 관련 Health/Timer/Codec/Recovery 테스트를 읽었다.
worktree에 AGENTS.md가 없어 제공된 지침과 원본 checkout의 AGENTS.md를 확인했다.
동등한 Coordinator는 없었다. 기존 timer_token/HealthTick/Codec을 재사용했다.

### 변경 파일과 책임

- `streaming/state/health_timer_coordinator.py`: plan_health_timer,
  coordinate_health_timer, restore_health_timer와 최소 요청/결과 descriptor.
- `tests/unit/phase4/test_health_timer_coordinator.py`: 신규 36개 테스트.
- `docs/streaming/equipment-health-contracts.md`: 최신 상태와 12절 API·요청 상태·
  재시작/ACK 경계·테스트 증거.
- `docs/reports/phase-4-verification.md`: 이번 실행 기록.

Coordinator는 최신 snapshot과 정책/명시 시각을 caller에게 받는 순수 함수다.
현재 token은 기존 timer_token을 재사용하며 snapshot을 내부에 저장하지 않는다.
plan은 WAITING/DUE/ALREADY_EVALUATED/REFERENCE_REVERSED 요청을 구성한다.
coordinate는 현재 대기 token을 실행하지 않고, 실제 전달된 만료·중복·옛 token·
역행 시각은 원본 reduce_health에 위임한다. 반환 상태/ID/직렬화를 재구현하지 않았다.
완료 timer 조회는 상태 불변이며, 늦은 중복 전달의 no-op cursor 갱신은 기존 Core
결과를 그대로 보존한다. 옛 token 전달로 새 현재 timer까지 자동 실행하지 않는다.

restore는 decode_health_snapshot으로 payload 검증 후 요청을 재구성하며 자동 예약·
평가·파일 I/O를 하지 않는다. 대기 복원은 재등록 필요성을 caller에게 넘기고 만료
복원은 명시적 coordinate 호출로 평가한다. 이미 완료된 STALE 복원은 새 예약을
요구하지 않는다. 장비/policy/profile/version 문맥이 다르면 요청 구성 전에 거부하며
교차 장비 token은 Core에서 OBSOLETE_TIMER다. 여러 장비의 자동 Registry/consumer
라우팅은 없고 각 장비의 정책·snapshot은 호출자가 명시한다.

### 로컬 재시작·경합·실패 주입

기존 Codec bytes를 tmp_path에 저장한 뒤 새 snapshot으로 읽어 deadline 전/정확한
경계/후, STALE, 신규 관측 직후 옛 token 잔존, 반복 복원, timer 평가 전후 재시작을
검증했다. 원본 snapshot 전체 필드·token·요청 상태·health_change_id가 유지되었고
기록된 순서 replay는 매 단계 파일 재적재와 Core 직접 실행의 결과를 대조했다.
관측 먼저면 옛 tick 무효, tick 먼저면 STALE→HEALTHY라는 이력 차이를 유지했다.

두 장비의 서로 다른 deadline과 store/equipment 문맥을 검증했다. 장비 A 평가가
장비 B를 갱신하지 않고 다른 store의 같은 equipment_id token도 교차 적용되지 않는다.
이 추가 binding과 10/20초 timeout은 로컬 fixture 가정이며 실등록·운영 정책이 아니다.

주입한 reducer는 계산 전 또는 계산 직후·caller의 다음 snapshot 확정 전에 예외를
발생시킨다. 이미 저장한 이전 파일 bytes가 그대로이며, 그 파일에서 재시도한 결과/
논리 Health ID가 직접 실행과 같음을 확인했다. 저장된 snapshot만으로 외부에 전달한
출력·실행 ACK를 복구했다고 주장하지 않는다.

### 실제 테스트 실행

테스트를 먼저 작성하여 모듈 부재의 collection error 1건을 확인한 뒤 구현했다.
완료 요청 조회와 실제 중복 tick 전달을 분리해 Core의 no-op cursor 정책을 보존했다.
최종 결과는 아래와 같다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-timer-coordinator-red tests/unit/phase4/test_health_timer_coordinator.py
# ModuleNotFoundError: streaming.state.health_timer_coordinator, collection error 1건

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-timer-coordinator-final-unit tests/unit/phase4/test_health_timer_coordinator.py
# 36 passed in 0.09s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-timer-coordinator-phase4 tests/unit/phase4
# 518 passed in 0.43s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-timer-coordinator-regression -m 'not integration'
# 1232 passed, 10 deselected, 1 warning in 3.30s
```

경고는 기존 Authlib httpx deprecation 1건이고 외부 integration 10개는 제외했다.
79열·AST·문서 링크/표·git diff --check를 최종 확인한다. 기존 Core/Codec·Operational
FSM/Qualifier/Registry·기존 테스트·fixture/config 26개 파일의 시작/종료 SHA-256을
대조해 변경하지 않았음을 확인한다. 기존 미커밋 변경은 시작 시 없었다.

### 미구현·미검증과 Git

실제 scheduler/background thread·clock·영속 timer queue·CAS 기반 snapshot 확정,
Spark checkpoint, timer ACK/재시도 영속 관리, Kafka/History/DynamoDB 이중·삼중 쓰기,
outbox/부분 실패 복구·외부 exactly-once는 미구현·미검증이다. 프로세스 종료 후
Coordinator/예약이 자동 유지된다는 보장이 없고 저장한 snapshot의 현재 세대만
재구성한다. 저장하지 않은 관측이나 출력 전달 여부를 snapshot에서 복원하지 않는다.
AWS/Databricks/Confluent 실행, Job 4 연결·외부 sink·Alert 구현은 하지 않았다.

정확한 staging 대상은 위 4개 파일이다. 권장 메시지:
`feat(phase4): add health timer coordinator and local restart tests`.
Git add/commit/push/merge는 수행하지 않았고 이번 논리 단위 완료 후 사용자 검토를 기다린다.


## Equipment Health Registry Adapter — 2026-10-21 로컬 검증

시작 branch는 feat/phase-4-twin-state, HEAD는
2caa165b11e9af9abbf3cc836de769799d678137이며 working tree는 깨끗했다.
직전 Health Timer Coordinator commit 2caa165와 Codec commit a564beb가 반영되어 있다.

### 변경 파일과 설계

- streaming/state/health_registry.py: 명시적 HealthPolicy tuple을 선택하는 별도 Adapter.
- tests/unit/phase4/test_health_registry.py: 신규 42개 검증.
- docs/streaming/equipment-health-contracts.md: 13절 API·권한·거부 경계.
- docs/streaming/equipment-state-contracts.md: 별도 Health 연결과 Operational API 보존.
- docs/reports/phase-4-verification.md: 이번 로컬 증거와 한계.

새 profile 설정 저장소 없이 기존 HealthPolicy/EquipmentBinding/ProfileKey를 재사용한다.
Fryer oil_temperature_celsius/celsius, Hood fan_rpm/rpm과 1.0.0만 현재 정책 계약대로
허용하며 source/timeout/Health key는 명시 주입이다. 테스트의 10초와 fixture-simulator는
운영 승인값이 아니다. Refrigerator/Freezer/Dishwasher는 Registry 지원을 보류한다.

resolve/evaluate에서 identity·정확한 Health profile/version·metric/unit/schema/source를
검사하고 snapshot의 정책/fingerprint/버전 호환성을 확인한다. 오류는 Core 실행 전
HealthRegistryError다. admission을 통과한 수치/중복/역순/충돌/만료는 Core에 위임한다.
잘못된 header를 가진 ID 충돌은 Registry 오류가 먼저이며 Core ordering 규칙은 그대로다.
무효 이벤트는 freshness와 timer를 연장하지 않는다. HealthResult를 새로 감싸거나 전이,
Health ID, 직렬화, timer 규칙을 다시 구현하지 않았다.

### 로컬 비교·복원 증거

직접 Core와 Adapter 경유 관측의 전체 결과를 단계별 비교했다. deadline 등호 STALE,
복구, 중복/충돌/역순/동시각/만료, 반복 replay ID를 검증했다. timer 계획/실행은 기존
Coordinator 결과와 같고 신규 관측 뒤 옛 generation·교차 장비 token이 무효다.
로컬 파일의 기존 Codec bytes를 복원해 같은 token·deadline·Health ID를 확인했으며
손상 payload는 거부했다. 다른 store/snapshot/profile/policy 혼용도 거부한다.

동일 Hood 입력을 직접 Qualifier/Operational Registry/Health에 각각 적용해 결과와
snapshot 독립성을 비교했다. Fryer Operational synthetic 처리도 기존 Registry 경유
결과를 보존하고 Health에는 synthetic mode/fault를 허용하지 않는다. 관측/tick 경합은
기존 Coordinator/Core의 명시적 입력 순서를 유지한다. 새 scheduler나 순서 재배열은 없다.

### 실제 테스트 실행

테스트를 먼저 작성해 모듈 부재의 collection error를 확인한 뒤 구현했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-registry-red tests/unit/phase4/test_health_registry.py
# ModuleNotFoundError: streaming.state.health_registry, collection error 1건

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-registry-final-unit tests/unit/phase4/test_health_registry.py
# 42 passed in 0.10s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-registry-phase4 tests/unit/phase4
# 560 passed in 0.46s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-health-registry-regression -m 'not integration'
# 1274 passed, 10 deselected, 1 warning in 3.57s
```

기존 Authlib httpx deprecation 경고 1건이며 integration 10개는 제외했다.
기존 Core/Codec/Coordinator·Operational FSM/Qualifier/Registry·테스트·fixture/config
28개 파일의 시작/종료 SHA-256 대조, 신규 Python AST/79열, 문서 링크/표와
`git diff --check`로 최종 변경 범위를 확인했다. 기존 구현 파일은 변경하지 않았다.

### 미결정·미구현·미검증

운영 timeout·Health profile/source 승인 및 실제 인증/binding 공급 계약은 미정이다.
Adapter는 주입한 allowlist를 확인하며 실제 source 인증을 증명하지 않는다.
실제 Consumer/Registry 런타임 연결·Job 4·scheduler·영속 queue·CAS·checkpoint·ACK·
outbox·외부 sink/부분 실패 복구와 exactly-once는 미구현·미검증이다. 로컬 파일 복원은
외부 영속 복구 증거가 아니다. FAULT 입력/추론·Phase 5 Alert도 추가하지 않았다.
AWS/Databricks/Confluent 및 외부 서비스는 실행하지 않았다.

정확한 staging 대상은 위 5개 파일이다. 권장 메시지:
`feat(phase4): connect equipment health policies to registry`.
Git add/commit/push/merge는 수행하지 않았으며 이번 논리 작업을 마치고 검토를 기다린다.


## State History / State Changes / Current State — 2026-10-22 설계 검토

### 현재 상태와 범위

branch=feat/phase-4-twin-state,
HEAD=a1effcbd13a62a98a19d98187b2680ddf6a8db8b, 시작 working tree는 깨끗했다.
직전 Health Registry Adapter commit a1effcb 및 Timer Coordinator/Codec/Core commit을
확인했다. 기존 코드·설정·테스트는 변경하지 않고 설계 및 테스트 시나리오만 작성했다.

변경 파일:

- docs/streaming/state-output-contracts.md: 신규 상세 논리 계약과 32개 설계 시나리오.
- docs/streaming/equipment-state-contracts.md: 12절 출력·영속 경계 참조.
- docs/streaming/equipment-health-contracts.md: 14절 Health payload/Current 복구 경계.
- docs/reports/phase-4-verification.md: 이번 검토 증거와 미검증 범위.

### 설계 결과·호환성

기존 두 native ID를 유지하고 change_kind로 구별하는 state-output-v1 tagged envelope를
권장한다. source lineage·정책·순서/revision·event/deadline/evaluation 의미를 분리했다.
Health의 같은 논리 만료 ID가 서로 다른 평가 시각에서 재생성될 수 있으므로 최초
command/evaluation/envelope를 durable 확정하고 재시도에 그대로 사용하도록 정의했다.

History는 (change_kind,native ID)의 canonical 불변 변경이며 같은 payload는 no-op,
다른 payload는 conflict다. 실제 UC History table/DDL이나 Iceberg MERGE 지원을 가정하지
않는다. Kafka key는 기존 equipment_id를 유지하고 개별 callback/offset ACK를 요구한다.
Current는 기존 PK=store_id/SK=equipment_id 아래 두 축 map을 독립 CAS로 갱신하는
논리 권장안이다. event_time 대신 승인 generation과 per-axis projection revision을
비교한다. Operational 공통 revision은 현재 Core에 없어 외부 durable 조정 계층이 필요하다.

HEALTHY 유지 관측도 freshness를 바꾸므로 Current-only 투영 의도를 별도 추적한다.
이때 History/topic의 가짜 변화나 새 health_change_id를 만들지 않는다. 상태 변경 이력만
읽어서 완전한 Snapshot·원장·후보·timer를 복원할 수 있다고 하지 않는다.

Snapshot+기록 명령+불변 의도를 원자 확정하는 Durable Publication Ledger를 논리 권장하며
Outbox/History-first/Checkpoint-only/Kafka-first 대안과 한계를 비교했다. 새 저장소나
인프라는 확정하지 않았다. 실제 원자성·fencing·Operational Codec·pending 스캔/ACK 기록
구현이 선행돼야 누락 방지 계약을 만족한다. 기본 처리안은 commit→History→Kafka→Current
완료 후 checkpoint이며 외부 저장소 사이 분산 transaction은 가정하지 않는다.

### 근거 확인

AGENTS 지침, 원본 checkout의 로컬 기준 실행 계획 Phase 4 DoD, architecture/ADR,
기존 state/health 계약·구현·ID/replay/ordering 테스트, Phase 2 Silver/validated retry,
Kafka/Gold/storage/rebuild 설계를 읽었다. Phase 3 Sink는 이 checkout에 없다. 접근 가능한
phase-3-gold-windows의 MemoryGoldReference 테스트와 streaming 설계는 메모리/설계 증거로
구별했고, 실제 영속 Gold 또는 세 Sink 복구를 검증했다고 처리하지 않았다.

공식 Databricks Iceberg·AWS DynamoDB UpdateItem/Conditional Write·Kafka Producer 설정을
문서 조회로 보완했다. API의 일반 기능과 실제 KTD runtime 지원을 구별했으며 외부
서비스/compute/API 실행은 하지 않았다. 상세 근거 링크는 신규 계약 10절에 있다.

### 검증과 미검증

32개 시나리오에 입력/초기 상태·실패 위치·각 Sink 기대 결과·재시도·ID/revision 검사를
명시했다. 세 Sink 조합 실패, checkpoint 실패, 개별 ACK 직후 종료, 반복 batch/restart,
역순 Current, 같은 ID 충돌, timeout 지연 평가, 무변화 freshness, 두 축 CAS를 포함한다.
**전부 미실행 설계**이며 이번 작업의 신규 테스트 통과 수는 없다.

문서 로컬 링크·anchor/표 열 수·schema/시나리오 필수 항목·변경 범위와
`git diff --check`를 검사했다. 시작 시 추적 파일 147개 SHA-256 baseline을 저장해
기존 코드·테스트·config 불변과 문서만 변경됨을 대조했다. 이번에 pytest를 재실행하지
않았고 이전 560/1274 passed는 Health Registry commit의 기존 기록으로만 남긴다.

미결정은 Ledger 저장소/원자성, History 물리 DDL/지원 쓰기, epoch 발급/fencing,
Operational 영속 Codec/revision mapping, wire schema/소비자 migration, 운영 source/
timeout·retry/retention이다. Sink·Outbox·Job 4·Scheduler·Alert·클라우드 실행은 없고
실제 부분 실패 수렴/exactly-once/Phase 4 DoD 완료는 미검증이다.

정확한 staging 대상은 위 4개 문서다. 권장 메시지:
`docs(phase4): define state output contracts and recovery design`.
Git add/commit/push/merge를 실행하지 않았으며 이번 논리 작업 완료 후 검토를 기다린다.

## Autonomous State Output & Recovery Batch — 2026-10-22

이번 요청은 Task A~E를 연속 수행하도록 승인한 로컬 구현 배치다. 앞 절의 “설계만”은
해당 작업 시점 기록이며, 최신 로컬 구현·증거는
[Phase 4 Local Recovery 보고서](phase-4-local-recovery.md)를 따른다.
시작 HEAD a1effcb/branch feat/phase-4-twin-state와 직전 Health Registry commit을 확인했다.
기존 미커밋 설계 문서 4개의 내용을 보존하고 output 계약/이 보고서에는 append만 했다.

- A: 출력·Current-only 투영 계약과 SQLite 로컬 선택의 대안/운영 경계를 명시했다.
- B: change_envelope.py에서 기존 native ID·typed lineage·UTC·canonical bytes를 보존한다.
- C: publication_store.py에서 opaque Snapshot bytes·기록 명령/평가/판정·불변 출력·pending을
  SQLite transaction/CAS로 함께 확정한다. 정확한 명령 재시도는 새 revision을 만들지 않는다.
- D: sink_protocols.py/local_sinks.py/publication.py에서 독립 파일 Fake와 명시적 ACK·
  attempt·UNKNOWN·충돌·최신성·동기 reconcile을 모델링했다. 외부 서비스 클라이언트는 없다.
- E: 실제 subprocess exit 7 cases를 포함한 로컬 복구 18 cases를 실행했다. Snapshot-only
  partial commit 방지와 ACK 직후 종료의 재시도, pending 재조회/최종 수렴을 확인했다.

최종 신규 테스트는 76개(unit 58 + local integration 18)다. Phase 4는 618 passed in 1.03s,
전체 로컬 회귀는 1350 passed, 10 deselected, 1 warning in 4.45s다. 별도 로컬 복구 실행은
18 passed in 0.80s다. 명령·Task 경계 기록·요구 18개별 실제 결과·정확한 Task별 staging
대상/메시지는 상세 보고서에 있다. 기존 Authlib deprecation 경고 1건이며 외부 integration
10개는 실행하지 않았다. 신규 local recovery는 외부 서비스 marker를 붙이지 않아 로컬
회귀에 포함된다. 이를 외부 E2E 통과로 표시하지 않는다.

Snapshot/의도 원자성은 로컬 SQLite transaction에만 해당한다. Health typed 복원은 기존
Codec을 재사용했고 Operational typed Codec/migration은 미구현이다. 기존 native Core,
Registry, Codec, Timer, Phase 2/3·기존 테스트·config/checkpoint는 수정하지 않았다.
시작 baseline 148개 중 output 계약/verification만 append로 변경했고 나머지 146개는
SHA-256 대조로 유지됨을 확인했다. 새 Python 9개 AST/79열/whitespace, 문서 링크/표와
`git diff --check`를 검사했다.

실제 Kafka/DynamoDB/Iceberg·Avro wire·Job 4·Spark checkpoint 원자성·생산용 outbox/lease/
세대 발급·운영 재시도/보존·State Rebuild·Alert·클라우드 실행은 미구현·미검증이다.
실제 서비스를 SQLite/Fake로 대체한 검증이며 Phase 4 전체 DoD 완료는 아니다.
Git add/commit/push/merge는 실행하지 않았고 승인 배치 완료 후 중단한다.

## Autonomous Local Integration Readiness — 2026-10-23

Task A~F 승인 배치를 수행했다. 시작 branch는 feat/phase-4-twin-state, HEAD는 a1effcb이며
직전 Output/Recovery A~E의 코드·테스트·문서가 미커밋으로 존재했다. 이 내용과 원문을
보존하고 새 모듈 중심으로 확장했다. 상세 경로·장애 결과·정확한 staging/commit 계획은
[Local Application 보고서](phase-4-local-application.md)에 있다.

A는 Operational Snapshot typed Codec(unit 30), B는 StateChangeEvent Avro 초안/로컬
framing·offline compatibility(unit 21), C는 validated mapping→기존 Registry/Core→SQLite
Application(unit 28), D는 기존 Timer→Application publication 연결(unit 10), E는 다중 장비/
재시작 통합(local 19), F는 [Job 4 운영 통합 설계](../streaming/state-job4-integration-plan.md)다.
총 새 테스트 108개가 통과했고 실제 process exit/reopen cases는 13개다.

Fryer numeric은 Health만 계산하며 Operational cycle은 명시 synthetic 입력으로만
검증했다. Hood 후보·Health는 독립 Snapshot이다. 기록된 명령/평가 시각을 재사용하고,
중간 축 저장 후 종료 시 남은 축을 재개한다. atomic 보장은 축별 SQLite transaction이며
Spark checkpoint/운영 Outbox/외부 Sink 원자성으로 확대하지 않는다. opaque Operational
Snapshot 자동 migration, 새 domain policy/source 권한·timeout·미지원 장비 추론은 없다.

최종 신규 108 passed in 3.66s, Phase 4 707 passed in 1.72s,
전체 로컬 회귀 1458 passed, 10 deselected, 1 warning in 7.70s다. 별도 새 local application
integration은 19 passed in 2.72s다. 경고는 기존 Authlib httpx deprecation 1건이고 외부
integration 10개는 실행하지 않았다. 실행 명령·환경 버전은 상세 보고서 9절에 있다.

새 Python 11개 AST/79열/whitespace, Avro schema, 문서 링크/표와 git diff --check를
확인했다. 시작 baseline 158개 중 output/verification 문서만 append로 바뀌었고 나머지
156개는 SHA-256 대조로 그대로다. 기존 Core/Registry/Health/Codec/Timer/이전 Envelope·
Ledger·Fake·테스트·Phase 2/3·설정·checkpoint는 변경하지 않았다.

실제 Schema Registry 등록, Kafka/DynamoDB/Iceberg 쓰기, Spark 설치/Job 4/운영 Scheduler,
클라우드 실행과 Git add/commit/push/merge는 하지 않았다. 공식 API 문서는 조회했으나
대상 runtime·운영 보장을 검증하지 않았다. Phase 4 전체 DoD는 미충족이며 이번 승인
배치는 로컬 readiness 구현/검증 및 운영 설계까지 완료하고 중단한다.

## Concurrency, Fencing & Recovery Hardening — 2026-10-23

검토 HEAD/종료 HEAD `a1effcb`, branch `feat/phase-4-twin-state`.
시작 시 이전 미커밋 28경로를 보존했다. 이번에는 신규 10경로와 이 문서/Job 4 계획의
append 2경로만 변경했다. 기존 코드/테스트/Avro/config/Phase 2·3 파일은 변경하지 않았다.
[전체 보고서·Task별 staging/증빙 명령](phase-4-concurrency-recovery.md),
[동시성 계약](../streaming/state-concurrency-contracts.md),
[Job 4 readiness 11절](../streaming/state-job4-integration-plan.md)을 따른다.

### 실제 구현 경계

- 새 임시 DB 전용 FencedPublicationStore(user_version=2). v1 자동 migration 없음.
  로컬 authority epoch/조건부 UPDATE, 기존 root CAS·출력/ACK 저장 재사용, nested savepoint.
- FencedStateApplication은 기존 Application/Health Timer를 조합한다. 장비별 명령을
  ADMITTED/PARTIAL/COMPLETE로 기록해 미완료 축 추월과 완료 전 출력 claim을 차단한다.
- ClaimedDispatcher는 명시 clock/양수 lease·output sequence로 기존 ACK coordinator를
  사용한다. 늦은 ACK/옛 claim 거부, bounded scan의 BLOCKED 분리, Fake Kafka 중복 유지.
- 기존 v1 entry point에 자동 적용하지 않는다. 새 경로를 명시 선택해야 한다. authority
  epoch는 기존 generation/monitoring epoch/timer generation/native ID를 변경하지 않는다.

### 신규 실행 증거

| 범위 | 실제 명령 선택 | 결과 |
|---|---|---|
| 신규 Unit + Local Integration | test_writer_fencing / test_fenced_application / test_dispatch_claims / test_phase4_concurrency_recovery / test_phase4_fencing_recovery | **50 passed**, 5.56초 |
| Phase 4 unit | tests/unit/phase4 | **740 passed**, 2.61초 |
| Phase 4 및 기존·신규 Local Integration | 위 unit + test_phase4_local_recovery / test_phase4_local_application / 신규 integration 2파일 | **794 passed**, 11.59초 |
| 전체 비외부 로컬 | pytest -m 'not integration' | **1508 passed, 10 deselected**, 14.34초 |

명령 공통: `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider`.
각 실행은 `/private/tmp/ktd-concurrency-final-{new,unit,phase4,all}`의 별도 basetemp다.
외부 Kafka/Schema Registry integration marker 10개는 실행하지 않았다.
모든 실행의 warning은 기존 Authlib httpx deprecation 1개다. 구문/79열·diff·문서 로컬
링크·baseline 보존 검사를 수행했다. 새 Python 8파일 AST 통과, baseline 172파일 중
공유 문서 2개만 append, 나머지 170파일 SHA-256 동일이다.

신규 unit 33개는 writer 14/application 7/dispatch 12다. 신규 local integration 17개 중
14개 crash 사례에서 실제 os._exit(81)를 총 17회 실행했고, 별도 stale writer 오류 종료,
두 live writer·늦은 dispatcher ACK 경합을 검증했다. subprocess timeout과 finally kill/
reap을 사용했다. 반복된 partial/ACK 유실 뒤 전체 Snapshot·Envelope/native ID를 연속 실행과
대조하고 Current 최신성·pending 수렴을 확인했다. 두 시각 동률 입력 순서는 기록대로
보존하며 서로 다른 순서의 이력이 같다고 하지 않는다.

### 남은 게이트

PySpark 미설치, 실제 Spark/DBR/Schema Registry/Kafka/History/DynamoDB·운영 authority/
분산 clock·lease 연장·checkpoint/권위 로그 원자성·운영 Outbox·migration·전체 Phase 4 DoD는
미검증이다. invalid admitted 명령의 관리 취소/skip 정책은 미결정이며 자동 복구하지 않는다.
이번에 cloud/외부 쓰기/유료 실행·자동 Git add/commit/push/merge를 하지 않았다.

## Git Reconciliation & Spark Job 4 Readiness Audit — 2026-10-23

최신 누적 Git 계획·12개 최종 보고 항목은
[Readiness 감사 보고서](phase-4-readiness-audit.md)를 따른다.
branch `feat/phase-4-twin-state`, HEAD `a1effcbd13a62a98a19d98187b2680ddf6a8db8b` 유지.
시작 미커밋 38경로를 보존하고 최종 누적 40경로를 중복 없는 6개 commit 그룹으로 나눴다.
공유 문서는 마지막 그룹에 한 번만 stage하도록 이전 hunk 추출 계획을 대체했다.
Git index/publication·외부 자원·체크포인트·운영 DB 변경 없음.

### 결함·수정·신규 적대적 증거

writer_fencing.py의 after_claim hook이 try 밖에 있어 ordinary Python 예외에서 claim이
남았다. 새 회귀의 첫 실행 1 failed/8 passed로 재현했다. hook을 try/finally 안으로 옮겨
일반 예외는 release, 실제 os._exit 종료는 lease까지 유지하도록 계약과 맞췄다.
기존 FSM/Reducer/Codec/Registry/ID/schema/시간 정책은 수정하지 않았다.

신규 test_readiness_adversarial.py **10개**는 위 결함, sequence 변조·옛 dispatcher complete,
v1/v2 상호 거부·충돌 admission·영구 ADMITTED/CONFLICT·독립 DB authority 부재를 검증한다.
기존 충분한 concurrent command/expected revision/epoch/PARTIAL/profile/lease/process/replay
테스트는 재사용했다. pending=0만으로 전체 command 완료를 뜻하지 않으며 독립 DB의 동일
장비 전역 fencing이 없음을 새 테스트로 명시했다. 관리 정책을 임의로 구현하지 않았다.

### 실행 명령·환경·기대값·실측

환경 Python 3.11.15 / SQLite 3.50.4 / pytest 9.1.1. 공통 실행 prefix:
`PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider`.

| 실행 인자 | 전용 basetemp | 기대 | 실제 |
|---|---|---|---|
| tests/unit/phase4/test_readiness_adversarial.py | /private/tmp/ktd-readiness-new | 새 10개 통과 | 10 passed, 0.36초 |
| tests/unit/phase4 | /private/tmp/ktd-readiness-unit | 기존 740+새 10 통과 | 750 passed, 3.04초 |
| test_phase4_local_recovery / local_application / concurrency_recovery / fencing_recovery (tests/integration/) | /private/tmp/ktd-readiness-local | 기존 54 통과 | 54 passed, 10.64초 |
| -m 'not integration' | /private/tmp/ktd-readiness-all | 외부 Kafka/Registry 제외 전체 성공 | 1518 passed, 10 deselected, 16.89초 |

Phase 4 합계는 별도 두 실행의 합 **804개**다. 새 process 테스트를 추가한 것은 아니다.
Authlib httpx deprecation warning 1개는 기존과 동일하다. 상세 재현 명령은 감사 보고서에 있다.

AST/구문 검사, 신규 Python 79열, 문서 링크, git diff --check, 40경로 commit 분리의
집합 동등성/중복 0과 보호 baseline hash를 대조했다. baseline 182개 중
writer_fencing.py와 문서 3개만 이번 변경, 나머지 178파일 동일이다. 신규는 test와 보고서다.

### Runtime/DoD 결과

PySpark/import spec·spark-submit·pandas·pyarrow가 없다. Java -version은 Java 24이며
이번 확인한 Spark 3.5.6(Java 8/11/17), Spark 4.0.0(Java 17/21) 공식 목록 밖이다.
Python 3.11은 문서 범위 안이지만 runtime 전체 호환 실증이 아니다. 실제 State Job 4
entry point도 없다. 설치·Spark 실행·Workspace/Cloud/Registry 접근은 하지 않았다.

Fryer/Hood 로컬 Handler/Health는 검증, Refrigerator/Freezer/Dishwasher Operational
handler=None이며 Health Registry도 미지원이다. 설계나 numeric telemetry를 실행 지원으로
표시하지 않았다. 운영 state authority·clock/timer·실패 명령 관리·checkpoint/Publication
원자성·Avro Registry/wire pinning·실제 세 Sink·권한/비용/범위 승인이 E2E 선행 게이트다.
Phase 4 전체 DoD 및 State Rebuild/FAULT/추가 장비 구현 완료를 주장하지 않는다.
