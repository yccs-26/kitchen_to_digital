# 장비별 Operational State 계약과 FSM Registry 확장 설계

작성일: 2026-10-17 (Asia/Seoul). 검토 HEAD:
`9c3dabbd1340d3324267d6624a803012482b1ac5`,
브랜치 `feat/phase-4-twin-state`. 시작 작업 트리는 clean이었다.

상태: **계약 분석·사용자 검토용 설계 제안**.
현재 구현은 P4-1/P4-2의 synthetic 조리 Reference FSM뿐이다.
이 문서의 새 enum·정책·registry·신호는 구현되지 않았다. 실제 장비 제어기 사양도
제공되지 않았으므로 장비별 후보 상태를 실제 하드웨어 지원 사실로 해석하지 않는다.

## 1. 직접 확인한 근거

| 근거 | 이번 검토에 사용한 내용 |
|---|---|
| [Simulator 설정](../../simulator/equipment.py) | 장비 6항목, 유형·metric·생성 범위 |
| [Simulator 실행](../../simulator/main.py) | numeric만 선택, generate_event의 numeric/unit 제한 |
| [값 생성기](../../simulator/generator.py) | numeric 난수, boolean, categorical 후보 |
| [모델](../../simulator/models.py), [Producer](../../simulator/producer.py) | numeric 이벤트 모델, bool/string 값 거부, 장비 key 일치 |
| [검증 설정](../../config/validation_rules.json) | 등록 장비 4개와 metric/unit 허용 목록 |
| [Domain validator](../../streaming/validation/domain_validator.py) | registry·type·metric·unit·numeric 유효성 검사 |
| [Avro](../../schemas/avro/sensor_metric_event.avsc), [validated publisher](../../streaming/sinks/validated_publisher.py) | 기존 10필드와 UTC/장비 key 유지 |
| [계약 타입](../../streaming/state/contracts.py), [FSM](../../streaming/state/fsm.py), [README](../../streaming/state/README.md) | 명시적 보고 전이, 순서/중복·Hysteresis 범위 |
| [P4 테스트](../../tests/unit/phase4), [fixture](../../tests/fixtures/phase4) | synthetic 순환·반복 보고·시간/identity 계약 |

기준 실행 계획은 원본 checkout의
`/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/docs/plans/ktd-forward-execution-plan-2026-10-06.md`
중 Phase 4 reference·health 분리·신호 보완·identity 경계를 읽었다.
그 계획서의 사본을 추적 문서로 옮기지 않았다. 공통 계약의 근거는 이번에
직접 읽은 Avro·publisher·validator이며 handoff 원문을 새로 대조했다고 주장하지 않는다.

## 2. 실제 장비·센서 목록

모든 항목의 store_id는 `store-001`이다. 범위는 Simulator의 **난수 생성 범위**이며
운영 임계값·허용 물리 범위·Hysteresis 설정이 아니다.

| equipment_id | equipment_type | metric_name | 값 형태·unit | 생성 범위/후보 | main 발행 대상 | 현재 registry/metric 허용 |
|---|---|---|---|---|---|---|
| fridge-001 | refrigerator | temperature_celsius | numeric / celsius | 2.0~6.0 | 예 | 예 |
| freezer-001 | freezer | temperature_celsius | numeric / celsius | -22.0~-16.0 | 예 | 예 |
| fryer-001 | fryer | oil_temperature_celsius | numeric / celsius | 160.0~190.0 | 예 | 예 |
| hood-001 | hood | fan_rpm | numeric / rpm | 800.0~1800.0 | 예 | 예 |
| fridge-door-001 | refrigerator | door_open | boolean / unit 없음 | True/False | 아니오 | 아니오 |
| dishwasher-001 | dishwasher | state | categorical / unit 없음 | idle/washing/rinsing/drying | 아니오 | 아니오 |

설정상 equipment_type은 `refrigerator`, `freezer`, `fryer`, `hood`, `dishwasher`
5종이다. metric_name은 `temperature_celsius`, `oil_temperature_celsius`,
`fan_rpm`, `door_open`, `state` 5종이다. 현재 발행 가능한 경로는 4항목·4유형·
3개 metric 이름이며 live Kafka 잔존 데이터나 성공 전달을 측정한 목록은 아니다.

`generate_metric_value()`에 boolean/categorical 분기가 있다는 사실은 발행 지원을
뜻하지 않는다. main은 numeric만 선택하고 generate_event는 비수치 항목을 거부한다.
Producer도 metric_value의 bool/string을 거부하며 현재 Avro 값은 double이다.
또한 두 항목은 장비 registry에 없고 metric/unit도 미지원이다.
숫자 0/1 또는 상태 정수 코드를 임의로 넣어 이 경계를 우회하지 않는다.

`fridge-door-001`은 `fridge-001`과 다른 equipment_id다. 현재 설정에는 parent
장비 연결이 없다. 이름만 보고 하나의 냉장고로 병합하거나 기존 Kafka key를
fridge-001로 바꾸지 않는다. 향후 센서/장비 관계 registry가 필요하다.

현재 numeric generator는 장비별 독립 난수다. 시간에 따른 예열·조리·냉각·
fan 정지 흐름을 생성하지 않는다. `fault_injection.py`는 누락 ID·잘못된 시각·
미지원 버전을 주입하는 **품질 오류**이며 명시적 장비 FAULT 신호가 아니다.

## 3. 관측 가능성과 세 가지 책임

| 장비 유형 | 현재 알 수 있는 사실 | 현재 확정할 수 없는 Operational State | 추가 근거 |
|---|---|---|---|
| refrigerator | 특정 시각의 온도 | 냉각 중/대기·제상·전원 상태; 문 상태 | 제어기 mode 또는 압축기 동작 보고; 문은 별도 센서 관계 |
| freezer | 특정 시각의 온도 | 냉각 중/대기·제상·전원 상태 | 제어기 mode; 제상을 지원한다면 별도 명시적 mode |
| fryer | 특정 시각의 유온 | 예열·조리·청소·청소 완료·전원 상태 | 모드 보고, 청소 활성과 정상 완료, 필요시 cycle ID |
| hood | RPM 관측; 정책 확정 후 회전 여부 파생 후보 | 전원 명령·풍량/흡입 성능·조리 중 여부 | RPM Hysteresis/지속시간; 실제 풍량 판정에는 추가 센서 |
| dishwasher | 설정에만 4개 문자열 후보 존재 | 실제 세척/헹굼/건조·완료 | 순서가 있는 제어기 phase 및 cycle 완료 보고 |

온도의 상승/하강만으로 압축기 또는 조리 의도를 확정하지 않는다. 온도 구간
qualifier는 P4-2의 별도 LOW/HIGH 파생 관측이며 장비의 Operational State가 아니다.

| 책임 | 표현·판정 대상 | 이 문서의 경계 |
|---|---|---|
| Operational State | 장비 동작: 냉각, 조리, 회전, 세척 단계 | 장비별 상태 공간·신호·전이 제안 |
| Health | HEALTHY/STALE/FAULT, 관측 freshness와 명시적 장비 장애 | 기존 분리 유지; timer/fault lifecycle 구현 제외 |
| Alert | 지속 이상 조건의 PENDING/ACTIVE/RESOLVED, alert_id | Phase 5 책임; 이번에 추가하지 않음 |

예: 냉장고 고온은 업무 이상 Alert 후보이며 자동 FAULT나 COOKING이 아니다.
Hood가 멈췄어도 가동 요구가 없다면 그 자체로 FAULT/Alert가 아니다.
STALE은 마지막 Operational State를 과거 상태나 IDLE로 바꾸지 않는다.

## 4. 공통 전이·초기화 조건

아래 전이표는 **미구현 후보 정책**이다. 현재 `OperationalState` enum에 새 값을
추가하거나 모든 장비를 Reference FSM에 넣는 변경을 하지 않는다.

- 각 profile은 자신의 state enum과 payload allowlist를 가진다. 같은 문자열 IDLE도
  fryer와 dishwasher 사이에서 같은 타입으로 취급하지 않는다.
- 새 profile은 첫 관측 전 `initialized=false/current=None`인 별도 snapshot을
  제안한다. 이는 Health의 UNKNOWN이나 Operational State enum 추가가 아니다.
  첫 권위 있는 보고/지속시간이 검증된 수치 구간으로 초기 관측을 설정하며
  존재하지 않았던 IDLE→현재 전이를 만들어내지 않는다. 초기화 기록의 출력 형식은
  새 profile 구현 전에 결정한다. 기존 reference의 호출자 지정 초기 상태는 유지한다.
- 같은 사건 ID·같은 내용은 중복, 같은 ID·다른 내용은 충돌이다. 새 ID·같은
  상태는 no-op다. 미래·역순·동시각 거부는 P4-2 기준을 유지한다.
- 신호 부재는 전원 OFF/IDLE·완료가 아니다. 해당 장비의 profile에 없는 mode는
  UNSUPPORTED_INPUT 등 별도 사유로 보류하며 정상 상태로 매핑하지 않는다.
- 아래 정상 흐름 외 건너뛰기·취소·오류 복구·누락 단계의 재동기화는 보류한다.
  그런 사례를 정상 완료나 직전 정상 상태의 새 관측으로 기록하지 않는다.
  current 유지와 별도로 입력 미해석/동기화 필요 사유를 남기는 설계가 필요하다.

### 4.1 조리 Reference — 구현됨, fryer에는 미연결

| 이전 | 명시적 보고 | 다음 | 허용/금지 |
|---|---|---|---|
| IDLE | ModeSignal(PREHEATING) | PREHEATING | 허용 |
| PREHEATING | ModeSignal(COOKING) | COOKING | 허용 |
| COOKING | CleaningSignal(True) | CLEANING | 허용 |
| CLEANING | CleaningCompletedSignal | IDLE | 허용 |
| CLEANING | CleaningSignal(False) | 유지 | 완료로 전이하지 않음 |
| CLEANING | ModeSignal(IDLE) | 유지 | 금지 |
| IDLE | COOKING/청소 활성 | 유지 | 금지 |
| PREHEATING | IDLE/청소 활성 | 유지 | 금지 |
| COOKING | IDLE/PREHEATING | 유지 | 금지 |
| CLEANING | PREHEATING/COOKING | 유지 | 금지 |

상태 집합은 IDLE/PREHEATING/COOKING/CLEANING이다. 전체 no-op·완료 신호의
시점별 처리는 기존 README·test_fsm의 행렬을 따른다. 구현된 synthetic fixture의
reference 화구를 실제 fryer-001과 동일 장비라고 간주하지 않는다.
fryer 정책 후보는 이 흐름과 명시적 청소 완료 의미를 만족할 때만 adapter로
재사용한다. 유온만 있는 현재 fryer-001에는 bind하지 않는다.

### 4.2 refrigerator — 냉각 동작 보고 profile 후보

최소 상태 집합은 STANDBY/COOLING이다. STANDBY는 전원이 꺼졌다는 의미가 아니라
제어기가 보고한 비냉각 대기다. 온도가 낮다고 STANDBY로 추정하지 않는다.

| 이전 | 필요한 신호 | 다음 | 허용/금지 |
|---|---|---|---|
| STANDBY | controller_mode=COOLING | COOLING | 허용 |
| COOLING | controller_mode=STANDBY | STANDBY | 허용 |
| 각 상태 | 동일 controller_mode | 유지 | no-op |
| 각 상태 | 온도/door_open만으로 동작 변경 요구 | 유지 | 금지 |
| 각 상태 | PREHEATING/COOKING/CLEANING 또는 미지원 mode | 유지 | profile 밖 입력, 금지 |

DEFROSTING/OFF는 실제 지원·보고 의미를 확인한 뒤 별도 profile 버전으로 확장한다.
새 제상 보고를 STANDBY에 합치지 않는다. door OPEN/CLOSED는 별도 관측 축이며
냉각 동작과 동시에 존재할 수 있으므로 동일 enum에 넣지 않는다. door 센서용
전이 후보는 CLOSED→OPEN(true), OPEN→CLOSED(false), 반복은 no-op지만 현재
발행·장비 연결 계약이 없어 활성화할 수 없다.

### 4.3 freezer — 독립 설정의 냉각 동작 보고 profile 후보

상태 집합은 STANDBY/COOLING으로 refrigerator의 구조를 공유할 수 있다.
장비 type·허용 source·profile binding은 별도다. 온도 숫자를 공유하지 않는다.

| 이전 | 필요한 신호 | 다음 | 허용/금지 |
|---|---|---|---|
| STANDBY | controller_mode=COOLING | COOLING | 허용 |
| COOLING | controller_mode=STANDBY | STANDBY | 허용 |
| 각 상태 | 동일 controller_mode | 유지 | no-op |
| 각 상태 | 영하 온도·온도 상승만으로 mode 변경 | 유지 | 금지 |
| 각 상태 | 조리 reference 신호·미지원 제상/OFF | 유지 | profile 밖 입력, 금지 |

제상 지원은 실제 장비 사양과 신호가 필요하다. 이 최소 profile이 모든 냉동고
기능을 모델링한다고 주장하지 않는다.

### 4.4 hood — RPM 관측 기반 profile 후보

최소 상태 집합은 STOPPED/ROTATING이다. 회전 관측이며 전원 명령이나 정상
흡입 성능을 표현하지 않는다. `run_threshold > stop_threshold >= 0`와
각 방향 양수 지속시간·최대 샘플 공백을 장비별로 확정해야 한다.

| 이전 | 필요한 신호·조건 | 다음 | 허용/금지 |
|---|---|---|---|
| STOPPED | fan_rpm >= run_threshold가 run_duration 충족 | ROTATING | 허용 |
| ROTATING | fan_rpm <= stop_threshold가 stop_duration 충족 | STOPPED | 허용 |
| 각 상태 | 임계값 사이·짧은 후보 반복 | 유지 | 확정 전이 없음 |
| 각 상태 | 누락·중복·역순·동시각·미래 입력 | 유지 | 전이 근거 추가 금지 |
| 각 상태 | 음수/잘못된 단위·비유한 RPM | 유지 | qualifier 입력 부적합; 정상 상태로 해석 금지 |
| 각 상태 | RPM만으로 COOKING/CLEANING/FAULT 결정 | 유지 | 금지 |

P4-2의 후보 취소·공백 reset·새 유효 샘플에서만 확정하는 규칙을 재사용한다.
Domain validator의 현재 finite numeric/unit 검사는 음수 RPM의 물리 타당성까지
검증하지 않는다. 추가 품질 검증과 qualifier 거부 경계를 후속 계약에서 정한다.
생성 범위 800~1800만으로는 정지·기동·경계 왕복을 검증할 수 없다.

### 4.5 dishwasher — 순서 있는 cycle profile 후보

최소 상태 집합은 IDLE/WASHING/RINSING/DRYING이다. generator에 같은 소문자
후보가 있지만 무작위 선택이며 순서·cycle ID·정상 완료를 보장하지 않는다.

| 이전 | 필요한 명시적 신호 | 다음 | 허용/금지 |
|---|---|---|---|
| IDLE | 새 cycle_id의 phase=WASHING | WASHING | 허용 |
| WASHING | 같은 cycle_id의 phase=RINSING | RINSING | 허용 |
| RINSING | 같은 cycle_id의 phase=DRYING | DRYING | 허용 |
| DRYING | 같은 cycle_id의 cycle_completed | IDLE | 허용 |
| 각 상태 | 같은 cycle/현재 phase 반복 | 유지 | no-op |
| IDLE | RINSING/DRYING | 유지 | 순서 건너뛰기 금지 |
| WASHING/RINSING | IDLE 또는 다른 cycle_id | 유지 | 완료 추론·cycle 교체 금지 |
| DRYING | 단순 IDLE 보고·다른 cycle_id | 유지 | 정상 완료 추론·cycle 교체 금지 |
| 활성 cycle | 이전 phase로 복귀·정의되지 않은 취소 | 유지 | 금지; 재동기화/취소 계약 별도 |
| 각 상태 | fryer 청소 완료 신호 | 유지 | 다른 profile 신호, 금지 |

식기 세척 WASHING과 fryer 장비 유지보수 CLEANING은 다른 의미다.
기존 `CleaningCompletedSignal`을 세척 cycle 완료로 강제 변환하지 않는다.
실제 단계 생략·선택 프로그램·취소·장애 중단은 기기 capability를 조사한 다음
별도 profile로 확장한다. sample mode IDLE만으로 정상 완료를 만들지 않는다.

## 5. 신규 신호 계약·Simulator/validated 변경 제안

모든 제안은 이번에 직접 수정하지 않는다.

| 대상 | 필요한 제안 | 현재 부족한 것 |
|---|---|---|
| refrigerator/freezer | 권위 있는 controller_mode 보고와 지원 mode 목록 | 압축기·제상·전원 근거 없음 |
| fryer | 명시적 mode, cleaning_active, cleaning_completed | 유온만 존재; 실제 reference 대응 미확정 |
| hood | 기존 fan_rpm의 단계별 scenario·경계/정지 fixture, RPM qualifier 정책 | 상태별 시계열·threshold/dwell 미정 |
| fridge-door-001 | typed boolean 관측 + 센서↔부모 장비 관계 | 발행·등록·관계 모두 미지원 |
| dishwasher | typed phase, cycle_id, 명시적 cycle_completed | 현재 값 후보만 존재, 순서/완료/발행 없음 |
| 공통 fault | source·code·발생/해제, 선택적 controller sequence | 데이터 오류 주입과 장비 FAULT는 다름 |

명시적 신호에는 사건 ID, aware UTC event_time, 원문 store/equipment identity,
승인 source, 계약 버전과 typed payload가 필요하다. cycle_id는 관련 장비에만
필요하다. Controller sequence를 채택한다면 범위·재시작 epoch·시각/순서 충돌
우선순위를 먼저 정한다. 수치 sample의 event_id는 그대로 유지하고 새 신호 ID와
출처를 혼동하지 않는다.

권장 방향은 numeric Avro 10필드를 유지하고, 명시적 보고를 별도 typed 계약으로
검증한 뒤 정책 입력으로 합치는 것이다. 문자열 mode나 boolean을 double로
은닉하지 않는다. `metric_name`에 추가하는 것만으로 현재 경로가 지원되지 않는다.

이 방향은 현재 7 topic·4 Job 계획에서 별도 신호 입력을 어디서 수신할지라는
추가 설계 결정이 필요하다. 새 topic 생성·Job 수 변경을 자동 승인한 제안이 아니다.
향후 공통 typed 계약 또는 버전 migration을 택할 경우 raw decode·domain validation·
quarantine·canonical storage·validated 전달·하위 호환 소비까지 영향 분석하고
별도 사용자 검토를 받아야 한다. P4 내부 입력만 추가하고 기존 Job 2를 거쳤다고
표시하지 않는다. numeric validated 경로는 그대로 두는 것이 이번 기본 제안이다.

Simulator 후속 변경은 numeric random 경로를 보존하는 별도 deterministic
scenario mode로 제안한다. 보고 source는 synthetic임을 유지하고 cycle·사건 ID·
시각·순서를 고정하며 retry 시 같은 ID를 사용한다. door/dishwasher 목록만
numeric loop에 포함하는 변경은 금지한다. Registry 등록·metric allowlist·unit과
검증 경로의 새 계약은 별도 작업이며 Phase 2 파일을 이번에 수정하지 않는다.

## 6. 권장 FSM Registry 구조

기존 `transition()`을 generic 엔진으로 재작성하지 않는다. type별 정책 선택을
그 앞의 얇은 adapter/registry에서 수행하는 **미구현 제안**이다.

| 계층 | 계약·책임 |
|---|---|
| 장비 binding | 전역 equipment_id → store_id, equipment_type, capabilities, profile_id/version |
| Policy Registry | `(equipment_type, profile_id, version)` → 순수 handler·입력 allowlist·snapshot 타입 |
| Reference adapter | 기존 synthetic 타입과 초기 상태를 검증한 뒤 기존 transition을 그대로 호출 |
| Numeric qualifier | hood 등 numeric event_id를 보존하며 별도 snapshot에서 Hysteresis 계산 |
| Profile handler | refrigerator/freezer/dishwasher 고유 enum·신호·전이 처리 |
| 출력 의도 envelope | 장비 identity·정책 identity·입력 lineage·state_change_id, I/O 없음 |

Registry 선택은 event의 equipment_type 문자열만 신뢰하지 않고 장비 binding과
store/type가 일치하는지 대조한다. profile이 없거나 필요한 입력 capability가
없으면 POLICY_NOT_CONFIGURED/INPUT_CONTRACT_UNAVAILABLE 등의 별도 사유를
반환하고 IDLE이나 조리 정책으로 fallback하지 않는다. 이 사유 enum도 아직 없다.

| type / profile 후보 | 현재 선택 가능 범위 | 활성화 조건 |
|---|---|---|
| synthetic reference / synthetic-reference-v1 | 기존 함수의 직접 synthetic 테스트 | 기존 4상태 계약·명시적 초기 상태 |
| fryer / explicit-cooking-reference-v1 | 미구현 binding/adapter | reference 의미·모드/청소/완료 source 검증 |
| refrigerator / cooling-controller-v1 | 미구현 | 승인 mode 입력 확보 |
| freezer / cooling-controller-v1 | 미구현; 구조 공유 후보 | freezer별 승인 mode·지원 기능 확인 |
| hood / rpm-observation-v1 | 미구현 | threshold/dwell·기동/정지 fixture와 qualifier 검증 |
| dishwasher / wash-cycle-v1 | 미구현 | typed phase·cycle identity·명시적 완료 입력 확보 |

synthetic reference는 새 실제 equipment_type을 Avro에 등록하자는 뜻이 아니다.
현재 실제 장비 4개의 registry 행에는 이 profile binding 필드가 없다.

### 재사용 가능한 것과 재사용할 수 없는 것

- **그대로 실행 재사용:** synthetic 조리 reference와 그 함수·타입·출력 ID.
  fryer도 의미와 typed 입력을 만족할 때만 future adapter로 연결한다.
- **규칙 수준 재사용:** 장비 identity, immutable snapshot, UTC 검사, 중복/충돌,
  단조 current·출력 의도 분리, P4-2의 numeric 후보 규칙. 신규 handler 공통화는
  먼저 각 profile 계약 테스트를 만든 뒤 검토한다.
- **그대로 재사용 불가:** 현재 OperationalState/ReportedMode enum과
  StateObservation/TwinState/StateChangeIntent는 조리 타입에 고정되어 있다.
  refrigerator·hood·dishwasher를 강제 cast하거나 현재 enum에 전부 넣지 않는다.
- Health/Alert는 해당 profile enum 밖에 둔다. 기존 cursor를 자동으로
  Health last_seen에 연결하거나 임계값 Alert로 상태를 변경하지 않는다.

새 profile snapshot의 식별은 `(equipment_id, profile_id, version)`으로 하고
store_id는 검증 metadata로 유지한다. 동일 numeric 사건이 여러 projection에
쓰여도 event_id와 원문 lineage는 보존한다. 별도 wire 계약의 ID 충돌 범위는
`(input_contract_id, event_id)` 등으로 명시하고 ID 자체를 다시 만들지 않는다.

미래의 새 profile 전이 ID에는 profile_id/version·장비·원본 사건 ID·확정 시각·
전후 상태를 포함하도록 설계한다. Reference의 기존 ID 알고리즘과
`synthetic-reference-v1`은 변경하지 않는다. 정책 교체는 명시적 snapshot migration
또는 rebuild로 다루며 profile 간 원장을 섞지 않는다. I/O 연결 시의 원장/출력
확정·outbox·부분 실패 처리는 별도 과제로 남는다.

## 7. 기존 테스트 호환성과 다음 작은 구현 순서

이번에는 Python 코드·설정·fixture·테스트를 수정하지 않았다.
기존 `test_signal_contracts.py`, `test_fsm.py`, `test_ordering.py`를 그대로 실행했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-device-design-phase4 tests/unit/phase4
# 151 passed in 0.11s
```

이는 기존 P4-1/P4-2 호환성 재확인이다. 새 장비 profile·registry·typed wire·
수치 Hysteresis를 실행 검증한 결과가 아니다. 문서 작업이므로 전체 회귀는
이번에 재실행하지 않았고 AWS/Databricks·Kafka·Spark 실행도 하지 않았다.

다음 한 단위의 우선순위는 **로컬 Policy Registry와 reference adapter의
선택 계약**이다. 새 장비 동작/Health/timer·외부 I/O는 그 단위에 넣지 않는다.
완료 기준은 기존 함수/ID 유지, 명시적 synthetic binding 성공, 미지원 type/
미등록 장비·store/type 불일치·입력 부족에 fallback하지 않는 테스트다.
이 문서 검토 후에만 해당 구현을 시작한다.

| 이후 순서 | 최소 구현 경계 | 필요한 테스트 |
|---|---|---|
| 1. Registry 선택 계약 | immutable binding/spec, 기존 reference adapter | 151개 유지, 잘못된 binding/capability·충돌·version 거부 |
| 2. Hood numeric qualifier | 순수 RPM 후보 계산, 실제 숫자 선택은 별도 근거 | 정지/기동, 임계값 등호, 경계 진동, dwell 미달, 공백 reset, 늦은/중복 입력 |
| 3. 명시적 장비 신호 계약 | controller 권한·완료·cycle·센서 관계, 로컬 scenario | 타입/장비 mismatch, 불가능한 단계, 중복/동시각/역순, 원본 ID 유지 |
| 4. 장비 profile을 하나씩 | 냉각 또는 세척 중 확보된 입력에 맞는 한 profile | 표의 모든 허용/금지, 초기 관측, 누락/취소 보류, 현재 상태 보호 |
| 5. 통합 진입 전 | 별도 입력 wire/검증 경로·정책 migration 선택 | 기존 Avro 소비 호환, snapshot/원장/출력 replay, 부분 실패 |

단계 3의 계약 결정은 해당 입력을 사용하는 단계 4보다 선행해야 한다.
Health/timer는 별도 작업으로 유지하며 필요한 장비별 유효 관측의 종류만
연결한다. threshold·mode source·제상 capability·세척 취소 같은 미정 값을
임의 구현해 표의 전이를 실제 장비 지원이라고 표시하지 않는다.

## 8. 검토·Git 범위

설계 문서·README 연결·검증 보고서의 이번 기록만 commit 대상으로 제안한다.
메시지: `docs(phase4): define equipment state contracts and registry design`.
Staging/commit/push/merge는 수행하지 않는다. 이 문서가 채택되기 전에는
새 장비 profile 구현이나 Simulator/validated 변경을 자동으로 시작하지 않는다.
