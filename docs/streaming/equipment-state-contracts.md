# 장비별 Operational State 계약과 FSM Registry 확장 설계

작성일: 2026-10-17 (Asia/Seoul). 검토 HEAD:
`9c3dabbd1340d3324267d6624a803012482b1ac5`,
브랜치 `feat/phase-4-twin-state`. 시작 작업 트리는 clean이었다.

상태: **장비별 계약 설계 + Fryer/Hood Registry adapter + Hood RPM qualifier**.
현재 구현은 synthetic 조리 Reference와 Hood RPM 계산을 연결한 순수 Registry다.
Hood qualifier는 8절, Registry 연결은 9절을 따른다. 냉장·냉동고/세척기 정책은
미구현이다. 실제 장비 제어기 사양이 제공되지 않았으므로 장비별 후보 상태를 실제 하드웨어 지원 사실로 해석하지 않는다.

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

아래 조리 Reference 외의 전이표는 **미구현 후보 정책**이다. 현재 `OperationalState` enum에 새 값을
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

### 4.1 조리 Reference — 구현됨, 실제 fryer 입력에는 미연결

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
fryer 정책은 이 흐름과 명시적 청소 완료 의미를 만족할 때만 adapter로
재사용한다. 유온만 있는 실제 fryer-001의 입력은 자동 bind하지 않는다.
동일 identity로 synthetic 능력을 명시한 로컬 테스트 binding과 실제 연결은 구별한다.

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
그 앞의 얇은 adapter/registry에서 수행한다. 현재 Fryer Reference와 Hood RPM
adapter가 구현되어 있다. 냉장·냉동고와 세척기 handler는 미구현이다.

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
없으면 선택 실패를 코드가 있는 RegistryError로 거부하고 IDLE이나 조리 정책으로
fallback하지 않는다. 실제 오류 코드와 API는 아래 Registry 구현 절을 따른다.

| type / profile 후보 | 현재 선택 가능 범위 | 활성화 조건 |
|---|---|---|
| synthetic reference / synthetic-reference-v1 | 기존 함수의 직접 synthetic 테스트 | 기존 4상태 계약·명시적 초기 상태 |
| fryer / explicit-cooking-reference-v1 | synthetic binding 주입 시 adapter 실행 가능 | 실제 controller 연결·source 검증은 미구현 |
| refrigerator / cooling-controller-v1 | profile 등록, handler 없음 | 승인 mode 입력 확보 |
| freezer / cooling-controller-v1 | profile 등록, handler 없음 | freezer별 승인 mode·지원 기능 확인 |
| hood / rpm-observation-v1 | numeric binding 주입 시 adapter 실행 가능 | HoodSnapshot의 명시적 정책; 실제 센서 연결은 미구현 |
| dishwasher / wash-cycle-v1 | profile 등록, handler 없음 | typed phase·cycle identity·명시적 완료 입력 확보 |

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

최초 장비 계약 설계 단위에서는 Python 코드·설정·fixture·테스트를 수정하지 않았다.
기존 `test_signal_contracts.py`, `test_fsm.py`, `test_ordering.py`를 그대로 실행했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-device-design-phase4 tests/unit/phase4
# 151 passed in 0.11s
```

이는 기존 P4-1/P4-2 호환성 재확인이다. 새 장비 profile·registry·typed wire·
수치 Hysteresis를 실행 검증한 결과가 아니다. 문서 작업이므로 전체 회귀는
이번에 재실행하지 않았고 AWS/Databricks·Kafka·Spark 실행도 하지 않았다.

최초 설계의 다음 한 단위는 **로컬 Policy Registry와 reference adapter의
선택 계약**이었다. 그 구현 결과는 아래 Registry 구현 절에 추가했다.
새 장비 동작/Health/timer·외부 I/O는 그 단위에 넣지 않는다.
완료 기준은 기존 함수/ID 유지, 명시적 synthetic binding 성공, 미지원 type/
미등록 장비·store/type 불일치·입력 부족에 fallback하지 않는 테스트다.
선택 계약 구현 이후에도 새 장비 profile은 별도 검토 후 시작한다.

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
위 세 문서는 최초 설계 작업의 commit 범위다. Registry 구현의 변경 범위는
아래에 추가했다. Staging/commit/push/merge 및 새 장비 profile 구현이나
Simulator/validated 변경을 자동으로 시작하지 않는다.

## 9. Equipment FSM Registry 구현 결과

구현 기준 HEAD: `897d745d4443d069824334ceb541eefc74da8d93`.
위의 장비별 전이 후보를 미리 구현하지 않고 순수 선택 계층 한 단위만 추가했다.

| 파일 | 구현 계약 |
|---|---|
| `streaming/state/bindings.py` | frozen ProfileKey, EquipmentBinding, InputKind와 profile key 상수 |
| `streaming/state/adapters.py` | FryerReferenceAdapter에서 기존 transition을 그대로 호출 |
| `streaming/state/registry.py` | frozen catalog·binding, resolve/evaluate, profile snapshot, 코드별 선택 오류 |
| `tests/unit/phase4/test_registry.py` | 새 Registry 테스트 51개 |

### binding·입력·snapshot

`ProfileKey(equipment_type, profile_id, version)`은 세 필드 모두 정확히 일치해야
한다. profile 명칭의 기존 `-v1` 서술을 실제 key에서는 이름과 버전으로 분리했다.
예: `FRYER_REFERENCE = (fryer, explicit-cooking-reference, "1")`.
문자열의 대소문자·공백을 정규화하거나 latest version을 선택하지 않는다.

`EquipmentBinding(store_id, equipment_id, profile, input_kind)`에는 기본 장비나
기본 입력 능력이 없다. equipment_type은 profile key에 포함되고 equipment_id는
Registry 안에서 전역 유일해야 한다. 같은 ID를 두 store에 등록해도 거부한다.
`build_registry(bindings)`는 파일·네트워크를 읽지 않는다. 호출자가 검증한
binding tuple을 주입해야 한다. 실제 validation_rules를 자동 복사·갱신하거나
실제 장비 ID를 보고 synthetic 입력 능력을 자동 부여하지 않는다.

`InputKind.NUMERIC_TELEMETRY`와 `InputKind.SYNTHETIC_SIGNAL`을 구별한다.
Fryer adapter는 후자에서만 실행한다. 이는 synthetic_test_only 신호를 처리하는
로컬 입력 능력이며 실제 제어기 계약이나 권한 검증을 뜻하지 않는다.
실제 fryer-001의 현재 입력은 유온뿐이므로 synthetic binding 테스트를 실제
장비 신호 확보·validated 입력 연결로 표시하지 않는다.

기존 SyntheticSignal에는 equipment_type이 없어서 `evaluate()`의 필수 키워드
`equipment_type`으로 받아 binding과 비교한다. 이는 외부 wire에 필드를 추가한
변경이 아니다. 향후 신호 adapter가 이 metadata의 신뢰 근거를 마련해야 한다.

`ProfileSnapshot(profile, state)`는 TwinState 또는 HoodSnapshot 원문과 profile을
묶는다. 장비별 enum·결과를 강제로 통합하지 않는다. 기존 Fryer 초기 상태와 Hood
정책은 호출자가 명시한다. Hood는 current=None에서 초기화할 수 있다. 다른 version의
snapshot은 binding과 불일치하여 거부한다. Health 계산은 추가하지 않는다.

```python
# initial_state, synthetic_signal, reference_time은 호출자가 제공한다.
registry = build_registry((EquipmentBinding(
    "store-001", "fryer-001", FRYER_REFERENCE, InputKind.SYNTHETIC_SIGNAL,
),))
previous = ProfileSnapshot(FRYER_REFERENCE, initial_state)
result = registry.evaluate(
    previous, synthetic_signal,
    equipment_type="fryer", reference_time=reference_time,
)
next_snapshot = result.snapshot
original_fsm_result = result.fsm
```

### 오류 우선순위와 handler 연결 방어

예상되는 선택 실패는 `RegistryError.code`로 구별한다. 기존 FSM의 Decision과
혼합하지 않는다. 오류 발생 시 기존 snapshot은 변경되지 않고 출력도 없다.
정상 선택 뒤의 금지 전이·중복·시간 판정은 원래 FsmResult를 그대로 따른다.

| 검사 순서·상황 | 코드 |
|---|---|
| evaluate의 event가 SyntheticSignal/HoodRpmEvent가 아님 | UNSUPPORTED_INPUT |
| equipment_id가 binding 목록에 없음 | UNKNOWN_EQUIPMENT |
| 등록 ID의 store_id 불일치 | STORE_MISMATCH |
| 입력 equipment_type과 binding type 불일치 | EQUIPMENT_TYPE_MISMATCH |
| snapshot/request profile key와 binding 불일치 | PROFILE_MISMATCH |
| binding과 요청은 일치하나 catalog에 key 없음 | PROFILE_NOT_FOUND |
| catalog에 profile은 있으나 handler=None | HANDLER_NOT_IMPLEMENTED |
| 선택 handler가 요구하는 InputKind와 binding 불일치 | INPUT_CONTRACT_UNAVAILABLE |
| numeric payload type과 요청 type 불일치 | EQUIPMENT_TYPE_MISMATCH |
| 선택 handler의 입력 allowlist 불일치 | UNSUPPORTED_INPUT |
| 선택 handler의 snapshot 타입 불일치 | SNAPSHOT_TYPE_MISMATCH |
| snapshot의 store/equipment identity와 입력 불일치 | SNAPSHOT_IDENTITY_MISMATCH |

선택 전에 잘못된 Python 타입·빈 식별자 같은 API 사용 오류는 TypeError 또는
ValueError로 거부한다. 구성 시 중복 device ID는 DUPLICATE_BINDING, 중복
profile key는 DUPLICATE_PROFILE이다. ProfileSpec의 handler가 선언한 key와
등록 key가 다르면 HANDLER_PROFILE_MISMATCH로 거부한다. 허용 handler는
FryerReferenceAdapter와 HoodRpmAdapter 두 concrete 타입으로 제한한다.

`build_registry()`는 Fryer/Hood handler와 refrigerator/freezer/dishwasher의
handler=None 항목을 만든다. 알려진 profile 미구현과 미등록 profile을 구별하고
다른 profile/version·다른 장비의 handler로 fallback하지 않는다. 실제 registry에
없는 dishwasher를 자동 장비 등록하지도 않는다. 필요하면 resolve()만 호출해
Reference용 상태 객체를 만들지 않고 미구현 여부를 확인할 수 있다.

### Reference 재사용·검증과 다음 작업

FryerReferenceAdapter는 기존 transition에 같은 previous/event/reference_time을
전달한다. 원래 `synthetic-reference-v1`과 전이 ID 알고리즘·출력 의도를 변경하지
않는다. Registry profile의 이름/버전은 선택 metadata이며 원본 FSM policy_version을
교체하지 않는다. 다른 profile handler가 생길 때 ID와 snapshot migration은
별도로 설계해야 한다.

RegistryResult는 다음 ProfileSnapshot과 원래 FsmResult 또는 HoodResult를
기존 `.fsm` 속성으로 반환한다.
같은 snapshot/입력/유효 기준 시각에서 재계산하면 동일 전이 ID이며, 이미 갱신한
snapshot에 replay하면 원래 FSM대로 DUPLICATE·빈 의도다. 순수 계산의 결정성만
검증했고 snapshot 영속 복원·sink 전달 복구·exactly-once 증거는 아니다.

검증: Phase 4 `202 passed`; 전체 로컬 회귀
`916 passed, 10 deselected, 1 warning`. 명령과 실행 시간은 검증 보고서의
Registry 구현 절에 기록했다. Avro·Simulator·기존 FSM·Phase 2/3는 변경하지 않았다.

이 절의 최초 검증 이후 Hood numeric qualifier와 Registry 연결을 추가했다.
최신 범위·검증은 아래 8~9절을 따른다. Health/timer와 외부 연결은 미구현이다.

이번 권장 commit 대상은 위 Python 네 파일과 이 설계 문서·검증 보고서 여섯
파일이다. 메시지: `feat(phase4): add equipment FSM registry and reference adapter`.


## 8. Hood RPM qualifier — 독립 순수 계산 구현

2026-10-17, `streaming/state/hood.py`를 구현했다. 앞선 후보 설계 중 Hood의
수치 판정만 이 절로 구체화했다. 당시 Registry는 Hood handler=None이었다.
후속 연결은 아래 9절에서 구현했으며 Qualifier의 전이 함수·enum·정책은 보존했다.
`HOOD_PROFILE = (hood, rpm-observation, "1")`을 명시적으로 선택한다.

### 입력·초기화와 의미

Simulator는 store-001/hood-001, hood/fan_rpm/rpm, 생성 범위 800~1800이다.
validation_rules에도 동일 장비·metric/unit이 등록되어 있다. 이 범위는 난수
생성 설정이며 실제 정상 RPM 범위나 정지/운전 기준으로 검증된 사양이 아니다.
`HoodRpmEvent`는 기존 validated 10필드를 그대로 보존한다. event_time은 내부
aware UTC datetime이며 wire string의 파싱·실제 등록/source 권한·Kafka key
검증은 caller 책임이다. 새 signal_id나 신호 wire 계약을 만들지 않는다.

`HoodSnapshot(store_id, equipment_id, policy)`의 current는 처음에 None이다.
첫 지속시간 충족 구간은 INITIALIZED로 확정하며 가상의 STOPPED→ROTATING
전이나 state_change_id를 만들지 않는다. current가 이미 알려진 로컬 snapshot도
호출자가 명시할 수 있다. STOPPED/ROTATING은 RPM 관측에서 파생한 상태이며
전원 명령·흡입 성능·COOKING/CLEANING/FAULT를 의미하지 않는다.
Health 필드나 timer를 계산하지 않는다.

### 명시적으로 주입하는 테스트 정책

| 파라미터 | 이번 fixture 값 | 확정 조건 |
|---|---|---|
| enter_rpm | 600 rpm | RPM >= 600인 ROTATING 후보 |
| exit_rpm | 200 rpm | RPM <= 200인 STOPPED 후보 |
| enter_duration | 3초 | 새 유효 샘플 시각 - 후보 시작 >= 3초 |
| exit_duration | 2초 | 새 유효 샘플 시각 - 후보 시작 >= 2초 |
| max_sample_gap | 2초 | 연속 소비 샘플 간격 <= 2초 |

실제 장비 사양이 아닌 **초기 테스트 파라미터**다. 정책에 기본 숫자를 숨기지
않고 호출자가 모두 제공한다. finite 수치, 0 <= exit < enter, 각 기간의 양수를
검사한다. snapshot이 정책을 보존하며 호출 때 다른 정책을 전달하는 인자는 없다.
운영 정책 변경·snapshot migration은 미구현이다.

확정 상태와 다른 구간에서 후보를 시작하며 동일 구간의 새 시간순 샘플만
후보 시작 시각을 유지한다. 양 임계값 사이에서는 current를 유지하고 후보를
취소한다. 기존 확정 구간으로 복귀해도 반대 후보를 취소한다. 미초기 상태에서
후보 방향이 바뀌면 새로 시작한다. 공백 > 2초이면 새 샘플에서 다시 시작하며,
정확히 2초는 연속성에 포함한다. 모든 확정은 새 샘플이 필요하다. 사건 부재나
reference_time 경과만으로 STOPPED/ROTATING·STALE을 만들지 않는다.
샘플 사이 조건 유지 가정은 관측 정책이며 물리적 연속 회전을 증명하지 않는다.

### 순서·부적합 입력·출력 identity

identity → 소비 event_id 중복/전체 10필드 충돌 → reference_time < cursor →
미래 사건 → 역순/동시각 → 입력 적합성 → 후보 계산 순으로 검사한다.
다른 store/equipment, DUPLICATE/ID_CONFLICT, 시간 거부는 snapshot 전체를 보존한다.
이들은 후보를 늘리거나 취소하지 않고 원장에도 넣지 않는다. 같은 ID의 int/float
동등 수치와 같은 순간의 UTC 표현은 중복이다. 새 ID의 동시각은 SAME_TIME으로
거부하므로 순열 불변성이 아닌 같은 순서 재실행 결정성만 보장한다.
reference_time은 마지막 소비 사건 시각과 비교하며 이전 처리 시각을 저장하지 않는다.

새 시간순 입력의 음수 RPM, 잘못된 type/metric/unit/version은 INVALID_SAMPLE이다.
current는 유지하고 후보를 취소하며, cursor와 원문 원장에는 소비 기록을 남긴다.
이후 유효 샘플은 후보를 새로 시작한다. Domain validator는 음수 RPM을 거부하지
않으므로 이는 Qualifier의 추가 방어다. 이 처리는 Phase 2 Quarantine 구현이 아니다.
비유한 값·bool/string 수치·빈 식별자·naive 시각은 모델 생성 시 예외로 거부한다.
이 경우 계산 결과 snapshot은 없으며 caller가 잘못된 입력을 임의 누락한 스트림의
연속성을 보장하지 않는다. 실제 연결 전 해당 오류의 후보 단절 인계가 필요하다.

TRANSITION에는 `HoodStateChange` 하나를 반환한다. SHA-256 입력은 고정 정책 버전,
Hood profile key, 모든 정책값(기간은 정수 μs), store/equipment, 확정 샘플 event_id,
UTC event_time, 이전/다음 상태와 후보 시작 시각이다. 수치 정책의 600/600.0은
같은 identity를 만든다. 처리 시각은 포함하지 않는다. 동일 이전 snapshot 재계산은
같은 출력, 다음 snapshot의 replay는 DUPLICATE/빈 출력을 반환한다.
초기화·PENDING·NO_CHANGE·거부는 전이 의도를 만들지 않는다.

전체 원문 tuple 원장은 무제한 로컬 참조용이며 영속성·TTL·checkpoint 복구는
미검증이다. 출력은 미전달 의도이며 outbox·sink ACK·exactly-once를 보장하지 않는다.
fixture는 synthetic 시계열로 0 RPM·경계 노이즈를 보완했고 Simulator는 수정하지
않았다. 테스트 결과는 검증 보고서의 Hood 절을 따른다.

후속 Hood 전용 Registry 연결은 아래 9절에 기록한다. Health freshness/timer,
실제 source 권한, validated 소비와 외부 sink 연결은 여전히 후속이다.


## 9. Hood Registry Adapter — 순수 연결 계층

2026-10-17. Fryer/Hood의 snapshot/result 구조를 유지하면서 Registry에서 선택할
수 있도록 연결했다. Fryer transition과 Hood qualify_rpm의 계산 로직은 수정하지
않았다. bindings.py의 key·InputKind·장비 binding 계약도 그대로 유지한다.

공통 adapter 표면은 `profile`, `input_kind`, `snapshot_type`, `accepts(event)`,
`evaluate(previous, event, reference_time=...)`뿐이다. 임의 handler Protocol이나
공통 Operational State/Health 모델을 도입하지 않고 두 concrete adapter의 닫힌
union으로 지원 범위를 명시한다. 기존 Fryer evaluate 인자·결과는 동일하다.

| 선택 key | 필요한 binding 입력 | 입력 / snapshot / 원본 결과 |
|---|---|---|
| fryer / explicit-cooking-reference / 1 | SYNTHETIC_SIGNAL | SyntheticSignal / TwinState / FsmResult |
| hood / rpm-observation / 1 | NUMERIC_TELEMETRY | HoodRpmEvent / HoodSnapshot / HoodResult |
| refrigerator, freezer, dishwasher 기존 key | handler 없음 | HANDLER_NOT_IMPLEMENTED |

`ProfileSnapshot.state`는 장비별 객체 전체를 보존한다. Hood의 policy/current/
candidate/candidate_since/last_event_time/receipts를 줄여서 재구성하지 않는다.
`RegistryResult.snapshot.state is RegistryResult.fsm.state`이며 `.fsm`은 원본
FsmResult/HoodResult다. PENDING·NO_CHANGE·INITIALIZED에서는 원본대로 intents가
비어 있고 state_change_id를 새로 만들지 않는다. 기존 SHA-256 생성도 각 엔진에
위임한다. 처음 미확정 상태와 이후 STOPPED→ROTATING→STOPPED를 그대로 지원한다.

선택 우선순위는 위 7절 표와 같다. resolve는 binding store/type/profile/version,
등록 handler와 InputKind를 검사한다. evaluate는 numeric payload equipment_type을
요청 equipment_type과 추가 비교한다. handler.accepts는 Fryer에 SyntheticSignal만,
Hood에 hood/fan_rpm/rpm/1.0.0의 HoodRpmEvent만 허용한다. 잘못된 metric/unit/schema
version 및 장비 간 입력 혼용은 UNSUPPORTED_INPUT이다. 선택된 handler와 state 타입
불일치는 SNAPSHOT_TYPE_MISMATCH이며, store/equipment가 다른 snapshot도 거부한다.
구성 시 handler를 다른 profile/version에 등록하는 것은 허용하지 않는다.

Registry 계약 거부는 예외이며 snapshot·후보·원장 전체가 유지되고 계산을 호출하지
않는다. 직접 qualifier의 INVALID_SAMPLE과 구별해야 한다. 직접 호출에서는 잘못된
metric/unit/version도 소비하며 후보를 끊지만, Registry에서는 실행 전 거부하므로
소비하지 않는다. caller는 이런 예외를 무시한 입력 구간을 유효한 연속 관측으로
간주하면 안 된다. 실제 consumer의 오류 인계·후보 단절 절차는 미구현이다.
음수 RPM은 adapter에서 재판정하지 않고 qualifier의 INVALID_SAMPLE/후보 취소에
위임한다. 구조 오류·nonfinite 값은 기존 HoodRpmEvent 생성 시 예외다.

Hood adapter는 정책 숫자를 소유하거나 기본값을 주입하지 않는다. snapshot에
명시된 정책으로 계산하며 숫자는 초기 테스트 파라미터다. 실제 binding·source 권한,
wire decode, Kafka key 검사, Health·Alert·timer, checkpoint·outbox·sink는 구현하지
않았다. factory는 handler catalog만 생성하고 장비 binding을 자동 추론하지 않는다.

기존 hood_rpm.json을 변경 없이 재사용해 direct qualifier와 Registry를 단계별로
대조했다. 후보 보존·초기화·노이즈·정상 양방향 전이, 처리 지연 재계산, 확정 snapshot
replay·중복/충돌·역순/동시각, 잘못된 계약과 cross-profile 입력의 engine 호출 차단을
검증했다. 새 Registry 테스트 25개이며 기존 지원 목록 테스트에서는 Hood의 미구현
기대 항목 하나를 제거했다. 기존 Fryer 순환·Health 보존·ID 검증은 유지한다.
정확한 실행 결과는 검증 보고서의 Hood Registry Adapter 절을 따른다.

다음 논리 작업 후보는 Health freshness/timer의 별도 snapshot·이벤트 계약이다.
운영 cursor와 Health last_seen을 공유하지 않고 시간 단조성·timer 경합을 먼저
설계해야 한다. 이 연결 단위에서 Health나 외부 실행을 자동 진행하지 않는다.


## 10. Health freshness / timer 계약 — 설계 전용

[Health 전용 계약](equipment-health-contracts.md)에 책임·상태 전이·last_seen·
시간 모델·snapshot 복원·출력 identity와 구현 전 테스트 시나리오를 정의했다.
기존 이 문서의 Operational State/Registry 구현과 중복되지 않도록 Health 상세
규범은 해당 문서에서 관리한다. 후속 로컬 Health Core 구현은 해당 문서 10절을
따른다. timer scheduler·Job 4·sink는 미구현이다.

last_seen은 Health가 수용한 유효 관측의 event_time이며 Operational last_event_time과
독립이다. 실제 numeric metric을 장비별 allowlist로 사용하고 별도 heartbeat/fault
센서를 가정하지 않는다. Health profile·timeout/source 승인은 현재 설정에 없으므로
임의 숫자나 기본 profile을 넣지 않는다. Hood의 max_sample_gap을 timeout으로 쓰지 않는다.

명시적인 외부 tick 평가와 관측 세대별 deadline token으로 무입력 STALE을 설계한다.
평가 시각 >= deadline부터 만료이며, 신규 관측 뒤의 이전 token은 무효다. 같은 순서의
관측과 기록된 평가 시각을 replay해야 결정성을 비교할 수 있다. timer/관측의 순서를
바꾼 이력이 수렴한다는 보장은 하지 않는다. current용 Health revision과 별도
health_change_id를 정의하며 기존 Operational state_change_id는 변경하지 않는다.

FAULT는 승인된 고장 계약이 있을 때만 가능하다. 현재 네 장비에는 해당 입력이 없고
Health/Alert를 Operational enum에 합치지 않는다. 설계표와 실행 가능한 Health
단위 테스트의 대응은 해당 문서 10절을 따른다. 기존 Operational 테스트 통과를
새 Health 동작이나 외부 실행의 검증 증거로 사용하지 않는다.
