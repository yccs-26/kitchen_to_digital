# Phase 4 신호 계약과 순수 reference FSM — P4-1

`transition(previous, event, reference_time=...)`은 다음 snapshot, 판정 이유,
상태 변경 출력 의도를 반환한다. 외부 I/O·전역 가변 상태·현재 시각 조회는 없다.
적용 범위는 **synthetic reference 화구**다. 실제 fryer에 이 정책을 적용했다고
간주하지 않는다. 초기 상태와 health는 호출자가 명시하며 자동 초기화하지 않는다.

장비별 실제 입력 목록·후보 전이표와 미구현 정책 선택 구조는
[장비별 FSM 계약·Registry 설계](../../docs/streaming/equipment-state-contracts.md)를
참고한다. 해당 문서는 확장 제안이며 현재 함수·enum의 적용 범위를 바꾸지 않는다.

## 기존 validated 계약과 별도 입력 제안

현재 Avro는 `event_id`, `event_time`, `store_id`, `equipment_id`,
`equipment_type`, `metric_name`, `metric_value`, `unit`, `schema_version`,
`source` 10필드다. `metric_value`는 double, 나머지는 string이다.
내부 event_time은 aware UTC, publisher wire 시간은 UTC ISO-8601이며 Kafka
key는 원문 equipment_id의 UTF-8 bytes다. event_id는 센서 이벤트 identity다.

현재 validation_rules는 냉장고/냉동고 온도, fryer 유온, hood RPM만 허용한다.
mode/cleaning/fault는 현재 validated 입력에서 얻을 수 없다. 이 이름을
metric_name에 넣는 우회나 Avro 변경, 기존 Phase 2/3 수정은 하지 않았다.

별도 명시적 제어기 보고를 로컬 `SyntheticSignal`로 제안한다.
`signal_id`, aware `event_time`, `store_id`, `equipment_id`, `source`, 단일
payload, 필수 `provenance=synthetic_test_only`를 갖는다. signal_id는 이 별도
신호의 identity이며 기존 센서 event_id를 대체하거나 생성하지 않는다.
센서 event_id를 소비하는 adapter는 미구현이다. 실제 제어기 event_id와
signal_id의 대응, 승인 source·장비 등록·전송 경로·wire 버전은 통합 전 결정한다.
FSM은 store_id와 equipment_id가 snapshot과 모두 일치하는지 확인한다.

| payload | 해석 |
|---|---|
| ModeSignal(PREHEATING/COOKING/IDLE) | 명령이 아닌 제어기 모드 관측 보고 |
| CleaningSignal(True) | 청소 활성 보고 |
| CleaningSignal(False) | 청소 비활성만 관측, 정상 완료·중단을 구별할 수 없음 |
| CleaningCompletedSignal() | 청소 정상 완료의 명시적 보고; 이번 추가 |
| FaultSignal(code, active) | 장애 코드 발생/해제 후보; 별도 health 계산으로 유보 |

보고를 즉시 적용하는 정책 `synthetic-reference-v1`이다. 온도·전력 기반
추정, threshold·hysteresis·지속시간 규칙은 없다. 보고 부재는 IDLE/HEALTHY/
청소 완료를 뜻하지 않는다. synthetic 완료 신호는 실제 센서 근거가 아니다.

## 전이표

| 이전 상태 | 신호 | 다음 상태 | 판정 |
|---|---|---|---|
| IDLE | ModeSignal(PREHEATING) | PREHEATING | TRANSITION |
| PREHEATING | ModeSignal(COOKING) | COOKING | TRANSITION |
| COOKING | CleaningSignal(True) | CLEANING | TRANSITION |
| CLEANING | CleaningCompletedSignal() | IDLE | TRANSITION |
| 모든 상태 | 현재와 같은 모드 보고 | 유지 | NO_CHANGE |
| CLEANING | CleaningSignal(True) | 유지 | NO_CHANGE |
| 모든 상태 | CleaningSignal(False) | 유지 | NO_CHANGE |
| IDLE | CleaningCompletedSignal() | 유지 | NO_CHANGE |
| 모든 상태 | FaultSignal | 유지 | HEALTH_DEFERRED |
| 그 외 | 다른 목표 상태 보고·시점이 맞지 않는 청소 완료 | 유지 | FORBIDDEN |

예: IDLE→COOKING, PREHEATING→IDLE/CLEANING, COOKING→IDLE/PREHEATING,
CLEANING→PREHEATING/COOKING은 금지다. CLEANING에서 ModeSignal(IDLE)도
금지다. 비활성/IDLE 보고를 청소 정상 완료로 해석하지 않는다.
health는 HEALTHY/STALE/FAULT 어느 값이든 그대로 보존한다. FAULT가 operational
전이를 차단하는 interlock도 이번 정책에 없다. 안전 제어용 정책이 아니다.
다중 fault/latch/reset, freshness/STALE timer 계산은 후속 단위다.

## 순서·재실행·출력

1. identity 불일치는 IDENTITY_MISMATCH다. 이후 signal_id가 원장에 있으면
   같은 전체 신호는 DUPLICATE, payload/source/시각 등 차이는 ID_CONFLICT다.
   동일 ID 충돌은 기존 원문을 보존한다.
2. 새 신호의 reference_time이 마지막 소비 시각보다 작으면
   REFERENCE_REVERSED, event_time이 reference_time보다 크면 FUTURE_EVENT다.
   reference_time은 미래 검사 기준이며 이전 호출의 처리 시각을 저장하지 않는다.
3. event_time이 마지막 소비 시각보다 작으면 OUT_OF_ORDER, 같으면 SAME_TIME다.
   distinct ID 동시각은 먼저 소비한 신호만 인정한다. 재정렬/sequence가 없으므로
   입력 순서를 바꾼 실행의 수렴까지 보장하지 않는다.
4. 위 거부는 snapshot·원장을 그대로 반환한다. 미래 신호는 시간이 지난 후
   재시도할 수 있다. 동시각 충돌은 기록/sequence 계약이 필요한 후속 과제다.
5. 시간 검사를 통과하면 금지 전이·no-op·health 유보도 소비 시각과 원장에
   기록한다. 따라서 거부했던 조리 보고가 이후 예열 뒤 재실행되어 전이하지 않는다.
   health 신호도 이 단일 순서 cursor를 공유한다. 별도 health 원장은 미구현이다.

snapshot과 payload는 frozen dataclass, 원장은 tuple이다. 같은 초기 snapshot,
같은 신호 순서와 기준 시각이면 결과가 같다. 유효 시각 범위의 처리 지연에도
출력 의도는 같다. state_change_id는 정책 버전·장비 identity·signal_id·UTC
사건 시각·전후 operational state의 canonical JSON을 SHA-256으로 계산한다.
reference_time이나 Python hash/random은 ID 재료에 넣지 않는다.

전이 때만 `StateChangeIntent`를 반환한다. 이는 발행/저장 완료가 아니다.
동일 이전 snapshot에서 retry하면 동일 의도와 ID를 다시 얻는다. 이미 계산한
다음 snapshot에 재입력하면 DUPLICATE이며 의도는 없다. 향후 I/O adapter는
의도 전달 전 snapshot만 확정해서 출력을 잃지 않도록 pending/outbox 또는
재실행 경계를 설계해야 한다. 현재 exactly-once나 sink 복구를 보장하지 않는다.

원장은 전체 원문을 보관하는 로컬 참조 구현으로 검색·복사 비용과 크기가
증가한다. 보존/TTL, checkpoint 복원·정책 migration, 실시간 서비스 적합성은
미검증이다. YAML은 읽기 I/O와 미사용 설정을 피하려고 이번 단위에 만들지 않았고
정책은 코드의 버전 상수·전이표에 고정했다. 실제 운영 설정은 후속 과제다.

## P4-2 — 순서 제어와 Hysteresis 적용 범위

P4-1의 `synthetic-reference-v1`과 전이 함수는 유지한다. 새 테스트의
`rapid_reports.json`은 10 ms 간격 반복 후보·중복·금지 복귀·동시각·늦은 보고를
포함한다. 이는 수치 threshold 주변 노이즈 fixture가 아니며 물리적 장비의
전이 속도를 검증한 것도 아니다.

### 현재 순서 제어를 해석할 때의 경계

| 조건 | 판정·snapshot 변화 |
|---|---|
| 다른 매장/장비 | IDENTITY_MISMATCH, 그대로 유지 |
| 저장된 ID와 같은 전체 신호 | DUPLICATE, 재출력·cursor 갱신 없음 |
| 저장된 ID와 다른 내용 | ID_CONFLICT, canonical 원문 유지 |
| 새 ID이며 기준 시각이 cursor보다 과거 | REFERENCE_REVERSED, 유지 |
| 새 ID이며 사건 시각이 기준 시각보다 미래 | FUTURE_EVENT, 유지; 이후 재시도 가능 |
| 새 ID이며 사건 시각이 cursor보다 과거 | OUT_OF_ORDER, 유지 |
| 새 ID이며 사건 시각이 cursor와 동일 | SAME_TIME, 유지 |
| 위 검사를 통과한 보고 | 전이 여부와 무관하게 cursor·원장 갱신 |

여기서 cursor는 `last_event_time`이다. DUPLICATE/ID_CONFLICT는 시각 검사보다
먼저 판정된다. 과거 ID replay는 OUT_OF_ORDER가 아닌 DUPLICATE이며,
금지/no-op/HEALTH_DEFERRED 보고도 이후 신호의 순서 경계가 된다.
이 cursor는 마지막 operational 전이 시각이나 건강한 관측의 last_seen과 다르다.
reference_time 자체를 단조 증가시키는 guard는 아직 없다. 이전 호출의 기준
시각보다 작아져도 마지막 소비 사건 시각 이상이면 허용될 수 있다.

동시각 distinct ID는 최초 소비가 금지/no-op여도 다음 신호를 막는다.
따라서 입력 도착 순서를 바꾸면 다른 결과가 나올 수 있다. 현재 보장은
**같은 순서의 입력 재실행 결정성**이다. sequence 없는 신호를 임의 정렬하거나
동시각 충돌을 버퍼링하여 재해석하지 않는다. 이런 입력은 caller가 판정 이유를
관측할 수 있지만 외부 보관·history correction은 아직 없다.

### 명시적 mode에는 Hysteresis를 추가하지 않는다

`ModeSignal`은 threshold 비교 결과가 아니라 제어기가 확정했다고 가정한
관측 보고다. 이 FSM에서 임의의 대기시간이나 재확인 횟수를 더하면 보고의
의미와 적용 시각이 달라진다. 같은 모드의 새 ID는 NO_CHANGE, 같은 ID는
DUPLICATE이며 되돌리기/건너뛰기는 FORBIDDEN으로 처리한다.

PREHEATING 직후 COOKING처럼 허용된 보고는 간격이 짧아도 즉시 적용한다.
합법적인 전체 순환의 빠른 반복도 억제하지 않는다. 이 동작은 수치 노이즈
제거 또는 debounce 보장이 아니다. 실제 제어기의 mode가 이미 안정화된
보고인지 통합 시 확인해야 하며, 그렇지 않다면 별도 명시적 debounce 정책과
버전 변경을 검토한다. 청소 완료와 fault 신호를 묵시적으로 지연/추정하지 않는다.

### 수치 기반 파생 상태의 별도 Hysteresis 정책 — 정의만, 미구현

향후 numeric qualifier는 기존 explicit FSM과 분리된 순수 계산 단계로 둔다.
예를 들어 독립적인 LOW/HIGH 온도 구간 판정이며, HIGH가 COOKING 또는 FAULT를
의미하지 않는다. operational state에 매핑하려면 장비별 의미·허용 전이를
별도 결정한다. 온도만으로 CLEANING/FAULT를 만들 수 없다.

장비·metric·unit·정책 버전별로 아래 값을 명시적으로 제공해야 한다.
임계값과 시간의 실제 숫자는 현재 센서 근거가 없어 미정이다.

| 정책 | 요구 |
|---|---|
| `enter_threshold`, `exit_threshold` | finite 수치, `enter > exit`, 입력과 같은 unit |
| `enter_duration`, `exit_duration` | 각 방향의 양수 지속시간 |
| `max_sample_gap` | 양수 최대 관측 간격; 샘플링 특성에 맞게 결정 |
| qualifier snapshot | 확정 구간, 후보 방향·시작 시각, 마지막 유효 관측, dedup 근거 |

고온 방향 reference 판정은 다음과 같이 정의한다.

1. LOW에서 `value >= enter_threshold`이면 HIGH 후보를 시작/유지한다.
   시간순의 새 유효 샘플에서 `event_time - candidate_since >= enter_duration`
   이고 연속 관측 조건을 만족해야 HIGH로 확정한다.
2. HIGH에서 `value <= exit_threshold`이면 LOW 후보를 시작/유지한다.
   LOW 확정도 동일하게 `exit_duration`을 충족해야 한다.
3. 각 방향의 조건이 깨지면 해당 후보를 취소한다. 두 임계값 사이에서는
   확정 구간을 유지하되 진행 중 후보는 취소한다. 이로써 경계 주변 반복이
   확정 구간을 왕복시키지 않는다. 반대 후보가 새로 시작하면 시작 시각도 새로 잡는다.
4. 연속성은 각 유효 샘플 간격 `<= max_sample_gap`으로 제한한다.
   초과 공백·잘못된 수치/단위는 후보를 끊는다. 공백 뒤 첫 적합 샘플에서
   후보를 다시 시작한다. 이 간격 내 조건 유지 가정은 관측 정책이지
   실제 물리적 연속 상태를 증명한 것은 아니다.
5. 중복·충돌·동시각·역순·미래 샘플은 근거를 추가하지 않는다.
   새 후보·지속시간·last_seen을 전진시키거나 취소하지 않으며 current를
   과거로 되돌리지 않는다. 시각이 지난 사실만으로 원래 후보를 확정하지 않는다.
6. 새 사건 없이 timer/reference_time만 흘러도 HIGH/LOW로 승격하지 않는다.
   관측 공백은 별도 Health freshness 계산으로 넘기며 qualifier의 확정 구간을
   자동 정상화하지 않는다. 숫자에서 파생한 후보의 출력 identity는 센서 event_id를
   보존하는 별도 계약이 필요하며 synthetic signal_id로 몰래 대체하지 않는다.

이 정책의 qualifier 함수·설정·numeric fixture/테스트는 이번에 추가하지 않았다.
따라서 Hysteresis **설계 범위 정의 완료**와 **수치 판정 구현·검증 미완료**를
구분한다. 장비별 숫자를 추정해서 YAML에 채우지 않는다.

### 다음 Health/timer 단위로 넘길 결정

- operational cursor를 Health의 last_seen으로 재사용하지 않는다. health snapshot에
  관측 freshness와 active fault 집합을 별도 보관한다. 이번에는 데이터 모델을 바꾸지 않는다.
- freshness의 기준은 승인 source의 새 유효 관측 사건 시각이며 reference_time은
  호출자가 주는 평가 시각으로 분리하는 안을 사용한다. 중복·충돌·금지 보고·
  미래/역순/동시각 거부를 단순 수신했다는 이유로 freshness를 연장하지 않는다.
  어떤 payload가 유효 관측으로 인정되는지는 실제 source 계약에서 확정해야 한다.
- 다음 단위에서 평가 시각의 단조성, `now >= last_seen + timeout`의 경계,
  관측 갱신과 이전 timer의 경합을 테스트한다. timer에는 관측 세대/기준 시각을
  연결해 새 관측 뒤 도착한 이전 timer가 current를 덮지 않게 한다.
- fault 코드별 발생/해제와 freshness를 독립 보존한다. 표시 우선순위 후보는
  FAULT > STALE > HEALTHY이나 실제 FAULT 집합·초기 unknown·복귀 규칙을
  구현 전 확정한다. 단일 코드 해제를 전체 HEALTHY로 바꾸지 않는다.
- timeout 숫자, fault source 권한, 실제 샘플링 간격은 미정이다. 이번 P4-2는
  Health/timer 구현·검증이나 실제 신호/E2E 완료를 뜻하지 않는다.
