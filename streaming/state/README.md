# Phase 4 신호 계약과 순수 reference FSM — P4-1

`transition(previous, event, reference_time=...)`은 다음 snapshot, 판정 이유,
상태 변경 출력 의도를 반환한다. 외부 I/O·전역 가변 상태·현재 시각 조회는 없다.
적용 범위는 **synthetic reference 화구**다. 실제 fryer에 이 정책을 적용했다고
간주하지 않는다. 초기 상태와 health는 호출자가 명시하며 자동 초기화하지 않는다.

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
