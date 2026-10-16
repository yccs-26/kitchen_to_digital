# Phase 4 신호 계약 준비

이 단위는 로컬 타입·synthetic 후보 입력의 형태 검증만 구현한다.
reference는 `IDLE → PREHEATING → COOKING → CLEANING → IDLE`이며
실행 가능한 전이표나 전이 함수가 아니다. `StateObservation`에는 기본값이
없다. 관측 전 IDLE/HEALTHY로 초기화하는 정책을 승인한 것이 아니다.
모든 operational state와 HEALTHY/STALE/FAULT 조합을 표현할 수 있다.

## 현재 입력 한계

현재 `schemas/avro/sensor_metric_event.avsc`는 10개 필드의 numeric telemetry다.
`config/validation_rules.json`은 refrigerator/freezer 온도, fryer 유온,
hood 팬 RPM만 허용한다. mode/cleaning/fault 필드는 없으며 해당 이름을
metric_name으로 넣어도 현재 domain validation을 통과하지 못한다.
화구 reference를 기존 fryer와 같은 장비라고 간주하지 않는다.
`validated_publisher.py`도 기존 센서 필드만 직렬화한다.

## 후보 의미

`SyntheticSignal`은 `synthetic_test_only` 표기를 필수로 요구한다.
fixture의 장비와 source는 가상 식별자이며 장비 등록 또는 실제 입력 증거가 아니다.
아래는 wire schema가 아닌 로컬 payload 후보다. JSON fixture 해석은 테스트에만 있다.

| 후보 | 의미 | 자동으로 결정하지 않는 것 |
|---|---|---|
| ModeSignal | IDLE/PREHEATING/COOKING 보고 | 실제 전이, 조리 완료 |
| CleaningSignal | 청소 active true/false 보고 | 청소 완료/중단 구별, IDLE 복귀 |
| FaultSignal | 특정 code의 active true/false 보고 | 전체 장애 해소, HEALTHY 복귀 |

신호 부재는 false, HEALTHY, IDLE을 의미하지 않는다. 온도로 CLEANING/FAULT를
만들지 않는다. STALE은 입력 신호 후보로 만들지 않았으며 freshness 계산과
timer 실행은 후속 단위다. 모드·청소·장애가 서로 충돌할 때 우선순위도 미정이다.

## 전이 구현 전 결정 게이트

1. 전체 reference를 유지한다면 실제 신호 생산 주체와 권위 있는 source,
   대상 장비 및 화구/fryer 대응을 정해야 한다. 기존 Avro의 호환 확장인지 별도
   입력 계약인지, 전송 경로·버전·Job 2 검증 범위를 별도 승인해야 한다.
2. mode가 명령인지 관측 보고인지, PREHEATING/COOKING의 시작·종료 의미,
   cleaning 비활성이 완료인지 중단인지, mode/cleaning 충돌 처리를 확정해야 한다.
3. fault 코드 목록·발생/해제·복수 장애·latch/reset 규칙과 STALE/FAULT 동시
   조건의 표시 우선순위를 정해야 한다. 단일 코드 해제를 HEALTHY로 바꾸면 안 된다.
4. 신호 identity, 장비 key, 시각/sequence, 중복·동시·역순·늦은 신호 및 누락
   처리, 초기 상태를 정해야 한다. UTC 정규화만으로 순서를 보장하지 않는다.
5. STALE의 last_seen 갱신 대상·시간 기준·timeout·정상 복귀·timer 경합 정책을
   정하고, 필요한 전이별 hysteresis/지속시간/threshold를 별도 검증해야 한다.

신호를 추가하지 않는다면 CLEANING/FAULT 및 명시적 모드 전이를 유보하는
최소 numeric 추정 범위로 reference와 DoD를 명시적으로 변경해야 한다.
이 단위는 어느 선택도 확정하지 않는다. main 병합·클라우드 통합 순서는 유지한다.

## 확인 한계

지정된 두 원본 계획서와 `docs/streaming/state-and-alerts.md`, 실제 Avro,
validation_rules 및 validator/publisher를 읽었다. 지정된
`../handoff/validated-contract.md`는 권한 확장 읽기에서도
`Operation not permitted`로 실패하여 현재 본문 대조가 미완료다.
wire adapter, 전이 실행, threshold, Job 4, sink, timer, Alert는 구현하지 않는다.
