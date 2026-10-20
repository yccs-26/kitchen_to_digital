# Equipment Health Freshness / Timer 계약 — 설계 전용

작성일: 2026-10-17 (Asia/Seoul). 검토 HEAD:
`8bed0f7b41f4a2d10c7064137ddf0bea410bb9c8`, `feat/phase-4-twin-state`.
상태: **계약과 기대 테스트 시나리오 정의. 구현·Health 동작 검증 없음**.
이 문서의 필드·명령·판정 이름은 후속 구현을 위한 논리 계약이며 Python 클래스,
Registry API, Avro 또는 state.changes wire schema를 추가한 것이 아니다.

## 1. 근거와 책임

직접 읽은 [장비 계약](equipment-state-contracts.md),
[검증 보고서](../reports/phase-4-verification.md), `streaming/state/`의 contracts,
FSM, Hood qualifier, bindings/adapters/registry와 Ordering·Dedup·Hood 테스트를
기준으로 한다. config/validation_rules.json에는 등록 장비 4개·수치 metric/unit·
버전 1.0.0이 있고, config/job2-runtime.example.json은 Job 2 연결 설정이다.
둘 모두 Health profile·freshness timeout·heartbeat·fault 입력 설정은 없다.
Hood의 2초 max_sample_gap은 후보 연속성 파라미터이며 freshness timeout이 아니다.

로컬 기준 계획은 원본 checkout의
`/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/docs/plans/ktd-forward-execution-plan-2026-10-06.md`
중 Phase 4, Health/STALE, last_seen 경합 경계를 확인했다. 계획서 본문을 복사하지
않았다. `docs/adr/`에는 Local-first와 Kafka 채택 ADR 두 개가 있고 Health 전용
ADR은 없다. design-decisions.md, planning-update.md와 state-and-alerts.md의
운영 상태/Health/Alert 분리 및 무입력 event-time 한계도 함께 대조했다.
worktree에 AGENTS.md는 없다. 사용자 제공 PEP 8 지침과 접근 가능한 원본 checkout의
AGENTS.md를 읽었으며 파일을 worktree에 새로 만들지 않았다.

| 책임 | 의미 | 이 계약의 제한 |
|---|---|---|
| Operational State | Fryer 조리 상태, Hood STOPPED/ROTATING 등 동작 관측 | Health가 이를 변경하거나 후보를 취소하지 않음 |
| Health | 관측 freshness와 권위 있는 명시적 고장 | HEALTHY는 freshness를 뜻하며 물리적 안전·성능 인증이 아님 |
| Alert | 업무 이상 조건의 별도 발생·해제 lifecycle | Phase 5 책임, 여기서 생성·종료하지 않음 |

STALE인 Hood의 ROTATING 또는 Fryer의 COOKING은 마지막으로 확정된 관측 그대로
남는다. freshness 부족을 STOPPED/IDLE/FAULT로 치환하지 않는다. 높은 온도,
0 RPM·음수 RPM·입력 오류·lag만으로 FAULT를 만들지 않는다.

## 2. Health 상태와 전이

freshness와 active fault 집합을 독립 보존하고 표시 Health는 다음 우선순위로
계산한다: **active fault 존재 → FAULT; 없고 만료 → STALE; 없고 새 관측 유효 →
HEALTHY**. HEALTHY는 승인된 관측 채널이 최근이라는 제한된 의미다.
FAULT 중에도 deadline/evaluation을 유지하여 고장 해제 후 STALE 여부를 알 수 있다.

| 이전 표시 | 입력/조건 | 다음 표시 | 출력 정책 |
|---|---|---|---|
| 미초기화(None) | 최초 수용 관측, 아직 미만료 | HEALTHY | 초기화 기록 1개, previous=None 명시 |
| 미초기화(None) | 최초 관측 없이 시작 deadline 만료 tick | STALE | 초기화 기록 1개; HEALTHY였다고 가정하지 않음 |
| HEALTHY | 현재 관측 세대의 tick, 평가 시각 >= deadline | STALE | Health 변화 1개 |
| STALE | 더 최신의 수용 가능한 관측, 새 deadline > 평가 시각 | HEALTHY | 복구 변화 1개 |
| HEALTHY | 새 수용 관측 | HEALTHY | deadline 갱신, 변화 출력 없음 |
| 각 표시 | 중복·충돌·역순·부적합·너무 오래된 관측 | 유지 | 관측 수용 없음; 별도 tick은 계속 필요 |
| HEALTHY/STALE/None | 승인된 fault 발생 보고 | FAULT | 해당 profile이 fault 계약을 지원할 때만 |
| FAULT | 수치 관측·timeout 또는 일부 fault만 해제 | FAULT | freshness는 별도로 평가, 표시 변화 출력 없음 |
| FAULT | 마지막 active fault 명시 해제 | HEALTHY 또는 STALE | 그때 freshness로 결정; 미관측·미만료면 None |

None은 HealthStatus enum의 새 값이 아니라 아직 증거가 없는 snapshot 초기 표시다.
등록·활성화만으로 HEALTHY를 만들지 않는다. 원본 FSM은 호출자 지정 health를
그대로 보존하므로 이 초기화 규칙을 기존 StateObservation에 소급 적용하지 않는다.

현재 네 numeric 장비 모두 fault 발생/해제 입력을 지원하지 않는다. SyntheticSignal의
FaultSignal은 로컬 후보이며 기존 FSM에서 HEALTH_DEFERRED다. 이를 실제 fault 권한으로
승격하지 않는다. 위 FAULT 전이 의미와 우선순위는 정의하되 활성화는 유보한다.
향후 지원하려면 source/code allowlist, fault별 발생·해제 identity/순서 및 latch/reset을
별도 확정해야 한다. 장애 한 개의 해제를 전체 HEALTHY로 해석하지 않는다.

## 3. Freshness 증거와 Profile 의존성

`last_seen`은 **Health에서 수용한 최신 유효 관측의 aware UTC event_time**이다.
ingestion/processing 시각이 아니다. `received_at`, `ingested_at`, 처리 지연은 진단
정보로만 별도 보관하며 last_seen·deadline·변화 ID를 연장하지 않는다. 수신만으로
freshness를 부여하지 않으므로 old backlog를 빨리 소비해도 복구를 가장하지 않는다.

아래는 현재 입력 계약에 근거한 **Health allowlist 설계**다. 지금 Registry가 Health를
실행한다는 뜻이 아니다. Health profile·binding을 명시한 후에만 활성화할 수 있다.

| 장비 identity (모두 store-001) | type / 인정 metric / unit | Health 수용 범위와 제한 |
|---|---|---|
| fridge-001 | refrigerator / temperature_celsius / celsius | finite numeric, schema 1.0.0; 낮은/높은 값으로 동작·FAULT 추론 없음 |
| freezer-001 | freezer / temperature_celsius / celsius | 위와 같음; refrigerator profile 자동 공유 없음 |
| fryer-001 | fryer / oil_temperature_celsius / celsius | 유온 관측만; mode/청소 신호의 확보 근거 아님 |
| hood-001 | hood / fan_rpm / rpm | finite numeric, bool 제외, RPM >= 0, schema 1.0.0; deadband·PENDING·0 RPM도 유효 관측 |
| fridge-door-001, dishwasher-001 | 현재 numeric validated 등록/지원 없음 | freshness 입력 불가, 이름으로 parent 또는 heartbeat 추정 금지 |

모든 수용에는 실제 binding의 store/equipment/type, 승인 source, metric/unit/schema
allowlist가 필요하다. 현재 source는 문자열일 뿐 승인 source 정책은 미확정이다.
고온 자체는 품질 오류가 아니다. 음수 RPM은 현재 Phase 2가 허용할 수 있어도 Hood
Qualifier의 부적합 기준에 맞춰 freshness에서 제외한다. bool·NaN·Inf·잘못된 단위/
버전·다른 장비 입력도 증거가 아니다. fault 보고는 별도 계약 없이는 heartbeat가 아니다.

별도 HealthProfile은 `(equipment_type, health_profile_id, version)` exact-match key와
다음의 immutable 정책을 가져야 한다: 유효 metric/unit/schema/source 목록, metric별
추가 적합성 규칙, 양수 `freshness_timeout`, fault 지원 여부, 계약 버전.
이번에는 timeout **숫자를 확정하지 않는다**. 설정이 없으면 활성화를 거부하며
다른 장비/최신 version/일괄 기본 timeout으로 fallback하지 않는다.
기존 Operational ProfileKey는 별도로 보존하고, Health binding이 어떤 장비 계약
revision에 의존하는지 명시한다. 기존 ProfileSpec에 timeout이 있다고 주장하지 않는다.

현재 각 장비의 수치 metric 하나를 필수 freshness 증거로 선택한다. 여러 metric이
추가되면 필수 channel별 last_seen/deadline을 따로 두고 모두 fresh일 때 HEALTHY로
표시하는 안을 권장한다. 하나의 metric으로 다른 필수 채널 단절을 가리지 않는다.
다중 채널 구현·집계 정책은 현 범위 밖이며 새 Health profile version이 필요하다.

## 4. 시간 모델과 수용 순서

### 4.1 선택한 기준

| 대안 | 무입력 중 만료 | 재실행 특성 | 결정 |
|---|---|---|---|
| 센서 event-time 또는 watermark 진행만 이용 | 진행이 멈출 수 있음 | 관측 순서 고정 시 재현 용이 | 단독 timeout 구동으로 채택하지 않음 |
| 순수 함수 안에서 processing/wall clock 읽기 | 평가가 호출될 때만 가능 | 호출 지연에 따라 달라짐 | 채택하지 않음 |
| 외부 scheduler/tick이 명시적 UTC reference_time 제공 | tick이 계속 오면 가능 | tick 시각·순서를 기록하면 재현 가능 | 논리 계약으로 선택; scheduler 구현 방식은 미결정 |

관측 나이는 event-time으로 측정하고 현재성 판정은 외부에서 주입한 평가 시각으로
한다. 따라서 processing-time에 의해 호출되되 함수는 시계를 읽지 않는다. 무입력
STALE의 liveness는 scheduler의 지속 실행에 의존한다. 프로세스 중단 중 정시에
발행한다는 보장은 없으며 복구 후 overdue tick으로 평가한다.

모든 시각은 UTC 정규화, μs 정밀도로 비교한다. `T = freshness_timeout > 0`,
`D = last_seen + T`, 수용된 관측이 없으면 `D = monitoring_started_at + T`다.
명시적 활성화 기록의 `monitoring_epoch`와 `monitoring_started_at`을 영속 보존하며
재시작 때 새 start/epoch를 생성해 deadline을 늦추지 않는다. 재등록은 별도 관리
결정이며 과거 current를 자동 초기화하지 않는다.

평가 시각 `r < last_evaluated_at`이면 거부한다. 같은 시각은 허용하되 기록된
명령 순서를 유지한다. 사건 시각이 과거 평가 시각보다 작아도 아래 freshness
창 안에서 더 최신 관측이면 수용 가능하다. event_time과 평가 cursor를 혼동하지 않는다.

### 4.2 관측 처리의 논리 순서

1. 구조·장비 identity·정확한 Health binding/profile을 확인한다. Health 전용 원장의
   같은 event_id를 비교해 전체 canonical 10필드가 같으면 DUPLICATE, 다르면
   ID_CONFLICT다. canonical은 UTC 동등성·600/600.0 동등성을 적용하며 원문을 바꾸지
   않는다. 처리 metadata는 비교 대상이 아니다. 다른 장비 ID를 같은 원장에 섞지 않는다.
2. 새 ID의 평가 시각 역전, 미래 `event_time > r`, 활성화 전 사건
   `event_time < monitoring_started_at`을 거부한다. last_seen이 있으면 작은 시각은
   OUT_OF_ORDER, 같은 시각 distinct ID는 SAME_TIME이다. 먼저 수용한 관측이 우선하며
   도착 순서의 순열 불변성은 보장하지 않는다.
3. metric/unit/version/source·수치 적합성을 검사한다. 실패한 새 ID는 수용 cursor나
   dedup 원장을 선점하지 않는다. 같은 시각에 나중에 온 정상 새 ID는 수용 가능하다.
4. `r >= event_time + T`이면 TOO_OLD_FOR_FRESHNESS로 거부한다. upstream에서 valid여도
   이미 만료된 backlog는 fresh evidence가 아니다. last_seen·deadline을 갱신하거나
   잠시 HEALTHY로 만들지 않는다. 정확히 age=T인 경우도 거부한다.
5. 위 조건을 통과하면 last_seen/event_id·canonical 원장·관측 세대·deadline을 함께
   갱신한다. last_evaluated_at=r로 진행하며 active fault가 없으면 HEALTHY다.
   관측 세대는 수용한 event_id로 식별한다. 현재성 출력은 표시가 바뀔 때만 생성한다.

중복·충돌·시간/계약/너무 오래된 입력 거부는 Health snapshot 전체를 유지한다.
수신 카운터만 별도 진단 경로에서 갱신할 수 있다. 거부 입력으로 timer 평가를 대신하지
않으며 입력 오류가 계속 와도 독립 tick이 굶지 않아야 한다. HEALTHY snapshot은
항상 `last_evaluated_at` 시점의 판단이다. 다음 tick이 없으면 실제 현재 wall-clock
까지 fresh라는 뜻이 아니다.

### 4.3 Operational State와의 차이

기존 FSM/Hood의 identity → dedup/conflict → 미래/역순/동시각 검사를 재사용하되,
원장과 cursor는 독립이다. 기존 last_event_time에는 금지·FAULT 유보·INVALID_SAMPLE도
소비될 수 있어 Health last_seen으로 복사할 수 없다. Health는 허용된 유효 수치
관측만 수용하며 더 강한 평가 시각 단조성과 age<T 검사를 추가한다.

Hood의 PENDING/NO_CHANGE에서도 새 유효 RPM은 freshness를 연장한다. Fryer 유온은
실제 수치 증거로 인정할 수 있지만 기존 synthetic Reference의 입력은 아니다.
따라서 Health profile이 명시된 경우 조리 Registry의 수치 입력 거부와 무관하게
Health에서 유온을 수용할 수 있다. Reference가 거부한 mode/금지 보고를 heartbeat로
전환하는 것은 아니다. Health 라우팅은 이번에 구현하지 않았다.

## 5. Timer 토큰, 경계와 신규 관측 경합

Timer는 `(store_id, equipment_id, Health profile+정책 fingerprint, monitoring_epoch,
observation_generation, expected_deadline)`을 담는다. 최초 관측 전 generation은
명시적 활성화 기록의 boot 세대다. 새 수용 관측마다 generation/deadline이 바뀐다.
이 필드가 현재 snapshot과 하나라도 다르면 OBSOLETE_TIMER이며 snapshot 전체를
보존한다. 특히 오래된 timer가 last_evaluated_at을 전진시켜 새 입력을 막지 못한다.

유효한 현재 세대 tick만 단조 평가 시각을 확인한다. r < D이면 fresh 여부를 유지,
r >= D이면 STALE로 표시한다(active fault가 있으면 표시는 FAULT 유지).
초기 boot 세대의 r < D는 None 유지다. deadline 직전은 `D-1μs`, 정확한 D부터
만료다. 이미 STALE이면 같은/늦은 tick에서도 새 Health 변화 출력이 없다.
STALE 재평가를 별도 새 관측 세대로 만들지 않는다.

같은 장비의 관측/tick은 단일 논리 순서로 직렬화하고 그 순서를 replay에 보존한다.
동일 시각의 처리 순서를 암묵적으로 정렬하지 않는다.

- 관측 먼저: 새 fresh 관측으로 세대를 바꾸면 이전 deadline token은 무효다.
  아직 평가하지 않은 과거 STALE 구간을 합성하여 STALE→HEALTHY 두 건을 만들지 않는다.
- tick 먼저: 현재 세대가 만료되면 STALE 1건, 이후 fresh 관측이면 HEALTHY 1건이다.
  두 변화는 실제로 처리한 서로 다른 논리 명령의 결과다. 관측 먼저인 실행과 이력은
  다를 수 있으며 마지막 상태만 같다고 이력까지 수렴한다고 주장하지 않는다.
- 지연 관측: last_seen보다 최신이고 age<T이면 STALE 이후 복구를 허용한다.
  사건 시각이 tick 평가보다 앞서도 현재 평가 시각을 되돌리지 않는다. age>=T이면
  복구도 last_seen 갱신도 하지 않는다. 과거 STALE 이력은 삭제하지 않는다.

복구 시 snapshot의 현재 token을 재구성해 명시적인 현재 평가 시각으로 한 번
확인한다. 아직 미만료면 다시 예약하고 overdue면 STALE을 판정한다. scheduler의
중복 전달을 허용하되 표시 변화 중복은 허용하지 않는다. 동시 실행은 snapshot
revision에 대한 compare-and-set 또는 동등한 직렬화가 필요하다. 방식은 미구현이다.

## 6. 최소 논리 Snapshot과 복원 조건

아래는 설계 필드다. 새 dataclass나 현재 ProfileSnapshot의 필드를 구현하지 않았다.

| 필드 | 필요 이유 |
|---|---|
| snapshot_schema_version, health_contract_version | 표현·판정 알고리즘 호환성 |
| store_id, equipment_id, equipment_type | 다른 장비로 snapshot/token 이동 방지 |
| health_profile_key, immutable 정책 본문 또는 복원 가능한 fingerprint | timeout·allowlist·source·fault 지원의 exact-match 복원 |
| equipment_binding_revision, 의존 Operational profile/입력 계약 revision | 장비 type·입력 계약 변경 시 Health snapshot 호환성 재검증 |
| monitoring_epoch, monitoring_started_at | 최초 무입력 deadline, 재시작 시 시작 시각 리셋 방지 |
| last_seen, last_accepted_event_id (최초에는 둘 다 None) | 최신 유효 관측과 관측 generation |
| freshness_deadline | 재평가/예약 기준; last_seen 또는 start + T와 일치 검증 |
| health_status (초기 None), last_evaluated_at | 표시와 그 판단 시각; 평가 시간 역행 방지 |
| canonical observation receipts | 오래된 ID replay·전체 payload 충돌 검출; durable 원장 참조도 가능 |
| health_revision, last_health_change_id | 초기화/표시 변화 순서와 재실행 chain; 다른 cause timestamp와 순서를 분리 |
| snapshot_revision | 같은 장비의 관측/tick 동시 적용 방지; 표시 no-op이어도 수용 갱신은 별도 revision |
| active_faults + fault별 identity/순서 원장 (조건부) | 승인 fault profile에만 필수; 현재 profile은 빈 집합이며 미지원 |

timer key는 generation+deadline+profile+epoch에서 재구성하므로 임의 scheduler task
ID를 논리 상태 identity로 사용하지 않는다. 단순히 마지막 event_id 하나만 저장하면
과거 ID replay·변조를 분류할 수 없다. durable 원장 보존 범위와 TTL은 미결정이며,
그 범위를 넘는 replay에는 충돌 판정을 보장하지 않는다. 원장과 snapshot은 일관된
논리 경계에서 복원해야 한다. 필수 근거가 없으면 조용히 HEALTHY로 복원하지 않는다.

Profile key/version 또는 fingerprint 불일치는 INCOMPATIBLE_SNAPSHOT으로 거부한다.
latest로 자동 이동하거나 새 timeout을 기존 snapshot에 덮어쓰지 않는다. 이전 정책
exact-match 복원 또는 별도 승인된 migration/rebuild만 허용한다. 기존 Operational
Profile 변경도 장비 입력/Health 의존성을 재검증하며, 명시적 호환성 판정 없이 새
binding으로 기존 Health snapshot을 재사용하지 않는다.

## 7. 재실행과 Health 출력 identity

Health 변경 identity는 별도 `health_change_id` namespace로 설계한다. 기존 Fryer와
Hood의 state_change_id 생성 규칙은 변경하지 않는다. 향후 state.changes에 실을 경우
change_kind와 ID 필드/envelope 호환성을 먼저 정해야 하며 현재 wire 지원은 아니다.

논리 identity는 canonical JSON의 SHA-256이다. 재료는 domain=equipment-health,
health_contract_version, Health profile key+정책 fingerprint, store/equipment/type,
monitoring_epoch, 다음 health_revision, previous health_change_id(최초에는 boot root),
이전/다음 health, record_kind(INITIALIZED/TRANSITION), 그리고 다음 cause다.

- 관측 초기화/복구 cause: 원본 event_id와 UTC event_time.
- STALE cause: 관측 또는 boot generation과 expected_deadline.
- fault cause: 승인된 별도 fault 계약 identity; 현재 활성화하지 않는다.

canonical JSON은 고정 필드 순서·UTF-8·고정 구분자를 사용하고 시각은 UTC ISO-8601
μs로 표현한다. 정책 fingerprint는 timeout의 정수 μs, 정확한 key/version, 정렬된
allowlist와 적합성/fault 지원 규칙의 canonical 표현을 SHA-256으로 만든다.
Python hash나 실행마다 바뀌는 직렬화·random 값은 쓰지 않는다.

r 자체·scheduler 전달 UUID·실제 호출 wall-clock은 ID에 넣지 않는다. 같은 세대의
만료 판정은 D에 실행하든 더 늦게 실행하든 같은 이전 snapshot에서 같은 변화 ID다.
초기화는 previous=None인 Health 기록이며 가상의 이전 Operational State 전이가 아니다.
HEALTHY 중 새 관측, PENDING, 중복 timer처럼 표시가 같으면 health_revision을 올리거나
새 변화 ID를 만들지 않는다. snapshot_revision은 증가할 수 있다.

Health 변화의 event_time/deadline은 **원인 근거 시각**이다. 지연 관측의 event_time이
이전 STALE deadline보다 앞설 수 있으므로 이를 current 적용 순서로 사용하지 않는다.
Health current/이력 순서는 health_revision 및 기록된 명령 순서를 따른다. 평가 시각은
snapshot/평가 로그에 따로 보관한다. 출력 wire timestamp 명칭과 payload는 후속 결정이다.

같은 이전 snapshot·명령 순서·명시적 평가 시각을 replay하면 같은 상태와 변화 ID여야
한다. 이미 확정한 snapshot에서는 관측 replay/동일 timer로 재출력하지 않는다.
실제 현재 시각으로 다시 평가하면 age 경계를 넘을 수 있어 결과가 다를 수 있다.
이는 replay와 live 재평가의 차이다. Health까지 처리 지연에 무관하게 항상 같은
결과라는 보장은 하지 않는다. 순수 함수와 명시적 tick만으로 sink 전달 중복/유실을
해결하지 못한다. snapshot과 pending intent/outbox의 원자적 확정 또는 동등한 복구
계약이 필요하며 이는 구현·검증하지 않았다.

## 8. 구현 전 테스트 시나리오 — 전부 미실행

표의 P는 승인 source가 테스트용으로 명시된 Hood Health profile, fault 미지원,
T는 **미확정 양수 timeout을 나타내는 기호**, ε=1μs다. t0는 고정 UTC 시각이고
T>4ε라 가정한다. 수치 예시는 800 rpm이며 별도 언급이 없으면 정확한 장비·
fan_rpm/rpm/1.0.0, 새로운 event_id를 사용하며 표에 사건 시각이 없으면 event_time=r다.
실제 T 숫자/운영 profile을 만들지 않는다.

- B: 명시적 활성화 snapshot. start=t0, last_seen=None, health=None, r_last=t0,
  boot token, D0=t0+T, 빈 원장/fault, health_revision=0.
- H: B에서 e1(event_time=t0)을 r=t0에 수용한 snapshot. HEALTHY, last_seen=t0,
  generation=e1, D=t0+T, health_revision=1, e1 canonical 원장 보유.
- S: H에 현재 token의 tick을 r=D에 적용. STALE, r_last=D, health_revision=2.
- 모든 케이스는 별도 초기 snapshot에서 시작한다. Operational State·Hood 후보는
  외부의 임의 확정값으로 두며 모든 결과에서 원문 그대로여야 한다.
- 출력 수는 Health 초기화/변화 의도의 수다. 기존 Operational state intent가 아니다.

| 번호 | 초기 snapshot | 입력과 평가 시각 | 기대 결과 |
|---|---|---|---|
| 1 | B | 최초 e1(t0), r=t0 | H, INITIALIZED 1개(previous=None), D=t0+T |
| 2 | H | 현재 token tick, r=D-ε | HEALTHY 유지, last_seen=t0, 출력 0 |
| 3 | H | 현재 token tick, r=D | STALE, last_seen/deadline 유지, 출력 1 |
| 3b | H | 현재 token tick, r=D+ε | STALE, 번호 3과 같은 논리 변화 ID, 출력 1 |
| 4 | H | 센서 무입력; tick만 r=D+T | STALE, 한 세대의 만료 출력 1; 무입력 자체는 호출이 아님 |
| 4b | B | 최초 관측 없음; boot tick r=D0 | None→STALE 초기화 1개, last_seen=None |
| 5 | S | e2(event_time=D+ε), r=D+ε | HEALTHY 복구 1개, last_seen=e2 시각, 새 세대/새 deadline |
| 6 | H | 같은 e1 원문, r=D-ε; 그 뒤 현재 tick r=D | duplicate snapshot 불변; D 연장 없음; tick의 STALE 1개 |
| 7 | H 변형(start=t0-2ε, 나머지 동일) | 새 ID의 event_time=t0-ε, r=t0+ε | OUT_OF_ORDER, last_seen=t0/원장 유지 |
| 8 | H | e1 ID로 RPM=801 또는 source/시각/unit 변조, r=t0+ε | ID_CONFLICT, canonical 및 freshness 불변, 출력 0 |
| 9 | H→S | 번호 3의 tick을 S에 같은 r 또는 D+ε로 반복 | STALE 유지, 추가 출력 0; 이전 H에서 재계산 시 동일 ID |
| 10 | H/S | 모든 필드를 재적재하고 같은 관측/tick·r·순서 replay | 초기 snapshot별 결과/ID 동일; 확정 S에는 중복 출력 없음 |
| 11a | H | r=D에서 e2(D) 먼저, 그 뒤 옛 e1 token tick | HEALTHY, 새 D2=D+T; 옛 token 무효, 변화 출력 0 |
| 11b | H | r=D에서 e1 token tick 먼저, 그 뒤 e2(D) | STALE 1개→HEALTHY 1개; 최종 last_seen=D, 11a와 이력 차이 명시 |
| 11c | S | 지연 e2(D-ε), r=D+ε | age=2ε<T, last_seen보다 최신이므로 HEALTHY 복구 1개; 평가 시각 역행 없음 |
| 11d | S | e2(t0+ε), r=D+ε | age=T로 만료; last_seen=t0 유지, STALE·출력 0 |
| 12 | H | 0/1800 RPM 정상 새 샘플 또는 fault 형태의 미지원 입력, r=t0+ε | RPM은 HEALTHY 유지·freshness 갱신; 미지원 fault는 거부; 어떤 경우도 FAULT 생성 없음 |
| 13 | H | Health profile version/timeout fingerprint/의존 binding 변경 후 복원, r=t0+ε | INCOMPATIBLE_SNAPSHOT; current·deadline 덮어쓰기/출력 없음 |
| 14 | H | 같은 event_time=t0의 distinct ID e2, r=t0+ε | SAME_TIME, 먼저 수용된 e1 canonical 우선, freshness 불변 |
| 15 | H | 새 ID 잘못된 metric/unit/version/음수·bool·NaN·Inf 후 같은 시각 정상 새 ID, r=t0+2ε | 오류는 증거·cursor를 선점하지 않음; 정상(t0+ε)만 수용·새 deadline; FAULT 없음 |
| 16 | H | 미래 e2(t0+2ε), 첫 r=t0+ε 후 r=t0+2ε 재시도 | 첫 snapshot 불변; 두 번째만 수용; 처리 시각을 event_time으로 대체하지 않음 |
| 17 | H에서 tick r=t0+2ε 적용 후 | 새 e2(t0+ε), r=t0+ε | 평가 시각 역전 거부; clock 역행으로 deadline 연장 없음 |
| 18 | H에서 e2(t0+ε) 수용 후 | 옛 e1 token tick, r=D+ε | OBSOLETE_TIMER; 새 generation/deadline 및 평가 cursor 불변 |
| 18b | 번호 18 이후 snapshot | 새 e2 token tick, r=D+ε | 새 deadline과 정확히 같으므로 STALE 1개; 옛 token 처리로 대체하지 않음 |
| 19 | H 또는 B 복원 | 재시작 현재 평가 r=D+ε의 현재 token tick | 기존 epoch/start 보존, STALE 1개; 이미 S를 복원하면 출력 0 |
| 20 | 조건부 P_fault의 FAULT, 두 active code | 한 fault만 해제 또는 tick으로 만료, r=D | FAULT 유지; 마지막 해제 시 STALE. P_fault는 가상 설계 케이스이며 현재 입력에 없음 |

번호 20의 수치 fault 임계값은 만들지 않으며 실제 fault payload/권한 테스트는
별도 계약 확정 후 작성한다.
이 표는 executable test/fixture가 아니며 어떤 케이스도 Health 테스트 통과로 집계하지 않는다.

## 9. 미결정 선택·권장안·진입 조건

| 미결정 항목 | 선택지와 권장안 | 근거/구현 전 필요한 증거 |
|---|---|---|
| 장비별 T | 장비별 명시 값 / 전역 fallback; 전자를 권장 | 실제 sampling·지연 분포·허용 관측 공백 검토, 값 없으면 활성화 거부 |
| source 권한 | 임의 문자열 / profile allowlist+입력 권한 확인; 후자 권장 | 실제 source 소유/장비 매핑 확인; 현재 문자열만으로 권위 부여 금지 |
| tick 구동 | Job 4 runtime timer / 별도 scheduler tick; 주입 가능한 tick 계약 권장 | idle 중 평가·복구·비용·장애 격리 검증 후 구체 기술 선택 |
| tick 주기·시계 skew | 고정 주기 / deadline 예약; deadline 예약+복구 재평가 권장 | 지연 허용치·UTC 동기화·시계 점프 관측; 함수의 단조 guard만으로 해결 안 됨 |
| snapshot/원장 저장·TTL | 전체 원장 / 보존 기간 제한+durable 참조; 요구 replay 범위부터 결정 | TTL 초과 replay/충돌과 복원 일관성의 증명 필요 |
| timer/관측 동시 쓰기 | 단일 writer / CAS; 장비별 직렬화와 revision guard 권장 | 중복 평가·경합·부분 실패 테스트; 분산 저장은 이번 미구현 |
| FAULT lifecycle | numeric 추론 / 승인된 별도 보고·확정 규칙; 후자만 허용 | 현재 센서 없음; code/source/latch/reset/순서 계약 확정까지 비활성 |
| Health 출력 wire/outbox | 별도 envelope / state.changes 확장; 기존 ID와 분리한 명시적 change_kind 권장 | consumer 호환성·pending intent/ACK·history/current/topic 복구 설계 필요 |
| 여러 필수 metric | any fresh / 모든 필수 채널 fresh; 후자 권장 | 새 metric별 profile version과 channel별 deadline 설계 필요 |

이번에 결정한 것은 책임·시간 기준·경계·수용/거부·세대 token·재실행 identity의
논리 정책이다. 실제 timeout/source와 운영 scheduler/storage 선택은 확정하지 않았다.
다음 구현은 이 계약 검토 후 별도로 승인된 범위에서 진행한다. Health FSM·scheduler·
Spark Job 4·Kafka/DynamoDB Sink·Alert Lifecycle은 이번에 구현하지 않는다.
