# Equipment Health Freshness / Timer 계약과 로컬 Core

작성일: 2026-10-17 (Asia/Seoul). 검토 HEAD:
`8bed0f7b41f4a2d10c7064137ddf0bea410bb9c8`, `feat/phase-4-twin-state`.
최신 상태(2026-10-21): **Health Core + Snapshot Codec·로컬 파일 복원 검증**.
1~9절은 확정된 논리 계약과 최초 설계표이며, 실제 API·시나리오별 구현 상태는
10절을 따르고 Snapshot JSON Codec은 11절을 따른다. Registry 연결·Avro/
state.changes wire·scheduler·sink는 미구현이다.

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

아래는 논리 snapshot 계약이다. 실제 HealthSnapshot의 저장/파생 필드 매핑은
10절을 따른다. 기존 Operational ProfileSnapshot은 변경하지 않았다.

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

## 8. 구현 전 테스트 시나리오 기준표 — 최신 대응은 10절

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
이 표 자체는 executable test/fixture가 아니다. 최초 설계 당시 전부 미실행이었으며,
2026-10-20의 실제 단위 테스트 대응과 보류 항목은 10절에 별도로 기록한다.

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
최초 설계 단위에서는 구현을 수행하지 않았다. 후속 승인된 Health Core의 현재 범위는
10절을 따른다. scheduler·Spark Job 4·Kafka/DynamoDB Sink·Alert Lifecycle은 미구현이다.


## 10. Health Core Reducer — 2026-10-20 로컬 구현

검토 HEAD `f03203fe81613dcf2194d55c5b191686a30c8158`, 시작 작업 트리 clean.
`streaming/state/health.py`만 새 구현 파일로 추가했다. tick도 같은 Health snapshot과
정책에 대한 순수 계산이므로 별도 health_timer.py나 scheduler를 만들지 않았다.
기존 Fryer/Hood 전이·Registry·bindings·contracts와 config는 변경하지 않았다.
기존 HealthStatus enum은 공통 값 타입으로 재사용하되 StateObservation/TwinState,
HoodSnapshot·Operational 원장/cursor는 공유하지 않는다.

### 실제 API와 저장/외부 입력 경계

- `HealthPolicy`: numeric EquipmentBinding, binding_revision, input_contract_revision,
  별도 Health ProfileKey, 양수 freshness_timeout, 단일 metric/unit, schema_versions,
  sources를 명시적으로 받는다. Operational ProfileKey를 Health key로 자동 해석하지
  않는다. binding.profile은 의존 Operational profile/version을 보존한다.
- `initialize_health(policy, monitoring_epoch=..., monitoring_started_at=...)`:
  명시적 활성화로 health_status=None인 snapshot과 boot deadline을 만든다.
- `HealthObservation`: 기존 numeric 이벤트와 동일한 10필드, 내부 aware UTC 시각.
  wire decode는 하지 않는다. bool/string/None/비유한 primitive를 담아 코어에서
  INVALID_OBSERVATION으로 거부할 수 있으나 수용 원장에는 정상 관측만 저장한다.
  naive 시간·빈 식별자·mutable 값 등 구조 오류는 생성 시 예외다.
- `timer_token(snapshot)`: 현재 policy/epoch/관측 세대/deadline token을 재구성한다.
  boot와 event generation은 `(태그, identity)` tuple로 구분하여 같은 문자열의
  epoch/event_id가 충돌하지 않게 한다. 실제 timer 예약은 하지 않는다.
- `reduce_health(previous, item, policy=..., reference_time=...)`: HealthObservation
  또는 HealthTick(token)을 판정하여 HealthResult(snapshot, decision, changes)를
  반환한다. 다른 입력은 UNSUPPORTED_INPUT이다. 시계·thread·I/O 호출이 없다.

snapshot은 immutable 정책 본문(장비 identity·profile/dependency 포함), epoch/start,
last_seen·last_accepted_event_id, deadline·last_evaluated_at, canonical receipts tuple,
Health 표시·health_revision·last_health_change_id, snapshot_revision, schema/contract
version을 저장한다. fingerprint와 timer generation은 이 필드들에서 결정적으로
재구성하므로 중복된 가변 필드로 저장하지 않는다. 시작 시각·deadline·원장 시간순서·
마지막 수용 관측·표시/평가 시각·revision의 모순은 모델 생성에서 거부한다.

모든 profile/version·timeout/source/binding dependency는 immutable 정책에 명시한다.
기존 config를 읽거나 실제 장비/승인 source를 자동 등록하지 않는다. 현재 네 numeric
유형의 단일 metric/unit 및 schema 1.0.0만 지원하고 미지원 policy는 생성 시 거부한다.
실제 timeout/source는 여전히 미정이며 테스트의 10초와 fixture-simulator는 명시적
테스트 파라미터다. 운영 장비에 적용된 값·인증된 source라는 의미가 아니다.

### 전이·원장·timer와 identity

수용 순서는 4~5절을 따른다. 정책/표현/계약 version 불일치는
INCOMPATIBLE_SNAPSHOT, 장비 불일치는 IDENTITY_MISMATCH이며 거부는 원본 snapshot을
반환한다. 원장의 동일 ID·canonical payload는 DUPLICATE, 변조는 ID_CONFLICT다.
시간 거부와 부적합 관측은 원장·last_seen·deadline·평가 cursor를 갱신하지 않는다.
UTC 동등 시각과 int/float 동등값은 중복이지만 bool을 숫자로 취급하지 않는다.

최초 수용 관측은 HEALTHY/INITIALIZED 출력 1개, 최초 무입력 timeout은
STALE/INITIALIZED 출력 1개다. 관측 age는 timeout 미만이어야 하며 tick은 deadline
정확히 일치부터 STALE이다. STALE 후 더 최신의 fresh 관측은 HEALTHY로 복구한다.
수치 값에서 FAULT를 추론하지 않는다. 표시가 유지되는 관측은 deadline과 원장은
갱신하지만 health 변화 출력은 없다. 옛 token은 평가 시각 검사보다 먼저 무효화하여
새 세대의 cursor를 진행시키지 못한다.

관측/tick은 매번 단일 순서로 호출한다. 동일 시각 관측 먼저면 옛 tick 무효/변화 없음,
tick 먼저면 STALE→HEALTHY 두 변화다. 결과 이력의 순열 불변성은 보장하지 않는다.
수용 관측·유효 tick은 snapshot_revision을 진행한다. 같은 평가 시각의 상태 불변 tick은
원본 snapshot을 반환하며 늦은 반복 tick은 평가 cursor만 진행할 수 있다. 두 경우 모두
Health 변화 출력·health_revision 증가는 없다. snapshot_revision은 로컬 counter일 뿐
외부 CAS 구현이나 동시 쓰기 안전성의 증거가 아니다.

health_change_id는 7절의 canonical SHA-256 정책을 따른다. version/profile fingerprint,
장비·epoch, Health revision/이전 변화 chain, 이전/다음 표시, 초기화/전이 종류와
원본 관측 또는 세대/deadline cause를 포함한다. 초기 boot root도 명시적 policy·epoch·
start에서 계산한다. reference_time·random/현재 시각은 ID 재료가 아니다. 같은 만료를
늦게 평가해도 변화 ID는 같지만 last_evaluated_at이 다른 snapshot 자체는 다를 수 있다.
정책 fingerprint는 timeout 정수 μs·정렬된 allowlist·적합성/fault 지원 규칙 및
binding/profile/입력 계약 revision까지 포함한다.

### 26개 설계 시나리오의 구현 대응

`H`는 tests/unit/phase4/test_health.py, `T`는 test_health_timer.py의 함수다.
아래의 단위 검증은 실제 scheduler/영속 복구/분산 sink 검증과 구분한다.

| 설계 번호 | 실제 테스트 함수 | 현재 범위 |
|---|---|---|
| 1 | H.test_first_observation_initializes_without_touching_original | 로컬 통과 |
| 2, 3, 3b, 4 | T.test_deadline_boundary_and_sensor_silence | 등호/직전/직후·무입력 tick 로컬 통과 |
| 4b | T.test_boot_silence_does_not_invent_prior_healthy_state | 초기 무관측 로컬 통과 |
| 5 | H.test_new_event_after_stale_recovers_and_preserves_operational_objects | 신규 관측 복구 로컬 통과 |
| 6 | H.test_duplicate_precedes_time_guard_and_never_extends_timeout | 중복 연장 방지 로컬 통과 |
| 7, 14 | H.test_old_or_equal_event_time_cannot_rollback | 역순/동시각 로컬 통과 |
| 8 | H.test_conflict_preserves_canonical_before_time_and_semantic_checks | 충돌 방어 로컬 통과 |
| 9 | T.test_delayed_tick_identity_and_repeated_tick_have_no_duplicate_change | 만료 ID/반복 출력 억제 로컬 통과 |
| 10 | H.test_json_snapshot_restore_and_same_input_give_same_result | 모든 필드 JSON 테스트 재구성 로컬 통과 |
| 11a, 11b | T.test_same_time_race_preserves_explicit_order | 같은 시각 양쪽 호출 순서 로컬 통과 |
| 11c, 11d | H.test_delayed_observation_recovers_only_inside_freshness_window | 지연 관측 fresh/age=T 경계 로컬 통과 |
| 12 | H.test_valid_rpm_value_never_implies_fault / test_unsupported_input_cannot_create_fault | fault 비추론/미지원 입력 거부 로컬 통과 |
| 13 | H.test_policy_or_dependency_mismatch_rejects_observation_and_tick | profile·정책·binding/dependency 불일치 로컬 통과 |
| 15 | H.test_bad_sample_cannot_preempt_valid_sample_or_extend_deadline | 부적합/정상 혼재 로컬 통과 |
| 16 | H.test_future_retry_and_before_activation | 미래 재시도/활성화 전 사건 로컬 통과 |
| 17 | T.test_reference_reversal_for_observation_and_current_tick | 평가 시각 역행 거부 로컬 통과 |
| 18, 18b | T.test_old_generation_then_current_deadline_tick | 옛 token 거부 후 새 deadline 판정 로컬 통과 |
| 19 | T.test_recovery_rebuilds_current_token_without_resetting_epoch | 로컬 모델 재적재/overdue 평가만 통과 |
| 20 | 실행 보류 | fault 보고·code/source·latch/reset·다중 고장 계약과 후속 fault 코어 담당 |

25개 시나리오의 로컬 코어 동작을 테스트했고 20번은 보류다. 10/19번은 테스트에서
모든 dataclass 필드를 JSON으로 내보내 재구성한 것이며 production storage codec,
원자적 snapshot/원장 재적재 또는 실제 checkpoint 재시작 증거가 아니다.
추가 recorded sequence replay는 매 단계 snapshot을 재구성해 두 번 실행하고 전체
snapshot/result·세 개의 초기화/STALE/복구 ID chain을 비교한다.

### 확정 계약과 구현 범위의 차이·미검증

판정 순서·deadline·age·초기화·세대 무효화·ID 규칙은 변경하지 않았다. 기존
HealthStatus.FAULT enum 값은 유지하지만 fault_supported=True인 정책과 비어 있지 않은
active_faults/FAULT snapshot의 복원은 거부한다. 미정인 fault 계약을 임의 구현하여
수치 관측으로 고장을 해제하는 것을 막는 제한이다. fault 입력은 UNSUPPORTED_INPUT,
실제 다중 fault lifecycle은 미구현이다.

원장은 무제한 immutable tuple이다. 승인 source의 실제 권한 확인·등록 파일 로더,
다중 필수 metric, 원장 TTL/저장·정책 migration, 실제 timer/scheduler, 직렬화/CAS,
Registry/Job 4 연결, History·Kafka·DynamoDB sink, wire envelope·outbox/ACK는 미구현이다.
따라서 로컬 결정성만 검증했고 분산 exactly-once나 운영 복구를 보장하지 않는다.
신규 Health 테스트 98개, Phase 4 393개, 전체 로컬 회귀 1107개가 통과했다.
명령·실행 시간·경고와 변경 범위는 검증 보고서의 Health Core 절에 기록한다.


## 11. Health Snapshot JSON Codec — 2026-10-21

검토 HEAD `721fb4502965d441ebc7aecb3a73d85398469964`. 기존 Health Core commit이
반영된 clean worktree에서 시작했다. 동등한 저장 Codec은 없었고 기존 test_health.py의
reload_snapshot은 테스트 전용 helper였다. 이를 운영 표현으로 승격하지 않고 별도
명시적 schema를 `streaming/state/health_codec.py`에 정의했다. 기존 Core와 테스트
helper·Operational FSM·Qualifier·Registry는 수정하지 않았다.

### 인터페이스와 적용 범위

- `encode_health_snapshot(snapshot) -> bytes`: canonical UTF-8 JSON bytes.
- `decode_health_snapshot(payload: bytes, *, policy: HealthPolicy) -> HealthSnapshot`:
  caller 정책과 schema·무결성·Core 불변식을 검증한 새 snapshot.
- `HealthCodecError(ValueError)`: `.code`, `.field`로 거부 사유/경로를 제공한다.
  bytes가 아닌 payload 등 잘못된 API 사용은 TypeError다.

함수는 파일·시계·외부 서비스를 사용하지 않는다. encode 결과만 보관하며 decode는
현재 wall-clock으로 재평가하거나 timer를 예약하지 않는다. deadline이 지난 상태의
복원도 저장된 last_evaluated_at을 그대로 보존한다. 현재성 재평가는 caller가 원래
reduce_health에 명시적 tick/reference_time을 전달해야 한다.

### 명시적 JSON schema와 version

JSON root는 아래 **9개 필드만** 가진다. 모든 객체의 키는 필수이며 알 수 없는 키도
거부한다. null은 명시적으로 nullable인 필드에만 허용한다. Python 클래스 이름,
__dict__, dataclasses.asdict 결과나 pickle에 wire 표현을 의존하지 않는다.

| root 필드 | JSON 표현/검증 |
|---|---|
| codec_version | 문자열 `"1"`, 이 JSON 표현의 독립 버전 |
| snapshot_schema_version | 문자열 `"1"`, 기존 Core snapshot 표현 버전 |
| health_contract_version | 문자열 `"health-freshness-v1"`, 기존 판정 계약 버전 |
| equipment | store_id, equipment_id, equipment_type 문자열; 주입 정책과 exact-match |
| health_profile | equipment_type, profile_id, version 문자열; 주입 정책과 exact-match |
| policy_fingerprint | 기존 HealthPolicy.fingerprint, 재생성 값과 일치 |
| policy | 아래 명시 정책 객체; 주입 정책의 canonical 표현과 타입까지 동일 |
| snapshot | 아래 전체 snapshot 상태 객체 |
| content_sha256 | 이 필드를 제외한 root의 canonical JSON bytes에 대한 SHA-256 hex |

policy 객체의 필수 키는 binding_revision, input_contract_revision,
operational_profile(위와 같은 profile 3필드), input_kind, freshness_timeout_us,
metric_name, unit, schema_versions, sources, fault_supported다. timeout은 float 초가
아닌 **정수 μs**이고 allowlist는 Core에서 정렬한 문자열 배열이다. fault_supported는
현재 false만 가능하다. payload의 정책을 임의로 활성화하지 않고 caller가 주입한
정책의 표현과 비교한다. True/1, false/0, 1/1.0의 Python 동등 비교로 타입을 숨기지
않도록 canonical JSON 표현으로 정책을 비교한다.

snapshot 객체의 필수 키는 monitoring_epoch, monitoring_started_at,
freshness_deadline, last_evaluated_at, last_seen, last_accepted_event_id, health_status,
receipts, health_revision, snapshot_revision, last_health_change_id, active_faults다.
last_seen/last_accepted_event_id는 초기 null, health_status는 null/HEALTHY/STALE,
last_health_change_id는 초기 null 또는 Core가 검증하는 SHA-256 hex다.
active_faults는 빈 배열만 지원한다. FAULT/활성 고장은 명시적으로 거부한다.

receipts는 수용된 전체 원문 배열이며 각 항목은 기존 10필드인 event_id, event_time,
store_id, equipment_id, equipment_type, metric_name, metric_value, unit,
schema_version, source를 모두 보존한다. metric_value는 finite int/float만 허용하고
bool/string/null/NaN/Inf를 원장 수치로 복원하지 않는다. 원장 payload와 cursor의
identity·시간·최종 ID 일치는 기존 HealthSnapshot 생성 검증을 재사용한다.

### Canonical 표현과 타입 보존

객체 키는 정렬하고 UTF-8·ensure_ascii=False·공백 없는 고정 구분자·allow_nan=False를
사용한다. 시각은 `YYYY-MM-DDTHH:MM:SS.ffffff+00:00`의 UTC·6자리 μs 문자열이다.
encoder는 UTC로 정규화하며 decoder는 이 정확한 시각 표현만 허용한다. naive/Z/다른
offset·6자리 초과 소수부를 추측·잘라서 복원하지 않는다. 다른 offset을 가진 내부
aware datetime도 Core가 UTC로 정규화하므로 인코딩 결과는 같다.

revision과 timeout은 정수이며 JSON bool·float·문자열을 정수로 coercion하지 않는다.
metric_value의 int/float 표기와 -0.0도 보존한다. 같은 구체 snapshot은 같은 bytes를
만들지만, 숫자 원문의 int/float 표현만 다른 논리 동등 snapshot까지 같은 bytes라는
보장은 하지 않는다. 기존 Core의 numeric 동등성·중복·ID 규칙은 그대로다.

원장은 이미 strict event_time 순서인 전체 tuple을 그대로 배열로 만든다. 정렬로
원장을 복구하거나 중복을 지우지 않는다. 뒤집힌 순서·중복 ID·cursor와 불일치하는
누락은 거부한다. decoder는 JSON 공백/객체 키 표시 순서 차이는 허용한다. 이는
동일한 논리 bytes 생성 규칙과 실제 상태 평가의 결정성을 혼동하는 근거가 아니다.
중복 JSON key·비유한 JSON token·잘못된 UTF-8·잘린 JSON도 거부한다.

### 검증·거부 정책과 무결성 한계

version 불일치, 장비 identity 불일치, Health profile/version 불일치, fingerprint
불일치, 정책/Operational dependency 변경은 각각 명시적으로 거부한다. migration,
latest profile 선택, 기본 HEALTHY 초기화, 알 수 없는 필드 무시는 하지 않는다.
필드 누락·타입 오류·잘못된 timestamp/enum/revision/ledger·상태/시각 모순도 실패다.

구조/호환성/원시 타입 검사 후 content_sha256을 확인하고 기존 HealthSnapshot의
불변식으로 상태를 재구성한다. 단일 채널·fault 미지원 코어의 도달 가능한 revision
근거도 확인한다: snapshot_revision >= 수용 원장 길이, health_revision <=
2×원장 길이+1(초기 boot 만료와 관측별 HEALTHY/STALE 변화 상한).
Core 전이/ID를 바꾸지 않고 Codec 경계에서 모순을 거부하는 검사다.

Codec 거부 코드는 MISSING_FIELD, UNKNOWN_FIELD, INVALID_SCHEMA, INVALID_JSON,
CODEC_VERSION_MISMATCH, SNAPSHOT_VERSION_MISMATCH, CONTRACT_VERSION_MISMATCH,
EQUIPMENT_IDENTITY_MISMATCH, PROFILE_MISMATCH, POLICY_FINGERPRINT_MISMATCH,
POLICY_MISMATCH, INVALID_TIMESTAMP, INVALID_HEALTH, INVALID_REVISION, INVALID_LEDGER,
INCONSISTENT_SNAPSHOT, UNSUPPORTED_FAULT, INTEGRITY_MISMATCH다. 복수 오류에서는
검사 순서상 먼저 발견한 사유가 반환되며 전 오류를 한 번에 열거하지 않는다.

content_sha256은 정상 수치가 다른 정상 수치로 바뀌는 등의 **우발적 손상**을
검출한다. 인증 서명이 아니므로 공격자가 본문과 digest를 함께 다시 만들면 이를
구별할 수 없다. 원래 원장의 완전한 존재, 이전 Health ID chain의 진위, 실제 source
권한을 증명하지 않는다. 정책 fingerprint도 상태 저장의 진위를 증명하는 값이 아니다.
원장 전체와 snapshot이 일관된 논리 경계에서 저장되었다는 외부 보장은 별도다.

### 로컬 복원 검증

신규 테스트는 test_health_codec.py와 test_health_recovery.py다. 초기/HEALTHY/STALE/
복수 원장 snapshot의 **전체 필드** round-trip과 deterministic bytes를 검증한다.
120000일+1μs timeout과 2^53 초과 revision도 float 변환 없이 보존한다. Unicode,
UTC 동등 표현, int/float 수치 표현, malformed·비호환·누락·타입 오류 거부도 포함한다.

실제 pytest tmp_path 파일에 bytes를 쓰고 새 객체로 읽어, 정상 관측 이후 중복·충돌·
새 관측·옛 tick 무효·새 deadline 만료·중복 tick·역순·복구를 연속 실행과 단계별로
비교했다. 같은 시각의 timer/관측 양쪽 처리 순서도 파일 재적재를 거쳐 비교했다.
원장·revision·파생 token/fingerprint와 동일 후속 입력의 health_change_id를 확인했다.
잘린 파일·정상 RPM 한 자리 손상·잘못된 UTF-8 파일은 복원 거부했다.

신규 89개, 기존 포함 Phase 4 482개, 전체 로컬 회귀 1196개가 통과했다. 명령과
실행 결과는 검증 보고서의 Snapshot Codec 절을 따른다. 이는 **순수 Codec+로컬 파일
직렬화·복원** 증거이며 Spark checkpoint, DynamoDB 내구성, scheduler 복구, 장애 후
출력 ACK/exactly-once를 검증한 것이 아니다. 저장 포맷 migration·실제 파일 원자적
교체/fsync·외부 sink·outbox·원장 TTL은 구현하지 않았다.
