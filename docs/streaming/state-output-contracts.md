# Phase 4 State History / State Changes / Current State 출력·복구 계약

작성일: 2026-10-22 (Asia/Seoul). 검토 branch: `feat/phase-4-twin-state`.
검토 HEAD: `a1effcbd13a62a98a19d98187b2680ddf6a8db8b`.
상태: **설계와 테스트 시나리오 정의만 완료**. 아래 필드·프로토콜은 후속 구현의
논리 계약이며 배포된 wire schema, DDL, Sink, Outbox 또는 Job 4가 아니다.
저장소·새 인프라의 선정/승인과 런타임 검증은 별도 게이트다.

## 1. 근거·기존 구현과의 경계

로컬 기준 실행 계획의 Phase 4 DoD는 상태 계산뿐 아니라 History/topic/current 대사,
rollback 방어와 세 Sink 부분 실패 수렴을 요구한다. 기준 계획은 원본 checkout의
`docs/plans/ktd-forward-execution-plan-2026-10-06.md`에서 읽었으며 본문을 복제하지 않았다.
현재 checkout에는 docs/plans가 없다. 상위 checkout AGENTS.md와 사용자의 PEP 8 지침을
확인했다. 구현 코드는 이번 작업에서 수정하지 않는다.

| 직접 확인한 자료 | 현재 근거와 이번 설계의 제한 |
|---|---|
| [Operational 계약](equipment-state-contracts.md), [FSM](../../streaming/state/fsm.py), [Hood](../../streaming/state/hood.py) | state_change_id와 순수 출력 의도 존재. 공통 Operational revision/Codec은 없음 |
| [Registry](../../streaming/state/registry.py), [Adapters](../../streaming/state/adapters.py), [Health Registry](../../streaming/state/health_registry.py) | 정확한 binding/profile 선택. 외부 출력/영속 ACK 없음 |
| [Health 계약](equipment-health-contracts.md), [Core](../../streaming/state/health.py), [Codec](../../streaming/state/health_codec.py), [Timer](../../streaming/state/health_timer_coordinator.py) | health_change_id, health_revision, snapshot_revision, epoch/generation 존재. Codec은 영속 저장 자체가 아님 |
| [Ordering 테스트](../../tests/unit/phase4/test_ordering.py), [Health Timer 테스트](../../tests/unit/phase4/test_health_timer.py), [Hood 테스트](../../tests/unit/phase4/test_hood.py) | 같은 명령 순서 replay, 지연 tick의 같은 ID, 후보/초기화 무출력 계약 |
| [Phase 2 Silver](../../streaming/sinks/silver.py), [Delta 구현](../../streaming/sinks/delta_silver.py), [발행](../../streaming/sinks/validated_publisher.py) | insert-only canonical 판정, Silver 중복이어도 발행 재시도. 개별 callback/offset ACK 확인 |
| [저장 설계](../data/storage-design.md), [Gold 모델](../data/gold-model.md), [Kafka 설계](../architecture/kafka-topics.md) | Current PK/SK 설계 존재. fct_state_transition은 논리 제안, 실제 History table/DDL 없음 |
| [플랫폼](../architecture/platform.md), [결정 목록](../architecture/design-decisions.md), [변경 이력](../architecture/planning-update.md), [ADR](../adr/02-adopt-kafka-over-redpanda.md) | Job 4 직접 current 저장 기본 방향, 별도 State Consumer는 대안. 분산 exactly-once 전제 없음 |
| [전달](delivery-and-idempotency.md), [복구](../operations/late-events-and-backfill.md), [현재 Avro](../data/data-contract.md) | Spark checkpoint와 consumer commit 구별. 수치 10필드는 상태 출력 schema가 아님 |

Phase 3 sink는 이 checkout에 없다. 접근 가능한 phase-3-gold-windows checkout의
`tests/unit/phase3/test_gold_sink_contract.py`는 MemoryGoldReference이며 실제 영속 Sink가
아니다. 해당 checkout의 `streaming/aggregations/phase3-streaming-design.md`는 단일 Gold
Sink와 절대 결과 멱등 쓰기를 제안한다. 이를 세 Sink 원자성 증거로 재사용하지 않는다.
Phase 2의 Delta MERGE 구현도 UC Managed Iceberg History의 MERGE 지원 증거가 아니다.

## 2. 결정·권장·보류 상태

| 항목 | 이번 논리 계약 / 권장안 | 구현 전 게이트 |
|---|---|---|
| 상태 축 | OPERATIONAL / HEALTH 독립; Alert 제외 | 기존 Core/ID 변경 없음 |
| state.changes | 단일 topic의 명시적 change_kind + tagged subtype 권장 | 새 상태 출력 schema/subject 등록·consumer 호환성 검사 |
| 변경 identity | 기존 두 ID 유지, (change_kind, 해당 native ID) | ID와 canonical payload 충돌 검사 |
| History | 불변 canonical 변경 + 동일 ID 재시도 no-op | 물리 table/format/unique 쓰기 검증 |
| Current | 기존 PK=store_id, SK=equipment_id, 축별 nested map | per-axis CAS, serializer와 동시성 검증 |
| 복구 기준 | Snapshot + 기록된 명령 + 불변 의도의 원자적 durable commit 권장 | 저장소/트랜잭션/CAS 선택은 미승인·미구현 |
| 처리 순서 | commit → History → Kafka → Current → 완료 → checkpoint 권장 | Job 4 구현 때 성능/장애 격리 검증 |
| 새 epoch | 승인된 State Rebuild/cutover만, 자동 승격 금지 | 세대 발급·writer fencing·migration 절차 미정 |

아래 MUST 수준 요구는 저장 매체와 무관한 정확성 조건이다. 물리 저장소를 결정한
것으로 해석하지 않는다. 조건을 충족하지 못하면 외부 출력 구현/운영을 보류한다.

## 3. 변경 envelope — 논리 schema state-output-v1

공통 envelope에는 `output_contract_version="state-output-v1"`, `change_kind`,
`record_kind`, store_id/equipment_id/equipment_type, 해당 축의 profile key
(equipment_type/profile_id/version), Core policy_version 또는 Health contract version,
정책 근거, `effective_at`, `source_ref`, `replay_ref`, 해당 축의 body를 둔다.

- `change_kind`: OPERATIONAL 또는 HEALTH. 정확히 한 종류의 body와 native ID만 존재.
  OPERATIONAL은 state_change_id, HEALTH는 health_change_id다. 공통 식별 인덱스는
  이 pair를 사용하며 ID 재해시·변경·다른 ID로 대체하지 않는다.
- `record_kind`: Health는 기존 INITIALIZED/TRANSITION 그대로. Operational 변경은
  TRANSITION만. Hood 최초 확정은 intents가 비어 있으므로 가짜 INITIALIZED 변경을
  만들지 않는다. 최초 Current baseline 투영은 7절의 별도 의도다.
- `effective_at`: Operational은 trigger event_time, Health 관측은 cause event_time,
  timeout은 expected_deadline. 업무 원인 시각이며 발행/적용 순서용 timestamp가 아니다.
- `source_ref`: input_kind, input_contract_revision, event_id 또는 signal_id, source,
  원본 event_time, provenance. 존재하는 항목만 명시적 nullable로 표현한다. Fryer
  synthetic은 synthetic_test_only를 유지하고 numeric event_id로 위장하지 않는다.
- `replay_ref`: 원자적 기록으로 확정한 command_ref, axis_generation, change_revision,
  previous_change_id, 입력 순서 참조. Operational의 선행 change ID/revision은 출력
  조정 계층에서 저장해야 할 신규 metadata이며 현재 FSM에 이미 있다고 가정하지 않는다.
- transport lineage: validated numeric 10필드에는 Kafka offset/ingestion time이 없다.
  런타임이 보존한 origin topic/partition/offset/ingested_at이 있을 때만 별도 nullable
  origin_ref에 최초 수용 근거로 고정한다. 모르면 null이며 event_id에서 추정하지 않는다.
  같은 event_id가 다른 offset으로 재전달된 이력은 attempt 로그에 추가한다.

### 3.1 Operational body

previous_state/current_state, transition_event_time, trigger reference, 정확한
Operational profile/version, Core policy_version을 저장한다. Hood는 ID에 사용한 전체
HoodPolicy(임계값·지속시간·공백)도 canonical 근거로 보존한다. 이 값은 운영 사양을
새로 확정하는 것이 아니다. Fryer는 기존 synthetic-reference 정책 근거를 사용한다.
기존 ID 생성 함수와 enum을 유지한다. 온도로 CLEANING/FAULT를 추론하지 않는다.

Fryer StateChangeIntent에는 공통 profile key/revision/처리 시각이 없고 Hood intent에도
공통 change counter가 없다. envelope는 binding·정책·기록된 명령 문맥에서 보완해야
하며 receipts 길이나 event_time 순위로 revision을 즉석 생성하면 안 된다.

### 3.2 Health body

기존 HealthChange의 previous/current, record_kind, health_revision,
previous_health_change_id, monitoring_epoch, policy_fingerprint, health_contract_version,
원본 cause tuple을 그대로 보존한다. 추가로 **해당 변화 직후 snapshot**에서 last_seen,
freshness_deadline, snapshot_revision_at_change, timer generation을 채운다.

- observation cause: event_id + 원본 event_time. HEALTHY 최초 수용/복구다.
- timeout cause: boot/event generation + expected_deadline. timer lineage는 store/equipment,
  Health profile, fingerprint, monitoring_epoch, generation, deadline을 포함한 token이다.
  직전 관측이 없으면 last_seen/event_id는 null이다. 가상 센서 이벤트를 만들지 않는다.
- `evaluation_time`: 해당 변화를 처음 확정한 명령의 명시적 reference_time.
  Core HealthChange 자체에는 없으므로 명령 기록에서 가져온다. 시스템 시각을 읽어
  재시도마다 채우지 않는다. HealthChange의 cause와 deadline 의미를 보존한다.
- health_revision은 표시 초기화/전이에서만 증가한다. snapshot_revision은 상태 불변
  관측/유효 tick에서도 증가할 수 있다. timer generation은 관측/boot identity이며
  정렬 가능한 revision이 아니다. 현재 FAULT는 미지원, 출력으로 새로 활성화하지 않는다.

### 3.3 결정성과 동일 ID 충돌

논리 canonical payload는 UTF-8 canonical JSON으로 정의한다. object key는 재귀적으로
정렬, array 순서는 계약 순서, timestamp는 aware UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`,
revision은 bool이 아닌 음이 아닌 integer, enum은 정확한 string, 부재는 명시적 null이다.
미정 필드를 임의 default/현재 시각으로 채우지 않는다. NaN/Infinity/중복 object key는
거부한다. 정책 수치는 해당 Core 정책의 canonical 표현을 보존하고 새 임의 반올림을
도입하지 않는다. `payload_digest`는 digest 필드 자신을 제외한 canonical bytes의
SHA-256이다. schema/codec 구현 및 숫자 표현의 golden fixture는 후속 검증 대상이다.

History canonical payload와 Kafka 논리 envelope는 동일하다. transport schema ID/framing은
별도다. wire 인코딩/subject version을 고정해 payload 또는 정확한 재전송 bytes를 durable
의도에 보존한다. 같은 논리 ID에 다르게 인코딩된 새 계약을 몰래 덮어쓰지 않는다.

**같은 ID가 같은 부가 metadata를 자동 보장하지는 않는다.** 같은 snapshot의 timeout을
D와 D+δ에 평가하면 HealthChange ID는 같아도 evaluation_time/snapshot이 다르다.
따라서 최초 commit한 command/evaluation_time과 envelope를 재사용한다. 재시도 시 최신
snapshot에서 envelope를 다시 만들지 않는다. commit 전 실패는 외부 출력이 없고, commit
경합의 패자는 승자의 저장 결과를 사용한다. 같은 ID·다른 canonical payload는 명시적
CONFLICT로 보존/격리하며 last-write-wins로 덮어쓰지 않는다.

발행 시각·retry_count·batch_id·process/run ID·broker ACK offset·오류·first_written_at은
attempt/publication ledger metadata로 분리한다. 매 재시도 다른 wall-clock을 canonical
payload에 넣지 않는다. 최초 계산 reference_time/origin은 canonical에 고정한다.

## 4. 순서·revision·epoch 계약

각 장비/축에 하나의 승인된 writer와 순차 명령 처리를 요구한다. Health와 Operational
사이에는 전역 순서를 주장하지 않는다. 한 numeric 관측이 두 축에 쓰여도 각각 평가하며
한쪽 snapshot을 다른 쪽 결과로 덮어쓰지 않는다. 두 축을 하나의 명령으로 수용할 경우
관련 snapshot/의도 모두를 한 durable 처리 단위로 확정하거나 독립 명령으로 추적한다.

| 축 | 변경 순서 | Current 투영 순서 | 기존 Core와의 관계 |
|---|---|---|---|
| Operational | axis_generation 내 change_revision 1,2,… | projection_revision: 투영할 snapshot 갱신 순서 | 둘 다 외부 durable 조정 계층 필요. state_change_id 변경 없음 |
| Health | monitoring_epoch에 묶인 health_revision | native snapshot_revision | 기존 counter를 그대로 사용, 출력 ordinal로 재계산 안 함 |

axis_generation은 승인된 control record가 발급한 단조 정수 세대다. 문자열
monitoring_epoch의 사전순/UTC/UUID 크기로 세대를 비교하지 않는다. 같은 generation에는
profile/fingerprint/monitoring_epoch가 고정된다. Operational에도 기존 profile/정책과
초기 snapshot에 연결한 외부 activation 문맥이 필요하다. 다른 activation은 자동 호환
아니다. generation 자체의 영속 발급·writer lease/fencing은 **후속 미결정 구현**이다.

revision은 event_time이나 Kafka offset이 아니다. Health의 지연 관측 복구는 STALE의
원인 deadline보다 이른 event_time일 수 있다. 동일 평가 시각 tick→관측이면 두 변화,
관측→옛 tick이면 변화 없음이라는 기존 순서를 보존한다. 역순/동시각 거부 입력을
History의 새로운 상태 변경으로 재해석하지 않는다. 처리 거부 로그는 별도 감사다.

## 5. State History 저장·멱등성

논리 자연키는 `(change_kind, native_change_id)`이며 native ID는 해당 종류의
state_change_id 또는 health_change_id다. identity/profile/정책/순서/body까지 비교한다.
다른 store/equipment에 같은 ID가 있으면 다른 정상 row가 아니라 충돌이다.
논리적으로 두 축의 immutable 변경을 하나의 tagged 모델에 저장하고 axis별 view로
조회하는 안을 권장한다. 물리적으로 하나/둘 table인지는 DDL 전 선택한다.

| 기존 canonical | 요청 | 결과 |
|---|---|---|
| 없음 | 유효 payload | INSERT, durable commit/read-back 검증 |
| 같은 key/digest/전체 payload | 재시도 | DUPLICATE_NOOP, 다른 Sink 완료 증거는 아님 |
| 같은 key/다른 payload | 변경/손상 | CONFLICT, 기존 canonical 유지·실패 추적 |
| target에 key 중복 | 어떠한 payload | INVARIANT_FAILURE, 임의 한 행 선택 금지 |

History는 idempotent insert/upsert-if-absent를 논리 요구로 한다. 기존 row를 최신 body로
UPDATE하는 mutable upsert가 아니다. UC Managed Iceberg의 runtime별 MERGE/unique 제약/
사전 lookup+append의 원자성을 가정하지 않는다. 경쟁 writer의 check-then-append는
중복을 막지 못한다. 단일 writer fencing + 검증된 쓰기 또는 durable canonical ledger
+ canonical read view가 필요하다. raw append를 택하면 물리 중복은 허용되지만 공개
canonical view에서 key별 payload 일치를 확인해 한 건으로 보이고 충돌은 실패시켜야
한다. compaction만으로 dedup했다고 간주하지 않는다.

`fct_state_transition`은 기존 Gold 논리명일 뿐 실제 table이 없다. catalog/schema/table,
format(Iceberg 유지 또는 승인된 Delta 대안), partition·commit metadata·고유성·보존은
미결정이다. 기존 Bronze UC Managed Iceberg를 바꾸지 않는다. 신규 History에 MERGE가
가능한지는 대상 런타임에서 별도 검증해야 한다. table 자체의 atomic commit도
History/Kafka/DynamoDB 또는 Snapshot 간 원자성을 뜻하지 않는다.

장비/축 이력 조회는 generation과 change_revision 및 선행 chain으로 정렬한다.
effective_at은 업무 분석용이며 processing/evaluation 순서와 다를 수 있다.
processing time은 고정 evaluation_time과 mutable sink attempt time을 구분한다.
History는 변화 근거이고 Snapshot은 후보·원장·cursor·timer를 포함한 계산 상태다.
History만으로 동일 계산 snapshot을 항상 복원할 수는 없다.

## 6. Kafka kitchen.state.changes 계약

Key는 기존 설계대로 UTF-8 equipment_id다. Registry의 전역 ID 유일성을 유지하며
payload store/equipment와 binding을 검사한다. 실제 ID namespace가 충돌한다면 발행을
보류하고 복합 key migration을 승인받아야 한다. 자동 key 변경은 하지 않는다.

권장 payload는 3절 envelope의 OPERATIONAL/HEALTH tagged union이다. numeric sensor
Avro에 필드를 붙이거나 센서 subject를 재사용하지 않는다. 별도
`kitchen.state.changes-value` subject 후보와 논리 version state-output-v1을 검토하되
물리 Avro schema/등록/compatibility mode/배포는 이번에 만들지 않는다. 기존 상태 출력
avsc/consumer가 저장소에 없다는 점과 실제 broker/Registry 미조회는 구별한다.

단일 tagged topic은 하나의 key 경로에서 두 축 변경을 소비할 수 있고 기존 topic
계획을 유지한다. Health 별도 topic은 격리 장점이 있지만 새 topic/운영/소비 계약이
필요하므로 초기 권장안에서 제외한다. 소비자는 change_kind별 enum/ID를 분기하고
알 수 없는 version/subtype을 성공으로 소비하지 않는다. 구형 Operational-only reader가
Health를 읽을 수 있다고 BACKWARD 설정만으로 가정하지 않는다. reader 선배포와
혼합 version fixture, 등록 호환성 검사가 선행돼야 한다. 현재 토픽 생성 상태는 미확인이다.

### ACK와 재전달

전용 Producer에 acks=all, idempotence 활성화와 해당 client의 ordering 호환 설정을
검증하는 방향이다. 구체 client/MSK 권한/운영 retry·timeout 값은 미정이다.
Phase 2 패턴처럼 enqueue/produce 반환 또는 flush()==0만으로 성공 처리하지 않는다.
개별 delivery callback 수신, error 없음, 올바른 topic/partition과 bool이 아닌 integer
ACK offset>=0을 확인해야 Kafka ACKED다. timeout/ACK 유실은 UNKNOWN이며 취소나 미발행
증거가 아니다. 동일 불변 의도로 재발행할 수 있고 consumer는 pair ID로 dedup한다.

동일 key는 고정 partition 구성과 순차 writer/dispatcher 아래에서 broker 기록 순서를
제공한다. idempotence는 새 프로세스의 애플리케이션 재발행 중복까지 제거하지 않는다.
구세대 worker fencing, 같은 장비의 미완료 발행 순서와 partition 변경을 관리해야 한다.
늦은 재시도/수동 replay 때문에 논리 이전 revision이 나중 offset에 나타날 수 있다.
consumer는 offset/event_time을 current 최신성으로 사용하지 않고 revision·identity를
검사한다. History에 있는 늦은 논리 변경은 topic에 같은 payload로 전달하며 consumer
projection은 최신 상태를 되감지 않는다. 거부된 옛 입력으로 새 변경을 생성하지 않는다.

Kafka ACK는 consumer 처리 성공이나 외부 History/Current commit 증거가 아니다.
Kafka transaction/EOS가 외부 두 저장소에 자동 확장된다고 주장하지 않는다.

## 7. DynamoDB Current State — 축별 최신성 보호

기존 PK=store_id, SK=equipment_id를 유지하고 `operational`, `health` nested map을
독립 갱신하는 논리안을 권장한다. 한 Item에 두 축이 있어도 whole-item Put으로
덮어쓰지 않는다. 각 map에 generation, profile/정책 근거, projection_revision,
change_revision/native change ID, projection_id/digest, current 표시, 관련 시각을 둔다.
Health에는 monitoring_epoch, last_seen, deadline, timer generation, native snapshot_revision,
last_health_change_id를 보존한다. Operational에는 state_started_at과 trigger 근거를 둔다.
latest_metrics/last_ingested_at은 실제 근거가 있을 때만 별도 갱신 계약으로 추가한다.
초기화되지 않은 Operational/Health를 임의 기본 상태로 채우지 않는다.

### 상태 변화와 Current 투영 의도의 차이

변화 없는 HEALTHY 관측도 last_seen/deadline/snapshot_revision을 갱신한다. 이 경우
health_change_id/health_revision은 유지되고 History/Kafka 변경은 **0건**이다.
Current freshness를 반영하려면 별도의 CURRENT_PROJECTION 의도를 durable commit한다.
projection_id는 domain=current-projection + store/equipment + axis + generation +
projection_revision + 해당 after-snapshot digest의 canonical SHA-256으로 설계한다.
이 ID는 state_change_id/health_change_id를 대체하지 않고 state.changes에 발행하지 않는다.
초기 Hood 확정/명시 baseline도 필요하면 이 경로로만 Current를 만들며 가짜 전이는 없다.

Health projection_revision은 snapshot_revision, Operational은 출력 조정 계층이 해당
snapshot 갱신에 부여한 counter다. state/Health 변화에는 같은 commit의 Current 의도를
붙인다. 변하지 않는 반복 입력에는 새 의도를 만들지 않는다. Health의 뒤늦은 no-op tick이
snapshot_revision을 갱신한 경우 그 최신 snapshot을 투영할 수 있지만 새 변화는 아니다.
각 투영은 delta 연산이 아닌 해당 축의 완전한 최신 map을 운반하여 revision gap을 허용한다.

### Conditional Write / CAS 논리 조건

| 기존 해당 축 | 요청 | 적용/판정 |
|---|---|---|
| 없음 | 승인 generation의 baseline/full projection | 존재 부재·승인 세대 조건으로 생성 |
| 같은 generation, 낮은 projection_revision | 높은 revision, 정책 문맥 일치 | 축 map만 조건부 교체 |
| 같은 generation/revision, 같은 projection ID/digest/body | 재시도 | ALREADY_APPLIED, 새 write/시각 갱신 불필요 |
| 같은 generation/revision, 다른 ID/digest/body | 충돌 | CONFLICT, 기존 보존 |
| 같은 generation, 더 높은 revision | 오래된 요청 | SUPERSEDED, 되감기 없음; 원래 의도는 ledger에서 대사 |
| 다른 generation/profile/epoch | 일반 처리 | INCOMPATIBLE/FENCED, timestamp 크기와 무관하게 거부 |

조건식은 해당 map의 generation/profile/fingerprint와 revision을 함께 검사하며
속성 부재 초기화도 원자적으로 처리해야 한다. 실제 DynamoDB expression은 구현 시
정의한다. 조건 실패를 모두 성공으로 취급하지 말고 consistent read/실패 반환값과
canonical ledger로 ALREADY_APPLIED/SUPERSEDED/CONFLICT/FENCED를 구분한다.
높은 revision이 기존보다 전진했어도 검증된 durable 의도인지 확인해야 한다.
ID를 `SET revision = revision + 1`로 매 재시도 생성하거나 event_time만 비교하지 않는다.

Conditional UpdateItem은 per-item/축 보호 수단이다. BatchWriteItem로 조건부 갱신을
대체하지 않는다. 두 축을 각각 갱신하면 잠시 서로 다른 처리 단계일 수 있고 원자적인
업무 결합 view가 아니다. 같은 장비의 Health STALE→복구 경합은 Core의 기록된 명령 순서와
snapshot CAS로 먼저 확정한 다음 projection_revision 순서로 반영한다. 옛 STALE 투영은
신규 복구/무변화 관측 뒤 도착해도 더 낮은 revision이라 거부한다.

Current는 조회 projection이며 FSM snapshot/원장 또는 출력 완료 ledger가 아니다.
History는 감사/복구 입력의 일부이며 History의 상태 변경만 replay하면 무변화 관측의
freshness/후보/timer를 복원할 수 없다. 완전 복원에는 일관된 snapshot + 이후 기록된
관측/tick·평가 시각·처리 순서·정책과 projection intents가 필요하다. 일반 backfill은
Current를 쓰지 않는다. checkpoint 유실/State Rebuild는 승인된 입력 범위·epoch cutover로
기존 writer를 fence한 뒤 별도 baseline을 적용한다. 과거 변경을 새 generation의 동일 ID·
다른 envelope로 다시 발행하지 않고 기존 canonical 변경을 참조한다.

## 8. 부분 실패 복구 프로토콜 — 권장안, 저장소 미선정

### 8.1 대안 비교

| 대안 | 장점 | 필요한 전제/한계 | 권장 상태 |
|---|---|---|---|
| Snapshot과 output intents의 Transactional Outbox | 상태 확정과 전달 의도 누락 방지 | 동일 트랜잭션 저장소 필요, Spark state와 외부 outbox의 원자성 미확인 | 요구를 만족하는 형태 중 하나, 새 인프라 미승인 |
| Durable Publication Ledger | canonical payload·명령·snapshot link·Sink별 진행 대사 | snapshot/명령 확정과 원자적 연결, CAS/fencing/스캔 복구 필요 | 논리 권장안; 저장소/스키마 후속 결정 |
| History-first + 재시도 | 기존 History 활용, 쓰기 경계 단순화 | 변화 없는 관측/소비 cursor·pending 의도 없음; History 후 checkpoint 유실 시 결과 재생성 실패 가능 | History가 충분한 durable log로 확장되지 않으면 단독 채택 불가 |
| 순차 쓰기 + checkpoint만 | 구성 단순 | 갱신 snapshot에서 replay가 DUPLICATE/빈 변화면 누락 출력 복구 불가 | 최종 복구 계약으로 불충분 |
| Kafka-first + 별도 Consumer | Kafka 로그에서 다른 projection을 재구성 | Snapshot+Kafka 원자성, consumer lifecycle 및 추가 의존. 별도 아키텍처 결정 | 대안 유지, 직접 Current 저장 기본안을 변경하지 않음 |

권장 Ledger와 Outbox는 배타적 제품명이 아니다. 동일 durable 상태 저장소에서 의도를
원자적으로 기록하는 Ledger는 Outbox 역할을 할 수 있다. 현재 Spark state와 외부 ledger
사이에 원자적 트랜잭션이 있다고 가정하지 않는다. 가능한 저장 매체/단일 writer/commit
프로토콜을 검증하기 전에는 이 권장안의 누락 방지 보장을 구현 완료로 표현할 수 없다.

### 8.2 필요한 durable 처리 단위 L

L에는 장비·축/정책 문맥, 승인 generation/fencing token, 안정적 command_ref,
원본 입력 및 순서, 명시 평가 시각, before/after snapshot 참조·digest·revision,
Core 판정, 변화 envelope와 Current projection 의도, 각 의도의 digest를 저장한다.
명령이 snapshot만 갱신하고 변화가 없어도 이 기록은 필요하다. 중복 입력 재수신은
기존 수용 원장/명령 기록과 대조하고 이미 처리한 결정을 재사용한다.

command_ref는 입력 source identity + 기록한 명령 순서를 식별하며 Spark batch_id만을
사용하지 않는다. timer token은 논리 만료 근거이며 모든 tick 평가 호출의 고유 ID가
아니다. 같은 token의 서로 다른 시각 no-op 평가에는 별도 명령 순서가 필요하다.
같은 시각 관측/tick 순서도 영속 기록한다. 입력 수용·명령 번호 발급·snapshot CAS의
동시성 프로토콜과 Operational snapshot Codec은 후속 필수 구현이다.

L은 after snapshot, 소비/판정 원장, canonical 의도 모두 확정되거나 모두 미확정이어야
한다. Snapshot만 저장하고 의도 저장에 실패하는 상태를 정상 commit으로 허용하지 않는다.
큰 원장/외부 blob을 쓴다면 모든 불변 blob의 durable 확인 후 하나의 원자적 root pointer로
공개하는 등 동등한 보장이 필요하며 구체 방식을 임의 확정하지 않는다.

각 Sink 상태는 PENDING / ACKED / UNKNOWN / RETRYABLE_FAILURE / CONFLICT 등으로 추적한다.
Current에는 ALREADY_APPLIED/SUPERSEDED terminal 결과도 기록한다. attempt 번호·오류 분류·
다음 재시도·broker receipt/History commit 근거/Current read-back은 mutable 전달 원장이다.
외부 ACK와 이 ACK 기록은 원자적이지 않으므로 UNKNOWN을 허용해야 한다.

### 8.3 처리·재시작 순서

1. 명령·평가 시각과 현재 snapshot을 선택한다. 이미 확정한 명령이면 L의 결과를
   사용하고 Core의 빈 replay 출력에서 기존 pending 의도를 잃지 않는다.
2. 기존 Core로 계산한 뒤 before revision CAS + 승인 writer fencing으로 L을 원자 확정한다.
   CAS 패자는 재읽기/재계산하며 외부 Sink에 아직 발행하지 않는다.
3. canonical 의도를 History → Kafka → Current 순서로 시도하고 Sink별 결과를 저장한다.
   History 중복이어도 Kafka/Current가 완료됐는지 별도 확인한다. Conflict는 실패 유지다.
4. 초기 권장안은 해당 batch의 모든 의도가 terminal 성공/승인된 superseded가 된 뒤
   batch를 성공 처리하여 Spark checkpoint 진행을 허용한다. 오류를 숨겨 skip하지 않는다.
   장비별 blocking·backoff 최대치/실패 격리는 후속 설계다. 자동 DLQ로 성공 대체하지 않는다.
5. 재시작 시 checkpoint replay뿐 아니라 L의 pending/unknown 항목을 스캔한다. 이미 진행한
   snapshot에서 Core가 변화 없이 끝나도 저장한 의도는 재전달한다. 오래된 batch의 snapshot을
   최신 root에 덮어쓰지 않고 L/CAS와 checkpoint의 처리 위치를 대사한다.

async dispatcher 채택 시 L commit 후 checkpoint 성공을 허용하는 다른 경계도 가능하다.
그러나 durable 스캔/소유권/재시도 lifecycle을 별도 검증·승인하기 전에는 적용하지 않는다.
이번에 thread/worker/Job 또는 background 동작을 구현하지 않았다.

### 8.4 Sink별 재시도·대사와 수렴 조건

History 담당은 canonical INSERT/NOOP/CONFLICT와 commit 근거를, Kafka 담당은 개별 ACK와
전송 receipt를, Current 담당은 per-axis conditional 결과를 기록한다. 공통 조정자는
L의 실패 항목을 재시도하고 모든 출력의 종결을 판정한다. 요청 timeout은 성공/미발행
단정 근거가 아니며 UNKNOWN에서 재조회/동일 payload 재시도로 회복한다.

완료 조건은 모든 canonical 변경이 History에서 유일하게 보이고 Kafka에 적어도 한 번
ACK되며 Current가 해당 투영 또는 검증된 더 최신 투영을 유지하는 것이다. 각 Sink의
누락 ID/digest/revision 대사와 pending age·unknown count·conditional conflict·revision gap·
writer fence 거부를 관측한다. Kafka retention 후 소비 여부/이력은 별도 보존 계약이
필요하며 broker ACK만으로 모든 consumer 완료를 알 수 없다. 재시도 횟수·retention·
원장 TTL을 임의 수치로 정하지 않는다. 미완료 항목은 삭제하지 않는다.

## 9. 정상·장애 테스트 시나리오 — 전부 미실행 설계

H=canonical History, K=Kafka, C=해당 축 Current, L=8절 durable 처리 단위.
O1/O2는 기록된 정상 Operational 변화, U1은 Health 초기 HEALTHY, S2는 timeout STALE,
R3는 신규 관측 HEALTHY 복구다. 그 숫자는 change_revision이고 C는 별도 projection_revision
p를 사용한다. L0는 입력 전 일관된 Snapshot/원장/정책을 가리킨다. H/K는 같은 변경 ID와
canonical 변경 digest를 사용한다. C는 해당 변경 ID를 참조하되 별도의 projection ID와
digest를 사용한다. 더 최신 C가 없는 한 C 결과는 해당 full projection이다.

모든 행의 기대 결과는 **L과 멱등 Sink 계약이 향후 구현된다는 조건의 설계**다.
실제 통과 수로 집계하지 않는다. 장애는 의존성 주입/고정 시각/로컬 저장 모델부터 검증한다.

| 번호·입력/초기 상태 | 실패 주입 위치 | 기대 H | 기대 K | 기대 C | 재시도·재시작 후 / ID·revision 검사 |
|---|---|---|---|---|---|
| 01 L0→O1 정상 | 없음 | O1 1건 | O1 ACK | p1 적용 | L 완료; 직접 Core native ID/정책/prev/current 대조 |
| 02 L0→U1→S2→R3 | 명시 T 직전/등호/직후 평가 | 초기화/STALE/복구 3건 | 3 subtype 순서 | 각 p 증가, 최종 HEALTHY | Health ID chain/epoch/last_seen/deadline과 직접 Core 비교 |
| 03 O1 L 확정 | H 성공 후 K 실패 | O1 유지 | 미확정/실패 | 초기 권장순서에서는 미실행 | L로 H no-op 후 K/C 완료, 동일 ID/digest |
| 04 O1 L 확정 | H·K 성공 후 C 실패 | O1 유지 | ACK 유지 | 이전 값 | C 재시도, ACK 불명 K 중복 허용; p1 동일 |
| 05 O1 L 확정 | K 성공·H 실패, 순서 위반 주입 | 없음 | O1 존재 가능 | 없음/이전 | L 기반 H 우선 복구; 조정 순서 위반 감지, ID 재생성 없음 |
| 06 O1 L 확정 | K 성공·C 실패 | 성공분 유지 | O1 ACK | 이전 값 | 04와 같은 결과, Sink별 완료 상태 대조 |
| 07 O1 L 확정 | C 성공·K 실패, 순서 위반 주입 | 없음/성공분 | 실패/UNKNOWN | p1 | L로 누락 H/K 재시도, C no-op; 기존 p1 되감지 않음 |
| 08 O1 모든 Sink 성공 | checkpoint commit 실패 | O1 유지 | 재발행 중복 가능 | p1 유지 | 같은 micro-batch L 조회, 같은 ID/digest/counter, 새 전이 0 |
| 09 O1 commit 요청 | H ACK 직후/L ACK 기록 전 종료 | O1 유지 | 미실행 | 이전 값 | H read-back/멱등 재시도, UNKNOWN 해소; H canonical 1건 |
| 10 O1 발행 요청 | K ACK 직후/L ACK 기록 전 종료 | O1 | 1건 이상 | 이전/미확정 | 동일 bytes 재발행 허용, consumer pair dedup, offset 변화가 ID 바꾸지 않음 |
| 11 O1 C 요청 | C ACK 직후/L ACK 기록 전 종료 | O1 | ACK | p1 | conditional 재시도+read-back ALREADY_APPLIED; 새 revision 없음 |
| 12 같은 micro-batch 반복 | L 확정 후 프로세스 종료 | canonical 동일 | ACK 유실분 중복 가능 | 동일/최신 | batch_id가 달라도 명령/ID/원장 근거 동일, snapshot rollback 금지 |
| 13 consumer/Job 재시작 O1 재처리 | snapshot은 이미 O1 수용 | 기존 O1 | pending만 재시도 | p1 또는 이후 | Core DUPLICATE/빈 intent에도 L의 pending 출력 복구 |
| 14 O1(p1)이 O2(p2) 뒤 도착 | C 반영 역순 | O1/O2 유지 | 역순 replay 가능 | p2 유지 | p1 SUPERSEDED 기록, History canonical O1 소실 금지 |
| 15 같은 ID·같은 payload | 임의 Sink 재전달 | NOOP | 중복 허용 | ALREADY_APPLIED | native ID/digest/전체 body 동일성 검증 |
| 16 같은 ID·다른 payload | History 또는 L canonical 충돌 | 기존 보존 | 새 payload 발행 차단 | 기존 보존 | CONFLICT 실패·추적, 자동 덮어쓰기/새 ID 발급 금지 |
| 17 같은 gen/p, 다른 projection ID/body | C CAS | 기존 canonical | 변경 없음 | CONFLICT | 동일 p 변조 검출, snapshot/의도 근거 대조 |
| 18 tick·관측 같은 평가 시각 | 입력 순서 양쪽 기록 | tick 먼저 S2/R3, 관측 먼저 변화 0 | 그 이력만 발행 | 최종 새 관측 p | 순서별 replay ID 동일, 서로 다른 순서 수렴 이력 주장 금지 |
| 19 U1 후 HEALTHY 새 관측 | 변화 없이 deadline 갱신 | 새 변경 0 | 새 변경 0 | 새로운 p/last_seen/deadline | Current-only L 의도 복원, health_revision/ID 유지 |
| 20 Hood 후보·최초 확정 | pending/initialized 구간 | 가짜 전이 0 | 가짜 전이 0 | 필요시 baseline/current-only | Operational native intent 없음 확인, 후보/원장 Snapshot 보존 |
| 21 같은 이전 Snapshot timeout | D와 D+δ에서 재계산 | 같은 ID/최초 payload | 최초 payload 재사용 | 최초 commit p | evaluation_time 차이를 새 payload로 발행 금지, L CAS 승자 대조 |
| 22 L commit 전 실패 | snapshot 또는 의도 준비 직후 | 0 | 0 | 이전 값 | 미확정 state 비공개, 같은 명령 재계산; partial commit 불가 주입 |
| 23 L commit 후 첫 Sink 전 종료 | dispatch 시작 전 | 0 | 0 | 이전 값 | pending 스캔으로 세 Sink 수렴, snapshot의 빈 replay 의존 금지 |
| 24 두 writer 같은 before revision | L CAS/fencing 경합 | 승자 변경만 | 승자만 | 승자 p | 패자 재읽기, counter 중복/임의 event_time tie-break 금지 |
| 25 다른 store/장비 token·입력 | Registry/Core 입구 | 새 변경 0 | 새 변경 0 | 다른 장비 불변 | identity와 timer generation 검사, L 거부 감사 |
| 26 profile/epoch/세대 불일치 | C 또는 Snapshot restore | 기존 보존 | 새 잘못된 출력 0 | FENCED/INCOMPATIBLE | 자동 migration/새 세대 승격 금지 |
| 27 오래된 새 input/중복/동시각 | Core 판정 | 새 변화 0 | 새 변화 0 | 갱신 없음 | 기존 Ordering/Dedup decision·전체 Snapshot 보존 대조 |
| 28 History 중복 physical append/충돌 | ACK 유실/동시 쓰기 모형 | canonical view 1건 또는 충돌 실패 | canonical만 | canonical만 | compaction 아닌 key/body 비교, target 다중 row 탐지 |
| 29 checkpoint 유실/State Rebuild | 과거 전이 재계산 | 기존 변경 재사용 | 무단 재발행 없음 | 승인 cutover만 | native ID→기존 payload/ordinal 참조, generation fencing 검사 |
| 30 snapshot/원장/의도 손상·누락 | 복원/대사 | 기존 보존 | 발행 보류 | 기존 보존 | Codec/digest/chain 실패, 조용한 HEALTHY 초기화 금지 |
| 31 다른 축 Current 병행 갱신 | nested map CAS 경합 | 각 축 이력 | 각 subtype | 둘 다 보존 | whole-item overwrite 없음, 축별 revision 비교 |
| 32 무지원 FAULT/Alert 입력 | policy/입력 검사 | 출력 없음 | 출력 없음 | 임의 FAULT 없음 | 기존 미지원 거부와 동일, 센서값 추론 금지 |

추가 구현 검증에는 schema 혼합 version reader, JSON/Avro golden round-trip, 잘못된 ACK
(offset=-1/bool/None, callback 없음, flush만 성공), partition 변경/옛 worker 재전송,
원장/History 보존 만료와 재처리 범위를 포함한다. 이번에 executable fixture/test를
추가하거나 이 시나리오를 통과했다고 표시하지 않았다.

## 10. 충돌·호환성 및 미결정 게이트

- 기존 state_change_id/health_change_id 알고리즘·enum·Registry/Core/Codec/Timer API는
  유지한다. 출력 schema는 새 논리 계약이며 기존 sensor Avro의 변경이 아니다.
- 기존 저장 설계의 단일 state_version/time 비교와 동일 시각 미결정을 이 문서에서
  축별 generation/revision/CAS로 구체화했다. nested map은 논리 권장안이고 기존 실제
  DynamoDB table/item과의 migration을 검증한 것이 아니다. 실제 table 존재는 미조회다.
- 기존 fct_state_transition의 state_change_id 단독 논리 필드는 Health를 표현하지 않는다.
  새 tagged 모델/별도 view를 권장하며 실제 DDL/reader migration을 승인해야 한다.
- 기존 문서의 Job 4 직접 저장 방향을 유지한다. 별도 Consumer/새 topic/스토리지 변경은
  권장안 승인과 runtime 검증 없이 채택하지 않는다. Alert lifecycle은 Phase 5다.

| 미결정 사항 | 선택지·권장과 근거 | 닫아야 할 시점 |
|---|---|---|
| durable 저장소/원자적 단위 | 기존 지원 transactional store 활용 vs 새 store; Snapshot+L 함께 확정 가능한 안 우선 | 외부 출력 구현 전, 인프라 사용자 승인 별도 |
| History 물리 모델 | UC Iceberg canonical append/view vs 검증된 insert-if-absent 대안; 지원 runtime로 선택 | Writer 구현 전 |
| Operational 영속 모델 | snapshot Codec·명령 원장·외부 revision/epoch mapping 추가 필요 | Ledger/Current 구현 전 |
| fencing/세대/cutover | 승인 세대 + 단일 writer + CAS 권장; 자동 wall-clock/UUID 최신성 금지 | 동시 실행/State Rebuild 전 |
| wire schema/consumer | tagged union 권장, 새 상태 subject와 호환성 fixture 필요 | state.changes 발행 전 |
| timeout/source 권한 | 기존 명시 policy 유지, 운영 숫자/새 fault 신호 없음 | 실제 장비 runtime 연결 전 |
| retention/TTL/재시도 예산 | 입력 replay 범위와 미완료 의도/consumer lag를 함께 검토 | 운영 전 |
| conflict 격리·스캔 복구 | durable 오류 근거와 pending 탐색/소유권/대사 구현 | 장애 테스트 전 |

공식 문서도 API 일반 지원일 뿐 현재 KTD 환경의 실측이 아니다. 2026-10-22 조회:

- [Databricks Iceberg](https://docs.databricks.com/aws/en/iceberg/): managed/foreign 구분과
  runtime 제약을 확인했다. 이 문서만으로 KTD History의 MERGE/다중 table 원자성을
  승인하거나 검증했다고 하지 않는다.
- [DynamoDB UpdateItem](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html),
  [Conditional writes](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html):
  조건부 갱신과 응답/조건 실패 처리를 검토했다. 실제 expression/동시 writer는 미검증이다.
- [Kafka Producer 설정](https://kafka.apache.org/41/generated/producer_config.html):
  idempotence/ACK/ordering의 일반 조건을 검토했다. 실제 client/MSK 설정은 미확인이다.

이번 검증은 문서 링크/표·계약 대조·변경 범위·git diff --check다. 신규 Sink/장애 테스트는
실행하지 않았다. AWS/Databricks/Confluent 실행, 유료 자원 변경, Git staging/publication,
FSM/Registry/Health 구현 변경을 하지 않았다. 전체 Phase 4 DoD 완료를 주장하지 않는다.

## 11. 승인된 로컬 구현 배치 — 2026-10-22

1~10절의 설계 내용은 보존한다. 이후 구현 현황은 이 절과 로컬 복구 보고서를 기준으로
구별하며, 운영 인프라·wire Avro/Schema Registry·Spark 원자성 승인을 뜻하지 않는다.
Task A는 기존 envelope/Current-only 계약을 사용한다. Task B는 canonical JSON bytes를
유일한 불변 표현으로 만들고, Task C는 SQLite transaction으로 장비/축별 opaque Snapshot
bytes·명령·출력 의도를 함께 확정하는 로컬 모델을 선택한다. Task D는 독립 SQLite 파일을
사용하는 세 Fake Sink, Task E는 명시적 실패 주입과 파일 reopen/프로세스 종료 검증이다.

Append-only ledger는 replay가 단순하지만 Snapshot CAS와 ACK 인덱스 재구성이 필요하다.
History-only는 무변화 관측과 pending 의도를 복구하지 못한다. SQLite transactional
ledger/outbox는 표준 라이브러리로 원자적 root/의도 저장을 검증할 수 있어 로컬 모델로
선택했다. 운영 저장소·writer fencing·Spark checkpoint 연결은 여전히 미선정/미검증이다.

### 11.1 Task A~E 구현 완료 범위

2026-10-22 로컬 구현은 [상세 복구 보고서](../reports/phase-4-local-recovery.md)를 따른다.
Task A~E를 연속 수행했고 기존 FSM/Registry/Health/Codec/Timer는 수정하지 않았다.
1~10절의 운영/분산 설계 요구 전체를 달성한 것이 아니라 다음 경계를 실행 가능하게 했다.

| 계층 | 실제 API/표현 | 구현 범위 |
|---|---|---|
| Envelope | operational_output / health_output, ChangeEnvelope.payload | 순수 typed 결과 변환, canonical JSON bytes, 기존 native ID 보존 |
| Current 의도 | StateOutput(change 또는 None, projection) | 무변화 freshness·후보/초기화의 Current-only 투영 분리 |
| 명령/상태 | PreparedCommand, PublicationStore.initialize/state/commit | 한 장비/축 Snapshot bytes + 입력/평가 시각/판정 + 출력 + pending을 SQLite transaction으로 확정 |
| 재시도 | PublicationCoordinator.dispatch/reconcile | Sink별 durable attempt/ACK/UNKNOWN/CONFLICT, 한 번의 동기 스캔 |
| Fake | LocalHistory / LocalKafka / LocalCurrent | 별도 파일의 독립 transaction; 실제 외부 서비스 아님 |

논리 change_kind는 로컬 JSON의 `axis` discriminator로 표현한다. OPERATIONAL envelope는
`state_change_id`, HEALTH envelope는 `health_change_id`만 갖는다. ID 값을 다른 공통 ID로
해시하거나 합치지 않는다. native 종류/ID pair는 논리 인덱스다. schema version은
`state-output-v1`, Current 전용 표현은 `current-projection-v1`이다.
replay_ref의 command_ref/generation/change_revision/previous_change_id는 로컬 schema에서
root 필드로 펼쳐 저장한다. Health의 monitoring_epoch/fingerprint/의존 revision은 policy,
cause는 body.reason, timer provenance는 source_ref/body.timer_token에 보존한다.
이는 신규 로컬 JSON 표현이고 Kafka Avro schema/subject 등록·호환성 검증이 아니다.

canonical payload는 재귀 key 정렬/UTF-8/UTC μs Z/명시적 null, 타입 구분과 SHA-256 손상
검사를 사용한다. decoded dict는 매번 새로 생성되어 원본 bytes를 변경하지 않는다.
동일 논리 ID라도 기록된 evaluation_time이 다르면 payload 충돌이며 최초 commit 결과만
재사용한다. 새 metadata로 덮어쓰거나 ID를 바꿔 충돌을 회피하지 않는다.
Fryer synthetic 신호는 원래 provenance/signal_id를 유지한다. 실제 입력 계약 revision이
없는 경우 null이며 Health는 policy의 input_contract_revision을 사용한다. 원천 Kafka
메타데이터를 모르면 origin_ref=null이고 임의 offset/도착 시각을 만들지 않는다.

### 11.2 로컬 원자성·복구의 정확한 의미

PublicationStore는 명시적으로 initialize한 축/generation에만 쓴다. before Snapshot
SHA-256과 root revision을 BEGIN IMMEDIATE transaction에서 검사한다. Snapshot, 명령,
출력 의도, Sink별 pending, cursor/revision을 함께 commit한다. 다른 payload의 같은
command/변경 ID, 정책 교체·generation 불일치·변화 revision gap은 거부한다.
Snapshot bytes가 바뀌는데 출력 의도가 없으면 commit을 거부한다. rollback/process exit
테스트는 Snapshot만 확정되거나 의도만 남는 partial commit이 없음을 로컬에서 확인했다.

동일 명령의 정확한 재시도는 기존 출력 ID를 반환하고 root를 다시 진행시키지 않는다.
외부 side effect 이전 attempt를 UNKNOWN으로 commit하며, ACK 직후 종료라면 reopen 후
동일 의도를 재시도한다. 재시도 횟수/결과/통제된 오류 근거/ACK receipt는 별도로 남는다.
늦은 이전 attempt의 ACK가 재획득한 새 attempt를 완료하지 않도록 번호를 검사한다.
Conflict는 pending에 남고 자동 재발행하지 않는다. 기본 순서는 History→Kafka→Current이며
다른 순서는 요구된 부분 실패 조합을 재현하기 위한 명시적 테스트 override만 제공한다.

History는 동일 ID/payload no-op, revision/선행 chain/generation/policy 충돌 검사를 한다.
Kafka Fake는 동일 bytes를 다시 append하므로 실제 중복 전달 가능성을 숨기지 않는다.
개별 ACK receipt의 callback/오류/topic/key/partition/non-bool offset/digest를 검증한다.
Current Fake는 축별 map의 generation/context/revision을 독립 비교하고 같으면 no-op,
낮으면 SUPERSEDED, 같은 revision/다른 payload면 CONFLICT다. SUPERSEDED를 완료로 인정하려면
반환된 더 최신 canonical projection과 정확히 같은 의도가 이 Ledger에 있어야 한다.
Current만 높아졌다는 근거로 미확정 변경의 완료를 선언하지 않는다.

### 11.3 부분 보장·운영 이관 제약

원자성은 **이 SQLite 파일 안에서 commit한 명시적 bytes/의도**에 한정된다. Core는 순수
함수이므로 caller가 commit 전 계산 결과를 외부 상태로 따로 확정하면 이 보장을 우회한다.
SQLite와 Fake 파일들 사이에도 분산 transaction은 없다. Snapshot+Outbox를 production
Spark state/checkpoint와 원자 확정한 것이 아니다. 단일 로컬 dispatcher를 전제하며
운영 lease/fencing/자동 세대 발급·원장 TTL/비용·scalable index 설계는 후속이다.

Health Snapshot은 기존 Codec으로 실제 typed 복원과 timer/ID를 검증했다. Operational은
모든 dataclass 필드를 담는 opaque JSON image를 보존하지만 typed decode/migration API는
새로 구현하지 않았다. bytes 저장을 운영 Operational checkpoint 복구로 표현하지 않는다.
한 commit은 한 축이며 두 축의 원자적 업무 view는 제공하지 않는다. reconcile은 오류가
나면 해당 pass를 중단하고 기록을 보존한다. 독립 축 dispatch는 가능하지만 스케줄링/
실패 격리 worker·무한 retry·background 실행은 없다.

현재 실제 보장은 로컬 파일 재시작/프로세스 종료·멱등 재시도 모델이다. 실제 Kafka ACK,
DynamoDB CAS, Iceberg unique 쓰기, Avro wire, 소비자 처리, MSK 권한, Spark Job 4 및
checkpoint/E2E는 미구현·미검증이며 Phase 4 전체 DoD 완료가 아니다.

추가 범위: 로컬 Current의 Operational map은 state/last_event_time/Snapshot digest를
보존한다. 공통 state_started_at·latest_metrics·last_ingested_at의 완전한 투영은
미구현이며 cursor에서 상태 시작 시각을 임의 추정해 채우지 않는다.

## 12. Local Integration Readiness — 2026-10-23

최신 로컬 연결은 [Application 보고서](../reports/phase-4-local-application.md)와
[Job 4 운영 통합 계획](state-job4-integration-plan.md)을 따른다. 11절의 기존 코드·설계와
미커밋 내용은 보존하고 새 모듈로 다음을 추가했다.

- operational_codec.py: operational-snapshot-v1, binding/profile/engine/subtype/정책과
  candidate/cursor/receipts의 명시 schema·digest·typed 복원. Health bytes 및 기존 opaque
  image는 구분하여 거부하고 자동 migration하지 않는다.
- change_wire.py + StateChangeEvent Avro 초안: state-change-avro-v1의 nullable subtype별
  ID, identity/profile/state/UTC/Health metadata와 canonical Envelope bytes/digest를
  왕복한다. 기존 센서 Avro는 변경하지 않았다. 설치된 fastavro/avro로 offline 호환성만
  검증했고 Registry 등록/배포는 하지 않았다.
- application.py: validated numeric 10필드와 기존 Registry/Core/Envelope/Ledger를 연결한다.
  numeric Fryer는 Health만, 명시 synthetic 신호는 별도 Operational 경로다. Hood는
  두 축을 독립 평가한다. 입력 shape·UTC·double 정규화와 기존 admission 검사를 유지한다.
- application_timer.py: 기존 timer 계획/평가 결과를 같은 Snapshot+의도 transaction에
  저장한다. 평가 시각/명령 순서는 caller가 명시하며 자동 Scheduler는 없다.

Application은 모든 계산을 먼저 준비하고 축별 CAS transaction을 실행한다. 첫 축 저장 후
종료되면 같은 기록 명령으로 그 축의 cached 결과를 재사용하고 남은 축을 완료한다.
cross-axis 원자성을 주장하지 않는다. Current-only와 pending retry·동일 ID/payload 정책은
기존대로다. Operational Current의 Snapshot digest만 새 Codec bytes에 맞춰 commit 전에
재봉인하며 기존 native 변경 Envelope/ID는 바꾸지 않는다. 이전 Ledger/Fake 코드는 그대로다.

새 테스트 108개(unit 89/local integration 19), Phase 4 707개, 비외부 로컬 회귀 1458개가
통과했다. 실제 자식 프로세스 종료/파일 reopen은 새 통합 테스트의 13 cases다. Avro 검증
후 Fake Kafka는 canonical JSON을 저장하며 실제 broker/producer 전달 증거가 아니다.
운영 Avro bytes/schema ID의 durable pinning, Spark state/checkpoint 원자성, 운영 Outbox,
writer fencing·다중 dispatcher·실제 Sink·비용/권한·정책 migration은 여전히 후속 게이트다.
실제 Job 4, source 권한/운영 timeout, FAULT/Alert 및 미지원 장비는 구현하지 않았다.
