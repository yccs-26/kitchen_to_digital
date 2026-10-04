# Kafka 관측·운영 설계와 구현 로드맵

상태: 후속 설계·개발 계획. 운영 DAG·경보·성능 검증 완료를 뜻하지 않는다.

이 문서는 지표로 장애 원인을 구분하는 방법, Airflow와 Databricks의 운영 책임, Phase별 검증 목표를 설명한다.
토픽 계약은 [Kafka 설계](kafka-topics.md), 저장·처리 구조는 [플랫폼](platform.md)을 기준으로 한다.

## Observability 설계

### 관측할 지표

| 계층 | 핵심 지표 | 판단 목적 |
|---|---|---|
| Pipeline Health | Kafka lag/시간 기반 backlog, Spark input·processing rate, micro-batch duration, scheduling delay, failed batch, checkpoint/restart | 처리 지연·실패와 병목 식별 |
| Stateful processing | state row count, memory/disk size, watermark에 의해 제거된 행 | dedup·window·장비 상태의 비용 관측 |
| Data Health | received, validated, quarantine, duplicate, late, reprocess, DLQ count와 비율 | 데이터 품질과 처리 실패 구분 |
| Equipment Health | HEALTHY / STALE / FAULT 수, active alert 수 | 장비 상태와 디지털 트윈 신뢰도 확인 |
| Storage | Iceberg file count, average file size, snapshot count | 작은 파일과 유지보수 필요성 판단 |

`quarantine / received`, `duplicate / received`, `late / received`를 기본 비율 후보로 둔다. 실제 구현 시 집계 구간과 재처리 포함 여부 등 분모 정의를 명시해야 한다.

Quarantine 급증은 Producer·계약·업무 품질 문제, DLQ 급증은 처리·다운스트림·인프라 문제부터 진단한다. 둘을 하나의 실패 비율로 합치지 않는다.

### 경보 정책과 초기 구현 범위

- 일시적인 lag보다 크기·추세·지속시간을 함께 본다.
- 기본 방향은 `consumer delay > X seconds for Y minutes`. X와 Y는 아직 미정이다.
- Kafka partition 수와 watermark를 조정할 때 처리량·지연뿐 아니라 state 비용도 함께 비교한다.
- 처음에는 Kafka/AWS/Databricks 제공 지표와 구조화된 애플리케이션 지표를 수집한다. 통합 대시보드는 필요성을 확인하면서 추가한다.
- 기본 count는 Phase 2부터 수집하고, Phase 8에서 dashboard·운영 경보·장애 실험으로 통합한다.

## 장애 복구 Runbook 방향

공통 기록 형식은 **탐지 → 진단 → 영향 억제 → 복구 → 검증**이다. 아래는 전체 운영 절차 설계다. Phase 0 로컬 장애와 Phase 1 checkpoint 재시작·격리 failure-state 결과는 각각 [Phase 0](../reports/phase-0-verification.md)·[Phase 1](../reports/phase-1-verification.md)에 기록했다. Phase 1 실행 절차는 [runbook](../runbook.md)을 따른다.

| 시나리오 | 진단과 복구 | 복구 검증 |
|---|---|---|
| Consumer lag 지속 증가 | Producer rate, 처리량, partition 분포, skew, Spark batch duration, downstream 확인 후 병목 제거·용량 조정 | backlog가 정상 범위로 회복하는지 |
| Quarantine 급증 | failure_reason, producer_id, schema/version별 분석 → 원인 수정 → 대상만 Reprocess | 품질 실패율 회복, 재처리 결과와 중복 여부 |
| Spark Job 실패 | 실패 Job의 기존 checkpoint로 재시작 | 진행 재개, sink 중복·누락 및 state 정합성 |
| 처리·시스템 실패 | bounded retry + exponential backoff → 소진 시 DLQ → 원인 해결 후 Reprocess | 같은 event 재처리 시 결과 수렴 |
| Checkpoint 유실·손상 | Kafka retention 내 replay 가능성 검토, 장기 historical 복구는 Bronze Backfill | 복구 범위·시작 위치·결과 정합성 |

Spark checkpoint와 직접 Consumer commit의 차이, 정상 재시작·Replay·Backfill·State Rebuild의 경계는
[복구 설계](../operations/late-events-and-backfill.md)를 따른다.

## Airflow와 Databricks 책임

Databricks Jobs가 4개 Streaming Job의 실행·재시작 등 lifecycle을 담당한다. Airflow는 종료 가능한 Backfill·Reprocess·Maintenance workflow를 담당한다.

| DAG | 입력·실행 방식 | 핵심 처리 |
|---|---|---|
| `ktd_reprocess` | 수동/조건부, source·시간 범위·failure_type/reason·equipment_id·max_records | 실패 원인 해결 후 reprocess 토픽으로 발행, Job 2 재검증 |
| `ktd_backfill` | 수동/파라미터 기반, start/end·store/equipment·target·reason | Bronze 기반 Silver/Gold 재계산 및 검증 |
| `ktd_iceberg_maintenance` | 주기적 검사 | 파일·snapshot 상태 확인 후 필요할 때 maintenance 실행 |

Data Quality Audit은 초기에는 maintenance 또는 monitoring workflow에 포함하고, 필요하면 별도 DAG로 분리한다.

Reprocess는 `source`, 기간, 실패 유형, 장비와 `max_records`로 범위를 제한하고 원래 `event_id`를 유지한다.
validated로 직접 우회하지 않고 Job 2의 검증을 다시 통과한다. raw와 reprocess 간 자원 경쟁·실행 분리는 구현에서 검증한다.
Backfill의 `target` 후보는 SILVER / GOLD / ALL이며, 기반 Silver도 잘못됐으면 Silver부터 재계산한다.
`processing_type=BACKFILL`, `backfill_run_id`, `processed_at`으로 원본 identity와 처리 이력을 구분한다.
실시간 처리와 겹치지 않는 충분히 과거 구간을 우선 대상으로 삼으며 동시 writer·commit 충돌 정책은 검증 전이다.
상세 처리 순서·lookback·동시 쓰기 제약은 [Backfill 실행 계약](../operations/late-events-and-backfill.md#backfill-실행-계약)에 둔다.

### Maintenance와 감사 이력

주기적으로 검사하되 `needs_compaction` 조건이 참일 때만 실행한다. 작은 파일 수·평균 파일 크기 임계값과 정책은 실험 후 결정한다. Compaction·metadata 정리의 구체적인 실행 조건은 아직 구현 대상이다.

운영 감사 필드 후보:

```text
run_id, requested_by, reason
start_time, end_time
records_read, records_written, records_rejected
started_at, finished_at, status
```

개별 이벤트 처리 retry와 Airflow task retry를 구분한다. 전자는 이벤트 실패, 후자는 Backfill 같은 batch operation 실패를 대상으로 한다.

## Phase 0~9 구현 로드맵

작게 동작하는 E2E를 만든 뒤 정확성 → 장애 복구 → 부하 검증 순서로 확장한다. 아래는 계획상의 완료·검증 기준이며 실행 결과가 아니다. 작업 번호·진입 조건·상세 DoD는 로컬 개발 계획서에서 관리한다.

| Phase | 구현 범위 | 완료·검증 기준 |
|---|---|---|
| 0 — Local Environment | Docker 기반 Kafka + Schema Registry, 개발환경 준비 | `SensorMetricEvent`를 Avro로 직렬화해 raw에 발행하고 다시 역직렬화 |
| 1 — Kafka → Bronze | 냉장고 1대·temperature 1개로 Job 1 E2E | topic/partition/offset lineage 추적, 동일 event_id 재발행도 Bronze 원본에 보존 |
| 2 — Validation → Silver | Job 2 계약·domain validation, dedup, 초기 10분 watermark | 정상·중복·invalid unit/range/equipment·late 입력을 정책대로 분류, count 수집 |
| 3 — Window → Gold | Job 3, 1분 tumbling 먼저, 이후 5분 sliding | 허용 지연 이벤트 집계 반영, 닫힌 window 정책 검증, Gold 재현성·state size 측정 |
| 4 — State → DynamoDB | Job 4 화구 reference FSM, 별도 health, conditional write | 중복·out-of-order 입력으로 current twin이 과거 상태로 rollback되지 않음 |
| 5 — Alert Lifecycle | 냉장 온도 이상 ACTIVE → RESOLVED, 우선 kitchen.alerts까지 | 동일 논리 경보 식별·중복 방지와 lifecycle 검증, 외부 전달 worker는 이후 연결 가능 |
| 6 — Retry / DLQ / Reprocess | downstream 실패 주입, retry 소진, 실패 이벤트 재처리 | 같은 이벤트를 여러 번 재처리해도 최종 결과가 수렴 |
| 7 — Airflow | Reprocess → Backfill → Maintenance 순서 | 각 workflow 실행·결과 검증과 운영 감사 이력 |
| 8 — Observability / Failure Injection | dashboard·운영 경보, 부하 증가·잘못된 unit 등 장애 주입 | 장애 → metric 변화 → 원인 진단 → 복구의 증빙 |
| 9 — Performance Experiments | 보류한 설계 가설 비교 | 동일 조건의 결과와 선택 근거를 문서화 |

Phase 0에서 Spark·Airflow 개발환경을 준비할 수 있지만, 실제 Airflow 운영 DAG 구현은 Phase 7이다. AWS·Databricks 전체 연결은 Phase 0의 필수 완료 조건이 아니다.

Phase 2의 watermark 초과 이벤트는 실시간 경로에서 제외하고 Bronze 원본을 이용해 historical 복구한다. 구체적인 late 판정·집계 방식은 runtime 동작을 검증해 구현한다.

## 성능 결정

partition 수, Silver 쓰기 방식, watermark, Iceberg partition, sliding 간격, compaction 정책은
[성능 실험 계획](../experiments/performance-plan.md)에서 비교한다.
Silver 영속 멱등성은 유지해야 할 요구사항이다. append 대안은 정기 dedup 전 중복 노출과 downstream 영향까지 평가한다.
실측 전 처리량·지연·개선율을 달성 성과로 기록하지 않는다.
