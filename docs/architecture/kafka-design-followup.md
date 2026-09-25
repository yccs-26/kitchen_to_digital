# Kafka 파티션 설계 후속 정리 — 운영 설계와 구현 로드맵

상태: 후속 대화에서 정리한 설계·구현 계획. 구현 완료 및 성능 측정 결과를 의미하지 않는다.

## 1. 정리 범위와 기준점

이 문서는 [Kafka 파티션 설계 대화](https://chatgpt.com/c/6ab4e9c3-d3e4-83ee-9140-57f07acd65ba)에서 기존 디렉터리 문서화 이후 진행한 내용을 정리한다.

기존 [planning-update-2026-09-24.md](planning-update-2026-09-24.md)는 Avro 데이터 계약 제안까지 반영했다. 이번 추가 범위는 그 이후의 **Observability·장애 복구 → Airflow 운영 DAG → Phase 0~9 구현 로드맵 → 새 구현 대화 인수인계**다. 마지막에 사용자가 “새로운 프롬프트 만들어서 구현 계획대로 진행”하기로 한 시점까지 반영했다.

일부 항목은 기존 문서에도 원칙 수준으로 존재한다. 아래에서는 이후 구체화된 책임·검증 기준과 최신 인수인계 기준을 함께 기록한다. 대화에서 제시된 예시 수치와 권고는 실측 결과로 바꾸지 않는다.

## 2. 이번에 구체화된 내용

| 항목 | 후속 정리 |
|---|---|
| 관측 구조 | Pipeline Health / Data Health / Equipment Health의 3계층 |
| 운영 경보 | 시간 기반 backlog와 지속시간 중심, 임계값은 부하 테스트 후 결정 |
| Spark 복구 | Job별 독립 checkpoint로 offset과 processing state 복구 |
| 일반 Kafka Consumer | 별도 Alert Delivery Worker 등에 manual commit + at-least-once 적용 |
| 운영 책임 | Databricks Jobs는 Streaming lifecycle, Airflow는 유한한 운영 workflow |
| Airflow DAG | `ktd_reprocess`, `ktd_backfill`, `ktd_iceberg_maintenance` |
| 구현 방식 | 작은 E2E부터 완성하고 Phase별 검증 후 다음 단계로 이동 |
| 현재 다음 행동 | Phase 0의 DoD 확인 후 Kafka + Schema Registry + Avro 왕복 검증 |

## 3. Observability 설계

### 관측할 지표

| 계층 | 핵심 지표 | 판단 목적 |
|---|---|---|
| Pipeline Health | Kafka lag/시간 기반 backlog, Spark input·processing rate, micro-batch duration, scheduling delay, failed batch, checkpoint/restart | 처리 지연·실패와 병목 식별 |
| Stateful processing | state row count, memory/disk size, watermark에 의해 제거된 행 | dedup·window·장비 상태의 비용 관측 |
| Data Health | received, validated, quarantine, duplicate, late, reprocess, DLQ count와 비율 | 데이터 품질과 처리 실패 구분 |
| Equipment Health | HEALTHY / STALE / FAULT 수, active alert 수 | 장비 상태와 디지털 트윈 신뢰도 확인 |
| Storage | Iceberg file count, average file size, snapshot count | 작은 파일과 유지보수 필요성 판단 |

`quarantine / received`, `duplicate / received`, `late / received`를 기본 비율로 제안했다. 실제 구현 시 집계 구간과 재처리 포함 여부 등 분모 정의를 명시해야 한다.

Quarantine 급증은 Producer·계약·업무 품질 문제, DLQ 급증은 처리·다운스트림·인프라 문제부터 진단한다. 둘을 하나의 실패 비율로 합치지 않는다.

### 경보 정책과 초기 구현 범위

- 일시적인 lag보다 크기·추세·지속시간을 함께 본다.
- 기본 방향은 `consumer delay > X seconds for Y minutes`. X와 Y는 아직 미정이다.
- Kafka partition 수와 watermark를 조정할 때 처리량·지연뿐 아니라 state 비용도 함께 비교한다.
- 처음에는 Kafka/AWS/Databricks 제공 지표와 구조화된 애플리케이션 지표를 수집한다. 통합 대시보드는 필요성을 확인하면서 추가한다.
- 기본 count는 Phase 2부터 수집하고, Phase 8에서 dashboard·운영 경보·장애 실험으로 통합한다.

## 4. 장애 복구 Runbook 방향

공통 기록 형식은 **탐지 → 진단 → 영향 억제 → 복구 → 검증**이다. 아래는 절차 설계이며 실제 장애 실험 결과는 추후 기록한다.

| 시나리오 | 진단과 복구 | 복구 검증 |
|---|---|---|
| Consumer lag 지속 증가 | Producer rate, 처리량, partition 분포, skew, Spark batch duration, downstream 확인 후 병목 제거·용량 조정 | backlog가 정상 범위로 회복하는지 |
| Quarantine 급증 | failure_reason, producer_id, schema/version별 분석 → 원인 수정 → 대상만 Reprocess | 품질 실패율 회복, 재처리 결과와 중복 여부 |
| Spark Job 실패 | 실패 Job의 기존 checkpoint로 재시작 | 진행 재개, sink 중복·누락 및 state 정합성 |
| 처리·시스템 실패 | bounded retry + exponential backoff → 소진 시 DLQ → 원인 해결 후 Reprocess | 같은 event 재처리 시 결과 수렴 |
| Checkpoint 유실·손상 | Kafka retention 내 replay 가능성 검토, 장기 historical 복구는 Bronze Backfill | 복구 범위·시작 위치·결과 정합성 |

Spark Kafka source의 진행 상태는 Spark checkpoint로 관리한다. 일반 Consumer의 `poll → process → commit` 루프를 Spark에 그대로 적용하지 않는다. 외부 알림용 일반 Consumer에는 manual commit과 별도 멱등성 처리를 적용한다.

Checkpoint 경로는 raw-ingestion / validation / metric-aggregation / state-machine별로 분리한다. 정상 재시작과 checkpoint 유실 복구는 별도 절차다. 일반 Backfill은 현재 DynamoDB 상태 복구를 대신하지 않으며, 필요하면 별도 State Rebuild를 사용한다.

## 5. Airflow와 Databricks 책임

Databricks Jobs가 4개 Streaming Job의 실행·재시작 등 lifecycle을 담당한다. Airflow는 종료 가능한 Backfill·Reprocess·Maintenance workflow를 담당한다.

| DAG | 입력·실행 방식 | 핵심 처리 |
|---|---|---|
| `ktd_reprocess` | 수동/조건부, source·시간 범위·failure_type/reason·equipment_id·max_records | 실패 원인 해결 후 reprocess 토픽으로 발행, Job 2 재검증 |
| `ktd_backfill` | 수동/파라미터 기반, start/end·store/equipment·target·reason | Bronze 기반 Silver/Gold 재계산 및 검증 |
| `ktd_iceberg_maintenance` | 주기적 검사 | 파일·snapshot 상태 확인 후 필요할 때 maintenance 실행 |

Data Quality Audit은 초기에는 maintenance 또는 monitoring workflow에 포함하고, 필요하면 별도 DAG로 분리한다.

### Reprocess

```text
Quarantine / DLQ
  → 원인 수정 및 범위 선택
  → kitchen.sensor.reprocess
  → Job 2: Schema / Domain Validation → Dedup·Idempotency
  → Silver + kitchen.sensor.validated
```

- 검증을 우회해 validated 토픽으로 직접 보내지 않는다.
- 원래 `event_id`는 유지하고 재처리 이력은 별도 metadata로 남긴다.
- `max_records`와 실패 유형·기간 필터로 재투입 범위를 제한한다.
- raw와 reprocess를 같은 처리 경계에서 다루면 자원 경쟁을 고려해야 한다. 세부 실행 분리는 구현 단계에서 확인한다.

### Backfill

- 기본 경로는 Bronze → Silver → Gold. 선택적 target 처리는 의존성을 확인한 뒤 적용한다.
- `target` 후보는 SILVER / GOLD / ALL이며, Gold의 기반 Silver도 잘못됐다면 Silver부터 재계산해야 한다.
- 실시간 처리와 겹치지 않는 충분히 과거 구간을 우선 대상으로 삼는다. 실제 처리 진행 상태와 동시 writer·commit 충돌 정책은 구현에서 검증한다.
- `읽기 → transform → idempotent write → validation` 순서로 구성한다.
- `event_id`는 유지하고 `processing_type=BACKFILL`, `backfill_run_id`, `processed_at` 등으로 추적한다.
- 일반 Backfill은 Silver/Gold historical data를 수정하며 DynamoDB current state는 수정하지 않는다.

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

## 6. Phase 0~9 구현 로드맵

작게 동작하는 E2E를 만든 뒤 정확성 → 장애 복구 → 부하 검증 순서로 확장한다. 아래 완료 기준은 대화의 DoD 및 핵심 테스트를 정리한 것이며 완료를 체크한 목록이 아니다.

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

## 7. 성능 실험과 향후 산출물

| 우선순위 | 비교할 가설 | 주요 관측 항목 |
|---|---|---|
| 1 | Kafka partitions 3 / 6 / 12 | 처리량, lag, p95 latency, Spark processing rate |
| 2 | Silver micro-batch MERGE vs append + 주기적 dedup/compaction | 멱등성 충족 시점, 처리량·지연, 파일 수, commit overhead |
| 3 | watermark 5 / 10 / 20분 | late 수용률과 state 크기·비용 |
| 4 | Iceberg partition strategy | 읽기·쓰기 비용과 파일 분포 |
| 5 | 5분 sliding window의 slide 간격 | 집계 요구와 state·처리 비용 |
| 6 | compaction policy | 작은 파일 개선과 maintenance 비용 |

실험 기록 형식: **Hypothesis → Test Setup → Workload → Metrics → Result → Decision**.

Silver의 영속 멱등성 요구는 유지하지만 쓰기 구현 방식은 아직 미정이다. append 대안은 정기 중복 제거 전 소비자가 보게 되는 데이터까지 고려하여 요구 충족 여부를 검증해야 한다. 실측 전 처리량·지연·개선율을 성과로 작성하지 않는다.

향후 문서 후보는 `architecture-decisions.md`, `performance-report.md`, `runbook.md`, `schema-evolution-test.md`다. 실제 작성 시 기존 design-decisions·ADR·performance-plan 문서와 연결해 중복을 줄인다. 이번 파일은 그 결과 문서가 아니라 후속 계획 기록이다.

## 8. 최신 구현 인수인계 기준

마지막 새 대화 프롬프트에서는 다음을 구현 기준으로 정리했다. 기존 문서에서 최신 제안으로 남아 있던 수치형 계약도 이 인수인계에 포함되었다. 최종 스키마 파일·호환성 테스트 구현 완료를 뜻하지 않는다.

- Kafka key는 `equipment_id`, 센서 이벤트 identity는 `event_id`.
- Avro + Schema Registry, 기본 compatibility는 BACKWARD.
- 기본 계약은 수치형 `SensorMetricEvent`, `metric_value=double`; equipment_type / metric_name / unit은 string으로 두고 Job 2에서 domain validation.
- 4개 Spark Job: Raw Ingestion / Validation·Dedup·Silver / Metric Aggregation / State Machine·Alert. checkpoint는 각자 분리.
- 저장은 S3 + Iceberg + Glue Catalog. Bronze는 `days(ingested_at)`, Silver는 `days(event_time)` 초기안.
- Gold는 `equipment_metric_1m`, `equipment_metric_5m`, `equipment_state_history`, `alert_history`.
- DynamoDB는 current serving state, PK=`store_id`, SK=`equipment_id`. Spark processing state와 Iceberg history는 역할을 분리.
- 화구 상태 머신을 첫 reference로 하고 hysteresis·지속시간 조건을 사용. health는 HEALTHY / STALE / FAULT로 분리.
- Alert는 deterministic `alert_id`와 ACTIVE → RESOLVED lifecycle, 상태 전이는 별도 `state_change_id`.
- Quarantine과 DLQ, Retry와 Reprocess, Backfill과 State Rebuild의 책임을 구분.
- Kafka partition 수, watermark 최적값, Silver 쓰기 방식, slide 간격, compaction 임계값은 실험 대상.

기존 문서의 미결 사항인 Silver/validated 이중 쓰기 복구, runtime 호환성, 동일 timestamp tie-breaker, 장비 ID 유일성, 경보 gap/stale 정책 등은 이 후속 대화만으로 해결됐다고 간주하지 않는다.

## 9. 다음 구현 대화에서 시작할 일

설계 논의를 처음부터 반복하지 않고 Phase 0부터 시작한다.

1. 현재 저장소 구현을 확인하고 Phase 0 DoD와 비교한다.
2. 필요한 디렉터리 구조와 Docker 개발환경 구성 순서를 정한다.
3. Kafka + Schema Registry에서 Avro SensorMetricEvent 1건 왕복을 검증한다.
4. 완료 조건을 확인한 후 Phase 1의 Bronze ingestion으로 확장한다.

협업 방식은 **목표·DoD 확인 → 컴포넌트와 흐름 설명 → 작은 작업 단위 구현 → 오류 원인·디버깅 → 테스트 → 다음 Phase**로 한다. 전체 완성 코드를 한 번에 생성하지 않고 사용자가 각 단계의 목적과 결과를 직접 확인하며 진행한다.
