# KTD 최종 구조와 처리 흐름

2026-09-24 · 채택된 논리 구조를 표시한다. 구현 완료도나 분산 트랜잭션 보장도가 아니다.

Mermaid를 지원하지 않는 뷰어에서는 [전체 아키텍처 SVG](../diagrams/ktd-architecture.svg)를 연다. 아래 코드 블록은 편집 가능한 도식 원본이다.

[PNG 이미지](../diagrams/ktd-architecture.png)도 함께 제공한다.

![KTD architecture](../diagrams/ktd-architecture.png)

## 1. 전체 아키텍처

```mermaid
flowchart TB
  P[Sensor Simulator / Producer] -->|Avro event| R[kitchen.sensor.raw]
  SR[Schema Registry] -. 계약 조회·호환성 .-> P
  SR -. 역직렬화 schema .-> J2
  R --> J1[Job 1 · Raw Ingestion]
  R --> J2[Job 2 · Validation / Dedup]
  J1 --> B[(Iceberg Bronze · 원본)]
  J2 -->|품질 실패| Q[kitchen.sensor.quarantine]
  J2 -->|trusted history| S[(Iceberg Silver)]
  J2 -->|trusted stream| V[kitchen.sensor.validated]
  V --> J3[Job 3 · Metric Aggregation]
  V --> J4[Job 4 · State Machine / Alert]
  J3 --> G[(Iceberg Gold · 집계·상태·경보 이력)]
  J4 --> G
  J4 --> D[(DynamoDB · Current Twin)]
  J4 --> SC[kitchen.state.changes]
  J4 --> A[kitchen.alerts]
  B --> BF[Airflow + Spark Batch · Backfill]
  BF --> S
  BF --> G
  CP[(query별 S3 checkpoint)] -. 복구 .-> J1
  CP -. 복구 .-> J2
  CP -. 복구 .-> J3
  CP -. 복구 .-> J4
```

Iceberg 데이터는 S3, catalog는 Glue, 목표 Spark 실행 환경은 Databricks다. Registry는 데이터가 통과하는 브로커가 아니라 계약 관리 서비스다. Silver와 validated는 Job 2의 두 출력이며 원자적 이중 쓰기 보장은 미정이다. 일반 backfill은 DynamoDB와 실시간 알림을 건드리지 않는다.

## 2. 검증·실패·재처리

```mermaid
flowchart TD
  IN[Raw 또는 명시적 Reprocess] --> C{계약·업무 품질 유효?}
  C -->|아니오| Q[Quarantine · 원본+오류 보존]
  C -->|예| L{실시간 시간 정책 허용?}
  L -->|아니오| B[Job 1 Bronze 원본 / late 지표]
  L -->|예| DP[event_id bounded dedup]
  DP --> W[Silver / validated 쓰기]
  W -->|성공| OK[진행 상태 확정]
  W -->|시스템 실패| RT[Bounded retry + backoff]
  RT -->|성공| OK
  RT -->|소진| DLQ[DLQ 보존]
  DLQ -->|ACK| OK
  Q --> FIX[원인 분석·수정]
  DLQ --> FIX
  FIX --> RP[kitchen.sensor.reprocess]
  RP --> IN
  B --> BF[Airflow Backfill · 동일 검증·변환]
  BF --> H[Silver / Gold 이력 보정]
```

보존 ACK가 실패하면 진행 상태를 성공으로 확정하지 않는다. 직접 consumer의 offset commit과 Spark checkpoint 완료는 서로 다른 구현이다. 개별 재처리가 너무 늦으면 실시간 경로를 강제하지 않고 backfill한다.

## 3. 현재 상태와 경보

```mermaid
flowchart LR
  E[Validated Event] --> FSM[공통 엔진 + 장비별 규칙]
  FSM --> OP[Operational State]
  FSM --> HE[Health: STALE / FAULT 구분]
  OP --> CW{새 time/version?}
  HE --> CW
  CW -->|예| DB[(DynamoDB Current Twin)]
  CW -->|아니오| HIST[이력 처리·기록]
  FSM --> HIST
  E --> AL[지속시간 + hysteresis]
  AL --> LIFE[Alert ACTIVE / RESOLVED]
  LIFE --> AT[kitchen.alerts]
  LIFE --> AH[(Gold Alert History)]
```

```mermaid
stateDiagram-v2
  [*] --> NORMAL
  NORMAL --> PENDING: 임계 초과 관측
  PENDING --> NORMAL: 지속 조건 중단
  PENDING --> ACTIVE: 초과 지속시간 충족
  ACTIVE --> ACTIVE: 동일 이상 구간 관측 갱신
  ACTIVE --> RESOLVED: 복귀 threshold와 지속시간 충족
  RESOLVED --> NORMAL: 다음 이상 구간 대기
```

ACTIVE는 앞선 OPEN과 의미를 통일한 문서 표기다. 외부 lifecycle enum, 결측 구간 처리와 timer 구현은 후속 명세에서 정한다.

## 4. 복구 선택

```mermaid
flowchart TD
  F[장애·과거 정정 요청] --> CK{정상 checkpoint로 재개 가능?}
  CK -->|일시 장애이며 가능| RS[기존 checkpoint Restart]
  CK -->|손상·비호환·과거 로직 정정| BR[Bronze Replay / 기간 Backfill]
  BR --> SG[검증·dedup·Silver/Gold 정정·결과 비교]
  F --> CUR{현재 Twin 자체 복구 요청?}
  CUR -->|예| RE[별도 State Rebuild]
  RE --> LAST[최신까지 replay 후 최종 상태 검증]
  LAST --> DB[(DynamoDB)]
```

과거 backfill과 current-state rebuild는 별개 작업이다. 모든 그림의 구체 실행 순서·writer 원자성·late 정렬 방식은 구현 전에 검증한다.
