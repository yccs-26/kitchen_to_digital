# 실행 환경과 4개 Spark Job

상태: 채택된 목표 설계, 구현·검증 전 · 2026-09-24

[전체 도식](diagrams.md) · [변경 이력](planning-update-2026-09-24.md)

## 플랫폼

Kafka를 이벤트 로그로, Databricks PySpark Structured Streaming을 연산 환경으로, AWS S3 + Apache Iceberg를 저장 계층으로, Glue Catalog를 메타데이터 catalog로 사용한다. 로컬 MVP 우선 원칙은 기존 ADR을 유지한다. runtime·connector 버전과 실제 쓰기 호환성은 구현 검증 대상이다.

## Job 경계

| Job | 입력 | 책임 | 출력 | 논리 소비 그룹 |
|---|---|---|---|---|
| 1 Raw Ingestion | kitchen.sensor.raw | bytes와 Kafka 메타데이터 원본 보존, 업무 검증·event_id dedup 없음 | Bronze | ktd-raw-ingestion |
| 2 Validation + Dedup + Silver | kitchen.sensor.raw, 명시적 reprocess 경로 | Avro 해석·계약·의미 검증, late 분류, bounded dedup | quarantine / Silver / kitchen.sensor.validated | ktd-validation |
| 3 Metric Aggregation | kitchen.sensor.validated | 1분 tumbling + 5분 sliding 수치 집계 | Gold metric facts | ktd-metric-aggregation |
| 4 State Machine + Alert | kitchen.sensor.validated | 장비별 FSM·health·지속시간 경보 | DynamoDB / state.changes / alerts / 상태·경보 이력 | ktd-state-machine |

**Job 1과 Job 2는 raw 전체를 독립 소비한다. Job 2가 Bronze 적재 완료를 기다리는 직렬 경로가 아니다.** Bronze 누락 방지를 위해 Job 1의 lag와 Kafka retention 여유를 관측해야 한다. Job 3과 Job 4도 validated를 독립 소비한다.

Spark query마다 독립된 영속 checkpoint를 둔다. 표의 group은 논리적 독립 소비 의도이며 Spark가 자동 생성하는 group ID를 무조건 고정값으로 덮어쓰라는 설정 명세가 아니다.

Silver는 영속 canonical history, validated는 실시간 canonical interface다. Job 2가 둘에 쓰는 것은 분산 원자적 트랜잭션으로 확정되지 않았다. 한쪽 성공 후 장애가 발생했을 때 재전달·대사·누락 복구를 어떻게 할지는 구현 전 해결할 쟁점이다. 다이어그램의 분기는 이 두 출력을 표현한다.

Job 4는 초기에는 validated에서 자체 stateful alert를 계산한다. Job 3 집계를 재사용할 필요가 생기면 metric interface를 추가 검토한다. kitchen.metrics 토픽은 현재 도입하지 않는다.

**이유:** 장애·state·checkpoint·배포 경계를 분리한다. **트레이드오프:** Job 수와 관측 비용, sink 간 일관성 관리가 늘어난다. **면접 포인트:** 저장 이력과 실시간 인터페이스를 왜 분리했고, 각각의 실패를 어떻게 복구하는지 설명한다.
