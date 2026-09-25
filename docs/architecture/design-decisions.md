# KTD 분야별 설계 기준

갱신: 2026-09-24 · 설계와 구현 상태를 구분한다.

| 분야 | 기준 문서 |
|---|---|
| 변경 이력·결정 상태·남은 작업 | [계획 업데이트](planning-update-2026-09-24.md) |
| 전체 구조·4개 Spark Job | [플랫폼](platform.md), [도식](diagrams.md) |
| Kafka 토픽·병렬성·신뢰성 | [Kafka](kafka-topics.md) |
| Avro 계약·호환성·최신 제안 | [데이터 계약](../data/data-contract.md) |
| 데이터 품질·오류 분류 | [검증](../data/validation.md) |
| 계층별 중복 방어 | [멱등성](../streaming/delivery-and-idempotency.md) |
| 윈도우·metric 집계 | [집계](../streaming/window-aggregation.md) |
| 상태 머신·경보 | [상태와 경보](../streaming/state-and-alerts.md) |
| Gold fact·dimension | [Gold](../data/gold-model.md) |
| DynamoDB·Iceberg·유지보수 | [저장 설계](../data/storage-design.md) |
| checkpoint·reprocess·backfill | [복구](../operations/late-events-and-backfill.md) |
| 측정 후 결정할 항목 | [실험 계획](../experiments/performance-plan.md) |

Local-first와 Kafka 채택 ADR은 유지한다. 최신 플랫폼 방향은 Kafka + Databricks PySpark + S3/Iceberg + Glue Catalog이며, IoT Core/Kinesis는 필수 경로가 아니다. 코드와 토픽 생성 스크립트의 전환은 이번 문서 작업에 포함되지 않는다.

다음 설계는 관측 지표·경보 기준·Runbook 상세화다. 우선 해결할 구현 쟁점은 Silver/Kafka 이중 쓰기, stateful late-event 재정렬, DynamoDB 동시 시각 처리, 계약 전환, streaming/backfill 동시 쓰기다.
