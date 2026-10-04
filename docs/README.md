# KTD 문서

Phase 0 로컬 Avro 전송과 Phase 1 MSK → Bronze는 구현·검증했다.
Phase 2 이후의 Silver·Gold·현재 상태·경보는 설계와 실행 결과를 구분해서 읽는다.

## 구현과 실행 기록

- [Phase 1 검증 기록](reports/phase-1-verification.md): 실제 MSK 적재, 원본 보존, 정상 재시작과 failure-state recovery.
- [실행·복구 runbook](runbook.md): Databricks Git Folder 실행, 격리 실험, checkpoint 재시작과 유실 대응.
- [Databricks·AWS 구성](databricks-aws-setup.md): MSK IAM, Service Credential, Managed Iceberg와 UC Volume.
- [로컬 환경과 실행](local-development.md): Kafka·Registry·Simulator 설정과 검증 명령.
- [Phase 0 검증 기록](reports/phase-0-verification.md): Avro 전송과 로컬 장애 실험의 실행 이력.
- [스키마 호환성·장애 검사](schema-evolution-test.md): 호환성 결과, Producer 종료와 서비스 복구 관찰.
- [데이터 계약](data/data-contract.md): 현재 Avro 필드와 Bronze metadata, identity·버전 정책.

## 현재 결정과 후속 설계

- [플랫폼과 Job 경계](architecture/platform.md): 검증된 Job 1과 설계 단계의 Job 2~4.
- [저장 설계](data/storage-design.md): 현재 Bronze와 후속 Silver·Gold·DynamoDB 모델.
- [전체 목표 도식](architecture/diagrams.md) · [분야별 설계 목차](architecture/design-decisions.md)
- [설계 변경과 미결정 사항](architecture/planning-update.md)
- [품질 검증](data/validation.md) · [Gold 모델](data/gold-model.md)
- [멱등성](streaming/delivery-and-idempotency.md) · [집계](streaming/window-aggregation.md) · [상태와 경보](streaming/state-and-alerts.md)
- [Replay·Backfill 설계](operations/late-events-and-backfill.md)
- [관측·운영과 Phase 로드맵](architecture/kafka-design-followup.md) · [성능 실험 계획](experiments/performance-plan.md)
- ADR: [Local-first](adr/01-local-first-architecture.md), [Kafka 채택](adr/02-adopt-kafka-over-redpanda.md)

실행 결과는 검증 보고서에, 명령과 복구 순서는 runbook에 남긴다.
설계 후보와 미측정 수치를 구현 성과로 표시하지 않는다.
