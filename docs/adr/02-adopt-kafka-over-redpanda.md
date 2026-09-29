# ADR-001: Redpanda 대신 Apache Kafka 채택

- 상태: Accepted
- 날짜: 2026-08-15
- 구현 상태: 현재 로컬 전송은 [Phase 0 보고서](../reports/phase-0-verification.md), 후속 토픽·복구 기능은 설계 문서를 따른다.

## Context

주방 IoT Digital Twin은 장비 상태 변경과 온도 경보를 독립 소비자에게 전달할 이벤트 로그가 필요하다.
장비별 파티셔닝, Consumer Group 확장, offset·retention 기반 재처리, lag 관측,
스키마 호환성 및 장애 시 유실·중복 방어를 로컬에서 검증할 수 있어야 한다.

## Decision

이벤트 브로커로 Apache Kafka를 채택하고 로컬 Docker Compose에서 Kafka와 Schema Registry를 실행한다.
장비별 key는 `equipment_id`로 두되 Kafka 기록 순서와 event-time 순서를 구분한다.
확장 환경의 Amazon MSK는 검토 대상이다.

Kafka의 partition, offset, Consumer Group, replication을 직접 설계·검증하는 학습 목적과
Schema Registry·Kafka Connect·관측 도구 생태계가 선택 이유다.
P3 외부 소비 데모의 전달·DLQ·재시도·장애 검증에도 이 기반을 사용한다.

## Alternatives

| 대안 | 장점 | 선택 시 고려한 대가 |
|---|---|---|
| Redpanda | Kafka API 호환, 단일 바이너리 기반 운영과 경량 로컬 구성 | 경량 운영보다 Apache Kafka 자체의 운영 개념·사례·도구를 직접 다루는 프로젝트 목적을 우선함 |
| Apache Kafka | partition·offset·복제·재처리 개념과 폭넓은 연계 도구 | broker·Registry·모니터링 구성의 운영 복잡도 증가 |

이 비교는 선택 근거이며 두 제품의 성능을 실측한 결과가 아니다.

## Consequences

- 독립 소비자 확장과 장비별 파티셔닝, lag·실패·재시도 지표, 호환성 검사를 설계할 수 있다.
- KRaft 등 broker 설정과 주변 서비스 관리가 필요하다. Compose·환경 변수 템플릿으로 로컬 실행을 표준화한다.
- 단일 broker는 다중 broker 복제·장애 내성을 재현하지 못한다. broker/Consumer 중단·네트워크 지연 등 검증 범위를 구분한다.
- Kafka의 exactly-once 기능만으로 외부 sink까지 보장할 수 없다. `event_id` 기반 소비자·sink 멱등성과 부분 실패 복구가 필요하다.

토픽·보존 정책은 [Kafka 설계](../architecture/kafka-topics.md), 중복 방어는
[멱등성](../streaming/delivery-and-idempotency.md), DLQ·재처리는
[복구 설계](../operations/late-events-and-backfill.md)에서 상세화한다.
lag·DLQ·재시도 관측, 호환성의 CI 연계와 상세 장애 Runbook은 후속 작업이다.
