# ADR-001: Redpanda 대신 Apache Kafka 채택

- 상태: Accepted
- 날짜: 2026-08-15

## 맥락

주방 IoT 디지털 트윈 파이프라인은 장비 상태 변경, 온도 이탈 경보 등
도메인 이벤트를 다른 시스템이 실시간으로 구독할 수 있도록 이벤트 소싱 계층을 제공해야 한다.

이벤트 브로커는 다음 요구사항을 충족해야 한다.

- `equipment_id`를 파티션 키로 사용해 장비별 이벤트 순서를 보장할 수 있어야 한다.
- Consumer Group 기반으로 여러 소비자를 독립적으로 확장할 수 있어야 한다.
- Consumer lag, 처리 실패율, 재시도 횟수를 관측할 수 있어야 한다.
- 처리 실패 이벤트를 DLQ로 분리하고 재처리할 수 있어야 한다.
- 스키마 레지스트리와 연계해 이벤트 스키마의 하위 호환성을 검증할 수 있어야 한다.
- 브로커 장애 및 Consumer 재시작 상황에서 메시지 유실·중복 처리 방지 전략을 검증할 수 있어야 한다.
- 로컬 Docker Compose 환경에서 개발·장애 주입 테스트를 수행할 수 있어야 한다.

Redpanda와 Apache Kafka를 후보로 비교했다.

## 고려한 대안

### Redpanda

Redpanda는 Kafka API와 호환되는 이벤트 스트리밍 플랫폼이다.
단일 바이너리 기반 운영과 낮은 리소스 사용량을 장점으로 하며,
로컬 개발 환경에서 비교적 간단하게 실행할 수 있다.

그러나 본 프로젝트의 목적은 경량 브로커 운영 자체보다
Kafka의 파티션, Consumer Group, offset, replication, lag,
DLQ 및 재처리 전략을 명시적으로 설계하고 검증하는 것이다.
또한 Kafka 관련 운영 사례, 트러블슈팅 자료, 커넥터 및
관측 도구 생태계가 더 넓다는 점을 고려했다.

### Apache Kafka

Apache Kafka는 파티션 기반 순서 보장, Consumer Group,
offset 관리, 보존 정책, 복제, 재처리 등 이벤트 스트리밍의
핵심 개념을 직접 다룰 수 있는 표준적인 플랫폼이다.

Kafka는 프로젝트에서 다음을 구현·검증하는 데 적합하다.

- `equipment_id` 기준 파티셔닝을 통한 장비별 상태 이벤트 순서 보장
- Consumer Group 기반 다중 소비자 확장
- Consumer lag 모니터링과 임계치 기반 경보
- 실패 이벤트의 DLQ 라우팅 및 exponential backoff 재시도
- idempotency key 기반 중복 처리 방지
- Schema Registry 기반 Avro 또는 Protobuf 스키마 호환성 검증
- 브로커 또는 Consumer 장애 주입 후 offset과 재처리 동작 검증

## 결정

이 프로젝트의 이벤트 소싱 브로커로 Apache Kafka를 채택한다.

개발 환경에서는 Docker Compose로 Kafka와 Schema Registry를 실행한다.
프로덕션 또는 확장 시나리오에서는 Amazon MSK를 선택지로 검토한다.

## 결정 근거

- 이벤트 기반 데이터 플랫폼에서 요구되는 파티션, offset, Consumer Group,
  lag, 재처리, 복제 개념을 명시적으로 설계하고 포트폴리오로 증명할 수 있다.
- P3의 핵심 목표인 순서 보장, Exactly-once 전달 설계, DLQ, 재시도,
  장애 주입 테스트를 자연스럽게 구현할 수 있다.
- Schema Registry, Kafka Connect, Prometheus/Grafana 등 주변 생태계와
  학습·운영 참고 자료가 풍부하다.

## 결과

### 긍정적 결과

- 장비별 파티션 키 설계와 이벤트 순서 보장 전략을 구현할 수 있다.
- Consumer lag, 재시도율, DLQ 적재 건수 등 신뢰성 지표를 수집할 수 있다.
- 브로커 장애, Consumer 중단, 중복 이벤트 발생 상황에 대한
  복구 및 재처리 시나리오를 검증할 수 있다.
- 스키마 호환성 검증을 CI 또는 배포 과정에 연결할 수 있다.

### 부정적 결과 및 대응

- 로컬 환경에서도 ZooKeeper 기반 구성 또는 KRaft 설정, 브로커·Schema Registry·모니터링 도구 구성이 필요해 운영 복잡도가 증가한다.
  - Docker Compose와 환경 변수 템플릿으로 실행 절차를 표준화한다.

- 단일 노드 로컬 Kafka는 실제 다중 브로커 환경의 복제·장애 내성을 완전히 재현하지 못한다.
  - 브로커 중단·Consumer 재시작·네트워크 지연 등 재현 가능한 범위의 장애 주입 테스트를 수행한다.

- Kafka의 exactly-once 기능만으로 외부 Sink까지의 완전한 exactly-once를 보장할 수 없다.
  - `event_id` 또는 idempotency key를 저장·검증하여 소비자 측 중복 처리를 방지한다.

## 후속 작업

- `equipment_id`를 파티션 키로 하는 토픽 및 보존 정책을 정의한다.
- 이벤트 스키마를 Avro 또는 Protobuf로 정의하고 호환성 정책을 결정한다.
- Consumer lag, DLQ 적재량, 재시도율을 Grafana에서 관측한다.
- 브로커 중단 및 Consumer 재시작 장애 주입 테스트를 작성한다.
- DLQ 재처리 절차를 `docs/runbooks/kafka-dlq-reprocessing.md`에 문서화한다.