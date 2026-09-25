# Work 문서화 이후 KTD 계획 업데이트

## 범위와 근거

기준점은 「01. architecture 계획」에서 사용자가 “Iceberg 파티셔닝 전략에 대해서 진행하기 전에 … docs에 md 파일로 … work로 진행해”라고 요청한 시점이다. 그 이후 Iceberg 파티셔닝부터 복구·품질·상태 머신·4개 Job 분리까지, 이어진 「Kafka 파티션 설계」의 마지막 데이터 계약 제안까지를 정리했다.

- 대화: [01. architecture 계획](https://chatgpt.com/c/6ab14afa-9508-83e8-9c61-b3afac385200)
- 후속 대화: [Kafka 파티션 설계](https://chatgpt.com/c/6ab4e9c3-d3e4-83ee-9140-57f07acd65ba)
- 대조 기준: 이번 갱신 전 `docs/architecture`, `docs/data`, `docs/streaming`, `docs/operations` 문서.
- 구현 완료나 실측 성과는 확인하지 않았다. 기존 작업 중인 코드와 README 변경은 보존한다.

## 계획의 중심

주방 센서 데이터를 Kafka로 받아 원본은 Bronze에 보존하고, 검증된 이벤트를 Silver와 실시간 validated 토픽으로 제공한다. 집계와 상태 머신은 독립 Spark Job으로 실행한다. 현재 상태는 DynamoDB, 과거 이력은 Iceberg가 담당한다. 장애 시 checkpoint로 재시작하고, 과거 정정은 Bronze 기반 backfill로 처리한다.

P1 데이터 플랫폼의 신뢰성·재처리·운영 증빙을 우선한다. P2 dbt mart와 P3 외부 알림은 후속 소비 데모 범위이며 별도 대형 앱으로 확장하지 않는다.

## 이전안에서 달라진 점

| 항목 | 이전 Work 문서 | 이후 방향 / 상태 |
|---|---|---|
| Job 경계 | 논리 흐름 중심 | Raw 보존 / Validation·Dedup·Silver / Metric / State·Alert의 4개 Job, 채택 |
| validated 의미 | Pydantic 구조 검증만 통과 | Job 2의 계약·업무 품질·dedup을 통과한 실시간 trusted interface, 채택 |
| 원본과 검증 경로 | 순차로 읽힐 여지 | Job 1·2가 raw를 독립 소비, 채택 |
| 토픽 | 4개 | state.changes / reprocess / sensor.dlq 추가, 채택 |
| 직렬화 | 초기 JSON, Avro 후속 도입 가능 | 사용자가 Avro + Schema Registry를 처음부터 도입하도록 명시 |
| Bronze 파티션 | 후속 미정 → 첫 논의 days(event_time) | 최신 후속안 days(ingested_at), 앞선 event-time안 대체 |
| Silver 파티션 | day + bucket(16) 초기안 | 최신 후속안 days(event_time)만 시작, bucket은 측정 후 |
| Silver 멱등 쓰기 | MERGE 중심 설명 | 영속 멱등성 요구만 확정, micro-batch MERGE 대 append+정기 dedup 비교 보류를 사용자가 명시 |
| 5분 sliding | slide 1분 | 최신 논의에서 slide 간격을 다시 실험 항목으로 둠 |
| watermark | 10분 고정처럼 서술 | event-time 기반 10분 초기값, 지연 분포로 조정 |
| late 저장 | 별도 보존 경로 | Bronze 원본과 late 관측 지표 활용, 별도 late 토픽은 초기 미도입 |
| 상태 | 공통 FSM에 fault 혼재 | 장비별 운영 상태와 health 분리, STALE과 FAULT 구분 |
| 현재 상태 복구 | backfill과 경계 불명확 | 일반 backfill은 Silver/Gold만, DynamoDB는 명시적 State Rebuild |
| 값 표현 | numeric/string/boolean 3개 값 컬럼 | 마지막 답변은 numeric telemetry의 metric_value=double 제안. 사용자 후속 채택 미확인 |

## 채택된 방향과 이유

| 결정 | 이유 | 대가 / 검증할 점 |
|---|---|---|
| 4개 Job과 독립 checkpoint | 원본 보존·품질·집계·상태의 장애 및 배포 분리 | 실행·운영 비용 증가, 전체 진행 상황 관측 필요 |
| Silver + validated 유지 | 영속 이력과 낮은 지연의 소비 인터페이스 분리 | 이중 쓰기 실패 후 누락·중복 복구 설계 필요 |
| 품질 실패는 quarantine, 시스템 실패는 retry/DLQ | 원인별 대응과 재처리 경로 명확화 | 실패 레코드·시도 이력 관리 필요 |
| Spark bounded dedup + sink별 멱등성 | 재시작·replay·외부 부작용까지 책임 분리 | 식별자·버전·원자성 설계 필요 |
| 운영 상태 / health / alert 분리 | 데이터 단절을 실제 장비 고장과 혼동하지 않음 | 장비별 규칙 및 결측 정책 필요 |
| Bronze 기반 기간 backfill | Kafka retention과 분리된 장기 정정 | 실시간 로직 재사용·동시 쓰기·정합성 검증 필요 |

## 결정 상태를 과장하지 않는 기준

- **채택:** 사용자가 명시하거나 이후 요약에서 확정사항으로 반복한 구조. 후속 논의로 계속 사용된 추천 방향은 설계 기준으로 정리했다.
- **초기값:** Kafka 6/3 partitions, watermark 10분, 경보 임계치·지속시간, snapshot 정책 등. 운영 최적값을 뜻하지 않는다.
- **실험 보류:** Silver 쓰기 전략, sliding 간격, bucket 수, 파일 크기와 compaction 주기.
- **최신 제안:** 마지막 Avro 답변의 수치형 전용 계약, schema_version=int 등. 기존 계약을 승인 없이 확정 변경하지 않고 [계약 문서](../data/data-contract.md)에 차이를 남긴다.

## 구현 순서

1. Avro 계약의 미결정 필드·버전·상태 이벤트 입력을 정리하고 로컬 Schema Registry 연결.
2. Job 1 원본 보존과 Job 2 품질·dedup을 독립 경로로 구현.
3. Silver/validated 이중 쓰기 실패 복구와 영속 멱등성 기준 검증.
4. Job 3의 수치형 집계, Job 4의 화구 상태·냉장고 경보 reference 구현.
5. checkpoint 재시작, quarantine/DLQ 재처리, 기간 backfill, State Rebuild 검증.
6. 관측성과 운영 Runbook 작성 후 [성능 실험](../experiments/performance-plan.md) 수행.
7. 결과를 ADR·성능 리포트·데모·포트폴리오에 반영하고 P2/P3 확장.

## 아직 정해야 할 것

Silver/Kafka 이중 쓰기 프로토콜, Spark/runtime과 Iceberg/Glue 호환성, metric 사전·유효범위, 장비 ID 전역 유일성, 동일 timestamp tie-breaker, 상태 이벤트 계약, 경보 gap/stale 정책, late 측정 구현, streaming/backfill 동시 writer 제어, alert lifecycle 이벤트 키, 토픽 및 원천 데이터 retention, 관측 임계치와 장애 대응 절차.

면접에서는 “구현했다” 대신 현재는 “이렇게 설계했고, 다음 실험으로 검증할 계획이다”라고 설명한다. 실측 전 성능 개선률·처리량·지연 달성을 주장하지 않는다.
