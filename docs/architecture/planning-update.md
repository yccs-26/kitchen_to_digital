# KTD 설계 변경 이력과 미결정 사항

이 문서는 이전 설계에서 달라진 선택과 아직 닫히지 않은 결정을 설명한다.
현재 물리 계약은 [데이터 계약](../data/data-contract.md), 실행 결과는 [검증 보고서](../reports/phase-0-verification.md)를 기준으로 읽는다.
후속 플랫폼·처리 기능의 설계 채택은 구현 완료를 뜻하지 않는다.

## 이전안에서 달라진 점

| 항목 | 이전 설계 | 이후 방향 / 상태 |
|---|---|---|
| Job 경계 | 논리 흐름 중심 | Raw 보존 / Validation·Dedup·Silver / Metric / State·Alert의 4개 Job, 채택 |
| validated 의미 | Pydantic 구조 검증만 통과 | Job 2의 계약·업무 품질·dedup을 통과한 실시간 trusted interface, 채택 |
| 원본과 검증 경로 | 순차로 읽힐 여지 | Job 1·2 raw 독립 소비 목표; 개발 계획과 확정 상태 확인 필요 |
| 토픽 | 4개 | state.changes / reprocess / sensor.dlq 추가, 채택 |
| 직렬화 | 초기 JSON, Avro 후속 도입 가능 | Avro + Schema Registry 적용; 현재 계약은 데이터 계약 문서 참조 |
| Bronze 파티션 | 후속 미정 → 첫 논의 days(event_time) | 최신 후속안 days(ingested_at), 앞선 event-time안 대체 |
| Silver 파티션 | day + bucket(16) 초기안 | 최신 후속안 days(event_time)만 시작, bucket은 측정 후 |
| Silver 멱등 쓰기 | MERGE 중심 설명 | 영속 멱등성 요구 유지, MERGE 대 append+정기 dedup 비교 후 결정 |
| 5분 sliding | slide 1분 | 최신 논의에서 slide 간격을 다시 실험 항목으로 둠 |
| watermark | 10분 고정처럼 서술 | event-time 기반 10분 초기값, 지연 분포로 조정 |
| late 저장 | 별도 보존 경로 | Bronze 원본과 late 관측 지표 활용, 별도 late 토픽은 초기 미도입 |
| 상태 | 공통 FSM에 fault 혼재 | 장비별 운영 상태와 health 분리, STALE과 FAULT 구분 |
| 현재 상태 복구 | backfill과 경계 불명확 | 일반 backfill은 Silver/Gold만, DynamoDB는 명시적 State Rebuild |
| 값 표현 | numeric/string/boolean 3개 값 컬럼 | 현재 Avro는 metric_value=double; 상태 입력 계약은 별도 미정 |

## 결정 상태

- **목표 설계:** 컴포넌트 책임과 데이터 흐름. 상세 이유·트레이드오프는 [분야별 기준 문서](design-decisions.md)에 둔다.
- **초기 가설:** Kafka 6/3 partitions, watermark 10분, 경보 수치, snapshot 정책. 운영 최적값이 아니다.
- **실험 보류:** Silver 쓰기 전략, sliding 간격, bucket 수, 파일 크기와 compaction 주기. [실험 계획](../experiments/performance-plan.md)에서 비교한다.
- **미적용 계약 제안:** producer_id, int schema_version, logical timestamp. 현재 string 기반 계약과 구분한다.

## 확인이 필요한 설계 경계

- Job 2의 raw 독립 소비는 아키텍처의 목표 구조다. 로컬 개발 계획서는 이를 구현 제안으로 표시하므로 Phase 2 전에 확정 여부를 확인한다.
- Gold 설계는 fact 중심 논리 모델을, 개발 계획은 `equipment_metric_1m/5m` 등 이름을 사용한다. 논리 모델과 물리 테이블의 대응을 DDL 전에 확인한다.
- 초기 Local-first ADR의 전체 로컬 구성과 현재 Kafka/Registry 로컬 + Databricks/AWS 후속 방향은 범위가 다르다. ADR의 원래 결정은 역사로 보존한다.

Silver/validated 이중 쓰기 복구, runtime·Iceberg/Glue 호환성, metric 사전과 유효범위,
장비 ID 유일성, 동일 timestamp tie-breaker, 상태 입력·경보 gap/stale 정책,
late 측정, streaming/backfill 동시 writer, alert lifecycle 키, retention과 관측 임계치는 미결정이다.

P1 데이터 플랫폼의 신뢰성·재처리·운영을 우선하며 P2 dbt mart와 P3 외부 알림은 후속 소비 데모 범위다.
Phase별 목표·검증 기준은 [로드맵](kafka-design-followup.md)에 둔다. P1/P2/P3 구분은 Phase 번호와 별개다.
