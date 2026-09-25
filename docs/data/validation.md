# 데이터 품질과 오류 분류

상태: 설계 기준 · 2026-09-24

Job 2가 raw에서 trusted event로 넘어가는 최종 품질 경계다. Avro/Registry는 구조적 계약, Spark는 domain/referential 품질을 담당한다. Pydantic은 producer·입력 경계의 구조 검증에 활용할 수 있으나, Pydantic만 통과한 이벤트를 validated로 발행하는 이전 경로는 대체된다.

| 분류 | 예 | 처리 |
|---|---|---|
| 구조·직렬화 | Avro 해석 실패, 필수 필드·버전 오류 | quarantine |
| domain | 범위·unit·equipment_type/metric 조합 위반, 비정상 미래 시각 | quarantine |
| 참조 | UNKNOWN_EQUIPMENT | 초기에는 quarantine, 마스터 전파 지연 대기는 후속 |
| streaming | 중복, watermark 밖 late | dedup / Bronze 보존·backfill, 품질 오류와 별도 집계 |
| 업무 이상 | 유효한 고온이 지속 | Alert lifecycle |
| 시스템 | 네트워크·일시적 sink 장애 | bounded retry 후 DLQ, 보존 실패 시 성공 처리 금지 |

Registry 접속 장애처럼 시스템 원인인 경우, 정상 bytes를 스키마 불량으로 잘못 분류하지 않도록 구현 시 구분한다.

quarantine에는 original_payload, event_id(해석 가능 시), schema_id/version, source_topic/partition/offset, detected_at, error_stage, primary_error_code, error_details, reprocess_count/status/last_reprocessed_at을 남긴다. 해석할 수 없는 값은 만들어내지 않는다. 대표 오류는 운영 집계, 전체 오류 목록은 조사에 사용한다.

재처리는 동일 검증 로직을 다시 통과한다. 개별 수정은 reprocess 토픽, 대량 정정은 Airflow + Spark Batch를 사용한다. event_id는 유지하고 processing attempt를 따로 기록한다. PENDING/REPROCESSING/RESOLVED/FAILED/DISCARDED 상태는 재처리 이력 모델 방향이다. 최대 재시도 횟수는 미정이다.

초기 품질 규칙은 직접 구현하고 Great Expectations/Soda는 후속 배치 gate 후보로 둔다. 관측: total/valid/quarantine count, error code별 count, quarantine rate, reprocess 성공·실패, duplicate/late count. 숫자 임계값은 후속 Runbook에서 정한다.

**이유:** 읽을 수 있는 데이터와 업무적으로 올바른 데이터를 구분한다. **대가:** 규칙·오류 코드·참조 데이터 버전 관리가 필요하다. 관련: [계약](data-contract.md), [복구](../operations/late-events-and-backfill.md).
