# 실패 처리·Checkpoint·Replay·Backfill

상태: 설계 기준, 상세 Runbook·임계값 미완성 · 2026-09-24

## 원인별 처리

| 상황 | 동작 | 완료 판단 |
|---|---|---|
| 데이터 품질 오류 | quarantine | 원본·오류 보존 ACK |
| 일시적 시스템 오류 | bounded retry + exponential backoff | 원래 처리 성공 |
| 재시도 소진 | kitchen.sensor.dlq | DLQ 보존 ACK |
| quarantine/DLQ 저장도 실패 | 실패 유지·관측·재시도 | 성공/commit 처리 안 함 |
| 원인 해결 후 개별 재처리 | kitchen.sensor.reprocess → 같은 검증 | 검증·sink 성공 |
| watermark 밖 late·대량 로직 변경 | Bronze → Airflow + Spark Batch | Silver/Gold 정정·정합성 검증 |

직접 consumer는 성공 확인 후 offset commit한다. Spark는 query checkpoint와 batch 성공으로 진행을 관리한다. 레코드별 DLQ 발행 및 batch 재시도 결합 방식은 구현 시 검증한다. 초기 별도 retry topic은 사용하지 않는다. 무한 재시도 방지를 위한 횟수·최대시간은 미정이다.

## Checkpoint와 복구 유형

S3 등 영속 저장소에 query별 독립 checkpoint를 둔다. 정상 재시작은 기존 checkpoint를 재사용한다. checkpoint 삭제는 정상 재시작 수단이 아니다.

- Restart: 프로세스·cluster 장애 후 기존 checkpoint로 offset/state 복구.
- Replay: Bronze 원본으로 과거 이벤트를 다시 처리해 새 로직·복구 검증.
- Backfill: 기간을 지정해 과거 Silver/Gold 결과 정정.
- State Rebuild: 현재 상태 복구가 필요할 때 별도로 최신 이벤트까지 계산하고 최종 상태만 DynamoDB 반영.

state schema/operator 등 호환 불가 변경은 새 checkpoint + 계획된 replay/backfill 대상으로 다룬다. 정상 복구와 다른 배포·전환 절차가 필요하다. sink 성공 후 checkpoint 완료 전 장애에 따른 재처리는 멱등 sink로 방어한다.

## Event time과 late 정책

event_time이 의미 기준이고 ingested_at은 도착 지연 관측에 사용한다. 10분 watermark는 초기값이며 실제 p95/p99 지연·state 비용을 보고 조정한다. 현재 시각 또는 ingested_at-event_time과 단순 비교하는 규칙이 아니다. 각 query에서 관측한 event time 진행과 operator의 window/state 경계가 중요하다.

Job 1은 원본을 보존한다. Job 2는 정책상 너무 늦은 이벤트를 실시간 canonical 경로에서 제외하고 late 지표를 남긴다. 별도 late 토픽은 초기 도입하지 않는다. watermark 설정만으로 late 저장·정확한 계수가 자동 구현된다고 가정하지 않는다.

Job 3은 아직 유지되는 window에 late를 반영해 결과가 수렴하도록 한다. Job 4는 허용 late의 이력 처리와 현재 Twin 갱신을 분리한다. 오래된 이벤트가 DynamoDB를 되돌리지 않도록 조건부 갱신한다. window state가 제거된 기간은 backfill로 정정한다.

## Backfill 실행 계약

입력: start_time/end_time, source_layer/target_layer, 선택 store_id/equipment_id, reason, backfill_run_id. 범위·시간대·경계 포함 규칙은 물리 DAG 작성 시 고정한다.

순서: 파라미터 검증 → Bronze 읽기 → 공통 검증·변환·event_id dedup → Silver 멱등 반영 → 영향 Gold 재계산 → 멱등 반영 → 결과 비교.

실시간·배치에서 validate/transform/aggregate 로직을 재사용한다. 상태와 sliding window 계산은 target 이전 lookback 또는 초기 상태가 필요하다. lookback 10분을 모든 상태에 충분한 값으로 고정하지 않는다. 경보 30분 지속 등 규칙별 필요한 문맥을 검증한다.

일반 backfill은 DynamoDB current state를 갱신하지 않고 과거 Slack 알림도 재발송하지 않는다. 동일 범위 재실행 시 행 수·중복 키·집계 결과가 수렴해야 한다. streaming writer와 충돌하는 범위의 lock·staging·merge 전략은 미정이다.

## 검증 및 다음 Runbook 설계

검증: before/after count, duplicate key, null/quality error, aggregation difference, 실행 ID와 수정 범위. 재처리 event_id는 유지하고 시도 이력을 분리한다.

다음 설계에서 consumer lag, Spark batch duration/state size, late rate, quarantine/DLQ rate, Iceberg small file, DynamoDB conditional rejection의 수집 방법·경보 임계치·조치·복구 완료 조건을 정한다.
