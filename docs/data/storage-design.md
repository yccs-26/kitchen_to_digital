# DynamoDB·Iceberg 저장 및 유지보수

상태: 최신 설계 방향 / 운영 수치는 초기안 · 2026-09-24

## DynamoDB current-state

PK=store_id, SK=equipment_id로 특정 장비와 매장 전체 장비 조회를 지원한다. 한 매장 hot partition은 부하 테스트에서 관측 후 재설계한다.

Item 논리 필드: equipment_type, operational_state, health_status, state_started_at, state_version, last_event_id, last_event_time, last_ingested_at, latest_metrics, updated_at.

새 event time/version에 대한 conditional update로 stale write를 막는다. 동일 timestamp tie-breaker, 다중 metric별 최신 시각, 상태 버전 생성 방식은 미정이다. 단순 전체 item 시간 비교로 다른 metric의 정상 갱신을 잃지 않는지 검증한다. 일반 backfill은 여기 쓰지 않는다.

## Iceberg 초기 partition

| 계층 | 최신 초기안 | 역할 |
|---|---|---|
| Bronze | days(ingested_at) | 원본 bytes + topic/partition/offset/timestamp + schema metadata. 잘못된 원본과 중복도 보존 |
| Silver | days(event_time) | 검증·정규화된 canonical event + 운송·처리 metadata |
| Gold metric/state window | days(window_start) | window 사실 집계 |
| Gold alert/transition | 해당 의미의 event time 기준 day | 업무 경보·상태 전이 이력 |

이전 days(event_time) Bronze 및 day+bucket(16) Silver/Gold는 최신 단순 시작안으로 대체한다. equipment_id identity partition은 초기 사용하지 않는다. bucket(N,equipment_id)는 query scan·파일 수·크기 측정 후 실험한다. event_time을 해석 못 하는 원본도 Bronze에 보존할 수 있어야 한다.

Gold 물리 테이블·컬럼명은 DDL 전 확정한다. [Gold 논리 모델](gold-model.md)을 기준으로 하며 대화의 equipment_metric_1m/5m는 물리 분리 확정으로 해석하지 않는다.

## 유지보수 방향과 초기 운영안

| 항목 | 논의된 초기안 | 결정 상태 |
|---|---|---|
| compaction | Bronze/Silver daily, 최근 partition 중심, binpack | 방향 채택, 최신 논의에서 주기·크기 측정 조정 |
| 파일 크기 | 256MB를 초기 실험값으로 제시 | 최종 목표 아님, 128/256/512MB 후보 비교 가능 |
| Gold compaction | 필요성 관측 후 도입 | 보류 |
| snapshot | 7일 + 최소 최근 10개 유지 | 초기 정책, 비용·복구 요구로 검증 |
| snapshot expiration | daily | 초기 주기 |
| orphan cleanup | weekly, 3일보다 오래된 파일 후보 | 실행 중 writer·최장 backfill과 안전 시간 확인 후 |
| time travel | 배포 전후 비교·품질 변경·장애 분석 | 활용 방향 |
| 배포 전 snapshot tag | 중요 배포 보호 | 후속 고급 기능 |

snapshot retention과 raw 데이터 retention은 다르다. 원천 보존 기간은 아직 미정이다. compaction 후에도 snapshot이 참조하는 파일이 남을 수 있으므로 실제 저장 비용을 관측한다. 정리 작업은 긴 write/backfill과 겹치는 상황을 검증하고 실행한다. 이번 문서화는 정리 작업 실행을 뜻하지 않는다.

**이유:** 시간 범위 replay와 query pruning을 지원하면서 과도한 partition·small file을 피한다. **대가:** compaction 비용과 snapshot 보관 비용, maintenance/writer 충돌 관리가 필요하다.
