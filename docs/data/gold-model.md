# Gold 데이터 모델

상태: 확정 설계, 구현·검증 전 · 갱신: 2026-09-24

[설계 문서 목록](../architecture/design-decisions.md)

## Gold: Fact 중심 모델과 Dimension
- **결정:** `fct_metric_window`, `fct_state_window`, `fct_alert_event`, `dim_equipment`, `dim_store`로 core를 구성하고 소비 목적별 mart는 후속 dbt 계층으로 분리한다.
- **이유:** 수치와 상태의 의미를 명확히 하면서 metric마다 물리 테이블이 늘어나는 것을 방지한다.
- **트레이드오프:** 하나의 generic 테이블보다 모델이 많고, dimension join과 추가 mart 변환이 필요하다.

| 테이블 | 행 단위 / 주요 컬럼 | 논리적 유일 키 |
|---|---|---|
| `fct_metric_window` | 장비·metric·window별 1행. window_start/end, window_type, store_id, equipment_id, equipment_type, metric_name, avg_value, min_value, max_value, sample_count | `(window_start, window_type, store_id, equipment_id, metric_name)` |
| `fct_state_window` | 장비·상태·window별 1행. window_start/end, window_type, store_id, equipment_id, equipment_type, state_name, duration_seconds, state_ratio, transition_count | `(window_start, window_type, store_id, equipment_id, state_name)` |
| `fct_alert_event` | 경보 단위. alert_id, event_id, event_time, store_id, equipment_id, alert_type, severity, detected_value, threshold_value, started_at, ended_at, status | `alert_id` 기준 설계. 동일 논리 이상 구간의 안정적인 ID 사용, lifecycle 물리 저장 정책은 후속 확정 |
| `dim_equipment` | 장비 마스터. equipment_id, store_id, equipment_type, manufacturer, model, installed_at, active_flag | equipment_id의 전역 유일성 및 이력 정책 후속 확정 |
| `dim_store` | 매장 마스터. store_id, store_name, region, timezone | store_id, 이력 정책 후속 확정 |

`window_type`은 `1m_tumbling`, `5m_sliding`로 구분한다. 계산하지 않는 집계 컬럼은 0으로 채워 의미를 왜곡하지 않는다. 상태명은 `door_open`, `dishwasher_running`처럼 충돌하지 않도록 관리해야 한다. dimension의 제조사·모델 등 상세 속성은 센서 이벤트마다 반복하지 않는다.

위 스키마는 논리 모델이며 물리 DDL이나 자동 PK 제약 선언이 아니다. 누적 metric의 `usage_delta` 저장 컬럼, 경보 ID의 재처리 안정성, dimension의 SCD 방식은 미정이다. Gold의 논리 키는 멱등 쓰기와 backfill의 기준이고 Iceberg 파티션 키와는 다르다.

## 이후 결정 반영

상태 전이 이력을 위해 `fct_state_transition` 추가 방향을 기록한다. 논리 필드: state_change_id(이전 제안명 transition_id), store_id, equipment_id, previous_state, new_state, event_time, state_started_at, previous_state_duration, trigger_event_id, transition_reason. 실제 필드명과 DDL은 미정이다.

경보는 최신 ACTIVE/RESOLVED 명칭으로 설명한다. alert_id당 현재 lifecycle 1행을 갱신할지 lifecycle 이벤트 여러 행을 남길지는 물리 모델에서 정한다. 두 경우의 유일 키는 같지 않으므로 확정 전 append 이벤트에 alert_id 단독 PK를 가정하지 않는다.

장비 운영 상태와 health는 별도 축이다. 수치형 전용 raw 계약은 마지막 제안 단계이며 상태형 집계의 입력 계약과 함께 해결해야 한다. 5분 sliding의 slide 간격은 실험 항목으로 되돌렸다. [저장 파티션](storage-design.md), [상태·경보](../streaming/state-and-alerts.md).
