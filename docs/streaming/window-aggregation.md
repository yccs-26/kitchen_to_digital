# Window 집계 규칙

상태: 확정 설계, 구현·검증 전 · 갱신: 2026-09-24

[설계 문서 목록](../architecture/design-decisions.md)

## Window aggregation과 metric별 규칙
- **결정:** `event_time` 기준 1분 tumbling과 5분 sliding을 사용하며 slide 간격은 최신 논의에서 실험 대상으로 다시 열었고 1분은 후보값이다. 조리 사이클은 우선 상태 머신으로 처리한다.
- **이유:** 분 단위 운영 요약과 최근 추세를 함께 제공하고 수치·상태·누적값에 맞는 집계를 적용한다.
- **트레이드오프:** sliding은 동일 이벤트가 여러 window에 들어가 계산량과 state가 증가한다. 상태 집계는 이전 상태와 지속시간 추적이 필요하다.

| Metric | 1분 tumbling | 5분 sliding / slide 미정 | 규칙과 이유 |
|---|---|---|---|
| `temperature` | avg, min, max, sample_count | avg, max, sample_count | 평균과 순간 극값을 함께 확인 |
| `rpm` | avg, max, sample_count | avg, sample_count | 부하·가동 추세 파악 |
| `door_state` | open_count, open_duration_seconds | open_ratio | 샘플 평균 대신 실제 열린 시간과 전이 횟수 |
| `dishwasher_state` | running/active_duration | active_ratio | cycle_count는 상태 전이에서 도출 |
| `fryer_usage` | usage_delta 또는 사용 상태의 active_duration | recent_usage_ratio | 누적 사용 시간을 평균하지 않고 차분 사용 |
| `equipment_state` | active/idle/fault 등 state별 duration | state별 ratio | 가동률·유휴율·fault 비율 |

시간 비율은 해당 상태 지속시간을 window 길이로 나눈다(1분 60초, 5분 300초). 상태 구간이 window 경계를 넘으면 겹치는 시간을 나눠 반영한다. `open_count`는 open 샘플 수가 아니라 open으로의 전이 횟수다. 식기세척기 cycle 시작의 예는 `idle → running`이다.

이전 대화의 초기 구현 우선 범위는 temperature 1m/5m, rpm 1m, door 1m/5m, equipment state 1m/5m였다. 표는 나머지 metric의 설계 방향도 함께 기록한 것이며 전부 구현되었다는 뜻은 아니다. 결측 구간·초기 상태·stale timeout, active 상태 매핑, 누적 counter reset 및 경계 간 차분 배분 규칙은 추가 결정이 필요하다. 5분 평균을 1분 평균의 단순 평균으로 대체하지 않는다.

## 이후 논의 반영

Job 3은 validated에서 numeric metric을 집계한다. Gold는 watermark 내 지연을 반영해 결과가 수렴하도록 하고 실제 output mode와 sink 제약은 구현 검증 후 결정한다. 상태 duration/ratio는 Job 4의 상태 구간과 연결하되 구체 writer 경계는 후속 설계다. 마지막 수치형 전용 계약 제안을 채택하면 door/dishwasher 등 상태 입력은 별도 계약이 필요하며, 현재 구현 범위에 자동 포함되지 않는다.
