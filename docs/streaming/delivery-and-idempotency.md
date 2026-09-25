# 전달 보장과 계층별 멱등성

상태: 요구사항 채택 / Silver 구현 전략 보류 · 2026-09-24

전체 시스템을 exactly-once로 표현하지 않는다. at-least-once 재전달을 허용하고 각 결과 경계에서 중복 영향을 제어한다.

| 경계 | 식별자 | 책임 |
|---|---|---|
| Producer | event_id | 같은 물리 측정의 재전송은 동일 ID, producer idempotence 활용 |
| Bronze | Kafka source metadata | 원본과 원천 중복 보존, event_id 기반 정제 안 함 |
| Spark Job 2 | event_id | watermark 내 bounded dedup, 영구 ID 저장소가 아님 |
| Silver | event_id | 장기 replay/reprocess에서도 canonical 결과 수렴 |
| Gold | fact 논리 키 | window 재계산·멱등 갱신 |
| State Machine | event_id/time/version | 중복·오래된 이벤트로 현재 상태 되감기 방지 |
| DynamoDB | 장비 + time/version | conditional update, 동일 시각 tie-breaker 미정 |
| State changes | state_change_id | 같은 논리 전이에 안정적인 ID |
| Alert | alert_id + lifecycle 전달 구분 | 중복 부작용 억제, 생성·해제는 구분 필요 |

직접 Kafka consumer는 처리 또는 quarantine/DLQ 저장 ACK 후 commit한다. 미완료 offset을 건너뛰지 않는다. Spark query는 checkpoint 기반으로 복구하며 이 수동 commit 루프를 그대로 사용하지 않는다.

## Silver 실험 보류

사용자는 매 micro-batch MERGE와 append + 주기적 dedup/compaction을 비교한 뒤 결정하도록 명시했다. MERGE는 후보이며 확정 구현이 아니다. 요구사항은 persistent idempotency다.

append 후 정기 dedup은 정리 전 중복 노출 가능성을 함께 평가해야 한다. compaction만 수행하는 것을 event_id dedup으로 취급하지 않는다. canonical 읽기 경계·정리 작업 실패·validated 토픽 중복·Gold 오염까지 검증해야 두 전략을 공정하게 비교할 수 있다.

## 구현 전 해결할 원자성 경계

- Silver 성공 / validated 실패 및 반대 상황의 누락·중복 복구.
- DynamoDB 성공 / state.changes 또는 alerts 실패 시 재발행과 대사.
- 같은 event_id로 수정된 payload를 재처리할 때 충돌·정정 규칙.
- 같은 alert_id의 ACTIVE와 RESOLVED를 서로 중복으로 제거하지 않는 delivery identity.
- 외부 sink의 수락 후 ACK 유실 상황. 중복 억제 목표와 실제 보장 범위를 구분한다.

**이유:** checkpoint와 bounded dedup 밖에서도 결과를 보호한다. **대가:** 쓰기 비용과 복구 복잡성이 증가한다. 관련: [실험 계획](../experiments/performance-plan.md).
