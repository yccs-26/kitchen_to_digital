# Kafka 토픽·파티션·신뢰성

상태: 구조 채택, 수치는 초기 가설 · 2026-09-24

| 토픽 | 역할 | 파티션 초기 후보 |
|---|---|---:|
| kitchen.sensor.raw | 검증 전 원본, Job 1/2 독립 입력 | 6 |
| kitchen.sensor.validated | Job 2의 품질 검증·dedup 후 실시간 trusted event | 6 |
| kitchen.sensor.quarantine | 데이터 계약·업무 품질 실패 | 3 |
| kitchen.state.changes | 상태 전이 이벤트 | 6 |
| kitchen.alerts | 업무 경보 lifecycle 이벤트 | 3 |
| kitchen.sensor.reprocess | 원인 수정 후 명시적 개별 재처리 | 3 |
| kitchen.sensor.dlq | bounded retry를 소진한 처리·시스템 실패 | 미정 |

파티션 수 6은 사용자가 고정하지 않도록 명시한 baseline이다. 3/6/12를 부하 테스트한다. 장비별 key는 equipment_id, 센서 이벤트 identity는 event_id다. 매장 간 ID 중복 여부와 복합 키 전환 필요성은 확정해야 한다. 잘못된 메시지에 장비 ID가 없을 때 오류 토픽 key 규칙도 미정이다.

동일 key의 Kafka 기록 순서와 event_time 순서는 같다고 가정하지 않는다. 파티션 증설 시 key 매핑·진행 중 상태의 영향을 검증한다. 독립 목적의 Job은 [별도 소비 경계](platform.md)를 사용한다.

## 환경별 신뢰성 초기안

- 평상시 로컬 개발: RF=1. min ISR은 단일 broker 환경에 맞춰 구성한다.
- 신뢰성·chaos 테스트: broker 3개, RF=3, min.insync.replicas=2, producer acks=all.
- producer idempotence를 활용하되 sink 멱등성을 대체한다고 보지 않는다.
- 직접 구현하는 Kafka consumer: enable.auto.commit=false, 성공한 sink 또는 오류 보존 ACK 뒤 commit.
- Spark는 checkpoint로 진행 상태를 복구한다. 직접 consumer의 commit 루프와 구분한다.

**이유:** 장애 시 유실보다 재처리를 선택하고 역할별 확장·복구를 가능하게 한다. **대가:** 복제 비용, ISR 부족 시 쓰기 거부, hot key와 partition blocking 가능성. 초기 별도 retry 토픽은 도입하지 않고 실제 blocking이 관측되면 검토한다.

retention 기간, producer timeout/retry 횟수, DLQ 파티션 수는 아직 미정이다. 관련: [실패·재처리](../operations/late-events-and-backfill.md), [실험](../experiments/performance-plan.md).
