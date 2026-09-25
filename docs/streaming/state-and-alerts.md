# 상태 머신·현재 Twin·경보

상태: 설계 방향 채택 / 규칙 숫자는 실험값 · 2026-09-24

## 서로 다른 세 가지 상태

- operational_state: 장비가 무엇을 하는지. 공통 엔진과 장비별 전이 규칙으로 구성한다.
- health_status: 관측 신뢰도·건강 상태. STALE은 데이터 freshness 부족이며 실제 장애 신호인 FAULT와 구분한다.
- alert lifecycle: 운영자가 알아야 할 조건의 발생·해제. 운영 상태와 독립 관리한다.

화구를 reference FSM으로 삼아 IDLE → PREHEATING → COOKING → CLEANING → IDLE를 구현하는 방향이다. 전이는 단일 threshold보다 hysteresis + 지속시간 조건을 사용한다. 장비마다 동일 상태·전이를 강제하지 않는다. 잘못된 전이는 현재 상태를 유지하고 원인을 기록한다. 세부 온도·전력 조건과 조리/청소 의도를 구분할 입력은 미정이다.

Spark state는 다음 이벤트 계산용이고 DynamoDB는 외부 조회용 현재 상태다. state.changes와 Iceberg는 전이 이력이다. 현재 상태와 이력 보정은 분리한다. watermark 안의 늦은 이벤트라도 DynamoDB를 과거로 되돌리지 않는다. 재정렬·buffer·history 계산 방식은 구현 전 상세화한다.

## 냉장고 Alert reference

내부 NORMAL → PENDING → ACTIVE → RESOLVED 흐름. 이전 논의의 OPEN은 최신 ACTIVE와 같은 의미로 이 문서에서 표기 통일했다. 실제 wire enum은 계약 작성 시 확정한다.

- 초과 지속 시 열기: 예시 5℃ 초과 30분.
- 정상화 지속 시 닫기: 앞선 예시 4℃ 이하 5분, 최신 논의는 N분으로 열어둠.
- 단기 정상화로 PENDING 조건이 끊기면 취소.
- 이미 ACTIVE이면 새 alert를 만들지 않고 관측 정보를 갱신.
- 같은 논리 이상 구간은 안정적인 alert_id, 상태 전이는 state_change_id를 사용.
- WARNING/CRITICAL과 YAML/config 기반 규칙은 설계 방향이며 임계값은 검증 전.
- 과거 backfill은 alert history를 보정하되 Slack 등 실시간 알림을 재발송하지 않는다.

위 온도·시간은 프로젝트 시뮬레이션 규칙 예시이며 실제 현장 안전 기준을 확정한 것이 아니다. 샘플 누락 중 지속시간 인정, STALE 전환, 정상 복귀, alert correction 정책은 미정이다. 이벤트가 멈췄을 때 event-time만으로 timeout이 진행된다고 가정하지 않는다.

**이유:** 순간 노이즈와 알림 반복을 줄이고 운영 상태·센서 단절·실제 장애를 명확히 한다. **대가:** timer·상태 복구·late-event 순서 처리가 복잡하다. [흐름도](../architecture/diagrams.md), [저장 모델](../data/storage-design.md).
