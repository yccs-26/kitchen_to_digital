# Kafka 토픽과 파티션 키 — 면접 준비

설계 의도를 설명하는 개인 메모다. 구현·검증이 끝나기 전에는 완료 실적으로 표현하지 않는다.

[설계 기준](../architecture/kafka-topics.md)

## Kafka 토픽과 파티션 키

“장비별 처리 순서를 위해 `equipment_id`, 재전달 중복 방지를 위해 `event_id`를 사용하도록 역할을 분리했습니다.”
