# 전달 보장과 멱등성 — 면접 준비

설계 의도를 설명하는 개인 메모다. 구현·검증이 끝나기 전에는 완료 실적으로 표현하지 않는다.

[설계 기준](../streaming/delivery-and-idempotency.md)

## Kafka manual commit과 at-least-once

“재전달 가능성을 인정하고, 처리 후 commit과 저장 계층 멱등성을 조합했습니다.”

## Spark dedup과 Iceberg 멱등 쓰기

“실시간 중복 제거와 최종 저장 멱등성은 서로 다른 실패 구간을 방어합니다.”
