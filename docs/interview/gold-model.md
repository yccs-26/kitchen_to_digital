# Gold 데이터 모델 — 면접 준비

설계 의도를 설명하는 개인 메모다. 구현·검증이 끝나기 전에는 완료 실적으로 표현하지 않는다.

[설계 기준](../data/gold-model.md)

## Gold: Fact 중심 모델과 Dimension

“공통 집계 구조를 가진 numeric metric은 통합하고, 지속시간 중심 상태와 업무 경보는 별도 fact로 분리했습니다.”
