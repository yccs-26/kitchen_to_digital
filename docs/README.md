# KTD 문서

## 구현·환경·검증

- [로컬 환경과 실행](local-development.md): 필수 설정, 실행·검증 명령과 복구 주의사항.
- [데이터 계약](data/data-contract.md): 현재 Avro 필드와 identity·버전 정책.
- [스키마 호환성·장애 검사](schema-evolution-test.md): 검사 방법, 실제 관측과 한계.
- [Phase 0 검증 보고서](reports/phase-0-verification.md): 작업 시점별 환경·입력·실행 결과.

## 설계·계획

후속 아키텍처는 구현·배포·성능 달성 기록과 구분한다.

- [전체 구조와 흐름도](architecture/diagrams.md)
- [분야별 설계 기준](architecture/design-decisions.md)
- [설계 변경 이력과 미결정 사항](architecture/planning-update.md)
- [관측·운영 설계와 Phase 로드맵](architecture/kafka-design-followup.md)
- [성능 실험 계획](experiments/performance-plan.md)
- ADR: [Local-first](adr/01-local-first-architecture.md), [Kafka 채택](adr/02-adopt-kafka-over-redpanda.md)

## 문서 작성 기준

설계·환경 문서는 목적, 선택 이유, 핵심 구조·설정, 트레이드오프와 현재 상태를 중심으로 작성한다.
상세 설명은 담당 문서에 두고 다른 문서에서는 링크한다. 명령과 코드 블록은 재현·구조 이해에 필요한 만큼만 둔다.

계약은 필드·타입·의미·변경 정책을, ADR은 맥락·결정·대안·결과를 보존한다.
Runbook은 증상·확인·복구·주의사항을 유지한다. 개발 계획의 DoD와 검증 보고서의 실제 증거는 축약 대상이 아니다.
실행 로그·증빙은 보고서에서 관리하고, 예상 결과·미실행 계획을 실제 결과와 구분한다.
