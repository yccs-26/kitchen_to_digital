# Phase 3 집계 입력 계약

이번 구현은 집계 키·단위 검증과 UTC 1분 경계의 로컬 순수 함수다.
Gold 집계 완료나 Spark·클라우드 통합 검증을 의미하지 않는다.

## 동작 흐름

1. 기존 decoder가 만든 timezone-aware datetime을 받는다. wire string을
   전달하면 기존 `normalize_event_time`을 재사용한다. naive 시각은 거부한다.
2. UTC로 변환한 시각의 초·마이크로초를 내리고 1분을 더해 `[start, end)`를
   만든다. 정확히 end인 이벤트는 다음 window에 속한다.
3. 호출자가 주입한 기존 `validation_rules.json`의 `metric_units` 구조로
   장비 유형별 metric/unit을 확인한다. 이름 축약과 단위 변환은 하지 않는다.
4. `(window_start, window_type, store_id, equipment_id, metric_name)` 키와
   `window_end`, 검증된 `unit`을 반환한다. window_type은 `1m_tumbling`이다.

unit은 키의 일부가 아니다. 같은 규칙 묶음에서 동일 metric에 상이한 단위를
설정하면 거부한다. 호출자는 처리 단위 동안 일관된 규칙 묶음을 사용해야 한다.
장비 등록·필수 Avro 필드·수치 유효성 전체 검증은 기존 upstream의 책임이다.
순수 함수는 파일·네트워크·시계·상태 저장에 접근하지 않는다.

## 후속 결정

- 규칙 버전 간 단위 변경과 과거 결과의 변환·이관 정책.
- downstream event_id 중복 방어의 보존 범위와 재시작 계약.
- 수치 집계, late/watermark 및 최종·잠정 출력 정책.
- 5분 sliding 간격, Spark query/checkpoint 및 sink 멱등 저장 방식.
- 실제 Gold 물리 스키마에서 unit 저장과 규칙 버전 추적 방식.

위 기능은 이번에 구현하지 않았다. 로컬 선행 개발과 별개로 main 병합과
클라우드 통합은 기존 Phase 순서를 따른다. 공통 파일 변경은 필요하지 않았다.

검증 fixture는 `tests/fixtures/phase3/`, 테스트는 `tests/unit/phase3/`에 있다.
wire decoder는 재작성하지 않았다. 지정 handoff 원문은 실행 환경에서
읽기가 거부되어 확인하지 못했으며, 실제 Avro·publisher·validation 코드와
Gold/window 설계 문서에서 확인한 계약을 기준으로 구현했다.
