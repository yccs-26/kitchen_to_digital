# Phase 3 집계 입력 계약

현재 구현은 집계 키·단위 검증, UTC 1분 경계 및 유한 입력의 로컬 수치 집계다.
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
- 스트리밍 event_id 중복 방어의 보존 범위와 재시작 계약.
- late/watermark 및 최종·잠정 출력 정책.
- 5분 sliding 간격, Spark query/checkpoint 및 sink 멱등 저장 방식.
- 실제 Gold 물리 스키마에서 unit 저장과 규칙 버전 추적 방식.

위 기능은 이번에 구현하지 않았다. 로컬 선행 개발과 별개로 main 병합과
클라우드 통합은 기존 Phase 순서를 따른다. 공통 파일 변경은 필요하지 않았다.

검증 fixture는 `tests/fixtures/phase3/`, 테스트는 `tests/unit/phase3/`에 있다.
wire decoder는 재작성하지 않았다. 지정 handoff 원문은 실행 환경에서
읽기가 거부되어 확인하지 못했으며, 실제 Avro·publisher·validation 코드와
Gold/window 설계 문서에서 확인한 계약을 기준으로 구현했다.


## 2026-10-17 유한 입력 중복 방어와 1분 집계

`minute_aggregation.py::aggregate_minutes(events, metric_units=...)`는
한 호출의 입력 전체를 처리하고 자연키 순서의 불변 결과 tuple을 반환한다.
기존 `assign_metric_window`, UTC 정규화기, `classify_event_identity`를 재사용한다.
공통 파일·Avro·Phase 2 코드는 수정하지 않았다.

- event_id 비교는 window별이 아니라 호출 전체에서 수행한다. 같은 ID가 다른
  window로 이동한 payload도 충돌이다. transport metadata는 비교에서 제외한다.
- 시각을 UTC로 정규화한 뒤 기존 9개 payload 필드를 비교한다. 동일 payload는
  한 번만 반영한다. 다른 ID의 동일 payload는 별도 샘플로 집계한다.
- 같은 ID의 충돌 payload는 `EventConflictError(event_id, differing_fields)`로
  전체 호출을 실패시킨다. first/last winner를 반환하지 않으며 부분 집계 결과도
  반환하지 않는다. Quarantine 발행이나 영속 오류 저장은 구현하지 않는다.
- 키는 `(window_start, window_type, store_id, equipment_id, metric_name)`이며
  `window_type=1m_tumbling`이다. UTC `[start,end)`와 기존 unit 정책을 유지한다.
  unit은 결과 속성이며 키에 포함하지 않는다. metric 이름 축약·단위 변환은 없다.
- 결과는 count, sum, avg, min, max다. count는 고유 event_id의 샘플 수다.
  avg=sum/count이며 시간 가중 평균이 아니다. 빈 입력은 빈 tuple이고 비어 있는
  window는 만들지 않는다. finite 수치만 허용하고 bool은 거부한다.
- 전체 고유 입력을 메모리에 보관한다. 정렬과 math.fsum으로 입력 순서에 따른
  합산 차이를 줄인다. double 정밀도 한계는 남고 합계 범위 초과는 예외로 전파된다.
  전체 도메인 검증은 upstream 책임이며 이 함수는 decoder를 대체하지 않는다.

### 검증

새 테스트를 먼저 실행했을 때 모듈 미구현으로 수집 단계에서
`ModuleNotFoundError`가 났다. 이는 assertion 실패와 구분한다.
구현 후 `tests/unit/phase3/test_minute_aggregation.py`의 25개 사례를 포함해:

```text
.venv/bin/python -m pytest -q tests/unit/phase3
79 passed

.venv/bin/python -m pytest -q tests/unit
793 passed, 1 warning
```

warning은 기존 Authlib/httpx deprecation이다. 1분 경계 전후·timezone 동등성,
중복 count/sum 방어·avg/min/max·차원 분리·9필드 충돌·unit/수치 방어·입력 보존을
검증한다. 이전 222건은 이번 실행 수치로 인용하지 않는다.

### Spark 구현 전에 결정할 항목

1. ID 상태의 보존 시간, event-time watermark와 eviction 관계, 늦은 재전달 처리.
   이 함수는 호출 간 상태가 없으므로 foreachBatch마다 호출하는 것만으로는
   배치 간 중복 제거가 되지 않는다. 재시작 시 상태 복원 방식도 필요하다.
2. 충돌 시 query 중단 또는 증거 보존 후 격리 정책. 이번 로컬 예외 정책만으로
   운영의 poison record·부분 쓰기·재시도 문제가 해결되지 않는다.
3. append/update 출력, window 확정 시점, watermark 이후 수정 또는 별도 정정 경로.
4. Gold 자연키 기반 영속 멱등 쓰기와 checkpoint 실패 후 재실행 수렴 방식.
5. 규칙 버전별 unit 변경, 수치 정밀도·합계 overflow 처리와 Spark 결과 허용 오차.

Spark Job·Gold sink·5분 sliding은 미구현이며 클라우드 실행을 하지 않았다.
이번 결과는 Phase 3 전체 완료가 아닌 순수 Python 집계 계약 검증이다.
