# Phase 4 검증 기록 — P4-1 순수 operational FSM

검증일: 2026-10-17 (Asia/Seoul). 브랜치 `feat/phase-4-twin-state`,
시작 HEAD `9600ee9aabc47d1c85a329ba5a4900940465e835`.
`b8c6ee2`의 후속 커밋임을 확인했다. 시작 Git status는 clean이었다.
기존 신호 타입·형태 테스트·synthetic fixture를 재사용했다.

## 구현한 한 단위

| 파일 | 변경 |
|---|---|
| `streaming/state/contracts.py` | 명시적 청소 정상 완료 payload 추가, 기존 후보 유지 |
| `streaming/state/fsm.py` | immutable snapshot, 순수 전이, 판정 이유, 전이 출력 의도·안정 ID |
| `streaming/state/README.md` | 신호 제안·전이표·시간/중복/재실행 정책·통합 한계 |
| `tests/fixtures/phase4/reference_cycle.json` | synthetic 4단계 정상 순환 |
| `tests/unit/phase4/test_fsm.py` | 정상/금지/중복/충돌/시각/identity/결정성 검증 |
| `docs/reports/phase-4-verification.md` | 이번 증거와 미검증 경계 |

순수 함수는 이전 상태, synthetic 명시적 신호, aware 기준 시각을 받는다.
IDLE→PREHEATING은 예열 모드 보고, PREHEATING→COOKING은 조리 모드 보고,
COOKING→CLEANING은 청소 활성, CLEANING→IDLE은 명시적 청소 정상 완료로만
전이한다. 전체 허용/금지 표와 no-op 조건은 `streaming/state/README.md`에 있다.
청소 비활성이나 IDLE 보고만으로 청소 완료를 판단하지 않는다.

동일 ID·동일 전체 입력은 DUPLICATE, 동일 ID·다른 내용은 ID_CONFLICT다.
store_id/equipment_id 불일치, 미래 사건, 역순 사건, distinct ID 동시각,
마지막 소비 시각 이전 기준 시각을 거부한다. 시간 검사 통과 후 금지 전이도
원장에 남겨 같은 입력을 다른 상태에서 새 전이로 재해석하지 않는다.
원장과 상태는 호출자가 전달하며 전역 cache·시계·외부 I/O가 없다.

Health는 operational state와 분리해서 그대로 보존한다. FaultSignal은
HEALTH_DEFERRED이며 health 발생/해제 구현 완료가 아니다. 전이 차단 interlock,
다중 장애, STALE/freshness, last_seen timer는 미구현이다.

## 확인한 입력과 기준 문서

현재 10필드 Avro·domain rules·validated publisher를 직접 확인했다.
기존 sensor event_id, equipment_id key, UTC event_time 계약을 변경하지 않았다.
새 synthetic signal_id는 별도 신호의 identity로 기존 event_id의 대체물이 아니다.
실제 validated numeric 이벤트를 이 FSM으로 연결하는 adapter는 없다.
CLEANING/FAULT를 수치로 추정하거나 metric_name으로 몰래 추가하지 않았다.

지정 실행 계획은 원본 checkout의
`docs/plans/ktd-forward-execution-plan-2026-10-06.md`에서 읽었다.
기존 상태 설계도 함께 확인했다. 계획서 사본을 추적 파일로 복사하지 않았다.
`ktd-worktrees/handoff/validated-contract.md`는 일반 읽기 및 권한 확장 읽기
모두 OS의 `Operation not permitted`로 실패했다. 따라서 handoff 원문과의 최신
대조는 **미검증**이다. 코드 확인을 handoff 본문 확인으로 대신 표기하지 않는다.

## 실제 실행한 테스트

저장소 루트에서 실행했다. 모든 명령은 bytecode/cache 기록을 끄고 이 작업
전용 임시 디렉터리를 사용한다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p41-unit tests/unit/phase4
# 131 passed in 0.12s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p41-regression tests/unit
# 845 passed, 1 warning in 5.18s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-p41-full -m 'not integration'
# 845 passed, 10 deselected, 1 warning in 3.05s
```

마지막 PEP 8 줄 길이 정리 후 같은 전체 로컬 회귀를
`--basetemp /private/tmp/ktd-p41-final`로 다시 실행했다.
결과: `845 passed, 10 deselected, 1 warning in 3.19s`.
테스트 인자 줄바꿈을 마무리한 뒤 `--basetemp /private/tmp/ktd-p41-style`로
Phase 4 단위를 재확인했다: `131 passed in 0.08s`.
경고는 설치된 Authlib httpx 모듈 deprecation 1건이다.

검증 시나리오: 4상태 × 6개 operational payload의 전체 조합, 모든 health
초기값에서 정상 순환, 모든 상태/health 조합에서 fault 입력의 분리,
과거 ID 재입력·변조·금지 입력 재입력, 장비/매장 불일치, 미래→정상 재시도,
역순/동시각/기준 시각 역전, UTC 동등성, 동일 snapshot 재계산·처리 지연 후
동일 출력, 잘못된 시간·numeric payload·불일치 원장 거부.

## 보장 한계와 후속 결정

- synthetic fixture 기반 순수 Python 증거다. 실제 센서 신호·Kafka/MSK·Spark·
  Databricks·DynamoDB·Iceberg E2E 증거가 아니다. 외부 실행은 하지 않았다.
- 제어기 source 권한, 실제 장비 대응, 별도 신호 wire 계약/전송 경로,
  event_id 대응, 실제 청소 완료 의미는 통합 전 확정해야 한다.
- 이번 정책은 명시적 관측 보고를 즉시 적용한다. 수치 threshold·hysteresis·
  지속시간·장비별 정책은 구현하지 않았다. 운영/안전 제어 규칙으로 검증하지 않았다.
- 동시각은 먼저 소비한 신호만 인정하며 입력 순서를 바꾼 결과 수렴은 보장하지
  않는다. timestamp만으로 제어기 순서를 복원하지 않는다. 재정렬·sequence는 후속이다.
- 전체 원문 tuple 원장은 로컬 참조용이다. 무제한 크기·선형 검색/복사 비용이
  있으며 운영 dedup 보존/복구 정책과 checkpoint 직렬화는 미검증이다.
- state_change_id와 출력 의도는 결정적이지만 전달 ACK가 아니다. 다음 snapshot
  확정 후 replay는 DUPLICATE로 출력이 없다. 실제 I/O 연결 전에 outbox/부분 실패
  복구·확정 순서를 설계해야 한다. 분산 exactly-once를 보장하지 않는다.
- `config/state_machine.yaml`은 만들지 않았다. 이 단위는 버전이 고정된 명시적
  보고 정책이며 설정 로더 I/O와 미사용 설정은 추가하지 않았다.
- Phase 2/3·Avro는 수정하지 않았고 Job/producer/sink를 추가하지 않았다.
  P4 전체 DoD 완료 또는 main 병합 준비 완료가 아니다.

## Git 및 사용자 검토

작업 브랜치와 HEAD를 유지하고 staging/commit/push/merge를 수행하지 않았다.
권장 commit 범위는 위 6개 파일만이며 메시지 제안은
`feat(phase4): add deterministic synthetic reference FSM`이다.
원문 handoff 대조 한계, 청소 완료의 분리, 시간·중복 정책과 health 유보 범위를
검토한 뒤 사용자가 직접 commit한다. 다음 구현 단위는 자동으로 진행하지 않는다.
테스트 화면을 증빙으로 남긴다면 'P4-1 synthetic 순수 FSM 로컬 단위 검증'으로
표시하고 명령·결과와 연결한다.
