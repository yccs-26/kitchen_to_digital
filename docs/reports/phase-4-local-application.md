# Phase 4 — Autonomous Local Integration Readiness Batch

작성일: 2026-10-23 (Asia/Seoul). Worktree:
`/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`.
branch=`feat/phase-4-twin-state`, HEAD=`a1effcbd13a62a98a19d98187b2680ddf6a8db8b`.
Task A~F의 **로컬 구현/설계 완료**. Spark Job 4/운영 Sink/E2E는 미구현·미검증이다.

## 1. 시작 상태와 보존

pwd/branch/status/log를 확인했다. 직전 Health Registry Adapter commit은 반영되어 있지만
직전 Output & Recovery 배치 A~E는 아직 미커밋이었다. 기존 코드·테스트·문서가 모두
존재함을 확인하고 baseline 158개(추적+미추적 파일)의 SHA-256을 저장했다.

기존 미커밋을 삭제/덮어쓰기/revert/stash하지 않았다. 새 모듈을 추가하고 기존 output
계약과 verification에는 append만 했다. 원문 prefix bytes도 확인한다. 기존 FSM/Qualifier/
Registry/Health/Codec/Timer 및 직전 배치의 Envelope/Ledger/Fake/조정 코드·테스트는
변경하지 않았다. dependency/lock/Phase 2/3·checkpoint도 변경하지 않았다.

## 2. Task별 변경 파일·결과

| Task | 변경 파일 | 결과 |
|---|---|---|
| A Operational Codec | [operational_codec.py](../../streaming/state/operational_codec.py), [codec 테스트](../../tests/unit/phase4/test_operational_codec.py), [복원 테스트](../../tests/unit/phase4/test_operational_recovery.py) | 신규 unit 30개 |
| B State wire | [change_wire.py](../../streaming/state/change_wire.py), [StateChangeEvent 초안](../../schemas/avro/state_change_event.avsc), [테스트](../../tests/unit/phase4/test_change_wire.py) | 신규 unit 21개, offline compatibility |
| C Application | [application.py](../../streaming/state/application.py), [테스트](../../tests/unit/phase4/test_state_application.py), [명시 fixture](../../tests/fixtures/phase4/application_fixture.py) | 신규 unit 28개 |
| D Timer 통합 | [application_timer.py](../../streaming/state/application_timer.py), [테스트](../../tests/unit/phase4/test_application_timer.py) | 신규 unit 10개 |
| E 통합/재시작 | [local application 테스트](../../tests/integration/test_phase4_local_application.py) | 신규 local integration 19개 |
| F 운영 설계/증거 | [Job 4 계획](../streaming/state-job4-integration-plan.md), 이 보고서, [출력 계약](../streaming/state-output-contracts.md), [verification](phase-4-verification.md) | 설계/문서 검사 완료, 실제 Spark 실행 없음 |

총 신규 108개 = unit 89 + local integration 19다. Task A 최초 29개/Phase 4 647개,
B 최초 21개/Phase 4 668개, C 최초 25개, D 10개와 당시 Phase 4 703개가 통과했다.
마지막에 정책 수치 표현·입력 UTC/double·정책 불일치 테스트를 보강해 최종 707개가 됐다.
F는 문서 설계만 수행하고 같은 최종 회귀/문서 검사로 검증했다.

## 3. Operational Snapshot wire와 복원

`encode_operational_snapshot(ProfileSnapshot, binding=...) -> bytes`,
`decode_operational_snapshot(bytes, binding=..., hood_policy=...) -> ProfileSnapshot`.
version=operational-snapshot-v1. binding/profile/input kind, engine version, snapshot subtype,
policy fingerprint, 모든 state/candidate/cursor/receipts와 digest를 명시적으로 저장한다.
Fryer의 Mode/Cleaning/CleaningCompleted/Fault receipt는 명시 kind로 구분한다. legacy
StateObservation.health_status는 원래 필드로 보존할 뿐 새 Health 판정에 사용하지 않는다.

Hood threshold는 Core identity와 같이 float, 지속시간은 정수 μs로 canonical 표현해
600과 600.0의 동등 정책이 다른 bytes가 되지 않게 했다. 원장 순서·후보 공존·identity·
UTC μs Z·enum/boolean/type/필드·digest를 검사하고 기존 모델의 불변식도 재사용한다.
동일 Snapshot 직렬화와 전체 필드 round-trip, 매 단계 복원 후 native 전이/ID를 비교했다.
Health Codec bytes와 이전 opaque Operational image는 거부하며 자동 migration하지 않는다.

초기 missing-field 테스트가 digest를 다시 붙이는 seal helper를 호출해 한 번 실패했다.
테스트의 payload 구성만 canonical missing-field 입력으로 바로잡고 전체 관련 테스트를
재실행했다. 기존 테스트를 삭제하거나 도메인 기대값을 바꾸지 않았다.

## 4. State Changes Avro 초안

wire version=state-change-avro-v1, record=ktd.state.StateChangeEvent.
axis enum으로 OPERATIONAL/HEALTH를 나누고 해당 state_change_id/health_change_id만 non-null이다.
store/equipment/type/profile, 이전/현재 state, 원인/평가 시각, Health last_seen/deadline,
revision/fingerprint/timer provenance와 원본 canonical Envelope bytes/digest를 보존한다.
nullable 필드는 null-first union/default null이며 두 ID를 함께 채우지 않는다.

설치된 fastavro로 encode/decode, avro 라이브러리로 reader/writer 호환성을 검사했다.
optional/default 추가는 호환되고 required 무기본값/타입 변경/Health enum 제거는
비호환이다. Reader resolution도 수행했다. 실 Registry BACKWARD/TRANSITIVE 설정을
조회하거나 등록하지 않았으며 기존 numeric sensor schema/subject도 바꾸지 않았다.

Confluent framing은 caller가 명시한 schema ID로만 로컬 생성/검증한다. 테스트의 17/23/
123은 등록 증거가 없는 fixture ID다. 잘못된 key/frame/schema ID/outer fields/trailing
bytes를 거부한다. 같은 Envelope+schema context는 같은 wire bytes이고,
canonical_change가 주어졌을 때 동일 ID·다른 payload를 명시 거부한다. 영속 canonical
판정은 기존 Ledger 책임이다. 실제 운영 Avro bytes/schema ID를 outbox에 고정하는
단계는 runtime 통합 게이트이며 현재 Ledger는 canonical JSON 의도를 보존한다.

## 5. Local Application 책임과 API

LocalStateApplication은 기존 EquipmentFsmRegistry/EquipmentHealthRegistry와 명시적인
HoodPolicy map을 주입받는다. 별도 profile 설정 저장소나 운영 timeout/source/default를
만들지 않는다. initialize_operational/initialize_health는 caller의 명시 상태/generation을
저장하며, restart는 initialize로 덮어쓰지 않고 load 메서드로 복원한다.

- process_validated(mapping, command_ref=..., reference_time=...): 정확한 numeric
  10필드, aware 시간과 유한 double을 검사한다. Phase 2 UTC normalizer를 재사용한다.
  source/metric/unit/version/identity/profile는 기존 Registry가 검사한다.
- numeric Fryer는 Health만 계산한다. Hood는 두 Registry를 각각 호출한다.
- process_signal(...): 별도 명시 synthetic Fryer 경로다. 실제 controller/validated
  권한이 아니다. numeric 유온을 mode/cleaning/fault로 바꾸지 않는다.
- load_operational/load_health와 reconcile(coordinator): typed 복원과 기존 pending
  재처리. coordinator/store가 다르면 거부한다.

기존 Core의 DUPLICATE/ID_CONFLICT/OUT_OF_ORDER/SAME_TIME/NO_CHANGE/TRANSITION 등은 그대로
반환한다. 입력 계약 오류는 Application/Registry 예외, CAS 충돌은 OutputConflict,
저장/전달 장애는 원래 예외로 전파한다. 무효 입력을 정상 성공으로 삼키지 않는다.

## 6. 원자성·결정성·Current-only

계산 결과는 모든 admission 검사 후 메모리에 준비하고, 각 축의 root Snapshot bytes,
기록 입력·평가 시각·Core 판정·출력 의도를 기존 PublicationStore transaction으로 저장한다.
Operational의 Current image digest만 새 versioned Codec bytes에 맞춰 **commit 전에**
재봉인한다. 기존 native 변경 Envelope/ID는 바꾸지 않는다. 이전 opaque image 기준 digest를
사용하면 store의 Snapshot/intent 대조가 실패하므로 새 연결 경계에서만 변환했다.

두 축 전체를 한 transaction으로 보장하지 않는다. 첫 축 commit 후 실패하면 같은
command_ref/input/evaluation_time 재실행에서 그 축의 저장 결과/의도를 재사용하고 남은
축을 계산·확정한다. 이미 앞선 후속 명령이 있다면 오래된 replay로 root를 되감지 않는다.
같은 command_ref의 다른 payload/시각은 충돌이다. 새로운 command_ref의 같은 event_id는
기존 Core 중복 판정을 따른다. root command revision과 Health snapshot/change revision은
서로 다른 counter이며 event_time이나 receipts 길이로 상태 변경 순서를 만들지 않는다.

건강 표시가 같은 신규 관측과 Operational 후보 갱신도 필요한 Current-only 의도로
남긴다. History/topic에 가짜 상태 변화는 없다. 같은 event를 두 축이 보더라도 Snapshot은
독립이다. 두 새 DB에서 같은 기록 명령을 실행해 전체 Snapshot/History/Kafka/Current와
wire bytes가 동일함을 비교했다.

이 원자성은 로컬 SQLite에 한정되며 Spark checkpoint/외부 Sink 원자성이 아니다.
Application은 원천 Kafka key/offset·운영 admission/명령 번호 발급을 구현하지 않는다.
Caller가 기록 순서를 제공하고 오류를 skip하지 않아야 한다.

## 7. Timer·publication 연결

LocalHealthTimers.plan은 기존 token/deadline 계획을 반환한다. evaluate는 명시 평가 시각과
기존 HealthRegistry/Coordinator의 결과로 Snapshot/STALE 의도를 같은 ledger에 저장한다.
새 scheduler/자동 tick/background thread/현재 시각 읽기는 없다.

deadline 직전 WAITING, 등호 STALE, 무관측 boot INITIALIZED STALE, 새 관측 HEALTHY 복구,
옛 generation·다른 장비/store/profile token 무효, 평가 시각 역행·반복 tick을 검사했다.
같은 시각 tick→관측은 STALE/HEALTHY 두 변화, 관측→tick은 옛 token 무효라는 기존 순서다.
복원 후 같은 timer/ID를 재구성하고 저장된 pending 출력은 Core의 빈 replay와 독립적으로
재전달한다. Fake Kafka는 ACK 유실 때 동일 payload 중복이 남는다.

## 8. 통합 20개 요구와 실제 검증

각 행은 관련 unit/integration assertion으로 실제 비교했다. 단일 테스트 개수와 요구
번호가 일대일인 것은 아니다. 새 integration은 19 cases, 실제 프로세스 종료는 13 cases다.

| 요구 | 조건/기대 | 실제 결과 |
|---|---|---|
| 1 Fryer Operational | explicit signals로 reference cycle | 모든 전이와 IDLE 복귀 비교 통과 |
| 2 Hood Hysteresis | 0/1/3초 연속 RPM·deadband·정지 | 후보 보존·ROTATING→STOPPED 비교 통과 |
| 3 HEALTHY 초기화 | 최초 유효 관측 | native INITIALIZED/last_seen 확인 |
| 4 STALE | 정확 deadline/무입력 boot tick | deadline/STALE 또는 INITIALIZED STALE 확인 |
| 5 HEALTHY 복구 | STALE 뒤 fresh 관측 | revision/변경 이력과 Current 확인 |
| 6 중복·역순 | 다른 명령의 기존 ID/옛 event_time | 기존 Core decision, Snapshot/후보 보존 |
| 7 동시각 충돌 | 같은 ID 변조/다른 ID 같은 시각 | ID_CONFLICT/SAME_TIME 구분 |
| 8 identity 격리 | 다른 store/type/equipment·두 장비 | 거부/독립 roots·Current 확인 |
| 9 profile/version | 잘못된 키/변경 policy | 계산 전 거부/restore 불호환 |
| 10 Codec 복원 | 전체 Snapshot/후보/ledger 파일 왕복 | 원본과 모든 필드·후속 전이/ID 동일 |
| 11 process restart | 앱 저장 경계 5/Timer 3/전달 5 지점 종료 | exit 81 후 reopen/부분 축 재개 통과 |
| 12 동일 입력 replay | cached 명령·두 새 DB의 같은 순서 | root rollback 없음/동일 결과 |
| 13 동일 Change ID | Core/복원/재실행 | native ID와 Envelope bytes 비교 |
| 14 History 멱등성 | ACK 뒤 죽고 재쓰기 | canonical row 중복 없음 |
| 15 Fake Kafka 재발행 | ACK 직후 종료 | 같은 payload 2건 가능, 같은 ID 유지 |
| 16 Current 최신성 | 반복/역순 입력·두 축 갱신 | 기존 revision 보호·축 독립 유지; 기존 Fake CAS 회귀도 통과 |
| 17 Sink 부분 실패 | history/kafka/current/완료 경계 | pending 재조회 후 수렴 |
| 18 timer 경합 | 같은 시각 양쪽 순서 | 각각의 정확한 이력 유지 |
| 19 유효 no-transition | HEALTHY 유지/후보 축적 | Current-only 보존·가짜 History 0 |
| 20 미지원 장비 | refrigerator/freezer/dishwasher | 명시 거부, Fryer fallback 없음 |

실제 exit 지점: after_calculation/before_operational_commit/after_operational_commit/
before_health_commit/after_health_commit 5개, Timer after_calculation/before_health_commit/
after_health_commit 3개, 전달 after_history/before_kafka/after_kafka/after_current/
before_complete 5개다. 프로세스를 새로 실행하고 SQLite 파일을 다시 열었다.

추가로 기존 Phase 2 decode_sensor_event를 로컬 Registry double과 실제 numeric Avro body에
적용해 Application으로 연결했다. 실제 Kafka 소비·Job 2 전체 품질/런타임 검증은 아니다.
WireCheckingKafka fixture는 실제 Avro 왕복 후 기존 Fake의 canonical JSON을 저장한다.
Fake가 Avro broker log를 구현하거나 실제 producer ACK를 증명한 것은 아니다.

## 9. 환경·명령·결과

Python 3.11.15, SQLite 3.50.4, fastavro 1.12.2, avro 1.12.2,
confluent-kafka 2.15.1, pytest 9.1.1. 추가 설치/의존성 변경은 없었다. .venv의 PySpark는
미설치다. 모든 새 테스트는 외부 서비스를 쓰지 않으므로 기존 integration marker를
붙이지 않았고, 로컬 회귀에도 포함했다. 외부 integration은 명시적으로 제외했다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-readiness-final-new tests/unit/phase4/test_operational_codec.py tests/unit/phase4/test_operational_recovery.py tests/unit/phase4/test_change_wire.py tests/unit/phase4/test_state_application.py tests/unit/phase4/test_application_timer.py tests/integration/test_phase4_local_application.py
# 108 passed, 1 warning in 3.66s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-readiness-e-integration-final tests/integration/test_phase4_local_application.py
# 19 passed, 1 warning in 2.72s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-readiness-final-phase4 tests/unit/phase4
# 707 passed, 1 warning in 1.72s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-readiness-final-local -m 'not integration'
# 1458 passed, 10 deselected, 1 warning in 7.70s

git diff --check
# 통과
```

Phase 4 707개 실행 뒤 추가한 2개는 integration 파일에만 있고 마지막 전체 회귀에도
포함됐다. 경고는 기존 Authlib httpx deprecation 1건이다. 실제 외부 integration 10개는
실행하지 않았고 local recovery 결과를 외부 E2E로 표현하지 않는다.
새 Python 11개 AST/79열/whitespace, Avro schema parse/호환성, 문서 링크·표·변경 범위를
검사했다. baseline 158개 중 output 계약/verification만 append로 바뀌고 나머지 156개는
그대로다. 직전 미커밋 코드/테스트도 SHA-256 대조로 보존했다.

## 10. 미구현·위험·Phase 4 DoD

운영 Snapshot/outbox 저장소·입력 명령 admission/순번·writer fencing·multi-dispatcher,
실제 Schema Registry/Avro 전달·Kafka/DynamoDB/Iceberg·Spark query/checkpoint/timer,
정책/opaque Snapshot migration·State Rebuild·일부 Current 조회 필드는 미구현이다.
무입력 trigger 동작·μs/ms timer 변환·권한/source·운영 timeout·비용/retention을 확정하지
않았다. FAULT/Alert/미지원 장비 추론을 추가하지 않았다.

운영 통합의 선택지·기능 검증 게이트는 [Job 4 통합 계획](../streaming/state-job4-integration-plan.md)에
있다. 공식 Spark API 문서 조회와 KTD runtime 실측을 구분했다. 실제 API/compute 실행은
없고 public 문서만 조회했다. 기존 checkpoint를 삭제/변경하지 않는다.
Phase 4 DoD에는 Job 4의 실제 입력·권위 state/의도·checkpoint·timer와 세 Sink failure
실측이 남는다. Local Readiness 완료를 Phase 4 전체 완료로 표현하지 않는다.

## 11. 정확한 Git 계획 — 실행하지 않은 명령

직전 배치 14개 변경이 아직 미커밋이므로 새 Task A~F만 단독 commit하면 선행 모듈이
Git 이력에 없을 수 있다. 아래는 **선행 변경 검토 후 P1~P5 → 이번 A~F**로 나누는
계획이다. 공유 output/verification 문서는 두 배치 내용이 함께 있으므로 마지막 F에
현재 문서 전체를 포함한다. 기존 보고서의 과거 staging 제안을 현재 상태에 맞게 보완한
것이며 실제 staging/commit은 하지 않았다. 파일 전체 diff를 사용자가 먼저 검토한다.

```sh
ktd_readiness_root='/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state'
cd "$ktd_readiness_root"

# P1~P5: previous batch prerequisites, still uncommitted
git add -- "$ktd_readiness_root/streaming/state/change_envelope.py" "$ktd_readiness_root/tests/unit/phase4/test_change_envelope.py"
git commit -m "feat(phase4): add deterministic state change envelopes"
git add -- "$ktd_readiness_root/streaming/state/publication_store.py" "$ktd_readiness_root/tests/unit/phase4/test_publication.py"
git commit -m "feat(phase4): add local transactional publication ledger"
git add -- "$ktd_readiness_root/streaming/state/sink_protocols.py" "$ktd_readiness_root/streaming/state/local_sinks.py" "$ktd_readiness_root/streaming/state/publication.py" "$ktd_readiness_root/tests/unit/phase4/test_state_sinks.py"
git commit -m "feat(phase4): model state sink delivery and reconciliation"
git add -- "$ktd_readiness_root/tests/integration/test_phase4_local_recovery.py" "$ktd_readiness_root/docs/reports/phase-4-local-recovery.md"
git commit -m "test(phase4): verify local publication crash recovery"
git add -- "$ktd_readiness_root/docs/streaming/equipment-state-contracts.md" "$ktd_readiness_root/docs/streaming/equipment-health-contracts.md"
git commit -m "docs(phase4): link health and operational output contracts"

# A: explicit Operational Codec
git add -- "$ktd_readiness_root/streaming/state/operational_codec.py" "$ktd_readiness_root/tests/unit/phase4/test_operational_codec.py" "$ktd_readiness_root/tests/unit/phase4/test_operational_recovery.py"
git commit -m "feat(phase4): add versioned operational snapshot codec"

# B: local state-change Avro draft
git add -- "$ktd_readiness_root/streaming/state/change_wire.py" "$ktd_readiness_root/schemas/avro/state_change_event.avsc" "$ktd_readiness_root/tests/unit/phase4/test_change_wire.py"
git commit -m "feat(phase4): draft state change Avro wire contract"

# C: local application and explicit shared test fixture
git add -- "$ktd_readiness_root/streaming/state/application.py" "$ktd_readiness_root/tests/unit/phase4/test_state_application.py" "$ktd_readiness_root/tests/fixtures/phase4/application_fixture.py"
git commit -m "feat(phase4): connect validated events to local state application"

# D: explicit timers through the application ledger
git add -- "$ktd_readiness_root/streaming/state/application_timer.py" "$ktd_readiness_root/tests/unit/phase4/test_application_timer.py"
git commit -m "feat(phase4): integrate local health timers and publication"

# E: process death, multi-equipment recovery and wire replay
git add -- "$ktd_readiness_root/tests/integration/test_phase4_local_application.py"
git commit -m "test(phase4): verify local application restart and replay"

# F: integration plan and consolidated current evidence
git add -- "$ktd_readiness_root/docs/streaming/state-job4-integration-plan.md" "$ktd_readiness_root/docs/reports/phase-4-local-application.md" "$ktd_readiness_root/docs/streaming/state-output-contracts.md" "$ktd_readiness_root/docs/reports/phase-4-verification.md"
git commit -m "docs(phase4): define Job 4 integration readiness and gates"

# Only after review and commits:
git push origin feat/phase-4-twin-state
```

origin=`https://github.com/yccs-26/kitchen_to_digital.git`를 read-only로 확인했다. fetch/push는
하지 않았다. main merge는 Phase 2/3 선행 병합과 전체 Phase 4 DoD 이후에만 검토한다.

## 12. 포트폴리오 증빙

9절의 신규 108개/로컬 Application 19개/Phase 4 707개/비외부 회귀 1458개 출력을
구분해 촬영할 수 있다. 제목은 “Validated Mapping → Local Application → SQLite/Fake
출력·재시작 검증”으로 한다. 실제 Avro 왕복·offline 호환성·13개 프로세스 종료 assertion을
함께 설명하되 실제 Schema Registry/broker/DBR/DynamoDB/Iceberg E2E라고 표현하지 않는다.
이번 승인 배치는 여기서 종료하고 다음 구현·Git 작업은 자동 진행하지 않는다.
