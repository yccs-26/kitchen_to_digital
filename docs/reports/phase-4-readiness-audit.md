# Phase 4 — Git Reconciliation & Spark Job 4 Readiness Audit

2026-10-23 (Asia/Seoul). 실제 worktree와 오프라인 테스트 기준 감사다.
로컬 모델의 완료와 Spark/운영 전달 완료를 구분한다. 이번 배치 완료 후 중단한다.

## 1. 현재 Git과 누적 작업 상태

- 작업 경로: `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`
- branch: `feat/phase-4-twin-state`
- 시작/종료 HEAD: `a1effcbd13a62a98a19d98187b2680ddf6a8db8b`
- 시작 미커밋: tracked 수정 3 + untracked 35 = **38경로**. 종료: 수정 3 + untracked 37 = **40경로**.
- index는 비어 있고 Git 이력·branch·remote·staging은 변경하지 않았다.
- origin: `https://github.com/yccs-26/kitchen_to_digital.git` (git remote -v 로컬 설정 확인).
- pwd/branch/HEAD/status/diff --check/diff --stat/ls-files --others를 실행했다.
  tracked diff --stat에는 untracked 신규 구현이 빠지므로 전체 범위를 뜻하지 않는다.
- 직전 Health Registry commit a1effcb 및 Timer/Codec/Core commit을 log에서 확인했다.

시작 baseline 182개 파일의 SHA-256/dirty 목록을 `/private/tmp/ktd-readiness-audit/`에
보관했다. 아래 목록은 실제 파일 기반으로 재구성한 누적 40경로 전체다.
이전 보고서의 P1~P5/Task A~F 명칭과 현재 그룹은 1:1이 아니다. 이전 구현이 서로 의존하고
공유 문서 append가 겹치므로 실제 dependency 기준으로 다시 묶었다.

## 2. 누락·중복 없는 최종 commit 그룹

G1→G2→G3→G4→G5→G6 순서다. 신규 파일을 과거 상태로 되돌리거나 문서 hunk를 추출하지
않는다. 전체 누적 결과를 각 그룹의 최종 파일 내용으로 commit한다. 공용 문서는 G6에서
한 번만 stage한다. 이전 보고서의 배치별 staging/hunk 분리 절차는 이 계획으로 대체한다.
G5에는 이전 fencing 구현과 이번 최소 claim 예외 해제 수정/회귀 증거가 함께 들어간다.
이미 앞선 배치가 미커밋이므로 별도 fix commit의 가상 이력을 만들지 않는다.

전체 경로와 소유권:

| 그룹 | 실제 전체 경로 |
|---|---|
| G1 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/change_envelope.py |
| G1 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_change_envelope.py |
| G1 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/operational_codec.py |
| G1 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_operational_codec.py |
| G1 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_operational_recovery.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/publication_store.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/publication.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/sink_protocols.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/local_sinks.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_publication.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_state_sinks.py |
| G2 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_local_recovery.py |
| G3 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/schemas/avro/state_change_event.avsc |
| G3 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/change_wire.py |
| G3 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_change_wire.py |
| G4 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/application.py |
| G4 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/application_timer.py |
| G4 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/fixtures/phase4/application_fixture.py |
| G4 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_state_application.py |
| G4 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_application_timer.py |
| G4 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_local_application.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/writer_fencing.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/fenced_application.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/fixtures/phase4/concurrency_fixture.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_writer_fencing.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_fenced_application.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_dispatch_claims.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_readiness_adversarial.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_concurrency_recovery.py |
| G5 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_fencing_recovery.py |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/equipment-state-contracts.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/equipment-health-contracts.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/state-output-contracts.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/state-concurrency-contracts.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/state-job4-integration-plan.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-local-recovery.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-local-application.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-concurrency-recovery.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-verification.md |
| G6 | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-readiness-audit.md |

파일 수: G1 5 / G2 7 / G3 3 / G4 6 / G5 9 / G6 10 = **40**, 경로 중복 0.
G1의 Codec/Envelope가 G2에 선행하고, G3 Wire가 fixture를 통해 G4에 선행한다.
G5는 G4 fixture/Application을 참조한다. 문서 G6는 모든 최종 구현/검증을 설명한다.

이번 감사의 실제 변경은 6경로다: writer_fencing.py 최소 수정, 새 test_readiness_adversarial.py,
새 이 보고서, 기존 concurrency 보고서/verification/Job 4 계획 append다. 나머지는 보존했다.

## 3. 계약 감사 결과와 코드·테스트 근거

아래는 정합성 감사이며 완전한 형식 검증이나 운영 보안 인증은 아니다.
장비/Health/출력/동시성 계약과 streaming/state의 현재 구현·기존 테스트를 대조했다.
worktree에 AGENTS.md는 없으며 사용자 PEP 8 지침과 원본 checkout AGENTS/접근 가능한
로컬 실행 계획의 Phase 4·두 ADR을 확인했다. 로컬 계획서를 복사/강제 추가하지 않았다.

| 감사 항목 | 실제 근거 | 판정·제한 |
|---|---|---|
| 1 Operational/Health 독립 | application.py numeric routing, health_registry.py, test_health_registry/test_state_application | Snapshot/원장 독립. numeric Fryer를 reference FSM에 넣지 않음 |
| 2 Profile/version | registry.resolve, health_registry._validate_snapshot, fenced_application._run | exact key·policy 검증; latest/migration 없음 |
| 3 Ordering/Dedup/Conflict | fsm.transition, hood.qualify_rpm, health.reduce_health; test_ordering/test_hood/test_health | arrival-order 기록 유지; Health freshness 추가 조건과 원장 분리 |
| 4 Change ID | fsm.py / hood.py / health._change, change_envelope.py; replay/process tests | native ID 보존. 같은 순서/정책/평가시각에서 결정적 |
| 5 Codec/version | health_codec CODEC_VERSION=1, operational_codec operational-snapshot-v1, test_*codec/recovery | incompatible/damaged payload 거부. old opaque Operational 자동 migration 없음 |
| 6 History | local_sinks.LocalHistory.write, test_state_sinks | pair ID/body 불변·revision/predecessor chain. 실제 Iceberg writer 없음 |
| 7 Kafka/ACK | publication._call, LocalKafka, test_state_sinks/dispatch_claims | callback/topic/key/offset/digest 확인 모형, UNKNOWN 중복 재전송. 실제 broker ACK 아님 |
| 8 Current | LocalCurrent.apply, test_old_current_delivery_cannot_roll_back_newer_current | 축별 generation/context/p·change revision 및 superseded witness. 실제 DynamoDB 아님 |
| 9 v1/v2 | PublicationStore.__init__, FencedPublicationStore.__init__, 새 mutual-open test | 같은 DB 상호 거부. 독립 DB 동일 장비는 전역 fencing 없음 |
| 10 epoch/CAS | acquire_writer/commit, test_writer_fencing + live writer process | epoch CAS와 기존 root CAS. epoch 인증/운영 authority 미구현 |
| 11 cross-axis barrier | run_command/commit, test_fenced_application | PARTIAL 후속 차단·동일 요청 재개. raw cross-axis 조회 원자성 없음 |
| 12 lease/claim | claim/_check_claim/dispatch_claimed, 새 adversarial tests | 순번/만료/옛 완료 방어. 일반 after_claim 예외 해제 결함 수정 |
| 13 pending 복구 | PublicationCoordinator, reconcile_claimed, 새 permanent-pending tests | pending=0만으로 전체 command 완료 판정 불가. CONFLICT 자동 재시도 안 함 |
| 14 Timer 경합 | LocalHealthTimers→기존 Coordinator/Core, 기존 동률/restart tests | tick 먼저와 관측 먼저 이력 차이 보존, 자동 scheduler 없음 |
| 15 State Rebuild | state-output-contracts/operational_codec/health_codec | 승인 cutover/보존 근거 필요. rebuild 실행기·자동 checkpoint 재생성 없음 |

## 4. 실제 결함과 최소 수정

`streaming/state/writer_fencing.py::ClaimedDispatcher.dispatch_claimed`의 after_claim hook이
try 바깥에 있었다. 일반 Python RuntimeError도 claim을 release하지 않고 lease까지 남겼다.
[회귀 테스트](../../tests/unit/phase4/test_readiness_adversarial.py)의
`test_after_claim_python_exception_releases_own_claim` 단독으로 재현할 수 있다.
현재 수정 위치는 writer_fencing.py 349~355행, 회귀는 test_readiness_adversarial.py 32행이다.
첫 실행 **1 failed, 8 passed**였고, 두 번째 dispatcher가 같은 BASE 시각에 ClaimBusy를 받았다.

hook을 try/finally 안으로 옮긴 2줄 이동만 수행했다. 일반 stack unwinding에는 자기 claim만
해제한다. os._exit 강제 종료는 finally를 실행하지 않으므로 기존 lease 재획득 계약 그대로다.
기존 실제 종료 테스트를 재실행했다. Core/FSM/Codec/Registry·ID·DB schema·deadline은 바꾸지 않았다.

추가로 확인한 제한은 임의 정책으로 수정하지 않았다:

- invalid admitted 명령은 ADMITTED, pending은 비어 있을 수 있다. 후속 명령은 CommandBusy다.
  재현: `test_invalid_admitted_command_stays_blocked_for_explicit_review`.
  명령 취소/수정/격리·후보 단절 정책은 사용자 결정 필요다.
- CONFLICT intent는 pending에 남고 자동 재발행하지 않는다.
  재현: `test_conflict_pending_requires_review_not_blind_retry`.
  bounded scan은 일반 Sink 오류를 전파하므로 다른 장비 실패 격리/운영 경보는 후속 개선이다.
- 별도 v1/v2 DB에 같은 장비를 넣으면 양쪽 모두 revision 1을 저장할 수 있다.
  재현: `test_separate_v1_v2_databases_do_not_share_writer_authority`.
  같은 파일 format 혼용 방어를 deployment 전체의 fencing으로 확대 해석하면 안 된다.
  운영 단일 권위·cutover 결정 전 dual writer 배포는 blocker다.

## 5. 신규 Concurrency 검증과 재사용 범위

새 파일 `tests/unit/phase4/test_readiness_adversarial.py`의 **10개**:
일반 claim 예외 해제, forged sequence 하락/상승 2개, exact-expiry 옛 완료 기록,
v1→v2/v2→v1 같은 파일 거부 2개, live 명령의 충돌 요청, 영구 ADMITTED,
영구 CONFLICT, 독립 DB authority 부재다.

동일 명령 concurrent admission/같은 expected revision/옛 epoch·ABA/PARTIAL 후속·policy
불일치/lease 경계·늦은 ACK/재시작 generation·revision은 기존 writer/claim/application 및
실제 process 테스트가 이미 검증하여 중복 추가하지 않았다. 이번 신규 process 테스트는 없다.

## 6. 실제 검증 환경·명령·기대·결과

환경: Python 3.11.15, SQLite 3.50.4, pytest 9.1.1, fastavro/avro 1.12.2,
confluent-kafka 2.15.1. PySpark/pandas/pyarrow 미설치. 기존 Authlib 경고 1개.
공통 명령은 `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider`다.

| 실행 범위/전용 basetemp | 기대 | 실제 결과 |
|---|---|---|
| tests/unit/phase4/test_readiness_adversarial.py / /private/tmp/ktd-readiness-new | 신규 거부·복구 시나리오 전체 통과 | 10 passed, 0.36초 |
| tests/unit/phase4 / /private/tmp/ktd-readiness-unit | 기존 740 + 신규 10, 기존 기대값 유지 | 750 passed, 3.04초 |
| Phase 4 local integration 4파일 / /private/tmp/ktd-readiness-local | 기존 54개 process/recovery 유지 | 54 passed, 10.64초 |
| -m 'not integration' / /private/tmp/ktd-readiness-all | 외부 marker 제외 전체 성공 | 1518 passed, 10 deselected, 16.89초 |

Phase 4 unit/local 합계 **804**다(750+54, 별도 두 실행의 합). 이전 1508은 역사 기록이다.
실패했던 신규 red를 숨기거나 기존 테스트 기대값을 낮추지 않았다.
AST/구문 52개 Python 파일, 신규 Python 79열, 로컬 문서 링크 26개 및 git diff --check를
검사했다. baseline 182개 중 변경을 허용한 코드 1개/문서 3개 외 178개 SHA-256이 동일하다.
40경로 staging 집합 동등성과 중복 0을 기계적으로 대조했다.

## 7. Spark Job 4 Runtime 준비 상태

[Job 4 계획 12절](../streaming/state-job4-integration-plan.md)이 최신 환경 대조다.
**진입 준비 안 됨**: PySpark/spec·spark-submit 부재, pandas/pyarrow 부재,
Java -version은 24다. Python 3.11은 아래 문서 최소 범위 안이지만 환경 전체가 호환된다는 뜻은 아니다.
Spark 3.5.6 공식 목록은 Java 8/11/17, Spark 4.0.0은 Java 17/21이다.
현재 Java 24는 그 목록 밖이다. DBR/JVM/Python/API 조합 선택·의존성 고정이 선행한다.

[3.5.6 환경](https://spark.apache.org/docs/3.5.6/index.html),
[4.0.0 환경](https://spark.apache.org/docs/4.0.0/index.html),
[foreachBatch Guide](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html),
[TransformWithState](https://spark.apache.org/docs/4.0.0/streaming/structured-streaming-transform-with-state.html)를
온라인 문서로만 확인했다. API 존재/기본 재실행 의미는 확인, target runtime·무입력 timer·restart는 미검증이다.
Spark 실행·설치·Workspace 접근 없음. streaming/jobs에 실제 State Job 4 entry point도 없다.

Job 2는 기존 10필드 numeric Avro·double·UTC string·equipment_id key이며 중복 전달 가능하다.
Job 4 ingress는 source offset/장비 명령 순서·최초 평가 시각을 영속 기록해야 한다.
foreachBatch의 재실행을 Spark batch_id 단독이나 executor/task ID로 fencing하지 않는다.

Snapshot State Store/Publication authority는 미선정이다. 후보는 foreachBatch+지원 transactional
authority 또는 Spark native state+별도 durable intent protocol이며 각각 원자성 증거가 필요하다.
중앙 control authority/저장소의 조건부 epoch 발급 후보도 설계 비교 대상이며 채택하지 않았다.
checkpoint와 Snapshot/command/intent/ACK 간 불일치 복구를 실제 runtime로 검증해야 한다.
새 checkpoint 고유 경로·profile/state schema 변경 정책은 사용자 승인 전 미확정이다.
운영 Registry subject/schema ID/compatibility와 로컬 Avro 초안·fixture schema ID=17은 별개다.
운영 wire bytes/schema ID pinning과 History/Kafka/DynamoDB 각 전달 경계는 여전히 미구현이다.

## 8. Phase 4 DoD Gap Analysis

로컬 실행 계획의 Phase 4 경계는 reference/health·History·state.changes·Current 및 부분 실패
복구·과거 입력 되감기 방지다. 아래 분류는 구현 단위의 범위를 명시한다.

| 대상 | 분류 | 실제 범위·잔여 |
|---|---|---|
| Fryer reference / Hood qualifier / Registry | 구현 완료 + 테스트 확인 | synthetic 신호와 numeric RPM 로컬 계산. 실제 제어기 증거 아님 |
| Health HEALTHY/STALE, Codec, 명시 Timer | 구현 완료 + 테스트 확인 | 정책 주입·관측/tick/restart; scheduler 없음 |
| Snapshot/Envelope/ledger/fencing/local 3 Fake | 구현 완료 + 테스트 확인 | 단일 로컬 authority 모델. 분산/외부 전달 제외 |
| State Change Avro draft/serializer | 구현 완료 + 외부 검증 미실시 | offline round-trip만. 운영 subject 등록/consumer/배포는 안 됨 |
| History/Kafka/Current 출력·복구 계약 | 설계 + 로컬 구현 완료 | 실제 Iceberg/Kafka/DynamoDB adapter와 E2E는 미구현 |
| Spark Job 4 ingress/state/timer/checkpoint | 미구현 | API/runtime·state 권위 결정 후 개발 필요 |
| 운영 authority/영속 Publication 저장소 | 사용자 결정 필요 | 단일 장비 소유권/cutover·원자 저장/보존/복원 선택 |
| 잘못된 admitted 명령/CONFLICT 해소 | 사용자 결정 필요 | 자동 skip/취소/정책 변경 없음, 관리·관측성 필요 |
| FAULT | 의미/enum 설계만, 발생 처리 미구현 | 승인 fault source/rule 없음. 숫자에서 추론하지 않음 |
| State Rebuild | 설계만 | 승인된 입력/원장 보존·cutover 필요, 실행기 없음 |
| Operational Current 확장 필드 | 미구현/범위 결정 필요 | state_started_at/latest_metrics/last_ingested_at 완전 투영 없음 |

장비별 지원은 Operational과 Health Registry를 따로 평가한다:

| 장비 | Operational Handler | Health Registry 입력 | DoD 판단 |
|---|---|---|---|
| Fryer | synthetic reference 실행 가능, numeric 조리 추론 없음 | 명시 policy로 oil_temperature_celsius/celsius | 로컬 구현/테스트 완료. 실제 신호/정책 권한 미확정 |
| Hood | RPM hysteresis 실행 가능 | 명시 policy로 fan_rpm/rpm | 로컬 구현/테스트 완료. 운영 threshold/timeout 승인 아님 |
| Refrigerator | 알려진 profile, handler=None | Registry 미지원; Core 표현 가능성과 별개 | 둘 다 연결 미구현, telemetry 존재만으로 지원 아님 |
| Freezer | 알려진 profile, handler=None | Registry 미지원 | 둘 다 연결 미구현 |
| Dishwasher | 알려진 profile, handler=None | Registry 미지원, numeric validated 등록도 없음 | 둘 다 연결 미구현, 문자열 후보를 구현으로 세지 않음 |

**E2E 전 blocker**: runtime/JDK/API 선택, validated 입력 순서/lineage, 운영 정책/source,
state/command/intent 권위와 checkpoint protocol, v1/v2 단일 authority/cutover,
invalid 명령 관리, 실제 3 Sink/wire Registry/권한·비용 승인.
장비 3종의 추가 지원이 전체 수용 범위에 필수인지 또는 명시 제외할지는 사용자 결정이다.
지원하지 않는 장비가 있다고 기존 Fryer/Hood 로컬 증거를 부정하지 않으며 Phase 전체 완료도 주장하지 않는다.
**후속 운영 개선**: 대량 scan/partitioning·lease 갱신·bounded retry/오류 격리·관측성·TTL/backup/
처리량 실험. 이 개선을 외부 원자성·scope 결정 blocker와 혼동하지 않는다. Phase 5는 제외다.

## 9. 실제 외부 실행에 필요한 승인·권한·비용

이번에는 외부 자원 조회/쓰기/인프라 변경을 하지 않았다. 아래는 실행 명령이 아니라 승인 범위다.
대상 runtime/자원명이 미정이므로 임의 ARN·IAM policy·비용 숫자를 만들지 않는다.

| 영역 | 승인에 필요한 구체 범위 | 비용·권한 검증 |
|---|---|---|
| DBR Job/compute | workspace/job/cluster/runtime, 실행 시간·중단·정리, 새 checkpoint 경로 | DBU/compute/storage 예상 상한, job 실행 권한·service identity |
| Kafka/Registry | cluster/topic/subject/schema ID·compatibility, network/인증, 쓰기·읽기 범위 | MSK/egress/보존 비용, resource-scoped Kafka/Registry 권한 |
| History | catalog/schema/table·format/write protocol·검증 쿼리 | UC 권한·storage/compute 비용, canonical 충돌/중복 복구 |
| Current | DynamoDB table/key/per-axis conditional expression | 정확한 IAM resource/action, capacity·요청/보존 비용 |
| authority/Outbox | 저장소/epoch 발급·관리·retention/backup | 별도 선택 승인, 역할 분리·복원·운영 비용 |

기존 checkpoint 변경/삭제·운영 DB migration·State Rebuild는 별도 승인이다.
Phase 2/3 선행 병합 및 전체 Phase 4 DoD 확인 전 main merge 금지는 유지한다.
원격 선행 병합 상태는 이번 오프라인 감사에서 새로 조회하지 않았다.

## 10. 사용자가 실행할 정확한 Git 명령

**Codex는 아래 명령을 실행하지 않았다.** 40경로 최종 diff를 검토한 뒤 worktree에서
차례로 실행한다. 시작 index가 비어 있는지 확인하고 다른 변경이 새로 생기면 계획을 다시 대조한다.
각 그룹 stage 뒤 cached diff/check와 내용을 검토하고 해당 commit만 확정한다.
G6 공용 문서를 한 번에 모아 기존 배치 staging 충돌을 해소한다. git add .는 쓰지 않는다.

```bash
cd /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state
git branch --show-current
git status --short
git diff --cached --stat

# G1
git add -- \
  streaming/state/change_envelope.py \
  tests/unit/phase4/test_change_envelope.py \
  streaming/state/operational_codec.py \
  tests/unit/phase4/test_operational_codec.py \
  tests/unit/phase4/test_operational_recovery.py
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "feat(phase4): add canonical state outputs and operational codec"

# G2
git add -- \
  streaming/state/publication_store.py \
  streaming/state/publication.py \
  streaming/state/sink_protocols.py \
  streaming/state/local_sinks.py \
  tests/unit/phase4/test_publication.py \
  tests/unit/phase4/test_state_sinks.py \
  tests/integration/test_phase4_local_recovery.py
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "feat(phase4): add local publication ledger and recovery sinks"

# G3
git add -- \
  schemas/avro/state_change_event.avsc \
  streaming/state/change_wire.py \
  tests/unit/phase4/test_change_wire.py
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "feat(phase4): add offline state change Avro wire contract"

# G4
git add -- \
  streaming/state/application.py \
  streaming/state/application_timer.py \
  tests/fixtures/phase4/application_fixture.py \
  tests/unit/phase4/test_state_application.py \
  tests/unit/phase4/test_application_timer.py \
  tests/integration/test_phase4_local_application.py
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "feat(phase4): connect local state application and health timers"

# G5
git add -- \
  streaming/state/writer_fencing.py \
  streaming/state/fenced_application.py \
  tests/fixtures/phase4/concurrency_fixture.py \
  tests/unit/phase4/test_writer_fencing.py \
  tests/unit/phase4/test_fenced_application.py \
  tests/unit/phase4/test_dispatch_claims.py \
  tests/unit/phase4/test_readiness_adversarial.py \
  tests/integration/test_phase4_concurrency_recovery.py \
  tests/integration/test_phase4_fencing_recovery.py
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "fix(phase4): fence local writers and harden claimed recovery"

# G6
git add -- \
  docs/streaming/equipment-state-contracts.md \
  docs/streaming/equipment-health-contracts.md \
  docs/streaming/state-output-contracts.md \
  docs/streaming/state-concurrency-contracts.md \
  docs/streaming/state-job4-integration-plan.md \
  docs/reports/phase-4-local-recovery.md \
  docs/reports/phase-4-local-application.md \
  docs/reports/phase-4-concurrency-recovery.md \
  docs/reports/phase-4-verification.md \
  docs/reports/phase-4-readiness-audit.md
git diff --cached --check
git diff --cached --stat
git diff --cached
git commit -m "docs(phase4): reconcile local evidence and Job 4 readiness gaps"

# 최종 검토 후 feature branch에만 push
git status --short
git log -6 --oneline
git push origin feat/phase-4-twin-state
```

main merge/rebase/reset/clean·기존 DB/checkpoint 변경은 이 절차에 없다.
위 명령은 사용자 실행 계획이며 commit/push 승인 또는 실행 결과를 의미하지 않는다.

## 11. 포트폴리오 재현 명령

전용 basetemp는 pytest가 초기화하는 테스트 폴더다. 기존 데이터/체크포인트를 지정하지 않는다.

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-readiness-new \
  tests/unit/phase4/test_readiness_adversarial.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-readiness-unit tests/unit/phase4
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-readiness-local \
  tests/integration/test_phase4_local_recovery.py \
  tests/integration/test_phase4_local_application.py \
  tests/integration/test_phase4_concurrency_recovery.py \
  tests/integration/test_phase4_fencing_recovery.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-readiness-all -m 'not integration'
git diff --check
```

신규 after_claim 회귀 이름/10 passed와 1518 passed 화면은 로컬 계약·복구 증거다.
Spark 실행 또는 실제 세 Sink exactly-once 증거로 표시하지 않는다. 비밀값 캡처는 하지 않는다.

## 12. 다음 구현 배치 우선순위

1. 이 Git 정합성·scope/DoD 제외 범위를 사용자 검토 후 확정하고 위 그룹으로 정리한다.
2. runtime/JDK·state authority·단일 장비 순서/실패 명령 관리 정책을 결정한다.
3. 결정된 계약의 오프라인 Job 4 ingress/state/Publication harness를 별도 승인 배치로 구현한다.
4. 실제 자원·비용·권한·checkpoint 승인 후 Runtime/Registry/3 Sink E2E를 별도 수행한다.

이번에 2~4의 구현이나 외부 실행으로 진행하지 않았다.
