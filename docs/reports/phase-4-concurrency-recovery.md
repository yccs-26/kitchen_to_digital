# Phase 4 — Concurrency, Fencing & Recovery Hardening

2026-10-23 (Asia/Seoul). 로컬 배치 완료 보고. 시작·종료 branch는
`feat/phase-4-twin-state`, HEAD `a1effcb`이며 Git index/publication을 변경하지 않았다.
시작 시 기존 미커밋 28경로가 있었고 baseline SHA-256 및 원본을 임시 증빙 폴더
`/private/tmp/ktd-concurrency-baseline/`에 보관했다. 임시 폴더는 Git 산출물이 아니다.

## 1. Task A~F 실제 결과

| Task | 결과 | 근거 |
|---|---|---|
| A 감사 | read/compute/CAS, 축별 partial, pending 중복선택, stale writer/ABA, 종료 후 잠금·Current 역순을 구분 | [동시성 계약](../streaming/state-concurrency-contracts.md) A절과 해당 회귀 시나리오 |
| B fencing/CAS | 새 v2 DB의 명시 writer epoch·권한 검사·기존 root CAS·오류 분류 | writer unit 14개, 동시 epoch 발급·stale/cached/ABA·저수준 Health-first 복구 |
| C 다중 dispatcher | output lease/sequence·늦은 ACK 차단·bounded scan·Fake 수렴 | dispatch unit 12개, 실제 중첩 dispatcher·ACK 유실/재종료 |
| D cross-axis | durable ADMITTED/PARTIAL/COMPLETE 장벽, 동일 명령 재개·후속 차단 | application unit 7개, 실제 축 commit 전후 종료·복구 도중 재종료 |
| E process 복구 | 신규 로컬 integration 17개 | crash 사례 14개에서 os._exit(81) 총 17회, 중첩 process 사례 2개, stale writer 오류 종료 1개 |
| F readiness | 공식 Spark version 고정 문서와 설치 환경 재확인, 운영 게이트 확장 | [Job 4 계획](../streaming/state-job4-integration-plan.md) 11절; Spark/Cloud 실행 없음 |

B/C/D 테스트는 서로 겹치는 보장을 검증한다. 숫자를 더해 독립 요구사항 수라고 하지 않는다.
이번 신규 executable cases는 unit 33 + local integration 17 = **50개**다.

## 2. 이번 변경 파일 전체 목록과 소유권

프로젝트 절대 경로 prefix:
`/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/`

| Task 소유권 | 전체 경로 | 내용 |
|---|---|---|
| A | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/state-concurrency-contracts.md | 감사·fencing·장벽·lease·복구 경계 |
| B/C | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/writer_fencing.py | opt-in v2 store, authority, admission, claims, ClaimedDispatcher |
| B | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_writer_fencing.py | CAS·epoch·ABA·identity·savepoint·schema 거부 |
| C | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_dispatch_claims.py | lease·ACK·clock·Current·bounded scan |
| D | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/fenced_application.py | 기존 Application/Timer를 감싸는 명령 admission |
| D | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_fenced_application.py | partial·policy·다른 장비·timer 순서·replay |
| E | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/fixtures/phase4/concurrency_fixture.py | 기존 fixture/Core 재사용, 별도 fenced.sqlite 생성 |
| E | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_concurrency_recovery.py | 실제 종료·재시작·lease 경과·반복 실패 |
| E | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_fencing_recovery.py | 두 writer 동시 계산/commit, live dispatcher takeover |
| F | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/state-job4-integration-plan.md | 기존 본문 보존, 11절 append만 |
| F | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-verification.md | 기존 본문 보존, 이번 실측 append만 |
| F | /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-concurrency-recovery.md | 이 보고서·정확한 staging/명령 |

신규 10개 + 기존 문서 append 2개 = **이번 배치 12경로**다. 이전 배치 코드/fixture/tests/
Avro 및 Health/장비 계약 문서는 수정하지 않았다. 현재 미커밋 전체는 38경로다.

## 3. 기존 코드·계약과의 호환성

기존 PublicationStore/Application/Timer/Public Coordinator 및 Fake Sink를 수정하지 않고
서브클래스/조합으로 연결했다. Fryer/Hood/Health Core·Codec·Registry·native ID를 그대로
사용한다. 기존 v1 DB/entry point는 유지되며 새 기능을 자동 적용하지 않는다.

사용자는 새 v2 store + FencedStateApplication + ClaimedDispatcher 경로를 명시 선택해야
한다. v1 DB를 v2로 열면 거부하며 migration은 없다. 서로 다른 version의 실제 데이터는
이 배치에서 변경하지 않았다. 테스트의 v1→v2 fixture 구성은 새 임시 DB에 명시 bootstrap
Snapshot을 복사하는 테스트 준비이며 사용자 DB migration 도구가 아니다.

Numeric Fryer는 Health만, synthetic Fryer 신호는 Operational만, Hood RPM은 두 축을
기존 방식으로 평가한다. FAULT/Alert·운영 timeout/source 권한·장비 지원 범위는 바뀌지 않는다.

## 4. Writer fencing 및 CAS 의미

- `acquire_writer(device,owner)`: SQLite transaction으로 로컬 epoch 증가·조건부 UPDATE
  rowcount 검증. A→B→A여도 옛 token은 거부한다. 운영 leader 발급/인증은 미구현이다.
- `run_command(token,ref,request,axes,operation)`: exact 요청과 장비별 순서 admission.
  장비 identity·epoch가 맞지 않으면 cached replay까지 시작 전에 거부한다.
- 축 `commit`: 동일 transaction에서 writer와 admission을 검사한 뒤 기존 before SHA/
  expected revision/domain generation CAS·Snapshot+intent+command 저장을 재사용한다.
- authority epoch는 출력 generation/Health timer generation을 바꾸지 않는다. 운영 cutover
  generation을 자동 발급하거나 기존 정책을 승격하지 않는다.
- RevisionConflict는 재읽기/재계산 가능, CommandBusy/ClaimBusy는 선행 복구/lease 대기,
  StaleWriter/StaleClaim은 소유권 상실, OutputConflict는 payload/계약 충돌이다.

## 5. SQLite transaction·Lock·Claim

BEGIN IMMEDIATE는 같은 파일 write를 직렬화한다. 계산 전체나 외부 Sink 호출을 잠그지
않는다. nested SAVEPOINT는 내부 예외를 caller가 잡아도 부분 저장을 rollback한다.
connection은 thread/process마다 별도다. lock busy는 sqlite3 오류 그대로 전파한다.

claim은 output 단위 owner/sequence/expires_at/last_checked를 영속 보관한다.
`now < expires_at`은 live, 등호부터 새 claim 가능하다. 성공 검사 시각 역행을 거부한다.
기존 attempt 번호와 claim 검사를 함께 사용한다. 정상 반환·예외에는 자기 claim만 release,
실제 종료는 lease 만료 뒤 재획득한다. 다른 owner/sequence를 옛 release로 지우지 않는다.

## 6. Cross-axis partial 복구

ADMITTED는 완료 축 없음, PARTIAL은 일부, COMPLETE는 모든 축의 snapshot+intent 확정이다.
Sink 완료와는 다르다. partial 뒤에는 같은 장비의 다음 관측/tick을 CommandBusy로 막는다.
같은 명령은 이미 저장한 축의 cached 원본 출력을 재사용하고 남은 축을 commit한다.
다른 장비는 계속 처리한다. 양 축 완료 전 출력 claim도 막으며 bounded scan은 BLOCKED를
보고하고 다른 출력을 진행한다. raw Snapshot 조회의 cross-axis 원자성은 제공하지 않는다.

기본 앱은 Op→Health 순서다. 저수준 Health-first partial도 별도 검증했다. 정책/profile/
입력/평가 시각 변경은 기존 요청과 충돌하며 자동 이관하지 않는다. writer epoch takeover는
원래 명령/정책 그대로 재개할 수 있다. domain generation 불일치는 기존대로 거부한다.
invalid admitted 명령은 자동 skip/cancel하지 않으므로 운영 관리 복구 정책은 후속이다.

## 7. Dispatcher 재시도와 ACK 불확실성

기존 PublicationCoordinator의 History→Kafka→Current·개별 ACK 검증을 사용한다.
외부 성공 뒤 종료하거나 호출 중 lease를 잃으면 UNKNOWN이 남을 수 있다. 재획득한
새 dispatcher가 원본 immutable bytes로 재시도한다. 늦은 옛 ACK는 거부하며 새 attempt를
완료하지 못한다. History canonical 1건, Current 최신 revision 유지, Kafka 동일 bytes
중복을 허용한다. lease는 네트워크 side effect 자체를 취소하거나 fence하지 않는다.

`reconcile_claimed(owner,clock,lease)`는 한 번만 스캔한다. live claim/partial은 BLOCKED,
완료는 COMPLETE로 반환하며 실패/충돌/손상은 숨기지 않는다. 자동 retry loop·스케줄러·
lease 갱신·영속 retry budget은 없다. legacy unclaimed dispatch는 v2에서 거부한다.

## 8. 실제 프로세스 종료 증거

- commit 계산 직후/직전, transaction after_snapshot/before_commit, Op commit 후,
  Health commit 후에 실제 os._exit(81). 재시작 결과를 연속 실행의 전체 root/Envelope와 대조.
- claim 직후, History/Kafka/Current 성공 후 ACK 기록 전, complete 기록 직전에 종료.
  별도 프로세스가 아직 live인 claim을 거부받고 명시 lease 경과 뒤 복구하는지 확인.
- ACK 유실 재시도 중 다시 종료하면 Kafka 3회 같은 bytes, History 1건으로 수렴.
  partial 명령 복구 중 다시 종료해도 이미 저장한 축 revision이 재증가하지 않음.
- 두 독립 writer 프로세스를 계산 후 rendezvous로 동시에 해제해 동일 준비 명령을 commit.
  각 축 revision 1·command 2개 유지. 늦은 writer epoch 오류 종료도 별도 확인.
- live dispatcher를 Kafka 성공 직후 정지하고 다른 connection에서 lease 만료 takeover.
  새 dispatch 완료 뒤 옛 process ACK는 StaleClaim 오류로 종료, 최신 기록 유지.

subprocess.run/communicate timeout 20초, rendezvous child 12초/parent 15초,
finally kill/reap 5초다. 고정 UTC 시각·파일 rendezvous이며 난수를 쓰지 않아 seed가 없다.
로컬 tests는 external integration marker를 붙이지 않으며 Kafka/Registry에 연결하지 않는다.

## 9. Snapshot·Change ID 결정성

복구 실행과 연속 실행의 전체 StoredState(Snapshot bytes 포함) 및 모든 StateOutput을
비교한다. 같은 입력/정책/command_ref/평가 시각/순서에서 native ID와 envelope bytes가 같다.
writer takeover는 이 도메인 결과를 바꾸지 않는다. claim/attempt/authority 실행 흔적까지
동일하다는 뜻은 아니다. 최초 canonical envelope를 다시 생성/수정하지 않는다.

동시각 tick 먼저는 HEALTHY→STALE→HEALTHY, 관측 먼저는 옛 tick 무효라는 기존 이력을
유지한다. 두 순서를 각각 두 디렉터리에서 재시작/replay해 전체 결과를 비교한다.
Current를 먼저 최신으로 보낸 후 옛 intent를 dispatch하면 기존 SUPERSEDED 검증으로
되감기를 막는다. 미래의 입력 순서까지 예측/재구성하는 보장은 없다.

## 10. 테스트 및 품질 결과

최종 수치는 검증 보고서의 이번 배치 절과 같다. 신규 50개(unit 33/integration 17),
Phase 4 unit 740개, 기존·신규 로컬 integration 포함 Phase 4 선택 실행 794개,
전체 비외부 로컬 1508개가 통과했다. 외부 integration 10개는 deselect다.
Authlib의 기존 httpx deprecation warning 1개 외 새로운 경고/실패는 없다.

첫 red는 writer_fencing 모듈 부재로 collection 실패였고 최소 구현 후 통과했다.
후속 Fake 테스트에서 messages()의 실제 반환 타입을 dict로 오인해 5개가 실패했다.
반환 ChangeEnvelope.payload의 완전 일치 비교로 고쳐 재검증했다. 기존 기대값/테스트는
완화·삭제하지 않았다. 최종 AST/79열/문서 로컬 링크/diff 검사와 보호 파일 hash를 확인했다.

## 11. 미구현·미검증

운영 authority/인증·leader 선출·분산 clock skew/lease 연장·파일 복제/손실/디스크 full/
network filesystem lock·대량 성능·TTL·관리 취소·운영 retry 예산은 없다.
Snapshot만으로 미기록 원천 입력/전송 여부를 복원하지 않는다. 같은 SQLite 파일 lock의
처리량 제한이 있으며 서로 다른 호스트의 독립 DB는 하나의 권위가 아니다.

실제 Spark/DBR/checkpoint·Schema Registry 등록·Kafka ACK·Iceberg History·DynamoDB CAS·
외부 exactly-once·전체 Phase 4 DoD는 미검증이다. Phase 5/운영 timeout/FAULT는 추가하지
않았다. 영속 Outbox 저장소 채택·운영 데이터 migration·cloud 실행·Git publication 없음.

## 12. 운영 이관 시 필요한 변경

승인 authority의 authenticated epoch 발급, 단일 장비 admission/partial gate·root/intent
원자 저장, 분산 claim/clock·관측성/보존·복원·관리 실패 정책이 필요하다. 로컬 SQLite를
executor 파일로 배포하지 않는다. 운영 storage는 미선정이다. Avro wire bytes/schema ID의
영속 고정·consumer dedup, 실제 History canonical 경합, DynamoDB per-axis 조건식을
각각 검증한다. 서로 다른 Sink를 하나의 transaction으로 간주하지 않는다.

## 13. Job 4 실행 전 게이트

[운영 계획 11절](../streaming/state-job4-integration-plan.md)의 API/DBR·state topology·
processing-time timer·입력 순서·checkpoint/authority 원자성·driver/executor retry와
권한/비용/자원 범위를 확정하고 사용자 승인을 받아야 한다. 문서상 Spark 3.5.6/4.0.0 API
존재만 확인했으며 PySpark는 없다. 기존 checkpoint 삭제/재사용/migration은 별도 승인이다.
Phase 2/3 선행 병합 및 전체 Phase 4 DoD 전 main merge 금지를 유지한다.

## 14. Git staging·commit 계획 — 실행하지 않음

이번 작업 전 28경로는 이전 P1~P5/Readiness A~F 소유다. 그 파일 목록과 이전 commit
순서는 [기존 Application 보고서](phase-4-local-application.md) 및
[기존 Recovery 보고서](phase-4-local-recovery.md)를 따른다. 이전 파일을 새 배치 commit에
통째로 섞지 않는다. 먼저 이전 배치의 기반 코드·tests·schema를 검토하여 확정해야 한다.

공유 문서 두 개는 별도 주의가 필요하다. verification은 이미 추적 중인 기존 변경 위에
이번 절을 append했다. Job 4 계획은 이전 배치부터 untracked이며 이번 11절을 append했다.
이전 기반 commit을 아직 만들지 않았다면 Job 4 문서는 사용자 실행 `git add -N --
docs/streaming/state-job4-integration-plan.md` 후 `git add -p`의 편집 기능으로 이전 1~10절과
이번 11절을 나누거나, 두 배치 공용 문서를 마지막 문서 commit으로 따로 검토한다.
전체 파일 add로 이전 내용이 새 Task F 소유라고 표시하면 안 된다. 기존 파일을 덮어쓰는
방식으로 staging하지 않는다. baseline 원본/현재 파일의 차이는 임시 증빙 폴더로 대조 가능하다.

아래는 **사용자 검토 후 실행할 명령**이다. 이 배치가 전제하는 이전 기반 모듈을 먼저
commit하고 실행한다. A는 새 B/C/D 구현 계약도 설명하므로 B~E 뒤 묶어 commit한다.

```bash
# B/C — store/dispatcher와 동작 단위 증거
 git add -- streaming/state/writer_fencing.py \
   tests/unit/phase4/test_writer_fencing.py
 git commit -m "feat(phase4): add local writer fencing and publication claims"

# D/E — wrapper, shared new fixture, claim/application tests, 실제 process 증거
 git add -- streaming/state/fenced_application.py \
   tests/fixtures/phase4/concurrency_fixture.py \
   tests/unit/phase4/test_fenced_application.py \
   tests/unit/phase4/test_dispatch_claims.py \
   tests/integration/test_phase4_concurrency_recovery.py \
   tests/integration/test_phase4_fencing_recovery.py
 git commit -m "test(phase4): harden cross-axis and concurrent restart recovery"

# A/F — 새 문서와 이번 append만 검토
 git add -- docs/streaming/state-concurrency-contracts.md \
   docs/reports/phase-4-concurrency-recovery.md
 git add -p -- docs/streaming/state-job4-integration-plan.md \
   docs/reports/phase-4-verification.md
 git diff --cached --check
 git diff --cached --stat
 git commit -m "docs(phase4): record concurrency contracts and runtime gates"
```

D/E의 test_dispatch_claims는 새 fixture에 의존하여 두 번째 commit에 둔다.
위 git commit도 자동 실행하지 않았다. origin/feat/phase-4-twin-state push는 사용자가
검토 후 별도로 수행한다. main merge/다음 구현 작업으로 진행하지 않는다.

## 15. 포트폴리오 증빙용 명령

아래 명령은 해당 worktree 루트에서 실행한다. 각 basetemp는 이 명령 전용 임시 폴더다.
pytest가 해당 basetemp를 초기화하므로 사용자 데이터/기존 checkpoint 경로를 지정하지 않는다.

```bash
# 신규 50개: fencing, claims, partial, 실제 process recovery
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-concurrency-final-new \
  tests/unit/phase4/test_writer_fencing.py \
  tests/unit/phase4/test_fenced_application.py \
  tests/unit/phase4/test_dispatch_claims.py \
  tests/integration/test_phase4_concurrency_recovery.py \
  tests/integration/test_phase4_fencing_recovery.py

# Phase 4 순수 unit
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-concurrency-final-unit tests/unit/phase4

# 기존 local process 복구까지 포함
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-concurrency-final-phase4 tests/unit/phase4 \
  tests/integration/test_phase4_local_recovery.py \
  tests/integration/test_phase4_local_application.py \
  tests/integration/test_phase4_concurrency_recovery.py \
  tests/integration/test_phase4_fencing_recovery.py

# 외부 Kafka/Registry marker 제외 전체 로컬
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-concurrency-final-all -m 'not integration'
git diff --check
```

첫 명령의 50 passed와 실제 종료·두 writer/dispatcher test 이름을 화면 증빙으로 남길 수
있다. 캡처는 로컬 복구 증거이며 cloud/Spark/Kafka exactly-once 증거라고 표기하지 않는다.

## 16. 후속 Readiness 감사 보정 — 2026-10-23

[Readiness 감사 보고서](phase-4-readiness-audit.md)가 누적 Git 분리 계획의 최신 기준이다.
14절의 배치별 staging은 역사 기록이며, 현재는 공유 문서 append가 중첩되어 있다.
최신 보고서는 실제 전체 파일을 중복 없이 6개 dependency 순서 그룹으로 나누고 공용 문서를
마지막 그룹으로 모은다. 기존 문서의 hunk를 추출하거나 untracked 파일을 부분 stage할 필요가 없다.

이번 감사에서 claim 직후 ordinary Python 예외가 try/finally 바깥에서 발생하면 claim이
lease 만료까지 남는 결함을 재현했다. writer_fencing.py의 after_claim hook을 try 안으로
옮겼다. 일반 예외는 claim을 해제하고, 실제 os._exit 종료는 claim이 남는 기존 계약을 유지한다.
Snapshot/ID·Core·schema·lease 만료 규칙은 변경하지 않았다.

추가 적대적 테스트 10개는 위 회귀, 변조 sequence/옛 완료 요청·v1/v2 같은 파일 상호 거부·
동일 ref 충돌 admission·영구 ADMITTED/CONFLICT·독립 DB의 authority 부재를 검증한다.
독립 v1/v2 DB가 같은 장비를 동시에 처리할 수 있으므로 전역 혼용 방어가 있다고 주장하지 않는다.
pending이 비어도 ADMITTED가 남을 수 있으며 관리 취소/skip은 사용자 결정 전 미구현이다.
이전 50개/1508개 수치는 이전 배치의 결과이고 최신 수치는 검증 보고서의 감사 절을 따른다.
