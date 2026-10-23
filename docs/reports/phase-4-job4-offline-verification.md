# Phase 4 — Job 4 Offline Implementation Verification

2026-10-23 (Asia/Seoul). 기존 pure Core·Local Application·SQLite v2를 사용하는
**로컬 실행 계층**이다. Spark Query/외부 Sink/운영 Authority를 구현한 결과가 아니다.
[Job 4 계획](../streaming/state-job4-integration-plan.md),
[출력 계약](../streaming/state-output-contracts.md),
[동시성 계약](../streaming/state-concurrency-contracts.md),
[직전 권위 결정 보고서](phase-4-runtime-authority-decisions.md)를 함께 따른다.

## 1. Git 사전 게이트

- 경로: `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`
- branch `feat/phase-4-twin-state`, 시작·종료 HEAD `3087e0b9b3e997b46dc335a5175a9e39c21268b3`.
- 시작 status clean/index empty. 직전 6파일은 b1e43d5(코드3)/3087e0b(문서3)에 commit되어 있다.
- 기준선 188개 tracked 파일의 SHA-256을 `/private/tmp/ktd-job4-offline/baseline.json`에 기록.
- 원본 AGENTS·접근 가능한 로컬 실행 계획 Phase 4 DoD와 실제 계약/코드를 대조했다.
  Phase 2 Job/validated publisher/decoder 및 별도 Phase 3 worktree의 Spark Job 구조는
  read-only 참고했다. 다른 worktree 코드를 복사하거나 병합하지 않았다.
- 이번 요청은 원격 확인 게이트가 없어 fetch하지 않았다. 최신 원격 push 여부는 이번에
  검증하지 않았으며 tracking ref만으로 최신 동기화를 주장하지 않는다.

## 2. Task A~G 결과

| Task | 실제 결과 |
|---|---|
| A 인터페이스 분석 | 기존 decode/validated_observation/Registry/Core/Codec/Timer/Fencing/ACK/evidence를 재사용, 실제 runtime/scheduler/외부 store는 제외 |
| B Input Adapter | Phase 2 실제 Avro decoder+주입 Registry, key/frame schema ID/10필드/UTC/lineage 및 오류 분류 |
| C Batch Application | 명시 sequence·deterministic command ref·durable manifest·장비별 source cursor·순차 실행·실패 progress |
| D Timer 경계 | 기존 token/Coordinator로 명시 tick 전달, deadline/복구/obsolete/replay·두 경합 순서 유지 |
| E Completion/Recovery | INPUT_COMMITTED와 전달/완료 기록 분리, manifest/입력 원본으로 resume/recover, default checkpoint gate 유지 |
| F Local Integration | 실제 Phase 2 Avro, Fryer/Hood, source FIFO·PARTIAL·ACK/claim·실제 child exit·결과 수렴 |
| G Spark 설계/최종 검증 | 문서상 callback/Row Adapter 책임·signature·진입 게이트와 실제 실행 한계 명시 |

## 3. 변경 파일과 역할

프로젝트 root는 `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`다.

| Task | 경로 | 역할 |
|---|---|---|
| B | streaming/state/job4_input.py | 실제 wire decode, source/timer command 준비·복원·data/system 오류 구분 |
| B/C | streaming/state/job4_store.py | 새 로컬 catalog 생성·입력/manifest/cursor·admission 결합·완료/evidence/attempt |
| C/E | streaming/state/job4_batch.py | apply/execute/resume/recover/complete, 명시 writer와 실패 progress |
| C | streaming/state/writer_fencing.py | backward-compatible admission_hook 하나 추가, transaction 안 local metadata 연결 |
| F | tests/fixtures/phase4/job4_fixture.py | 기존 fixture+새 DB·실제 Avro·Fake Registry/Sinks |
| B | tests/unit/phase4/test_job4_input.py | decoder/lineage/schema/key/오류/timer 복원 |
| C/E | tests/unit/phase4/test_job4_batch.py | manifest·dedup·순서·epoch·정책·오염/손상·DB 거부·증빙 |
| D/F | tests/integration/test_phase4_job4_batch.py | 다중 장비·정상 전이·timer·실패/ACK·입력 journal 복구 |
| F | tests/integration/test_phase4_job4_recovery.py | 실제 종료/재시작·claim 만료·stale writer·결과 대조 |
| G | docs/streaming/state-job4-integration-plan.md | 기존 본문 보존, 이번 로컬 연결·Spark 책임 append |
| G | docs/reports/phase-4-verification.md | 기존 본문 보존, 실측 append |
| G | docs/reports/phase-4-job4-offline-verification.md | 이 보고서·정확한 명령·한계 |

신규 9파일 + 기존 수정 3파일 = 12경로다. Core/Health Reducer/기존 ID·sensor Avro·
Phase 2/3·운영 timeout·장비 Handler를 변경하지 않았다.

## 4. Validated Input 계약

`ValidatedKafkaRecord(position: InputPosition, value: bytes, evaluation_time: datetime)`를 받는다.
position은 기존 topic/partition/offset/key/writer_schema_id 모델이다. topic은
kitchen.sensor.validated, key는 equipment_id UTF-8 bytes이고 frame의 schema ID와 metadata가
같아야 한다. schema ID를 Spark 기본 column이나 sensor schema_version으로 가정하지 않는다.

`prepare_input(record, source_epoch=..., registry=...)`는 기존 parse_confluent_frame /
decode_sensor_event / ValidatedCommand / validated_observation을 재사용한다. Registry는
caller가 주입하며 실제 client를 자동 생성하지 않는다. canonical 10필드·finite double·
UTC·key 검사를 통과한 뒤 Binding/type 선택과 metric/unit/version/source 검사는 기존
Application/Health Registry에 위임한다. Header 오류로 freshness가 연장되지 않는지 검증했다.

source epoch는 catalog 생성 시 caller가 명시하고 영속 보존한다. 관측 command_ref는
`job4-input:` + SHA-256(namespace/version, source_epoch, topic, partition, offset)이다.
**native state_change_id/health_change_id 생성 규칙은 바꾸지 않는다.** 같은 source offset의
다른 payload/wire digest/key/schema ID는 충돌이다. 다른 batch ID의 같은 record는 같은 ref다.

최초 원본 canonical payload·wire SHA·lineage·명시 평가 시각을 journal에 고정한다. 동일 입력을
나중 시각에 다시 받더라도 기존 평가 시각을 사용한다. 입력·Timer 모델의 Python 생성 오류는
ValueError/TypeError이며 decoder 데이터 오류는 InputContractError, 비지원 신호/장비는
UNSUPPORTED, 주입 의존성 timeout/429/5xx는 retryable, 권한/미분류 시스템 오류는 fail closed다.
Registry 계약 예외로 Native admission이 남으면 기존처럼 수동 검토 대상이며 자동 skip하지 않는다.

Fryer numeric는 Health만, Hood RPM은 두 축을 처리한다. synthetic Fryer mode/cleaning은
기존 직접 Reference 경로이며 sensor Avro나 이번 numeric Batch Adapter에 새 신호로 넣지 않았다.
Refrigerator/Freezer/Dishwasher는 fallback 없이 거부한다.

## 5. Batch 실행·순서·로컬 저장 경계

주요 API:

```python
Job4Store.create(path, source_epoch=...)  # 반드시 새 파일, explicit bootstrap
Job4BatchApplication(fenced_application, registry)
apply_inputs(batch: BatchIdentity, records, *, writers, hook=...)
execute(batch, records, *, writers, coordinator=None, owner=None,
        clock=None, lease=None, hook=...)
resume_inputs(batch_id, *, writers, hook=...)
recover(batch_id, *, writers, coordinator=None, owner=None,
        clock=None, lease=None, hook=...)
complete(batch_id, *, writers, coordinator=None, owner=None,
         clock=None, lease=None, hook=...)
```

기존 SQLite user_version=2와 schema를 재사용하며 Job 4 전용 catalog version=1을 둔다.
create는 exclusive 새 파일만 허용한다. 기존 v1/v2/빈 파일에도 자동 table 추가·migration하지
않는다. open은 catalog 소유권/version을 read-only로 먼저 검사한다. root Snapshot 초기화는
caller의 명시 동작이며 테스트는 기존 fixture Snapshot을 **새 DB**에 bootstrap한다.
사용자 DB를 이관하는 도구가 아니다. 생성 중 실패한 파일을 자동 삭제/초기화하지 않는다.

로컬 추가 table은 job4_meta / inputs / batches / cursors / attempts다. batch manifest는
BatchIdentity·expected ref 순서·source epoch·offset 범위를 보존한다. range는 [min,max+1)
요약이고 gap 없는 원천 소비 증거가 아니다. 같은 batch의 다른 manifest는 거부한다.
입력 내용은 checksum 검사, Native Snapshot은 기존 Codec/digest와 정책 검사를 재사용한다.

등록·실행은 순차적이며 thread/분산 worker를 만들지 않는다. event_time으로 정렬하지 않는다.
같은 장비 supplied offset 순서를 검사하고, 알려진 ref는 replay하며, 새 ref의 역순 offset/
partition 변경은 명시 cutover 전 거부한다. 같은 batch 내 같은 offset은 한 번 처리한다.

등록된 manifest의 선행 입력이 미admission인 상태에서 다음 batch가 추월하는 경계도 막았다.
장비별 등록 순서를 먼저 검사하고 Native ADMITTED/PARTIAL 장벽과 결합한다. 완료된 prefix만
있어도 미처리 tail을 우회하지 못한다. 다른 장비는 진행할 수 있다. batch_id를 전이 ID나
event-time 우선순위로 사용하지 않는다. 전체 Kafka partition progress/여러 query 간 원천 소유권은
실제 runtime의 expected manifest·단일 Authority·source cutover 책임이다.

Ingress admitted flag/cursor와 Native admission은 새 admission_hook을 통해 **같은 SQLite
transaction**에 저장한다. hook 실패/실제 종료에서 함께 rollback됨을 검증했다.
이후 각 축의 Snapshot+command+intent 저장은 기존 독립 transaction 그대로다. 두 축 전체나
전체 batch를 하나의 atomic transaction으로 바꾸지 않았다. 실제 운영 store 채택도 아니다.

## 6. Timer 연결

`TimerInput(timer_ref, device, native_token, evaluation_time)`를 명시 record sequence에 넣는다.
timer ref는 source epoch·장비·caller timer_ref로 정하고 token/최초 평가 시각을 보존한다.
동일 timer_ref의 다른 token은 충돌, 동일 token을 다른 ref로 전달하면 native duplicate/no-op
규칙을 따른다. 새 timer generation/만료 ID를 만들어내지 않는다.

deadline 직전/등호, STALE→HEALTHY, 옛 timer 무효, 반복 tick, 파일 reopen, 동일 Health ID를
검증했다. 같은 시각 tick 먼저는 STALE→HEALTHY, 관측 먼저는 옛 tick 무효라는 기존 이력을
유지한다. 서로 다른 순서의 이력이 같다고 주장하지 않는다. 등록 순서와 최초 평가 시각을
재사용하며 실제 Scheduler·시스템 현재 시각·자동 catch-up loop는 없다.

## 7. Batch Completion과 Recovery

| 결과 | 필요한 근거 | checkpoint_allowed |
|---|---|---|
| apply_inputs의 input_committed | 모든 expected 명령/축 COMPLETE, 입력/lineage·Snapshot·출력 의도 보존 | false |
| Publication pending | 위 근거는 있어도 일부 ACK/claim 미확정 | false |
| execute/recover/complete 완료 | 위 근거 + 기존 checkpoint_ready의 모든 필요한 Sink terminal + 완료 기록 commit | true |

입력 영속 완료와 Publication ACK는 별개다. Pending intent가 있어도 입력 저장 완료를
반환할 수 있지만 **전체 batch 성공으로 표시하지 않는다.** 기본 동기 완료 계약은 유지했다.
비동기 checkpoint 허용 정책은 별도 승인 대상이며 이번에 켜지 않았다. Sink를 주입하지 않아도
ACK가 미완료면 execute는 BatchExecutionError로 끝나고 ready를 반환하지 않는다.

Evidence는 complete flag나 pending=0만 믿지 않는다. expected manifest 전체, 입력 checksum,
admission provenance·축별 command/output·현재 root digest·Sink별 receipt를 일관된 local
transaction에서 읽는다. 완료 marker가 실패하면 원래 출력으로 재시도하고 ACK가 끝난 Sink는
재전송하지 않는다. input-only replay가 기존 complete marker를 0으로 되돌리지 않도록 했다.

재시작 후 동일 raw record를 제출하거나, 이미 저장된 batch_id로 resume/recover할 수 있다.
후자는 원래 manifest·canonical input·token·평가 시각을 읽으므로 Kafka/Registry에 재접근하지
않는다. manifest commit **이전**에 유실된 입력은 복원하지 못하고 원천 replay가 필요하다.
기록하지 않은 외부 전송 성공/원천 누락을 Snapshot만으로 추정하지 않는다.

BatchExecutionError에는 category/stage/batch_id/failed_ref/completed_refs가 있다.
wire preflight는 전체 sequence를 먼저 검사하므로 실패 시 어떤 입력도 실행하지 않는다.
실행 중 실패하면 완료된 prefix와 실패 ref를 구분하며 PARTIAL·영구 계약 오류를 자동 해제하지
않는다. attempts에는 단계·class 이름·완료 refs를 보존한다. DB 자체가 깨져 audit 기록도
실패하면 기록 성공을 보장하지 않고 원래 실패를 전파한다. 자동 무한 retry/skip은 없다.

## 8. Writer Fencing / CAS

writer token은 장비별로 명시 주입한다. 엔진이 운영 epoch나 leader를 자동 발급하지 않는다.
manifest 등록 전, 각 Native admission/commit, 완료 marker commit 전 권한을 검사한다.
기존 expected revision/hash CAS·소유권 상실·claim sequence·lease 만료·늦은 ACK 규칙을 유지한다.
Source cursor/ingress binding은 같은 admission transaction에서 갱신한다.

새 catalog의 선언/marker와 Native state 권한은 구별한다. 실패 attempts는 진단 로그이며
Native state를 수정하는 권한이 아니다. 전역 다중 호스트 권한·인증·clock skew·합의·운영
저장소는 미구현이다. local v1/v2의 독립 DB를 같은 장비 공통 fencing으로 간주하지 않는다.

## 9. 실제 process exit 및 장애 증거

신규 recovery 테스트는 timeout=20초인 자식 프로세스에서 os._exit(87)를 호출한다.
subprocess.run이 timeout 시 kill/reap하며, 성공 여부는 exit code와 stderr로 확인한다.
고정 UTC/명시 순서이며 난수를 쓰지 않는다.

종료 지점 11개: manifest 후, ingress/cursor binding transaction 안, Operational commit 전/후,
Health commit 후, command 후, input 완료 후, claim 후, Kafka 성공/ACK 기록 전,
batch 완료 기록 직전/직후다. 재개 결과를 연속 실행의 전체 StoredState/Snapshot bytes·
StateOutput/Change ID·manifest·History·Current 및 Kafka canonical payload 집합과 비교했다.
Kafka ACK 유실은 동일 bytes 추가 1건을 허용하며 pending은 수렴한다.

추가 사례: 종료 후 live claim이 만료 직전 batch ready를 막고 등호부터 복구, stale writer
자식 process 거부, PARTIAL 재개 중 다시 종료다. 총 recovery 14개 중 13개 crash 사례에서
os._exit(87) 총 14회, 나머지 1개는 stale writer의 통제된 오류 종료다.
기존 live writer/dispatcher 경합 테스트도 회귀에서 재실행했다.

## 10. 신규 테스트와 전체 회귀

기존 .venv Python3.11.15 / SQLite3.50.4 / pytest9.1.1에서 실행했다.
명령은 15절 및 [검증 보고서](phase-4-verification.md)의 이번 배치 절을 따른다.

| 실행 | 기대 | 실제 |
|---|---|---|
| 신규4 테스트 파일 | unit30 + local26 | **56 passed**, 6.39초 |
| Phase4 unit | 781+30 | **811 passed**, 4.18초 |
| Phase4 local integration 7파일 | 60+26 | **86 passed**, 17.02초 |
| 전체 로컬 -m 'not integration' | 1555+56 | **1611 passed, 10 deselected**, 23.07초 |

Phase4 unit/local 합897이며 전체 회귀와 중복 포함이다. 외부 integration marker10개는
실행하지 않았다. 기존 Authlib httpx deprecation warning1개 외 테스트 실패는 없다.
기준선188 중 기존3 수정 외185파일 hash가 동일하고, 문서2개는 원문 append만 수행했다.
AST·신규 Python79열·문서 링크·명령 syntax·staging12경로 완전성·git diff --check도 확인했다.
초기 신규 integration에서 Fryer Snapshot 필드명을 잘못 참조해 실패했다. 실제
TwinState.observation.operational_state에 맞춰 검사 코드를 고쳤다. 기존 테스트/기대값을
완화하거나 삭제하지 않았다. 새 등록 FIFO와 완료 marker 단조성도 구현 중 보강했다.

## 11. 미구현·미검증

실제 PySpark/DBR Query·checkpoint·Kafka/Registry 네트워크·운영 schema subject·실제
History/Kafka/DynamoDB·비용/권한·분산 authority·clock skew는 미검증이다.
wire input schema ID는 기록하지만 **출력 Avro bytes/schema ID의 durable pinning**과 실제
Registry consumer compatibility는 여전히 후속이다. Fake Kafka는 canonical JSON 모형이다.

manifest는 caller가 제공한 전체 expected sequence라는 전제가 있다. 누락된 Kafka rows,
partition 범위의 gap/미등록 원천 입력, 다른 query의 빠진 원천 소비를 이 local 엔진이
증명하지 못한다. local rowid 등록 순서와 메모리 sequence는 분산 source ordering 증거가 아니다.
대량 scan/retention/TTL/backup·init 도중 종료/전원 손실·디스크 full·복제는 미검증이다.

새 catalog는 샘플 전체 원장과 per-device 대기열을 scan하는 correctness 모델이다. production
처리량/확장성을 보장하지 않는다. 정책 오류의 수동 취소/격리·운영 migration/rebuild는
추가하지 않았다. source epoch 교체는 새로운 승인 cutover가 필요하다.

## 12. Spark Adapter 책임과 제안 signature — 구현 아님

```python
def job4_foreach_batch(batch_df, batch_id: int, *,
                      query_id: str, checkpoint_epoch: str,
                      row_adapter, authority_factory, writer_provider,
                      registry_factory, publication_factory, clock) -> None:
    ...


def rows_to_job4_records(rows, *, evaluation_time) -> tuple:
    ...
```

- Row Adapter는 실제 Kafka Row의 topic/partition/offset/key/value를 보존하고 frame header에서
  schema ID를 추출한다. sensor schema_version과 혼동하지 않는다. 실제 Registry는 승인
  factory로 주입하며 latest schema/자동 등록을 하지 않는다.
- DataFrame iterator/collect/shuffle 순서에 기대지 않고 source 순서 및 stable manifest를
  만든다. same batch retry는 최초 manifest를 대조하거나 저장 manifest로 recover한다.
  event_time sort·batch_id를 native command ID로 사용하는 방식은 금지한다.
- driver는 batch manifest/evidence·실패 전파·query lifecycle을 조정한다. executor client는
  초기화/serialization/권한/side effect 재시도를 별도 검증한다. 무제한 collect나 executor별
  SQLite 파일을 production state authority로 채택하지 않는다. 임의 thread 분산 모형도 없다.
- callback은 complete evidence가 없으면 예외를 전파하여 query가 checkpoint를 앞서가지
  않도록 한다. 실제 checkpoint와 외부 state/Publication은 한 transaction이 아니며 실패
  행렬·writer ownership·source replay를 실제 runtime에서 검증해야 한다.
- timer는 무입력에서도 호출되는 별도 승인 runtime이 필요하다. foreachBatch가 매번 자동
  tick을 제공한다고 가정하지 않는다. timer/관측의 logical 순서를 authority에 보존한다.
- 현재 engine은 같은 DB에 영속 입력/admission을 묶은 로컬 증거다. 운영 transactional
  store/client·Timer runtime·Actual Sink·출력 wire pinning·새 checkpoint 경로는 미선정/미검증이다.

이 문서의 signature는 Spark API 실행 증거가 아니다. package 설치·Query 생성/start 없음.

## 13. 정확한 Git 명령 — 사용자 검토 후 실행

Codex는 아래 staging/commit/push를 실행하지 않았다. Task C/E는 같은 authority transaction에
의존하여 함께 commit한다. fixture는 B의 실제 codec을 재사용하고 batch를 참조하므로 F에 둔다.

```bash
cd /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state
git status --short
# B: Input boundary
git add -- streaming/state/job4_input.py tests/unit/phase4/test_job4_input.py
# 이 unit은 F fixture에 의존하므로 아래 F 그룹까지 stage한 뒤 전체 diff를 검토한다.
# C/D/E/F: 동작 가능한 authority/batch와 공용 fixture·검증을 한 논리 구현 commit으로 묶음
git add -- streaming/state/job4_store.py streaming/state/job4_batch.py \
  streaming/state/writer_fencing.py tests/fixtures/phase4/job4_fixture.py \
  tests/unit/phase4/test_job4_batch.py \
  tests/integration/test_phase4_job4_batch.py \
  tests/integration/test_phase4_job4_recovery.py
git diff --cached --check
git diff --cached
git commit -m "feat(phase4): execute durable offline Job 4 batches and recovery"
# A/G: 설계·실측 보고
git add -- docs/reports/phase-4-job4-offline-verification.md \
  docs/streaming/state-job4-integration-plan.md \
  docs/reports/phase-4-verification.md
git diff --cached --check
git diff --cached
git commit -m "docs(phase4): document offline batch evidence and Spark adapter gates"
git status --short
git push origin feat/phase-4-twin-state
```

Task별 소유권은 3절에 명시했고, 서로 의존하는 codec/authority/fixture를 억지로 쪼개
중간 commit에 깨지는 테스트를 남기지 않는 두 commit 계획이다. main merge는 별도다.

## 14. Phase 4 DoD 잔여

실제 Job 4/runtime·운영 state/Publication authority·timer scheduler·checkpoint 불일치 복구·
운영 Registry/History/Kafka/DynamoDB E2E·State Rebuild·권한/비용 승인·선행 Phase2/3 병합
확인은 여전히 필요하다. 입력 topic에는 냉장/냉동 telemetry도 있을 수 있으므로 지원 제외/
라우팅/실패 관리 정책을 결정하기 전 전체 topic을 성공 소비할 수 있다고 하지 않는다.
Refrigerator/Freezer/Dishwasher Handler·Health 지원과 FAULT 추론은 추가하지 않았다.
Phase 5 Alert Lifecycle은 제외다.

## 15. 포트폴리오 재현 명령

현재 .venv에서만 실행한다. basetemp는 pytest 전용 임시 폴더이며 기존 DB/checkpoint를
지정하지 않는다. 정상 STOPPED→ROTATING→STOPPED 및 실제 종료/recovery 이름·전체 결과를
로컬 correctness 증거로 캡처할 수 있다. 실제 외부 E2E라고 표기하지 않는다.

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-job4-new-latest \
  tests/unit/phase4/test_job4_input.py tests/unit/phase4/test_job4_batch.py \
  tests/integration/test_phase4_job4_batch.py \
  tests/integration/test_phase4_job4_recovery.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-job4-final-unit tests/unit/phase4
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-job4-final-local \
  tests/integration/test_phase4_local_recovery.py \
  tests/integration/test_phase4_local_application.py \
  tests/integration/test_phase4_concurrency_recovery.py \
  tests/integration/test_phase4_fencing_recovery.py \
  tests/integration/test_phase4_offline_execution_contract.py \
  tests/integration/test_phase4_job4_batch.py \
  tests/integration/test_phase4_job4_recovery.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-job4-final-all -m 'not integration'
git diff --check
```

이번 배치 완료 후 다음 구현·설치·외부 실행으로 진행하지 않는다.
