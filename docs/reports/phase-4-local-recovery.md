# Phase 4 — Local State Output & Recovery Implementation Batch

일자: 2026-10-22 (Asia/Seoul). Worktree:
`/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`.
branch=`feat/phase-4-twin-state`, HEAD=`a1effcbd13a62a98a19d98187b2680ddf6a8db8b`.
상태: Task A~E 로컬 구현/검증 완료, **실제 서비스 E2E·Phase 4 전체 DoD는 미완료**.

## 1. 시작 상태·파일 소유권

pwd/branch/status/log를 확인했다. 직전 Health Registry commit a1effcb가 반영되어 있다.
시작 시 기존 변경은 equipment-state-contracts.md, equipment-health-contracts.md,
phase-4-verification.md와 미추적 state-output-contracts.md의 직전 설계 작업이었다.
삭제/덮어쓰기/revert/stash하지 않았다. 이번에는 output 계약과 verification에 append만
했고 원문 prefix를 bytes로 대조했다. 기존 equipment-state/health 문서는 이번에 더
수정하지 않았다. 로컬 기준 계획/AGENTS와 실제 소스·테스트·Sink 계약을 우선했다.

## 2. Task별 파일과 결과

| Task | 이번 파일 | 결과 |
|---|---|---|
| A 계약 | [state-output-contracts.md](../streaming/state-output-contracts.md) 11절 | 기존 설계 보존, schema/보장/한계를 구현과 대조 |
| B Envelope | [change_envelope.py](../../streaming/state/change_envelope.py), [test_change_envelope.py](../../tests/unit/phase4/test_change_envelope.py) | 신규 18개 단위 테스트 |
| C Ledger | [publication_store.py](../../streaming/state/publication_store.py), [test_publication.py](../../tests/unit/phase4/test_publication.py) | 신규 17개 단위 테스트 |
| D Sink/조정 | [sink_protocols.py](../../streaming/state/sink_protocols.py), [local_sinks.py](../../streaming/state/local_sinks.py), [publication.py](../../streaming/state/publication.py), [test_state_sinks.py](../../tests/unit/phase4/test_state_sinks.py) | 신규 23개 단위 테스트 |
| E 복구 | [test_phase4_local_recovery.py](../../tests/integration/test_phase4_local_recovery.py), 이 보고서, [verification](phase-4-verification.md) | 신규 18개 로컬 통합 테스트 |

신규 합계 76개 = unit 58 + local integration 18이다. Task 경계 최초 Phase 4 회귀는
A 560, B 576, C 590, D 608개가 통과했다. 이후 추가 거부/ACK/CAS 검증으로 최종 618개다.
기존 테스트 기대값은 변경하지 않았다. D의 새 테스트 작성 중 낮은 revision의 기대값을
CONFLICT에서 계약상 SUPERSEDED로 수정했고 같은 revision의 충돌은 별도 검증했다.
오류를 숨기거나 기존 테스트를 삭제하지 않았다.

## 3. 선택한 아키텍처와 대안

순수 Core 결과 → StateOutput(change optional + Current projection) → PreparedCommand →
PublicationStore.commit → 저장된 immutable output → injected sink protocols → reconcile.

| 대안 | retry/중복 | 원자성·결합도·복잡도 | 운영 이관 제약 |
|---|---|---|---|
| Append-only ledger | 재생/중복 조회 가능 | root CAS/ACK 인덱스·복구 scan을 별도 구축해야 함 | commit marker·compaction·보존 필요 |
| SQLite transactional ledger/outbox | 같은 명령 no-op, 불변 ID/payload, Sink별 시도 | 표준 라이브러리, Snapshot bytes/명령/의도를 한 transaction으로 확인 가능 | 로컬 선택일 뿐 운영 저장소 승인 아님 |
| History 기반 재구성 | 변화 이력 재전달 가능 | 무변화 관측·후보·cursor·pending 의도 복구 부족 | 추가 durable 입력/Snapshot/ACK 없이는 불충분 |

로컬 correctness 모델에는 SQLite transaction을 선택했다. 운영 DB·새 인프라를 만들거나
최종 선택하지 않았다. 개별 Sink Fake는 별도 파일이라 ACK 전후 부분 실패가 남는다.

## 4. Output/identity 계약

`axis`는 OPERATIONAL/HEALTH discriminator다. native ID는 각각 state_change_id/
health_change_id 필드로 보존한다. Health는 INITIALIZED/TRANSITION, Operational은 기존
intent가 있을 때만 TRANSITION이다. Hood 최초 확정·pending·무변화 관측은 가짜 변경을
만들지 않는다. 필요한 Snapshot 갱신은 Current-only projection으로 별도 보존한다.

정책/version, source/provenance, event_time/effective_at, 명시 evaluation_time, cause/token,
Health last_seen/deadline/fingerprint/revision을 보존한다. root에 펼친 generation/command_ref/
change_revision/previous_change_id는 기록 순서 metadata다. 실제 원천 Kafka 정보가 없으면
origin_ref=null이다. numeric Avro·기존 Kafka key 계약·기존 ID 생성 함수를 바꾸지 않았다.

UTC μs Z/key 정렬/UTF-8 canonical JSON, 명시적 schema version, digest, 별도 enum/typed
lineage를 검사한다. bool과 integer revision을 혼동하지 않는다. 같은 bytes는 같은 digest,
같은 ID의 다른 payload는 충돌이다. D와 D+δ에서 같은 Health 만료 ID라도 평가 시각이
다르면 payload가 달라질 수 있으므로 최초 commit된 명령/bytes만 재사용한다.

## 5. 영속성 및 Snapshot 원자성

PreparedCommand는 축, command_ref, generation, before root revision/digest, after Snapshot
bytes, 기록된 input/evaluation_time/Core decision, output을 담는다. PublicationStore는
BEGIN IMMEDIATE에서 CAS/정책/chain을 검증해 root/command/output/deliveries를 함께 commit한다.
Snapshot이 바뀌는데 output 의도가 없으면 거부한다. command 정확 재시도는 원래 ID를
반환하며 새 counter를 부여하지 않는다. CAS 패자/다른 정책·세대/변경 revision gap은 실패다.

Health typed 복원은 기존 encode/decode_health_snapshot을 재사용했다. Operational은
dataclass 전체 필드의 opaque JSON image를 보존한다. Operational typed Codec/migration은
미구현이다. caller가 별도로 먼저 상태를 저장하거나 Spark state를 먼저 확정하는 경계는
이 SQLite transaction이 보호하지 않는다. 한 축 단위이며 cross-axis 원자성은 없다.

SQLite synchronous=FULL/독립 파일 재개방과 os._exit 장애를 테스트했다. 디스크/파일시스템
내구성·전원 손실/호스트 장애·원격 파일시스템·backup 보장까지 실증한 것은 아니다.
기존 파일 schema/version과 맞지 않는 DB를 자동 migration하거나 초기화하지 않는다.

## 6. Sink별 보호·ACK·재시도

| Sink | 로컬 구현의 보호 | 제한 |
|---|---|---|
| History | native pair key, 같은 bytes no-op, 같은 revision/다른 ID 충돌, predecessor/정책·세대 검사 | 최초 chain은 revision 1부터; migration/역사 correction 경로 없음 |
| Kafka | equipment_id key, 동일 bytes append, 개별 ACK callback/error/topic/key/partition/정수 offset/digest 검사 | 중복 메시지 남음, 실제 broker/producer/idempotence/EOS 검증 아님 |
| Current | 축별 완전한 projection, 조건부 revision 비교, 동일 p no-op/다른 payload conflict, 옛 p superseded | 명시적 세대 고정; production DynamoDB expression·lease 없음 |

SUPERSEDED 성공 판정에는 더 최신 canonical projection이 반환되어야 하고 그 의도가 이
Ledger에 정확히 존재해야 한다. Current의 임의 높은 값만으로 완료를 선언하지 않는다.
미확정 더 최신 투영을 만난 테스트는 완료를 보류한 뒤 해당 의도를 정상 commit해 복구했다.

호출 전 UNKNOWN/attempt를 durable 저장한다. callback이 없거나 ACK가 부적합하면 FAILED로
추적하며 pending은 유지한다. 충돌은 CONFLICT로 남고 자동 재시도하지 않는다. ACK 직후
프로세스가 죽으면 UNKNOWN을 재시도한다. 이전 attempt의 늦은 ACK가 새 attempt를 완료하지
못하게 번호를 대조한다. 단일 로컬 dispatcher 가정이며 운영 소유권/lease는 별도다.
reconcile은 한 bounded pass이며 오류를 숨기지 않고 멈춘다. 독립 축 dispatch는 가능하다.

## 7. 요구된 18개 장애 항목의 실제 검증

아래는 설계 표의 예상만 옮긴 것이 아니라 신규 local test의 assertion 결과다.
`partial`=test_partial_failure_reopen_pending_and_converge의 parameter,
`death`=test_actual_process_death_after_sink_ack,
`atomic`=test_actual_process_death_at_atomic_snapshot_output_boundary다.
모든 시각·policy 숫자는 fixture 명시값이며 실제 장비 정책이 아니다.

| 요구 | Failure Injection Point | Expected | Actual | Retry Behavior | Duplicate Defense | Recovery Result |
|---|---|---|---|---|---|---|
| 1 History→Kafka 실패 | partial: before_kafka | H ACK/K 실패 | H ACK/K FAILED | 파일 reopen, pending 스캔 | H 1건, 같은 payload | 통과 |
| 2 History→Current 실패 | partial: before_current | H/K ACK, C 이전 | 해당 상태 확인 | C만 재시도 | H/K ACK 재사용 | 통과 |
| 3 Kafka→History 실패 | partial: 순서 override, before_history | K만 ACK | K ACK/H FAILED | 기본 순서로 누락 H/C | K 재발행 안 함 | 통과 |
| 4 Kafka→Current 실패 | partial: 순서 override, before_current | K ACK/C 실패 | 실제 상태 확인 | H/C 복구 | 기존 K 유지 | 통과 |
| 5 Current→Kafka 실패 | partial: 순서 override, before_kafka | C ACK/K 실패 | 실제 상태 확인 | H/K 복구 | C 그대로 | 통과 |
| 6 모든 Sink→완료 실패 | before_complete 예외 | ACK 유지, 미완료 | pending 1개/모든 ACK | reopen 후 완료만 | K 1건 유지 | 통과 |
| 7 ACK 후 프로세스 종료 | death after_history/kafka/current | side effect 있음/UNKNOWN | exit 73/파일 reopen 확인 | 같은 bytes 재시도 | H/C 1개, K ACK 유실은 2개 허용 | 통과 |
| 8 같은 batch 반복 | 동일 command 재commit | 같은 root/ID | root revision 재증가 없음 | 기존 output 조회 | command exact no-op | 통과 |
| 9 같은 ID 재발행 | LocalKafka 직접 동일 change publish | 동일 bytes 중복 가능 | 메시지 2개, ID 1개 | consumer pair 식별 모델 | History 1건/Current 유지 | 통과 |
| 10 늦은 Current | 복구 뒤 옛 STALE/초기 투영 | 최신 p 유지 | SUPERSEDED, HEALTHY | 원래 출력도 완료 대사 | generation/p 비교 | 통과 |
| 11 같은 revision 충돌 | 다른 input 같은 p | 기존 보존 | OutputConflict 확인 | conflict 자동 성공 처리 없음 | 정확 payload 비교 | 통과 |
| 12 History 중복 | 같은 change write 반복 | canonical 1건 | 1건 assertion | duplicate no-op | pair ID/bytes | 통과 |
| 13 Outbox 재시작 | commit 전/후 os._exit | rollback 또는 Snapshot+pending 함께 | exit 74/75, root와 pending 대조 | 같은 command 재commit | 원자적 transaction | 통과 |
| 14 Pending 재조회 | 각 실패 뒤 DB close/reopen | pending 유지 | 저장된 의도 재조회 | reconcile | ID/digest 유지 | 통과 |
| 15 복구 중 재실패 | before_kafka 2회 | H 1건/pending 지속 | attempts=2 확인 | 세 번째 정상 완료 | ACK된 H 재작성 없음 | 통과 |
| 16 두 축 독립 | Op 완료/Health pending 후 reopen | 한쪽 결과 비침범 | 두 axis Snapshot/Current 비교 | 독립 dispatch 후 reconcile | 축별 row/context | 통과 |
| 17 같은 Change ID | 파일 복원/같은 recorded command | 원래 native ID/bytes | 전체 output·원장 ID 대조 | 새 ID 발급 없음 | canonical identity | 통과 |
| 18 최종 수렴 | 정상 재시도 완료 | pending 0, H/K/C 정합 | 전체 Snapshot/timer/ID 및 row 대조 | 종결 상태 유지 | H/C no-op, K at-least-once | 통과 |

실제 로컬 통합은 18 cases다: partial 5 + 완료 실패 1 + death 4 + atomic 3 + 재실패 1 +
반복 batch 1 + STALE/복구·late C 1 + conflict/History 중복 1 + 축별 복구 1.
실제 자식 프로세스 종료 cases는 death 4 + atomic 3 = 7개다. 나머지는 제어 가능한 동기
예외와 파일 close/reopen이다. 단순 동일 객체 재호출만으로 재시작을 대체하지 않았다.

기존 출력 설계 9절의 32개 표 전체를 모두 실행했다고 주장하지 않는다. 위 18개 요청과
신규 unit 58개 범위만 실행했다. Consumer 실제 dedup·TTL/보존 만료·partition 증설·
클라우드 checkpoint/스키마 migration·인프라 장애는 미검증이다.

## 8. 실행 명령·최종 결과

모든 테스트는 로컬 .venv, bytecode/cache 비활성화와 별도 basetemp를 사용했다.
로컬 복구 파일은 외부 Kafka/Registry를 요구하지 않으므로 기존 integration marker를
붙이지 않았다. 그 marker는 실제 서비스/쓰기 의존을 뜻한다. 새 파일은 -m not integration
회귀에도 포함된다. 새 marker/pyproject/dependency를 도입하지 않았다.

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-output-final-new tests/unit/phase4/test_change_envelope.py tests/unit/phase4/test_publication.py tests/unit/phase4/test_state_sinks.py tests/integration/test_phase4_local_recovery.py
# 76 passed in 1.10s (최종 오류 기록 문구 보완 전; 이후 아래 전체 회귀도 통과)

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-output-final-phase4 tests/unit/phase4
# 618 passed in 1.03s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-output-final-local-recovery tests/integration/test_phase4_local_recovery.py
# 18 passed in 0.80s

PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider --basetemp /private/tmp/ktd-output-final-local -m 'not integration'
# 1350 passed, 10 deselected, 1 warning in 4.45s

git diff --check
# 통과
```

경고는 기존 Authlib httpx deprecation 1건이다. 외부 integration 10개는 제외했다.
1350 passed를 외부 integration 또는 cloud E2E 통과로 표현하지 않는다.
새 Python 9개 AST/79열/whitespace, 문서 링크/anchor/표·경로, 변경 범위와 보호 파일 해시를
검사했다. 시작 baseline 148개(미추적 계약 포함) 중 output 계약/verification만 append로
바뀌고 나머지 146개는 유지됐다. 기존 Core·Registry·Codec·Timer·Phase 2/3·설정·기존
테스트·checkpoint를 수정하지 않았다. runtime 설치·클라우드/API 쓰기·Git staging/publication은 없다.

## 9. 미구현·위험·Phase 4 DoD 잔여

- 실제 Kafka Producer/Avro subject/ACK, DynamoDB Conditional Update, UC Iceberg Writer/DDL,
  consumer dedup과 실제 세 Sink 부분 실패 수렴은 미구현·미검증이다.
- SQLite 원자성과 Fake CAS는 Spark state/외부 outbox/checkpoint 원자성 증거가 아니다.
  운영 저장소·상태 소유권·lease/fencing·세대 발급·CAS/명령 번호 정책을 닫아야 한다.
- Operational state_started_at/latest_metrics 등 완전한 조회 projection과 typed
  Snapshot Codec/migration·정책 전환·State Rebuild cutover는 남았다.
  Health는 기존 Codec exact-policy 복원을 유지한다. bytes 저장만으로 이행을 주장하지 않는다.
- 명시적 counter/generation 공급을 local fixture가 담당한다. 운영 domain/source 권한·
  timeout은 결정하지 않았다. FAULT·Alert·미지원 장비 추론은 추가하지 않았다.
- 재시도는 동기 bounded pass이며 자동 Scheduler/worker/백오프·TTL·scalable 인덱스·
  영속 소유권·다중 dispatcher·복구 SLA를 제공하지 않는다. 현재 superseding 의도 검사는
  로컬 scan이다. 성능/비용·복구 시간 수치는 측정·보장하지 않았다.
- Phase 4 전체 DoD에는 실제 상태 입력 승인·Job 4 state/checkpoint/timer 통합과 History/
  topic/current 실측 대사·장애 검증이 남아 있다. Phase 5로 자동 진행하지 않는다.

## 10. Task별 Git 계획 — 실행하지 않은 명령

거대한 한 commit을 권하지 않는다. 아래 순서로 사용자가 검토 후 직접 실행할 수 있다.
A의 equipment-state/health 문서는 **직전 작업부터 있던 미커밋 설계 연결**을 함께 다룬다.
이번 구현이 그 원문을 변경한 것은 아니다. verification에도 직전 설계 내용과 이번
append가 함께 있으므로 E에서 전체 diff를 확인해야 한다. Git 명령은 수행하지 않았다.

```sh
ktd_output_root='/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state'
cd "$ktd_output_root"

# A: output contract + preserved design references
git add -- "$ktd_output_root/docs/streaming/state-output-contracts.md" "$ktd_output_root/docs/streaming/equipment-state-contracts.md" "$ktd_output_root/docs/streaming/equipment-health-contracts.md"
git commit -m "docs(phase4): define local state output and recovery contracts"

# B: native results -> immutable output model
git add -- "$ktd_output_root/streaming/state/change_envelope.py" "$ktd_output_root/tests/unit/phase4/test_change_envelope.py"
git commit -m "feat(phase4): add deterministic state change envelopes"

# C: SQLite transactional root/command/output ledger
git add -- "$ktd_output_root/streaming/state/publication_store.py" "$ktd_output_root/tests/unit/phase4/test_publication.py"
git commit -m "feat(phase4): add local transactional publication ledger"

# D: injected protocols + independent persistent fakes + dispatch
git add -- "$ktd_output_root/streaming/state/sink_protocols.py" "$ktd_output_root/streaming/state/local_sinks.py" "$ktd_output_root/streaming/state/publication.py" "$ktd_output_root/tests/unit/phase4/test_state_sinks.py"
git commit -m "feat(phase4): model state sink delivery and reconciliation"

# E: actual process exit/file reopen tests + final evidence
git add -- "$ktd_output_root/tests/integration/test_phase4_local_recovery.py" "$ktd_output_root/docs/reports/phase-4-local-recovery.md" "$ktd_output_root/docs/reports/phase-4-verification.md"
git commit -m "test(phase4): verify local publication crash recovery"

# After the five commits and review only:
git push origin feat/phase-4-twin-state
```

확인한 origin은 `https://github.com/yccs-26/kitchen_to_digital.git`이며 fetch/push는 실행하지 않았다.
main merge는 Phase 2/3 선행 병합과 Phase 4 전체 DoD 충족 후에만 별도 검토한다.
이번 승인 배치는 완료했고 다음 작업은 실행하지 않는다.

## 11. 포트폴리오 증빙

위 8절의 신규 테스트 76개, 로컬 복구 18개, Phase 4 618개와 비외부 회귀 1350개 출력은
각각 구분해 촬영할 수 있다. 증빙 제목은 “SQLite/Fake 기반 로컬 출력·복구 검증”으로
표기한다. 자식 프로세스 exit 코드와 reopen assertion은 테스트 소스와 연결해 설명하며
실제 broker/DynamoDB/Iceberg 장애 실험 또는 exactly-once로 표현하지 않는다.
