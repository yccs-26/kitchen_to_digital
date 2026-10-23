# Phase 4 로컬 동시성·Fencing·복구 계약

2026-10-23. **선택적 로컬 v2 경로**다. 기존 v1 경로를 자동 승격하지 않는다.
[출력 계약](state-output-contracts.md), [Health 계약](equipment-health-contracts.md),
[Job 4 계획](state-job4-integration-plan.md),
[이번 검증 보고서](../reports/phase-4-concurrency-recovery.md)를 함께 따른다.

## A. 실제 감사 결과와 재현

| 경계 | 기존 구현에서 확인한 사실·위험 | 새 경로와 실행 증거 |
|---|---|---|
| Snapshot read → compute → write | read/load가 별도 호출. BEGIN IMMEDIATE 안의 before hash/revision 검사가 경쟁 쓰기를 거부 | 기존 CAS 재사용, RevisionConflict로 재읽기·재계산 분류; 같은 revision 경쟁 테스트 |
| 같은 명령/다른 명령 | 축별 command_ref+전체 payload 동일 replay만 멱등. 장비 전체 admission 없음 | 같은 요청/축/정책을 기록; 둘의 live writer가 같은 준비 결과를 commit해도 축별 root revision 1 |
| Operational/Health | Op 먼저 commit 후 종료 가능. 다음 이벤트가 남은 Health를 추월할 수 있음 | ADMITTED/PARTIAL/COMPLETE 장벽; 다음 명령은 CommandBusy, 기존 명령만 재개 |
| writer revision/ABA | domain generation 고정 검사만 있고 소유권 takeover 없음 | 장비별 authority epoch 증가. A→B→A여도 옛 A token 거부 |
| pending 다중 선택 | pending scan은 claim 아님. UNKNOWN 재획득이 즉시 가능 | output 단위 durable lease/sequence; 같은 시각 두 claim은 한 개만 성공 |
| 종료와 잠금 | SQLite는 종료 시 미완료 transaction rollback. 완료된 UNKNOWN에는 소유권 없음 | transaction 안 savepoint; 프로세스 종료 후 durable claim은 명시 lease 만료까지 유지 |
| 늦은 ACK | attempt 번호는 새 attempt 보호, 별도 owner/expiry 없음 | claim owner+sequence+expiry와 기존 attempt를 모두 검사 |
| Current 역순 | 기존 LocalCurrent는 per-axis generation/p/revision/context 비교 | 그대로 사용. 최신 Current 뒤 옛 출력은 SUPERSEDED, 되감기 없음 |
| 다른 장비 | SQLite 파일 쓰기 lock은 전체 파일 단위 | admission/authority/claim identity는 장비별. 장벽은 다른 장비 처리/출력을 막지 않음 |

SQLite transaction만으로 계산 소유권·장비 전체 명령 순서·외부 side effect 중복이
해결되지 않는다. 위 위험은 새 unit/process 테스트에 대응하며 기존 테스트는 보존했다.

## B. Writer authority와 CAS

`FencedPublicationStore`는 PublicationStore의 축별 저장/출력/ACK 구현을 재사용한다.
새 파일에서만 SQLite user_version=2를 생성하며 v1·관련 없는 DB·다른 version은 거부한다.
기존 사용자 DB 자동 변경·migration은 없다. 새 v2도 legacy PublicationStore로 열지 않는다.

`acquire_writer((store_id,equipment_id), owner)`는 **명시적인 로컬 takeover**다.
첫 epoch=1, 이후 BEGIN IMMEDIATE 안에서 현재 epoch를 읽고 조건부 UPDATE의 rowcount=1을
검사하여 증가한다. owner 이름 재사용으로 epoch를 되돌리지 않는다. token은 인증 수단이
아니며, 이 API 자체가 운영 leader election/권한 검증을 제공하지 않는다.

WriterToken epoch와 기존 Envelope generation, Health monitoring_epoch,
observation_generation은 서로 다르다. takeover는 domain generation이나 ID 재료를
바꾸지 않는다. policy/cutover를 자동 승격하지 않는다. bootstrap initialize는 신뢰하는
caller의 명시 동작이며 기존 root를 재설정하는 API가 아니다.

`run_command(token, ref, request_bytes, axes, operation)`가 장비 순서를 admission한다.
transaction 안에서 authority를 검사하고 동일 ref의 canonical request·예상 축을 비교한다.
새 명령은 앞선 미완료 명령이 있으면 거부한다. 동일 ref의 다른 입력/평가 시각/profile/
정책은 OutputConflict다. 동일 ref·요청은 새 epoch writer가 이어받을 수 있다.

각 `commit`은 같은 transaction에서 authority·device/ref·admitted axis를 검사하고 기존
PublicationStore.commit을 호출한다. root revision/hash/domain generation 전제와 UPDATE는
동일 BEGIN IMMEDIATE write lock 아래 실행되므로 compare-and-set 효과를 갖는다.
root UPDATE 자체는 기존 SQL을 유지하며 authority UPDATE에는 명시 epoch WHERE가 있다.
중첩 저장은 SAVEPOINT로 감싸 내부 실패를 caller가 catch하더라도 부분 root/의도가 남지
않는다. revision만 바뀐 패자는 RevisionConflict(재읽기/재계산 가능), authority 변경은
StaleWriter(새 권한 필요), payload/profile 충돌은 OutputConflict(자동 재시도 불가)다.
SQLite lock/busy 오류는 sqlite3 오류 그대로 전파하며 자동 무한 재시도하지 않는다.

## C. Cross-axis 완료 장벽

| 상태 | 의미 | 허용 |
|---|---|---|
| ADMITTED | 요청은 영속, 아직 완료 축 없음 | 같은 요청 계산/재시도, 다른 장비 명령 |
| PARTIAL | 예상 축 일부만 snapshot+command+intent 확정 | 동일 요청으로 남은 축 재개; 후속 장비 명령/해당 출력 발행 차단 |
| COMPLETE | 모든 예상 축이 각 transaction으로 확정 | 이후 명령 및 해당 출력 dispatch; Sink 완료와는 별도 |

마지막 축의 Snapshot/의도 commit과 done 축 표시는 같은 transaction이다. health 먼저
저장된 저수준 경로도 동일한 장벽을 적용한다. 기본 Hood Application 순서는 기존대로
Operational→Health다. 단일 cross-axis Snapshot read나 외부 원자적 업무 view는 제공하지
않는다. 저장 도중 raw root 조회자는 PARTIAL 데이터를 볼 수 있으므로 command_status를
확인해야 한다. 출력은 두 축 COMPLETE가 되기 전 claim 단계에서 차단한다.

`FencedStateApplication(existing_app)`는 numeric/synthetic/tick 호출을 위 장벽으로 감싼다.
입력·고정 평가 시각·해당 장비 binding/Health policy/Hood policy/profile을 요청에 담고,
기존 Application/Registry/Reducer/Timer/Codec만 실행한다. 다른 장비의 정책은 pin하지 않는다.
장비 순서는 caller의 **admission 순서**다. 원천 event-time 정렬·누락 명령 추정·우선순위
발급은 하지 않는다. 외부의 더 이른 명령이 아직 도착하지 않았는지는 알 수 없다.

invalid 입력이 admission 이후 실패하면 ADMITTED 장벽을 유지한다. 실패 명령의 조용한
skip/cancel/reset은 없다. 운영 quarantine·후보 단절 및 관리 취소 정책은 미결정이다.
정책 변경 중 PARTIAL은 원래 정책으로 복구하거나 별도 승인된 관리 절차가 필요하다.
이번에 자동 migration/취소 API는 만들지 않았다.

## D. Dispatcher claim과 ACK 불확실성

`claim(output_id, owner, evaluation_time=..., lease=...)`는 output 단위 lease를 저장한다.
평가 시각은 aware UTC, lease는 명시 양수 timedelta다. `now < expires_at`이면 기존 claim이
유효하고 `now >= expires_at`부터 새 sequence로 재획득한다. 완료/release해도 sequence는
삭제하지 않아 owner 재사용 ABA를 막는다. 마지막 성공 검사 시각보다 역행하면 거부한다.

`ClaimedDispatcher.dispatch_claimed(..., clock=..., lease=...)`는 기존 ACK 검증 coordinator를
그대로 호출한다. begin_attempt/finish_attempt/complete에서 현재 claim을 검사한다.
`reconcile_claimed`는 한 번의 bounded scan에서 PARTIAL/다른 live claim을 BLOCKED로
반환하고 다른 출력을 진행한다. Sink 오류/충돌/손상은 숨기지 않고 pass를 중단한다.
legacy dispatch/reconcile을 v2에서 직접 쓰면 claim 부재로 거부된다.

정상/예외 반환은 자기 claim만 release한다. 실제 process exit에는 finally가 실행되지
않으므로 재획득은 주입한 clock의 lease 만료를 기다린다. 자동 갱신·background 실행은
없다. Sink 호출이 lease를 넘으면 늦은 ACK를 저장하지 않고 UNKNOWN을 남긴다.
외부 side effect는 되돌릴 수 없다. 새 dispatcher가 다시 보내면 Fake Kafka에 동일 bytes가
중복 append된다. History는 동일 ID/payload 1건, Current는 기존 조건부 적용으로 수렴한다.
lease는 외부 Kafka 전송을 fence하지 않으며 현재 호출 중인 네트워크 쓰기를 취소하지 않는다.

## E. 결정성·복구·보장 한계

초기 Snapshot/정책·명령 순서·최초 평가 시각·command_ref가 같으면 domain Snapshot,
Envelope·native Change ID는 takeover/파일 재개 후에도 동일하다. authority epoch/claim
sequence/attempt 횟수는 실행 흔적이므로 연속 실행과 같다고 주장하지 않는다.
같은 시각 tick 먼저와 관측 먼저는 기존 Health 계약대로 이력이 다르다.

프로세스가 commit 전/transaction 중 종료되면 해당 축은 rollback한다. commit 후에는
완료된 축을 cached replay하고 나머지를 계속한다. 무작위 순서 대신 명시 rendezvous와
고정 시각으로 실제 중첩 실행을 재현한다. 자식 프로세스 timeout/kill/reap은 테스트 책임이다.

한 호스트의 독립 연결/프로세스와 새 임시 SQLite 파일에서만 검증한다. 파일 전체 write
lock의 처리량 한계·파일 손실/디스크 full·복제·분산 clock skew·network filesystem lock은
미검증이다. clock 역행 검사는 관측된 성공 평가 시각에 한정되며 운영 clock authority를
대체하지 않는다. 영속 보존/TTL/대량 pending scan/운영 leader·lease 발급/취소·재시도 예산은
별도 설계다. Spark checkpoint·History/Kafka/DynamoDB 전체의 exactly-once는 보장하지 않는다.
