# Phase 4 — Spark Runtime & State Authority Readiness Decisions

2026-10-23 (Asia/Seoul). **오프라인 계약/검증 완료, 운영 채택·설치·실행 미승인**.
[Job 4 계획](../streaming/state-job4-integration-plan.md),
[출력 계약](../streaming/state-output-contracts.md),
[동시성 계약](../streaming/state-concurrency-contracts.md),
[직전 감사](phase-4-readiness-audit.md)를 따른다. Task A~F를 수행하고 이 배치에서 멈춘다.

## 1. 사전 게이트·Git 및 작업 환경

- worktree: `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`
- branch: `feat/phase-4-twin-state`; 시작·종료 HEAD `fb920c05a2a7c98b80e9276ab88c1ab03868044e`.
- 시작 status clean/index empty. G1~G6는 아래 이력에 있고 선행 40경로가 모두 tracked/present다.

| 그룹 | commit | 내용 |
|---|---|---|
| G1 | 7fd480c | canonical outputs / Operational codec |
| G2 | f719fe6 | local ledger / sinks / recovery |
| G3 | 09a3446 | offline Avro wire |
| G4 | 4ae537f | Local Application / Health timers |
| G5 | 907e08c | writer fencing / claimed recovery |
| G6 | fb920c0 | accumulated docs / readiness audit |

`git fetch origin feat/phase-4-twin-state`를 실제 실행했다. 첫 sandbox 시도는 공용 Git
metadata의 FETCH_HEAD 쓰기 권한 때문에 실패했고, 사용자 승인 범위의 fetch를 권한 승격으로
재실행하여 성공했다. HEAD/FETCH_HEAD/origin tracking ref 셋이 fb920c0, ahead/behind=0/0이다.
이 결과는 **이번 fetch 시점**의 원격 확인이다. 이후 원격 변경을 감시하는 보장은 아니다.
fetch 외 Git index/이력·publication은 변경하지 않았다.

기존 추적 파일 184개의 시작 SHA-256을 `/private/tmp/ktd-runtime-authority/baseline.json`에
보관했다. Core/FSM/Codec/Registry/Timer·기존 v1/v2 구현·기존 테스트는 수정하지 않는다.
원본 checkout AGENTS/로컬 실행 계획의 Phase 4 DoD, 이 worktree 계약/보고서를 대조했다.
Phase 2 Job/tests와 접근 가능한 Phase 3 worktree 코드는 read-only 참고이며 복사/병합하지 않았다.

## 2. Task A — 현재 Runtime 호환성 분석

| 구성 | 실제 설치 | 문서상 필요/권장 후보 | 판정 |
|---|---|---|---|
| Python | 3.11.15 | Spark 3.5.6: 3.8+ / 4.0.0: 3.9+ | 최소 범위 안, runtime 전체 호환 실증 아님 |
| Java | Oracle 24 arm64, 설치 JDK 1개 | 3.5.6: 8/11/17 / 4.0.0: 17/21 | 현재 24는 두 version의 명시 지원 목록 밖 |
| PySpark / spark-submit | package/spec/명령 없음 | 비교용 baseline 4.0.0 권장 후보, 3.5.6 대안 | 설치/실행 안 됨 |
| pandas / pyarrow / numpy | 세 package 모두 미설치 | 4.0 SQL: pandas>=2, Arrow>=11; 3.5.6 문서 Arrow>=4,<13 | 분리 환경 필요 |
| pytest / fastavro / avro | 9.1.1 / 1.12.2 / 1.12.2 | 기존 lock/직전 회귀 유지 | 현재 pure local 실행 가능 |
| confluent-kafka / cloudpickle | 2.15.1 / 설치·dev lock 3.1.1 | runtime SDK/worker serialization 매칭 필요 | broker/remote worker 연결 증거 아님 |

공식 version 고정 문서를 이번에 조회했다:
[4.0 환경](https://spark.apache.org/docs/4.0.0/index.html),
[4.0 설치/SQL 의존성](https://spark.apache.org/docs/4.0.0/api/python/getting_started/install.html),
[3.5.6 설치/의존성](https://spark.apache.org/docs/3.5.6/api/python/getting_started/install.html).
4.0 Pandas API on Spark의 별도 pandas>=2.2 조건과 기본 SQL 표를 구별한다.

권장 **검증 후보**는 portable JDK 17 + Python 3.11 + Spark 4.0.0이다. native timer/API
비교가 가능하고 JDK17은 두 후보에 공통이다. 이는 최신 release/보안 수명/DBR production
version을 채택한 결정이 아니다. DBR target이 3.5 계열이면 3.5.6 비교 환경을 따로 만든다.
Pandas/Arrow/native library·cloudpickle·JVM/Scala 조합은 실제 설치 resolve와 callback
serialization·UTC·idle/restart timer smoke로 검증해야 한다. DBR 내장 PySpark를 local wheel로
덮어쓰지 않는다. DBR의 실제 version/권한/지원 API는 조회하지 않았다.

## 3. 설치·구성 필요사항과 정확한 준비 명령

**아래는 사용자 승인 후의 제안 명령이며 실행하지 않았다.** 전역 JDK/Python·현재 .venv·
pyproject/uv.lock는 바꾸지 않는다. 승인된 portable JDK17 Home의 실제 경로가 먼저 필요하다.
현재 그 artifact/path/checksum은 미확정이므로 임의 다운로드 URL·checksum을 만들지 않는다.
OS JDK 등록이나 전역 PATH 설정 없이 지정한 JDK를 이 shell/process에서만 사용한다.

```bash
# 사용자가 승인한 portable JDK17 경로를 먼저 JOB4_JAVA_HOME에 지정한 뒤 실행
(
  cd /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state
  : "${JOB4_JAVA_HOME:?승인된 portable JDK17 Home 경로를 지정하세요}"
  job4_runtime_dir=/private/tmp/ktd-job4-spark4-approved
  test ! -e "$job4_runtime_dir" || exit 1
  .venv/bin/python - "$JOB4_JAVA_HOME" <<'PY'
import re
import subprocess
import sys
from pathlib import Path
java = str(Path(sys.argv[1]) / 'bin' / 'java')
output = subprocess.check_output([java, '-version'], stderr=subprocess.STDOUT,
                                 text=True)
if not re.search(r'version "17(?:[.\"])', output):
    raise SystemExit('Approved JDK17 required; stopping before installation')
print(output)
PY
  test "$?" -eq 0 || exit 1
  mkdir -p "$job4_runtime_dir"
  uv export --frozen --group dev --group msk --no-emit-project \
    --output-file "$job4_runtime_dir/base-requirements.txt" || exit 1
  job4_env_dir="$job4_runtime_dir/venv"
  uv venv --python "$PWD/.venv/bin/python" "$job4_env_dir" || exit 1
  uv pip install --python "$job4_env_dir/bin/python" \
    --requirement "$job4_runtime_dir/base-requirements.txt" \
    'pyspark[sql]==4.0.0' 'pandas==2.2.3' 'pyarrow==12.0.1' \
    'numpy==1.26.4' || exit 1
  uv pip check --python "$job4_env_dir/bin/python" || exit 1
  uv pip freeze --python "$job4_env_dir/bin/python" \
    > "$job4_runtime_dir/resolved-requirements.txt" || exit 1
  JAVA_HOME="$JOB4_JAVA_HOME" PYSPARK_PYTHON="$job4_env_dir/bin/python" \
    "$job4_env_dir/bin/python" -c \
    'import pyspark,pandas,pyarrow; print(pyspark.__version__,pandas.__version__,pyarrow.__version__)'
)
```

위 pins는 비교용 후보이며 resolver/이 Mac의 wheel/통합 API 실측은 미확인이다.
위 명령은 version import까지만 하며 Spark Query를 시작하지 않는다. Spark smoke는 별도 승인 대상이다.
임시 환경에는 checkpoint/운영 데이터를 두지 않으며 기존 경로를 재사용·삭제하지 않는다.
3.5.6을 선택해도 별도 환경에서 PySpark pin을 3.5.6으로 바꾸고 Arrow<13 및
대상 API를 재검증한다. 두 버전을 한 환경에 설치하지 않는다.

## 4. Task B — State Authority 후보 비교와 권장안

| 비교 기준 | Spark stateful 중심 | 외부 transactional authority 중심 | Spark checkpoint + durable publication 조합 |
|---|---|---|---|
| Snapshot/의도 원자성 | Spark state와 외부 intent 사이 원자성 없음 | state+command+intent+장벽을 같은 transaction으로 설계 가능 | checkpoint와 intent 로그 사이 cut/commit marker 필요 |
| checkpoint replay | native replay 가능, side effect 재전달 방어 필요 | 재실행은 원장 조회/CAS 승자 재사용 | 두 로그 대사·유실 경계 증명이 핵심 |
| dedup 원장 보존 | state schema/TTL/checkpoint와 함께 관리 | Snapshot/receipts/command 보존을 권위 저장소에서 관리 | state TTL과 publication 보존의 불일치 위험 |
| Health timer 복원 | native timer 후보, idle/restart API 실측 필요 | 명시 tick을 동일 admission에 기록; timer runtime 별도 필요 | timer checkpoint+출력 의도 양쪽 복구 필요 |
| Operational/Health 축 | 묶인 per-key state 또는 복합 state 설계 | 기존 축별 transaction+durable 완료 장벽을 재사용 가능 | partial emission/순서와 snapshot version 연결 필요 |
| writer fencing | query/task attempt를 운영 epoch로 간주 불가 | 인증 authority의 durable epoch와 commit 조건을 결합 | Spark epoch와 외부 authority를 명시 대응해야 함 |
| multi-writer | task retry/speculation/dual query 통제 필요 | 단일 장비 admission+CAS, stale writer 거부 | 두 영역 권한·claim 조정 필요 |
| Publication 보존 | 별도 durable 출력 경로 필수 | Snapshot과 같은 원자 저장 경계로 보존 가능 | append/commit marker·scan·ACK 기록 필요 |
| State Rebuild | checkpoint 유실 시 별도 승인 replay/cutover | 원장/Snapshot/정책과 cutover 필요, 자동 무한 복구 아님 | 이중 로그의 보존 일치·cutover 필요 |
| DBR 호환성 | 대상 state/timer API·의존성이 중요 | foreachBatch+승인 client/runtime 접근 검증 | state API+추가 로그 commit protocol 모두 검증 |
| 복잡도/비용 | native state 활용, 외부 원자성 해결 비용 | 신규 authority 운영·요청/백업/네트워크 비용 | 추가 로그/대사/보존 및 두 경계 운영 부담 |

**권장안: foreachBatch + 외부 transactional authority 중심을 다음 설계/실험 기준으로 삼는다.**
기존 로컬 Snapshot+command+intent/CAS/장벽을 명시적인 운영 원자 단위로 옮길 수 있기 때문이다.
이는 최종 운영 저장소 채택이 아니다. relational transaction+unique/CAS 또는 조건부
transaction을 제공하는 key-value store를 비교할 수 있다. 특정 서비스/테이블/인프라는
선택·생성하지 않았다. SQLite 파일을 executor/다중 호스트 운영 authority로 쓰지 않는다.

포기/추가 부담: Spark native state/timer의 자동 lifecycle을 바로 얻지 못하고 authority
가용성·backup/보존·장비 순서·scheduler/tick·접근 비용을 직접 해결해야 한다.
Spark checkpoint와 authority의 분산 transaction은 여전히 없다. authority에 명령/의도를
확정하고 전체 성공 증빙 후 callback을 성공시키며, replay는 같은 기록 입력/평가 시각을
재사용해야 한다. source progress가 앞서는 경우를 허용하지 않는 실측이 필요하다.

필수 검증: 동일 장비 동시 CAS/epoch·partial 복구·원장 보존·intent 누락 방지·checkpoint
지연/재실행·외부 ACK 불확실성·명령 실패 격리·DBR client serialization/인증이다.
이 증거 없이 exactly-once 또는 runtime 수용 완료로 표시하지 않는다.

## 5. Task D — Writer Fencing Authority 및 dispatcher 제안

권장 authority 역할은 인증된 control plane이 발급하는 durable per-device writer epoch와
동일 권위 저장소의 commit-time 조건 검사다. DB의 단조 version을 서비스가 발급하거나
승인된 control service를 둘 수 있으나 둘 중 하나를 이번에 채택하지 않는다.

| 상황 | 제안 계약 | 이번 증거/제한 |
|---|---|---|
| epoch/generation | writer epoch는 재시작에도 단조 보존, domain generation/Health monitoring epoch와 분리 | 기존 local v2 테스트 재사용. 삭제 후 1로 재발급 금지 |
| lease/clock skew | authority 쪽 시각/만료 판정을 기준으로 ownership 판단, client 시각만으로 권한 연장 금지 | local clock 주입·역행 거부만 검증, 분산 skew 안전성 미검증 |
| network partition | 권위 확인/commit CAS 불가면 fail closed; lease만 믿고 offline commit하지 않음 | 분산 합의/lock service 미구현 |
| stale writer | 모든 state/command/intent write에 epoch+expected revision 검사 | 기존 CAS/ABA/process tests 재사용 |
| claim sequence | output별 영속 sequence, 재획득 증가, 늦은 ACK/complete/release는 옛 sequence로 불가 | 기존 claim+새 execution gate 테스트로 확인 |
| concurrent dispatcher | 만료 전 1개 live claim, 만료 뒤 takeover; side effect 자체는 취소 보장 없음 | lease는 외부 전송 fence가 아님 |
| ACK 유실/restart | UNKNOWN/실패 원장+canonical bytes 재시도, History/Current 멱등·Kafka 중복 허용 | 새 Fake ACK 유실 테스트; 실제 network 장애 실험 아님 |

v1/v2 혼용 방지 운영 cutover 제안:
기존 writer 입구 중지 → 미완료 명령/의도/ACK 대사 → Snapshot/profile/원장 검증 →
공통 authority의 ownership 발급/옛 credential·입구 차단 → 새 writer admission → 증빙 대사.
중단 시간·미완료 처리·domain generation 승격/rollback은 사용자 승인 후 확정한다.
독립 v1/v2 DB의 같은 장비는 공통 fencing이 없으므로 단순 새 DB 생성은 cutover가 아니다.
이번에 기존 DB/checkpoint·credential·epoch 데이터를 변경하지 않았다.

## 6. Task C — Command Failure Lifecycle와 수동 복구 계약

ADMITTED/PARTIAL/COMPLETE enum과 기존 전이를 유지한다. 운영 skip/cancel/quarantine 정책이
미확정이므로 새 CANCELLED/QUARANTINED 상태·자동 취소·reset·원장 편집은 구현하지 않는다.

| 원인 | 분류/현재 처리 | 재개 조건·책임 |
|---|---|---|
| DUPLICATE/OUT_OF_ORDER/ID_CONFLICT 등 기존 Core decision | 예외와 다른 정상 판정 결과. native Snapshot/intent 규칙 유지 | 기존 Application의 기록된 decision/완료 의미를 보존, 임의 실패 승격 안 함 |
| 잘못된 unit/source/profile 등 Registry 계약 예외 | 비재시도성 입력/구성 검토, ADMITTED 또는 PARTIAL 유지 | 같은 요청 변경 금지; 운영 격리/취소/정책 migration은 별도 결정 |
| 명확한 일시 시스템 오류/RetryableSinkError/SQLite busy | 재시도 후보, Snapshot/의도/ACK 완료분 유지 | 한정 budget/backoff 승인 후 같은 ref/입력/정책/최초 평가 시각 재시도 |
| RevisionConflict | 최신 권위 Snapshot 재읽기/재계산 후보 | 같은 admitted 명령과 유효 epoch 유지; CAS 패자는 새 native ID 발행 안 함 |
| StaleWriter/StaleClaim | 소유권 상실, 옛 token 재시도 불가 | 승인 authority에서 새 권한 후 원래 명령 재개 |
| 반복 PARTIAL 실패 | 후속 장비 명령 차단, 완료 축 cached replay | 원래 exact profile/정책·남은 축이 성공해야 COMPLETE |
| profile/version 변경 | 원래 기록과 충돌, 자동 최신 profile 적용 안 함 | 원래 정책/원장 복원 또는 승인 migration/rebuild 설계 |
| Snapshot/ledger 손상·ID/payload 충돌 | 자동 재시도/임의 초기화 금지, 수동 조사 | 검증 가능한 backup·원장/의도·정책 대사, 복구 불가 시 승인 rebuild |
| 인증/권한/알 수 없는 예외 | 단순 timeout으로 간주하지 않고 fail closed | 구성/보안/운영 조사 후 명시 분류; 무한 retry 금지 |

수동 개입은 원본 ref/request digest/정책 fingerprint/epoch/revision·완료 축·canonical 의도·
각 Sink status/attempt·오류 분류·actor/이유·명시 UTC 평가 시각·검증 근거를 보존하는
감사 절차여야 한다. 운영 audit 저장소/API는 미구현이다. 원래 Snapshot/요청/ID를 덮어쓰는
수정으로 barrier를 지우지 않는다. 동일 명령 재처리는 같은 canonical 요청이어야 하며,
수정 입력을 같은 command_ref로 바꾸는 것은 복구가 아니다.

현재 안전한 재개는 원래 명령을 같은 정책으로 성공시켜 모든 축을 확정하는 경우다.
비재시도성 잘못된 요청 때문에 이것이 불가능하면 장비를 blocked로 유지하고 사용자 관리
결정을 기다린다. pending=0 또는 bounded scan의 빈 반환으로 blocked를 완료 취급하지 않는다.
이 설계는 Phase 5 Alert Lifecycle이 아니라 명령 실행/원장 관리 책임이다.

## 7. Task E — Job 4 오프라인 실행 계약

새 순수 모듈: `streaming/state/job4_contract.py`. 시스템 시계/파일/Spark/외부 client를 읽지 않는다.

| 경계 | 이번 코드/기존 연결 지점 | 남은 runtime 책임 |
|---|---|---|
| Validated input | ValidatedCommand.from_event → 기존 validated_observation 10필드/UTC/double | Phase 2 decode_sensor_event와 schema ID별 실제 reader/권한 |
| identity/source | InputPosition(topic,partition,offset,key,writer_schema_id), key=equipment_id bytes | broker lineage 인증·권위 ingress 영속화; source 권한은 Registry |
| ordering | validate_input_order, 같은 장비 supplied offset 엄격 증가·partition 일치, 정렬 안 함 | cross-batch cursor/장비 ordinal·partition 변경 cutover |
| 명령/평가 시각 | caller command_ref + 고정 aware reference_time | ref/최초 평가 시각/lineage를 admission과 원자 저장 |
| Snapshot | 기존 FencedStateApplication→LocalStateApplication의 두 Codec/Registry | 운영 authority의 원자 저장과 장벽 |
| Timer | 기존 LocalHealthTimers/HealthTimerToken/FencedStateApplication.evaluate_timer 그대로 | durable schedule/무입력 callback·순서 기록·restart |
| output/의도 | 기존 StateOutput/ChangeEnvelope/PreparedCommand/PublicationStore | schema/bytes pinning과 실제 durable store adapter |
| History/Kafka/Current | 기존 ClaimedDispatcher+Fake 3 Sink | 실제 canonical History/개별 Kafka ACK/축별 Current CAS |
| batch/checkpoint | BatchIdentity(query_id,checkpoint_epoch,batch_id)는 감사 metadata | batch ID는 native ID/명령 ID/epoch 아님; 실제 checkpoint commit 없음 |
| completion | CommandEvidence+checkpoint_ready | 전체 expected command manifest·일관된 권위 읽기·외부 ACK 검증 필요 |
| recovery | 기존 codec/replay/cached intent/claim 재사용 | Snapshot만으로 미기록 입력/전송 성공을 복원하지 않음 |

application_arguments()는 기존 process_validated의 event/ref/time 인자만 반환한다.
**lineage/BatchIdentity는 현재 Application Ledger에 자동 저장되지 않는다.** 이 모듈이
그 원자성을 해결한 것으로 해석하지 않는다. 향후 runtime은 command+ingress record를
동일 권위 경계로 묶어야 한다. 이번에는 ingress codec/offset 저장소를 중복 추가하지 않았다.

checkpoint_ready는 모든 expected 명령 COMPLETE와 해당 출력의 세 Sink terminal 증빙을
함께 요구한다. Current NOT_REQUIRED·Kafka의 SUPERSEDED/ALREADY_APPLIED 같은 잘못된 역할은
인정하지 않고, Current-only는 History/Kafka 둘 다 NOT_REQUIRED일 때만 수용한다.
ADMITTED/PARTIAL/UNKNOWN/FAILED/CONFLICT/누락 증빙이면 false다. no-op 완료는 가짜 출력을
요구하지 않는다. caller가 전체 manifest를 누락하면 이 함수는 그것을 발견할 수 없으며,
증빙의 인증/실제 전달 자체를 증명하는 API가 아니다. 빈 manifest는 명시적 무입력일 때만 쓴다.

## 8. Task F — 신규 테스트와 회귀 증거

신규 unit은 ingress/key·정수/bool·schema ID bound·명시 시각·순서/partition·batch ID·
완료 증빙·역할별 ACK 검사를 다룬다. Fake 통합은 입력 replay/file reopen, 반복 PARTIAL과
writer takeover, ACK 유실 후 중복 수렴, invalid ADMITTED, 명시 timer, native DUPLICATE
무출력을 기존 Application에 연결한다. 기존 Core 정책을 재작성한 테스트는 없다.

첫 unit 실행은 새 모듈 부재로 collection 실패(red), 최소 구현 뒤 통과했다.
공통 실행 prefix는 `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider`다.
정확한 선택 인자와 전용 basetemp는 13절 명령을 따른다.

| 실행 | 기대 | 실제 결과 |
|---|---|---|
| 신규 unit+Fake local | 31 unit + 6 local 모두 통과 | **37 passed**, 0.49초 |
| Phase 4 unit | 기존 750 + 31 | **781 passed**, 2.63초 |
| Phase 4 local integration | 기존 54 + 6 | **60 passed**, 9.52초 |
| 전체 비외부 로컬 | 기존 1518 + 37 | **1555 passed, 10 deselected**, 14.62초 |

Phase 4 합계 841은 unit/local 두 별도 실행의 합이다. Authlib의 기존 httpx 경고 1개가 있다.
AST 55 Python 파일·신규 Python 79열·문서 로컬 링크 31개·shell syntax-only와
설치 명령의 embedded Python 구문·git diff --check를 통과했다. 기존 추적 184개 중
문서 append 2개 외 182개 hash가 동일하고 index는 비어 있다.
실제 Spark Query·checkpoint 동작·network partition·Registry/3 Sink E2E는 실행하지 않았다.

## 9. 확정 가능한 결정과 사용자 승인 경계

확정 가능: 기존 ID/정책·장비 지원을 보존하고 입력/순서/명령 완료/ACK 증빙을 분리한다.
실패한 admitted 명령을 자동 skip하지 않는다. 같은 시각 timer/관측 순서를 재작성하지 않는다.
운영 fencing은 DB 간 별개 local epoch로 대체할 수 없고 checkpoint 성공은 의도/ACK와 대사해야 한다.

사용자 승인 필요: Spark/DBR/JDK·package pins 설치, 운영 authority/storage/service,
입력 source/timeout·장비 범위·ordinal/partition 변경, 취소/격리/재시도 budget·보존,
state rebuild/cutover·새 checkpoint 경로, Registry schema/consumer 배포·3 Sink 권한/비용.
외부 transactional authority는 권장안이고 아직 채택되지 않았다.

## 10. 실제 Spark Job 4 진입 조건

1. portable JDK/runtime·DBR target·client serialization 조합을 승인하고 별도 환경에서 검증.
2. state/command/intent/lineage authority·fencing·실패 명령 관리 정책 결정.
3. 승인된 runtime ingress와 cross-batch 순서/고정 평가 시각 저장·timer 무입력/restart 실측.
4. checkpoint/권위 상태 불일치 행렬·canonical wire pinning·각 Sink ACK/중복/최신성 검증.
5. resource별 IAM/UC/Registry·network·실행 시간/중단·비용 상한·새 checkpoint 승인.

기존 checkpoint/DB 변경·삭제·migration은 별도 승인이다. Phase 2/3 선행 병합과 전체
Phase 4 DoD 확인 전 main merge/유료 배포로 진행하지 않는다.

## 11. 이번 변경 파일

프로젝트 root는 `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state`다.

- [streaming/state/job4_contract.py](/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/streaming/state/job4_contract.py) — 순수 계약 검사
- [tests/unit/phase4/test_job4_contract.py](/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/unit/phase4/test_job4_contract.py) — 신규 unit
- [tests/integration/test_phase4_offline_execution_contract.py](/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/tests/integration/test_phase4_offline_execution_contract.py) — 신규 Fake bridge
- [docs/reports/phase-4-runtime-authority-decisions.md](/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-runtime-authority-decisions.md) — 이 보고서
- [docs/streaming/state-job4-integration-plan.md](/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/streaming/state-job4-integration-plan.md) — 기존 본문 보존·append
- [docs/reports/phase-4-verification.md](/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state/docs/reports/phase-4-verification.md) — 기존 본문 보존·append

## 12. 정확한 staging/commit/push 계획 — 사용자 검토 후 실행

이전 G1~G6는 이미 commit/push되어 재stage하지 않는다. 이번 6경로만 다음 두 그룹이다.
Codex는 아래 명령을 실행하지 않았다.

```bash
cd /Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-4-twin-state
git status --short
git add -- streaming/state/job4_contract.py \
  tests/unit/phase4/test_job4_contract.py \
  tests/integration/test_phase4_offline_execution_contract.py
git diff --cached --check
git diff --cached
git commit -m "feat(phase4): define offline Job 4 execution evidence contract"
git add -- docs/reports/phase-4-runtime-authority-decisions.md \
  docs/streaming/state-job4-integration-plan.md \
  docs/reports/phase-4-verification.md
git diff --cached --check
git diff --cached
git commit -m "docs(phase4): compare runtime authority and manual recovery options"
git status --short
git push origin feat/phase-4-twin-state
```

## 13. 포트폴리오 증빙 명령

현재 .venv의 pure/Fake 테스트이며 승인 전 제안 runtime 환경은 사용하지 않는다.
전용 basetemp는 테스트가 초기화하므로 사용자 데이터/checkpoint 경로를 지정하지 않는다.

```bash
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-runtime-authority-new \
  tests/unit/phase4/test_job4_contract.py \
  tests/integration/test_phase4_offline_execution_contract.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-runtime-authority-unit tests/unit/phase4
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-runtime-authority-local \
  tests/integration/test_phase4_local_recovery.py \
  tests/integration/test_phase4_local_application.py \
  tests/integration/test_phase4_concurrency_recovery.py \
  tests/integration/test_phase4_fencing_recovery.py \
  tests/integration/test_phase4_offline_execution_contract.py
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest -q -p no:cacheprovider \
  --basetemp /private/tmp/ktd-runtime-authority-all -m 'not integration'
git diff --check
```

37개 신규 테스트와 최종 회귀 화면은 오프라인 계약+로컬 Fake 복구 증거다. Spark/외부 E2E
또는 authoritative checkpoint exactly-once로 표시하지 않는다.

## 14. Phase 4 남은 DoD와 다음 범위

로컬 Fryer reference/Hood qualifier/Health·Codec·Timer·Registry·ledger·publication/fencing은
검증했다. Refrigerator/Freezer/Dishwasher Operational Handler와 Health Registry는 여전히
미지원이고 FAULT 발생은 명시 신호/규칙 없이 구현하지 않는다. 장비 추가/수용 범위는 결정 필요다.

실제 Job 4/state authority/timer scheduler/lineage/Checkpoint protocol·State Rebuild 실행기·
운영 Avro/Registry·History/Kafka/DynamoDB E2E와 부분 실패 수렴은 잔여다. 비용·권한·운영
정책 승인 없이 후속 배치를 실행하지 않는다. Phase 5 Alert Lifecycle은 이번 범위 밖이다.
