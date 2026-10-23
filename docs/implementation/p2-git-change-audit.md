# Phase 2 누적 Git 변경 감사 / 사용자 commit 실행안

감사 기준: 2026-10-23 KST, 저장소
`/Users/uchan/Downloads/DE/02_project/kitchen_to_digital`.
브랜치 `feat/phase2-validation-silver`, HEAD
`42dd5c9edf92402f10de37540e3dbdf1e0b910fa`.
이 문서는 현재 파일·diff·Git 이력을 직접 읽은 결과다. Git staging/commit/push/merge,
reset/clean/stash, Cloud 조회·변경·발행은 이번 감사에서 실행하지 않았다.

## 1. 현재 상태와 감사 범위

요청한 status/branch/log20/diffstat/diffcheck를 먼저 실행했다. 시작 Modified10,
Untracked75개 **실제 파일** 총85개를 모두 읽고 JSON/JSONL·schema·Python source 및
문서/로그를 검사했다. git status의 디렉터리 축약 표기와 파일 개수는 다르다.
추가로 .gitignore3줄을 수정하고 본 문서만 생성했다. 제품 코드·기존 증빙 수정 없음.

시작85개 SHA256과 종료 시 바이트를 비교한다. 기존 ignored local runtime을 포함해
85개 모두 그대로 보존해야 한다. 신규 .gitignore 변경 때문에 runtime은 이후
status에서 사라지지만 삭제되지 않았다. 시작 index도 비어 있었고 끝까지 비어 있다.

## 2. 이미 commit된 P2 작업 / 미커밋 부분

| 실제 commit | 확인한 내용 | 아직 남은 부분 |
|---|---|---|
| df505e4~fade5d4 | decode/domain/Quarantine/dedup/event-time/Silver/publisher/Job2 통합 | 현재 해당 핵심 생산 코드 수정 없음 |
| dc99afa / 69caada | Managed Delta Silver 전환 및 runtime config | 현재 example credential 이름 정합성과 별도 오래된 stamp 변경 |
| a5b8f56 | P2-8a schema ID inventory/기본 보고 | Registry Migration 도구·bundle/후속Cloud·Delta 증빙15개 및 초기 fixture 지원 미커밋 |
| 72e3687~b8c6ee2 | Job2 launcher·runtime 진단·metadata/produce probe | 테스트의 config 기반 credential routing 정합성 미커밋 |
| 8392b77 | Quarantine bool/잘못된 ACK offset 거부 | 이 ACK 수정 자체를 다시 commit할 필요 없음 |
| f2fa1f2 / 888a59c | P2-8d~f provisioning 설계·preflight·실행 경로 문서 | P2-8g 실제 실행 §13과22개 증빙 미커밋 |
| e266a54 | **P2-8h** Registry 장애 전파 수정·로컬 fresh-process Delta 복구·테스트·4개 증빙·보고 | P2-8h 신규 commit 대상 없음 |
| 42dd5c9 | **P2-8i+P2-8j** cloud_e2e.py·60개 tool test·Runbook·누적 보고서 | i/j 증빙16개만 미추적. 현재 code diff는 P2-8k |
| 없음(working tree) | **P2-8k** offset 검사·추가12검사·Runbook/보고서·9개 증빙 | C6로 한 번만 commit |
| 없음(working tree) | **P2-8b** 단일 raw fixture/ACK·과거 보조 script | 증빙4개 보존, README 개인정보·보조 script3개는 보류 |

42dd5c9의 보고서는 a~j 이력을 이미 포함한다. 현재 보고서 diff는 최신 포인터와
P2-8k 절뿐이다. 파일 전체를 옛 작업별로 다시 나눌 필요가 없다. 이미 commit된
h/i/j 코드를 중복 commit하거나 기존 commit을 재작성하지 않는다.

## 3. 민감정보 및 ignore 결과

85개 원문 전체, JSON 중첩 필드/SSM script·stdout·stderr, 코드의 credential 취급,
Base64 후보181개를 검사했다. AWS access/secret/session key, 실제 Databricks token,
Registry API key/secret, SASL password, private key, literal Basic credential 및
인증 환경변수의 실값은 확인되지 않았다. SecretReference와 credential **이름**은 값과
구별했다. runtime JSON에는 endpoint/리소스명/참조만 있고 현재 비밀값은 없다.

확인한 개인정보 파일은 `docs/evidence/phase-2/p2-8b/README.md`다. 개인 이메일을
포함한 Workspace 경로가 있어 staging 제외한다. 값은 이 문서에 옮기지 않았다.
원본도 수정·삭제하지 않았다. tests/unit/test_registry_migration.py의 검출 후보는
`.example` endpoint의 합성 URL userinfo 거부 테스트여서 개인정보/실제 자격증명이 아니다.
AWS ARN/account ID·cluster/instance/command ID·endpoint는 인프라 metadata로 구분하여
해당 실행 증빙에 유지한다. 이 판단이 실제 credential 파일을 공개해도 된다는 뜻은 아니다.

.gitignore에 정확히 다음3개를 추가했다.

```gitignore
/config/job2-runtime.json
/.aws/
/.databrickscfg
```

실제 실행 config는 향후 비밀값이 들어갈 수 있고 환경별 파일이므로 계속 로컬 보존한다.
example은 추적한다. 기존 .env/.env.*, *.log/logs, __pycache__/pyc, pytest/venv/tooling
cache, docs/plans 규칙은 유지했다. 실제 credential-like 추적 파일은 발견하지 못했다.
증빙 디렉터리 전체나 *.json/*.txt를 ignore하지 않았다. test-results.txt는 과거 red/green과
실행 한계를 보여주는 의도된 증빙이며 임시 *.log와 구분한다. P2-8i 원본 log의
줄 끝 공백7개는 C5의 파일 한정 검사에서 명시적으로 다룬다. 기존 ignore를 강제 우회하지 않는다.

## 4. 테스트와 증거의 구분

아래 T1~T4/O1/S1은 **이번 감사에서 새로 실행한 결과**다.

| ID | 실제 명령/검사 | 결과 |
|---|---|---|
| T1 | `.venv/bin/python -m pytest -q tests/unit/test_registry_migration.py tests/unit/test_phase1_export_avro_fixture.py tests/unit/test_avro_producer.py --basetemp=/tmp/ktd-audit-registry` |46 passed/1 warning |
| T2 | `.venv/bin/python -m pytest -q tests/unit/test_job2_databricks_entrypoint.py --basetemp=/tmp/ktd-audit-routing` |29 passed/1 warning |
| T3 | `.venv/bin/python -m pytest -q tests/integration/test_job2_cloud_e2e_tools.py --basetemp=/tmp/ktd-audit-e2e-tools` |72 passed/1 warning |
| T4 | `.venv/bin/python -m pytest -q tests/unit tests/integration --basetemp=/tmp/ktd-git-audit-pytest` |823 passed/11 skipped/1 warning (3.38s) |
| O1 | `.venv/bin/python scripts/registry_migration.py offline` | bundle/hash 검사, 네트워크 없음 |
| S1 |85개 보안/구문·JSON 파싱,181 Base64 후보,증빙 내용·SHA/정리 결과 대조,git diff --check | 로컬 정적 검사 |

warning은 기존 Authlib deprecation이다. T1/T2/T3는 T4의 부분집합이며 합산하지 않는다.
이는 현재 working tree 통합 검증이다. 제안하는 각 중간 commit 상태를 생성해서
테스트한 것은 아니다. 사용자는 각 commit 직전 해당 T 검사를 재실행할 수 있다.

H-a/b/g/i/j/k는 **기존 증빙을 읽은 역사 결과**이며 이번에 재실행한 Cloud 테스트가 아니다.
H-a: NO_ACTIVE_SESSION 실패→Delta storage PASS, Registry40301 중단→ALL_VERIFIED.
H-b: raw1건 ACK, Silver/validated E2E 결과 없음.
H-g: output2개 partitions3/retention/RF·ISR 및 EC2 stopped/임시 정책 회수.
H-i: full787/skip11, local Delta1 passed45.28s. H-j: full811/skip11, local Delta1 passed48.32s.
H-k: red7→green7/full823/skip11, local Delta1 passed45.07s.
P2-8h의 local Delta 증빙은 이미 e266a54에 있다.
S1에서 P2-8g 양쪽 topic의partition3/RF3/ISR3·retention과cleanup오류 없음/권한회수를
확인했고, P2-8b fixture/summary/ACK identity 일치 및 p1/offset3을 대조했다.
세 i/j/k preview는 모두 격리 목적지를 지정했지만 source hash/실행 gate가 당시 값이라
현재 실행본으로 재사용하지 않는다. 이번 감사에서는 실제 Spark/Delta·
MSK·Registry·AWS·Databricks를 실행하거나 재조회하지 않았다. skip11개는 통과 증거가 아니다.

## 5. 모든 시작 파일의 분류

M=Modified, U=Untracked(감사 시작 기준). test 열은 위 ID의 실제 검증 범위를 따른다.
각 JSON은 key/status뿐 아니라 중첩 원문/정책/SSM 명령·출력/fixture와 함께 확인했다.
문서의 날짜별 준비/실패/성공을 섞지 않고 기록했다.

| 상대 경로 | 상태 | 목적 | 실제 변경/내용 | 관련 작업 | 테스트 | commit 포함 | 보류 이유/한계 |
|---|---|---|---|---|---|---|---|
| `config/job2-runtime.example.json` | M | 예시 runtime credential 정합성 | producer credential 이름 변경 + 과거 git processing_version 교체의 2개 hunk | 이전 credential routing / 별도 version stamp | T2 | 첫 hunk만 C3 | 두 번째 processing_version hunk는 현재 배포 SHA 증거가 없어 보류 |
| `config/job2-runtime.json` | U → ignored | 실행 환경별 로컬 설정 | bootstrap/Registry endpoint, SecretReference, UC table/checkpoint, 과거 git stamp. secret literal 없음 | P2-8a~k runtime | 내용·참조 검사 | 제외; 현재 ignore | 실제 실행용 설정. example과 분리; 파일 보존 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/README.md` | U | topic provisioning 증거 | 실행 승인 범위·topic 결과·정리·증빙 설명 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/after.json` | U | topic provisioning 증거 | EC2 stopped/temp policy 부재·원래 role/job2 정책 보존 체크 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/before.json` | U | topic provisioning 증거 | 관리 principal/cluster/role·원래 policies·EC2·의존성 snapshot | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/cleanup.json` | U | topic provisioning 증거 | stop·임시 policy 제거 시각/최종 상태/cleanup_errors | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/cli-results.txt` | U | topic provisioning 증거 | 실제 topic describe/config stdout와 Kafka 경고·정리 요약 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/client-inventory-request.json` | U | topic provisioning 증거 | SSM inventory 제출 command ID와 실행 script; 토큰 없음 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/client-inventory-result.json` | U | topic provisioning 증거 | inventory SSM 상태/exitcode/stdout·stderr | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/client-inventory.json` | U | topic provisioning 증거 | CLI/JAR/client.properties 경로·SASL 방식·credential 존재 bool; 파일 비밀 본문 없음 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/operations.jsonl` | U | topic provisioning 증거 | 정책 적용/시작/검증/중지/권한회수 타임라인8개 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/output-topic-provisioning-request.json` | U | topic provisioning 증거 | 두 topic 존재 재확인·미존재 생성·describe/config SSM script와 command ID | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/output-topic-provisioning-result.json` | U | topic provisioning 증거 | 생성/describe/config SSM 응답·stdout·exitcode | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/ready.json` | U | topic provisioning 증거 | EC2 상태와 SSM Online 기록; E2E readiness 양식과 별개 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/run-summary.json` | U | topic provisioning 증거 | topic 설정/cleanup/시간/추정비용·최종 감사 요약 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/temporary-policy-applied.json` | U | topic provisioning 증거 | 정확한 cluster/topic admin 임시 IAM policy 내용; credential 아님 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-commands.json` | U | topic provisioning 증거 | topic별 before/create/after/config 명령·exit·stdout 배열9개 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-list-readonly-request.json` | U | topic provisioning 증거 | read-only topic list SSM 명령/ID | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-list-readonly-result.json` | U | topic provisioning 증거 | list SSM 성공 응답 및 세 topic 목록 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-list-readonly.json` | U | topic provisioning 증거 | raw+validated+quarantine 실제 visible 목록 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-preflight-request.json` | U | topic provisioning 증거 | 생성 전 describe/config/client version 확인 SSM script | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-preflight-result.json` | U | topic provisioning 증거 | 존재 확인 SSM 응답; 실패와 미존재 응답을 구별하는 원문 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-preflight.json` | U | topic provisioning 증거 | 생성 전 topic describe/config/code 및 client/package 존재 관측 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topics-final.json` | U | topic provisioning 증거 | 두 topic partitions3·retention·RF/ISR·leader 실측 | P2-8g | S1 / H-g | C4 | IAM/ARN/command ID는 인증 비밀값 아님; mutation script는 역사 자료 |
| `docs/evidence/phase-2/p2-8a/delta-storage-smoke-07132640d2094e75819f06a5878c25d9.json` | U | 초기 계약·검증 이력 | table_preflight FAIL/NO_ACTIVE_SESSION; 실패 이력 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/delta-storage-smoke-5fc182dce0b84c3188750c98f711a51f.json` | U | 초기 계약·검증 이력 | Managed Delta schema·첫 insert·duplicate/conflict·canonical 보존 PASS | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/kafka-read-preflight.json` | U | 초기 계약·검증 이력 | topics별 Kafka 읽기 옵션/오류와 Silver 접근 관측 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/read-only-discovery-2026-10-09.json` | U | 초기 계약·검증 이력 | management/topic/기존 compute batch read 진단·baseline hashes·한계 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-cloud-migration-20261010T073037107700Z.json` | U | 초기 계약·검증 이력 | HTTP403/Registry40301 중단 이력; 무조건 재실행 금지 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-cloud-migration-20261010T080132485713Z.json` | U | 초기 계약·검증 이력 | raw/final ID-version·config 관측 ALL_VERIFIED; 후속 실제 이관 증거 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/manifest.json` | U | 초기 계약·검증 이력 | default context·raw ID1/2/version1/2·schema/request hash·PREPARED_NOT_EXECUTED | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/raw-v1.avsc` | U | 초기 계약·검증 이력 | 원문 canonical10필드 Avro v1 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/raw-v1.import.json` | U | 초기 계약·검증 이력 | 명시 ID1/version1/schemaType/references/schema IMPORT 본문 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/raw-v2.avsc` | U | 초기 계약·검증 이력 | 원문10필드+nullable firmware_version/default null 호환 실험 v2 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/raw-v2.import.json` | U | 초기 계약·검증 이력 | 명시 ID2/version2/schemaType/references/schema IMPORT 본문 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/registry-runtime.example.json` | U | 초기 계약·검증 이력 | URL 빈 값/provider·basic auth·SecretReference 예시; 비밀값 아님 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/source-export.json` | U | 초기 계약·검증 이력 | local Registry GET responses/ID·subject/version·schema 원문 export | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/validated-v1.avsc` | U | 초기 계약·검증 이력 | raw v1과 같은 canonical10필드 schema | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8a/registry-migration/validated-v1.register.json` | U | 초기 계약·검증 이력 | ID/version 지정 없는 canonical v1 등록 본문 | P2-8a | S1; schema bundle O1/T1, Cloud 결과는 H-a | C2 | 실패/준비/성공 시점을 함께 보존 |
| `docs/evidence/phase-2/p2-8b/README.md` | U | 과거 사용자 실행 안내 | 단일 raw ACK와 원격 실행/결과 export 안내; 아직 E2E 미검증 | P2-8b | 내용/개인정보 검사 | 보류 | 개인 이메일이 포함된 Workspace 경로(32/48행). 값 비공개, 원본 수정하지 않음 |
| `docs/evidence/phase-2/p2-8b/happy-path-fixture.json` | U | 과거 단일 raw fixture/ACK | 단일 합성 event의 Confluent bytes/key Base64. 재발행 금지 이력 | P2-8b | S1 / H-b | C2 | 합성 telemetry/식별자만 포함; 실제 E2E 재검증 아님 |
| `docs/evidence/phase-2/p2-8b/happy-path-summary.json` | U | 과거 단일 raw fixture/ACK | decode/domain/ON_TIME·schema ID1·canonical payload 준비 결과 | P2-8b | S1 / H-b | C2 | 합성 telemetry/식별자만 포함; 실제 E2E 재검증 아님 |
| `docs/evidence/phase-2/p2-8b/raw-publish-ack.json` | U | 과거 단일 raw fixture/ACK | raw p1/offset3 ACKED 이력. Silver/validated 성공 증거 아님 | P2-8b | S1 / H-b | C2 | 합성 telemetry/식별자만 포함; 실제 E2E 재검증 아님 |
| `docs/evidence/phase-2/p2-8b/raw-publish-request.json` | U | 과거 단일 raw fixture/ACK | SSM command/instance/event ID 기록. 재발행 대신 과거 시도 식별 | P2-8b | S1 / H-b | C2 | 합성 telemetry/식별자만 포함; 실제 E2E 재검증 아님 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/canonical-replay.json` | U | 도구·복구 준비 증거 | 실제 handler+SQLite/fake ACK first/restart canonical 대사 | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/cost-plan.json` | U | 도구·복구 준비 증거 | 비용/credit null·예산 확인 전 BLOCKED·시간 제한 양식 | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/databricks-request-preview.json` | U | 도구·복구 준비 증거 | 기존 cluster용 Jobs request JSON; 과거 미제출·구 readiness 없는 preview | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/local-delta-recovery.json` | U | 도구·복구 준비 증거 | OSS Delta/file checkpoint fresh-process 복구1 passed(45.28s) 이력 | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/local-summary.json` | U | 도구·복구 준비 증거 | P2-8i local tests787/skip11 및 Cloud 미실행·source hashes | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/manifest-preview.json` | U | 도구·복구 준비 증거 | 당시 UUID/fixture10/source hashes·명시적 격리 목적지. 현재 코드와 다른 offline 계획 | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/reader-policy-preview.json` | U | 도구·복구 준비 증거 | 당시 cluster/raw+output3topic/run group 읽기 정책 초안; 미적용 | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/test-results.txt` | U | 도구·복구 준비 증거 | 당시 unit751/full787/의도한 red 기록/local Delta1 로그 | P2-8i | S1 / H-i | C5 | 옛 manifest/request는 실행 재사용 금지 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/canonical-replay.json` | U | 안전성·관리 preflight 증거 | handler+SQLite/fake ACK first/restart fingerprints·lineage·reasons 대사 | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/local-delta-recovery.json` | U | 안전성·관리 preflight 증거 | OSS Delta/file checkpoint 복구1 passed(48.32s) 이력 | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/local-summary.json` | U | 안전성·관리 preflight 증거 | P2-8j full811/skip11·local gates·BLOCKED/source hashes | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/management-preflight.json` | U | 안전성·관리 preflight 증거 | 당시 관리조회25회(AWS24/DBR1)/role 정의·DBR 실패; trust Condition 값 미보존 | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/manifest-preview.json` | U | 안전성·관리 preflight 증거 | P2-8j 격리 table/checkpoint·fixture·당시 source hashes의 offline preview | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/output-reader-policy-proposal.json` | U | 안전성·관리 preflight 증거 | 당시 정확한 cluster/raw+outputs3topic/run group 정책 초안; 미적용 | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/readiness-blocked.json` | U | 안전성·관리 preflight 증거 | 미확인 principals·cost·writer·UC를 null/UNVERIFIED로 둔 BLOCKED | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8j-cloud-readiness/test-results.txt` | U | 안전성·관리 preflight 증거 | red4→green4/full811/local Delta1 및 safety audit 과거 로그 | P2-8j | S1 / H-j | C5 | 과거 source hash를 현재 hash로 덮어쓰지 않음 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/aws-reader-preflight.json` | U | offset·실행 준비 증거 | AWS 관리조회29회 및 실제 ARN14 evaluation; 미적용 delta (비용 inventory4회는 별도) | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/cost-resource-inventory.json` | U | offset·실행 준비 증거 | 별도 AWS read-only4회: EBS/EIP/NAT/MSK 현재 관측; billing 증거 아님 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/databricks-auth-diagnosis.json` | U | offset·실행 준비 증거 | invalid refresh token 원인·비민감 auth 출력·DNS/TLS; 토큰값 없음 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/local-verification.json` | U | offset·실행 준비 증거 | full823/skip11/tool72/local Delta1 및 hashes·남은 blockers | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/manifest-preview.json` | U | offset·실행 준비 증거 | P2-8k fixture/source/config hash·격리 run의 offline 계획; 현재 재실행 금지 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/offset-contract.json` | U | offset·실행 준비 증거 | 테스트 offset50/100/75와 snapshot→ACK→run 순서; 실제 Kafka값 아님 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/output-reader-policy-delta.json` | U | offset·실행 준비 증거 | 두 output DescribeTopic/ReadData만 추가하는 정확한 ARN 초안 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/readiness-blocked.json` | U | offset·실행 준비 증거 | 기존 role 재사용 후보/credential 이름을 채운 BLOCKED; grant 적용 증거 아님 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/evidence/phase-2/p2-8k-cloud-unblock/test-results.txt` | U | offset·실행 준비 증거 | baseline811/red7/green7/tools67→full823/local Delta1/최종 audit 로그 | P2-8k | S1 / H-k; 현재 코드 T3/T4 | C6 | preview는 commit 후 재생성할 실행본이 아님 |
| `docs/implementation/p2-8a-registry-migration.md` | U | 이관 준비 절차 보존 | bundle/REST/secret 참조/driver smoke/curl 절차. 준비 당시 미실행 문구와 후속 실측은 시점이 다름 | P2-8a Registry Migration | O1 + 증빙 대조 | C2 | 과거 절차를 현재 실행 승인으로 사용하지 않음 |
| `docs/implementation/p2-8d-msk-output-topic-provisioning.md` | M | 실제 provisioning 결과 | diff는 §13 P2-8g 결과146행만 추가: 두 topic 설정·정리·비용·미검증 | P2-8g (기존 d/e/f는 committed) | S1 / H-g | C4 | — |
| `docs/implementation/p2-8i-cloud-e2e-runbook.md` | M | 현재 실행 준비 계약 | P2-8k §11 및 CLI get-credential 정정·과거 커밋 상태 갱신; 정상1회 승인 패키지 | P2-8k; i/j 본문은 committed | T3 / 문서 대조 | C6 | 동일 작업의 정정 hunk라 분할 불필요 |
| `docs/reports/phase-2-verification.md` | M | P2-8k 검증 보고 | 최신 포인터1줄 + P2-8k 절 추가. 이전 a~j는 이미 HEAD에 포함 | P2-8k | T4 / 문서 대조 | C6 | 이전 보고서 섹션을 다른 commit에 중복 staging하지 않음 |
| `scripts/phase1_export_avro_fixture.py` | M | Cloud Registry 주입 지원 | export_fixture의 keyword-only registry_client를 serializer에 전달 | P2-8b 초기 fixture 지원 | T1 | C2 | — |
| `scripts/phase2/cloud_e2e.py` | M | 입력 범위 안전성 | snapshot topic/partition/low-high/READY와 ACK 하한 검사; publish·Spark IO 전 거부 | P2-8k Offset | T3 / T4 | C6 | i/j 구현 재커밋 아님 |
| `scripts/phase2_prepare_happy_path.py` | U | 과거 단일 fixture 준비 | 고정 runtime/증빙 경로, Databricks secret 조회 및 기존 exporter 주입; summary write_text | P2-8b 과거 보조 도구 | 구문만 확인; 직접 unit 없음 | 보류 | 직접 실행 시 네트워크/secret 접근. 새 E2E 도구와 중복, 현재 승인·보존 gate 없음 |
| `scripts/phase2_publish_happy_path.py` | U | 과거 SSM 단일 발행 | 고정 EC2/bootstrap으로 send-command; 시도 파일/원격 lock, ACK>=0만 검사 | P2-8b 과거 보조 도구 | 구문만 확인; 직접 unit 없음 | 보류 | 현재 readiness/역할/offset gate 없음; bool ACK 허용 경계도 남음. 감사 범위에서 수정하지 않음 |
| `scripts/phase2_verify_happy_path.py` | U | 과거 운영 Silver 대조 | runtime의 운영 Silver 조회, raw ACK1건·validated 전체 batch 읽기, 결과 write_text | P2-8b 과거 보조 도구 | 구문만 확인; 직접 unit 없음 | 보류 | 격리 run 목적지/범위 상한/현재 output 읽기 계약 부재; 직접 unit 없음 |
| `scripts/registry_migration.py` | U | ID/version 보존 이관 도구 | bundle hash/충돌 검사, IMPORT/READWRITE/BACKWARD, validated 등록, 실행 flag, 오류·redirect 노출 방지 | P2-8a Registry Migration | T1 + O1 | C2 | 실제 실행 명령은 이번에 실행하지 않음 |
| `scripts/registry_runtime_smoke.py` | U | driver Registry/decode smoke | ID1/2 정의·fixture hash·현재 raw 소량 batch read, framing 후보; executor 검증 아님 | P2-8a Registry Migration | T1 일부 framing 분기 | C2 | driver/Cloud smoke 재실행 안 함 |
| `simulator/producer.py` | M | 기존 Avro serializer 재사용 | 주입 client가 있으면 기본 Registry 생성 대신 사용; 전송 경계 유지 | P2-8b 초기 fixture 지원 | T1 | C2 | — |
| `tests/integration/test_job2_cloud_e2e_tools.py` | M | offset/SHA 회귀 | 세 partition mock·source options 검사, 잘못된 window7·publish3·SHA2 검사 추가 | P2-8k Offset | T3 / T4 | C6 | 기존 기대값 유지하며 실제 계약의 mock으로 보강 |
| `tests/unit/test_job2_databricks_entrypoint.py` | M | 자격증명 역할별 전달 회귀 | 설정에서 서로 다른 source/producer 이름을 읽어 각 API로 전달하는지 검사 | 이전 credential routing | T2 | C3 | — |
| `tests/unit/test_phase1_export_avro_fixture.py` | M | 비발행 직렬화 회귀 | 기본 Registry/Kafka transport 생성 금지와 주입 client schema lookup 검증 추가 | P2-8b 초기 fixture 지원 | T1 | C2 | — |
| `tests/unit/test_registry_migration.py` | U | 이관 경계 검증 | FakeRegistry 충돌·정확한404·부분 실패·tamper·redirect·비밀응답 숨김; .example URL은 합성 테스트 | P2-8a Registry Migration | T1 | C2 | 개인 이메일 검출 후보는 합성 URL userinfo여서 제외 사유 아님 |

이번 추가 파일: `.gitignore`(M,C1;로컬 runtime/인증 파일 제외)와
`docs/implementation/p2-git-change-audit.md`(U,C7;본 감사/명령)뿐이다.
본 문서 절대 경로:
`/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/docs/implementation/p2-git-change-audit.md`.

## 6. 권장 commit 순서와 정확한 명령

아래 명령은 **사용자가 검토 후 직접 실행**한다. 자동 script처럼 무조건 이어서
실행하지 말고 각 diff/검사에서 실패하거나 예상 밖 파일이 보이면 중단한다.
시작 시 branch가 위 branch인지, index가 비었는지 확인한다. 기존 다른 staged 변경이
있다면 이 계획과 섞어 commit하지 않는다. 전체 디렉터리/glob/git add .를 사용하지 않는다.

```sh
cd /Users/uchan/Downloads/DE/02_project/kitchen_to_digital
git branch --show-current
git status --short
git diff --cached --stat
```

### C1. 로컬 runtime/credential ignore (1개 경로)

검증 연결: git check-ignore -v config/job2-runtime.json .aws/credentials .databrickscfg.

```sh
git add -- \
  .gitignore
git diff --cached --stat
git diff --cached --check
git diff --cached
git commit -m "chore(git): ignore local Job 2 runtime and credentials"
```

### C2. Registry Migration / 초기 검증·fixture 지원 (26개 경로)

검증 연결: T1, O1; S1 및 과거 H-a/H-b (Cloud 재실행 아님).

```sh
git add -- \
  docs/evidence/phase-2/p2-8a/delta-storage-smoke-07132640d2094e75819f06a5878c25d9.json \
  docs/evidence/phase-2/p2-8a/delta-storage-smoke-5fc182dce0b84c3188750c98f711a51f.json \
  docs/evidence/phase-2/p2-8a/kafka-read-preflight.json \
  docs/evidence/phase-2/p2-8a/read-only-discovery-2026-10-09.json \
  docs/evidence/phase-2/p2-8a/registry-cloud-migration-20261010T073037107700Z.json \
  docs/evidence/phase-2/p2-8a/registry-cloud-migration-20261010T080132485713Z.json \
  docs/evidence/phase-2/p2-8a/registry-migration/manifest.json \
  docs/evidence/phase-2/p2-8a/registry-migration/raw-v1.avsc \
  docs/evidence/phase-2/p2-8a/registry-migration/raw-v1.import.json \
  docs/evidence/phase-2/p2-8a/registry-migration/raw-v2.avsc \
  docs/evidence/phase-2/p2-8a/registry-migration/raw-v2.import.json \
  docs/evidence/phase-2/p2-8a/registry-migration/registry-runtime.example.json \
  docs/evidence/phase-2/p2-8a/registry-migration/source-export.json \
  docs/evidence/phase-2/p2-8a/registry-migration/validated-v1.avsc \
  docs/evidence/phase-2/p2-8a/registry-migration/validated-v1.register.json \
  docs/evidence/phase-2/p2-8b/happy-path-fixture.json \
  docs/evidence/phase-2/p2-8b/happy-path-summary.json \
  docs/evidence/phase-2/p2-8b/raw-publish-ack.json \
  docs/evidence/phase-2/p2-8b/raw-publish-request.json \
  docs/implementation/p2-8a-registry-migration.md \
  scripts/phase1_export_avro_fixture.py \
  scripts/registry_migration.py \
  scripts/registry_runtime_smoke.py \
  simulator/producer.py \
  tests/unit/test_phase1_export_avro_fixture.py \
  tests/unit/test_registry_migration.py
git diff --cached --stat
git diff --cached --check
git diff --cached
git commit -m "feat(phase2): preserve Registry migration and initial validation artifacts"
```

### C3. runtime credential routing 정합성 (2개 경로)

검증 연결: T2; processing_version hunk는 보류.

이 파일만 partial staging이 필요하다. `git diff -- config/job2-runtime.example.json`의
첫 hunk(대략5~8행)는 producer_service_credential 이름 변경이므로 **y**.
두 번째 hunk(대략23~26행)는 processing_version의 과거 SHA 교체이므로 **n**.
합쳐 보이면 **s**로 나눈 후 선택한다. 나머지는 선택하지 않는다.
예상 staged JSON은 producer 이름만 새 값이고 processing_version은 HEAD 값을 유지한다.
두 번째 hunk는 working tree에 그대로 남는다. index를 잘못 골랐다면 commit하지 말고
중단해 검토한다. 이 계획은 reset/restore/stash로 고치는 명령을 포함하지 않는다.

```sh
git diff -- config/job2-runtime.example.json
git add -p -- config/job2-runtime.example.json
git add -- tests/unit/test_job2_databricks_entrypoint.py
git diff --cached -- config/job2-runtime.example.json tests/unit/test_job2_databricks_entrypoint.py
git diff --cached --stat
git diff --cached --check
git diff --cached
git commit -m "fix(phase2): align producer credential reference and routing test"
```

### C4. 실제 MSK output provisioning 결과 (23개 경로)

검증 연결: S1 + 과거 H-g; 제품 변경이 없어 cloud 재실행 불필요.

```sh
git add -- \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/README.md \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/after.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/before.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/cleanup.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/cli-results.txt \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/client-inventory-request.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/client-inventory-result.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/client-inventory.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/operations.jsonl \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/output-topic-provisioning-request.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/output-topic-provisioning-result.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/ready.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/run-summary.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/temporary-policy-applied.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-commands.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-list-readonly-request.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-list-readonly-result.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-list-readonly.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-preflight-request.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-preflight-result.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topic-preflight.json \
  docs/evidence/phase-2/msk-output-topics/20261022T051154Z/topics-final.json \
  docs/implementation/p2-8d-msk-output-topic-provisioning.md
git diff --cached --stat
git diff --cached --check
git diff --cached
git commit -m "docs(phase2): record output topic provisioning and cleanup evidence"
```

### C5. 이미 commit된 E2E Tooling/Safety의 누락 증빙 (16개 경로)

검증 연결: S1 + 과거 H-i/H-j; 현재 tool은 T3.

P2-8i 원본 test-results.txt의38/40/51/74/75/78/98행에는 pytest 출력의 줄 끝
공백7개가 있다. 기본 staged diff --check는 이7개를 경고한다. 원본 증빙을 임의
정규화하지 않고 보존한다. 아래에서 그 파일만 명령 단위 core.whitespace 설정으로
줄 끝 공백을 허용하고, 나머지 staged 파일은 기본 검사한다. 저장소/global Git 설정은
변경하지 않으며 소스 코드 검사를 완화하지 않는다.

```sh
git add -- \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/canonical-replay.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/cost-plan.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/databricks-request-preview.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/local-delta-recovery.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/local-summary.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/manifest-preview.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/reader-policy-preview.json \
  docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/test-results.txt \
  docs/evidence/phase-2/p2-8j-cloud-readiness/canonical-replay.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/local-delta-recovery.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/local-summary.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/management-preflight.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/manifest-preview.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/output-reader-policy-proposal.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/readiness-blocked.json \
  docs/evidence/phase-2/p2-8j-cloud-readiness/test-results.txt
git diff --cached --stat
git diff --cached --check -- . ':(exclude)docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/test-results.txt'
git -c core.whitespace=-blank-at-eol diff --cached --check -- docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/test-results.txt
git diff --cached
git commit -m "docs(phase2): preserve P2-8i and P2-8j local verification evidence"
```

### C6. P2-8k offset validation / Cloud readiness (13개 경로)

검증 연결: T3/T4 + 과거 H-k; 실제 E2E는 BLOCKED.

```sh
git add -- \
  docs/evidence/phase-2/p2-8k-cloud-unblock/aws-reader-preflight.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/cost-resource-inventory.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/databricks-auth-diagnosis.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/local-verification.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/manifest-preview.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/offset-contract.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/output-reader-policy-delta.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/readiness-blocked.json \
  docs/evidence/phase-2/p2-8k-cloud-unblock/test-results.txt \
  docs/implementation/p2-8i-cloud-e2e-runbook.md \
  docs/reports/phase-2-verification.md \
  scripts/phase2/cloud_e2e.py \
  tests/integration/test_job2_cloud_e2e_tools.py
git diff --cached --stat
git diff --cached --check
git diff --cached
git commit -m "fix(phase2): validate E2E input windows and record readiness blockers"
```

### C7. 누적 변경 감사 기록 (1개 경로)

검증 연결: 기존85개 보존/hash·표/명령 경로 일치·diff --check.

```sh
git add -- \
  docs/implementation/p2-git-change-audit.md
git diff --cached --stat
git diff --cached --check
git diff --cached
git commit -m "docs(phase2): document cumulative Git audit and commit boundaries"
```

C6의 Runbook 여러 hunk는 모두 P2-8k의 최신 포인터/CLI 정정/이력 정정/새 절이므로
함께 포함한다. 보고서도 P2-8k 두 hunk 전부다. provisioning 문서 diff는 P2-8g §13만,
exporter/producer/test 변경은 동일 Registry 주입 단위이므로 add -p가 필요하지 않다.
C5는 이미 commit된 코드에 증빙만 보충한다. 이미 들어간 h/i/j 코드를 다른 이름으로
재커밋하거나 report의 오래된 섹션을 잘라낼 필요가 없다.

## 7. 보류/제외 및 예상 종료 상태

| 항목 | 결정 / 다음 검토 |
|---|---|
| config/job2-runtime.json | ignored local 파일로 보존. 비밀값 미검출이어도 실행 config이므로 staging 제외 |
| docs/evidence/phase-2/p2-8b/README.md | 개인 이메일 포함. 보안상 staging 제외; 사용자가 공개용 정리를 승인하기 전 원본 유지 |
| scripts/phase2_prepare_happy_path.py | 현재 도구와 중복·직접 cloud secret 접근·증빙 덮어쓰기 및 직접 unit 부재. 별도 안전성 작업 전 보류 |
| scripts/phase2_publish_happy_path.py | readiness/role/범위 gate·엄격 ACK 부족, 직접 unit 부재. 이번에는 수정하지 않음 |
| scripts/phase2_verify_happy_path.py | 운영 Silver/전체 validated 읽기·범위 제한/직접 unit 부재. 현재 isolated 도구로 대체됐지만 삭제하지 않음 |
| config/job2-runtime.example.json의 processing_version hunk | 과거 git SHA 간 교체는 현재 배포 증거가 아님. credential commit과 분리하여 미커밋 상태 유지 |

제안82개 경로(.gitignore/본 문서 포함,example은 부분)의 commit을 마치면, 예상 status는
example의 보류1 hunk(M)와 legacy scripts3개+개인정보 README1개(U)만 남는다.
local runtime은 ignored 상태다. 의도적인 보류 때문에 clean이 아니어도 잘못된 상태가 아니다.
완전 clean을 만들려고 reset/clean/stash/delete/force-add를 실행하지 않는다.

## 8. Push 시점 / main merge 차단

현재 로컬 origin URL은 `https://github.com/yccs-26/kitchen_to_digital.git`.
로컬에 저장된 origin/feat/phase2-validation-silver ref도42dd5c9다.
이번에는 fetch/ls-remote를 하지 않아 서버 최신 HEAD까지 확인한 것은 아니다.
사용자가 위 각 commit 내용·민감정보·테스트를 검토한 뒤 feature branch에만 push한다.

```sh
git branch --show-current
git status --short
git log --oneline origin/feat/phase2-validation-silver..HEAD
git diff --check
git push origin feat/phase2-validation-silver
```

로컬 ref 기준 뒤처짐/분기가 있거나 원격 push가 거부되면 force-push하지 말고 중단한다.
현재 지침상 Codex는 push를 실행하지 않는다. 이 감사가 Cloud 실행 승인은 아니다.

main merge는 Phase2 실제 MSK/Registry/UC E2E, input/output ACK·canonical/lineage 대사,
Kafka checkpoint 재시작·부분 실패·retention/lag, 실제 정리·권한 회수·비용 증빙,
Databricks 인증/UC/출력 reader 권한·원격 SHA/Writer 독점·예산 조건을 충족한 뒤
사용자가 PR 검토로 결정한다.823 local tests와 topic provisioning만으로 통과시키지 않는다.
보류한 legacy 도구를 현재 실행 경로로 사용하지 않고, 포함 artifact가 historical preview임을
명확히 유지하는 것도 필요하다. main merge/reset/rebase는 이번 실행안에 포함하지 않는다.

## 9. 감사 종료 검증

- 처음 요청한 Git명령5개 실행, 초기85개 실제 파일의 diff/전체 내용 검사.
- 신규 제품 코드 변경 없음. 기존85개 SHA256 동일, runtime 파일 존재 유지 확인.
- .gitignore3줄과 본 문서만 이번 수정/생성. table85행과85경로,82후보+5보류 검산.
- JSON/JSONL·Python 구문,정책/fixture/기존 결과·민감정보·개인정보 검사.
- 새 실행 T1=46/T2=29/T3=72/T4=823/skip11. O1 offline bundle PASS.
- git diff --check 및 index 비어 있음 확인. 새 파일로 들어갈 과거 log7개 공백은 별도 기록. commit/add/push/merge/Cloud 동작 없음.
