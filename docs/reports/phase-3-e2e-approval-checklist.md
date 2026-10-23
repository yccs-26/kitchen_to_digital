> 후속 구현: Phase2 로컬 tip이 `3d772bd`로 진행되어 현재 누락12개다.
> 범위 승인 launcher·raw publisher·callback probe를 후속 작업에서 구현했다.
> 아래 감사는 당시 기록이며 최신 판정은 readiness의 「실행 경로 통합」을 따른다.

# Phase 3 E2E 실행 승인 전 기술 점검 및 승인표

점검일: 2026-10-23. 현재 checkout과 로컬 Git ref를 확인했다.
**로컬 구현 DoD는 기존 실행 증거 기준 충족, 실제 E2E 실행은 BLOCKED다.**
승인만 받으면 곧바로 실행할 수 있다는 뜻이 아니다. launcher·publisher·인증 연결·DBR 장애
절차 준비와 Phase 2 기준 정렬이 선행되어야 한다. 이번에는 문서 감사와 기존 정적 테스트만
수행했고 Spark/Delta/클라우드/Kafka를 실행하지 않았다.

## 1. 현재 Git 사실과 작업 범위

| 항목 | 로컬 확인 결과 |
|---|---|
| Worktree | `/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows` |
| branch | `feat/phase-3-gold-windows` |
| 시작 HEAD | `ad0f9992193609a5889528b8e01be88822c78670` |
| upstream ref | `origin/feat/phase-3-gold-windows` → `12024e7` |
| ahead / behind | `2 / 0` — 원격 서버가 아닌 저장된 tracking ref 대비 |
| local main / origin/main | 둘 다 `7006ae6` (Phase 1) |
| main과 HEAD merge-base | `7006ae6`; main은 HEAD의 조상, main 대비 HEAD만 42 commits |
| local Phase 2 tip | `42dd5c9`, 해당 tracking ref도 같은 값 |
| Phase 2와 HEAD merge-base | `b8c6ee2`; HEAD만12 / Phase2만5 commits |
| Phase 2 tip → main/HEAD 포함 | 둘 다 아님; merge-base ancestor 검사 exit1 |
| 작업 시작 상태 | tracked 수정·staging 없음; `docs/evidence/phase-3/` 아래 미추적155개 |
| 기존 증빙 추적 상태 | `git ls-files docs/evidence/phase-3`는0개. ignore된 log는155개에 포함되지 않음 |
| 이번 변경 | readiness 갱신 + 이 승인표 신규. 생산 코드/기존 증빙/설계/Runbook은 보존 |

직전 구현·문서를 포함한 commit은 `0246de9`(구현·설정·테스트21파일),
`ad0f999`(설계·ADR·문서7파일)이다. 이전 보고의 '생산 코드 미커밋' 설명과 Git 제안은
과거 스냅샷이다. 이번 Codex는 commit하거나 원격을 갱신하지 않았다.

현재 HEAD는 로컬 main을 포함하지만, **Phase 2가 main에 병합된 뒤 그 main을 기준으로
정렬된 상태는 아니다.** 실제 원격의 최신 병합 여부는 UNKNOWN이다. fetch 없이 확정하지 않는다.

HEAD에 없는 로컬 Phase2 후속5개:

- `8392b77`: Quarantine ACK offset을 non-bool 정수 >=0으로 엄격히 확인.
- `f2fa1f2`, `888a59c`: MSK output provisioning 준비 문서.
- `e266a54`: Registry HTTP404 중 error_code40403만 unknown-schema로 분류하도록 보강,
  Job2 로컬 복구 증거 추가. 현재 Phase3 decoder는 HTTP404 전체를 unknown-schema로 분류한다.
- `42dd5c9`: Phase2 cloud E2E 격리·검토 도구와 runbook 추가.
  `scripts/phase2/cloud_e2e.py`는 현재 Phase3 tree에 없는 후속 도구다.

이 차이는 단위 테스트를 더 추가해서 해소되지 않는다. Phase2 병합·기준 정렬을 별도 검토하고
그 후 기존 Phase3 회귀와 입력 오류 분류/연결을 재확인한다. 이번에 merge/rebase/cherry-pick은 하지 않았다.

## 2. Gold 형식 선택 자료 — 미승인

| 선택 | 영향과 추가 작업 | 비용 판단 범위 |
|---|---|---|
| UC Managed Delta 권장 후보 | 기존 이름 기반 후보 Writer·12컬럼·불변 MERGE·로컬 복구 증거 재사용. UC DDL/권한/실제 conflict abort/DBR callback 수용 필요 | 추가 구현·검증 노력은 상대적으로 작음. 실제 DBU·storage·maintenance 비용이 더 낮다고 확정하지 않음 |
| UC Managed Iceberg 대안 | 기존 계획의 형식 일관성. Gold Writer와 대상 Runtime의 MERGE/제약/전체 abort·checkpoint 복구를 별도로 구현·검증해야 함 | 실제 계정 비용 UNKNOWN. serverless metadata 유지·연결 등 요구조건도 확인 필요 |

형식 선택은 Gold에만 적용한다. Bronze UC Managed Iceberg와 Silver Delta의 기존 데이터·
테이블 형식·checkpoint를 변환하거나 이동하지 않는다. ADR 상태는 **제안 / 승인 대기**다.
두 형식의 신규 cloud 리소스나 테이블은 생성하지 않았다.

공식 요구조건을 다시 확인했다:
[DBR17.3은 Spark4.0.0](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts),
[Managed Iceberg 요구사항](https://docs.databricks.com/aws/en/iceberg),
[foreachBatch의 at-least-once·전량 소비·직렬화](https://docs.databricks.com/aws/en/structured-streaming/foreach).
지원 문서를 실제 Workspace 수용이나 계정 비용 근거로 사용하지 않는다.

## 3. 통합 승인표 — 민감정보는 기입하지 않음

UNKNOWN은 실환경 값을 확인하지 않았다는 뜻이고 REQUIRES_APPROVAL은 선택·실행 승인이
없다는 뜻이다. 참조명/리소스 식별자만 비공개 승인 기록에 작성한다. 비밀번호·토큰·Secret 값·
계정번호는 수집하지 않는다. 아래는 기존 두 runbook의 모든 placeholder와 JSON 미입력 값을 합친 표다.

| ID | 승인 항목 / 설정 경로 | 현재 확인값·상태 | placeholder / 승인 시 기록할 내용 |
|---|---|---|---|
| G1 | Gold `gold.format` | Delta 권장 후보, REQUIRES_APPROVAL | Delta/Iceberg 선택과 ADR 결정 근거 |
| E1 | Workspace `workspace_host`, 인증 profile | UNKNOWN / REQUIRES_APPROVAL | `<approved-auth-profile>`, `<approved-profile>`; 승인 host와 CLI profile 참조명 |
| E2 | `runtime.dbr`, `runtime.access_mode` | 후보17.3 LTS / STANDARD, 실제 UNKNOWN | 실제 build·Spark·access mode. 현재 parser는 이 후보 외 값을 허용하지 않음 |
| E3 | Compute 유형·식별자 | UNKNOWN / REQUIRES_APPROVAL | `<approved-cluster-id>`; Jobs/기존 compute 등 유형, node/worker 수, 종료 방식. SQL Warehouse를 Job3 실행기라고 간주하지 않음 |
| E4 | Job3 배포·Job ID | UNKNOWN / REQUIRES_APPROVAL, launcher 미완성 | `<approved-phase3-job-id>`, `<approved-job-id>`; 검토한 task/entrypoint·라이브러리·코드 SHA |
| U1 | `gold.catalog/schema/table` | 모두 UNKNOWN / REQUIRES_APPROVAL | 정확한 3-part name. 설계의 `ktd.gold.fct_metric_window`는 후보이지 승인값이 아님 |
| U2 | UC DDL·권한·owner | REQUIRES_LIVE_CHECK / REQUIRES_APPROVAL | USE CATALOG/SCHEMA, SELECT/MODIFY, 별도 CREATE TABLE 필요 여부; 12 NOT NULL/타입/provider/MANAGED, 기존 NULL·중복키 없음 |
| K1 | Kafka bootstrap/topic | bootstrap UNKNOWN; topic 계약 `kitchen.sensor.validated` | `kafka.bootstrap_servers`; 실존/partition/retention/연결·권한은 REQUIRES_LIVE_CHECK |
| K2 | `kafka.starting_offsets` | UNKNOWN / REQUIRES_APPROVAL | earliest/latest, 승인된 시작 offset 범위·배경 트래픽. 재시작은 같은 checkpoint 우선 |
| K3 | Kafka 인증 참조 | UNKNOWN / REQUIRES_APPROVAL | `kafka.service_credential`; 실제 실행 주체와 broker 권한·네트워크 확인. 인증값 수집 금지 |
| R1 | Registry 접근·schema | UNKNOWN / REQUIRES_APPROVAL | `registry.url`, schema ID·subject/version·executor 연결. 10필드 Avro 계약은 유지 |
| R2 | Secret scope/참조 | UNKNOWN / REQUIRES_APPROVAL | `registry.credentials.scope/key` 참조명과 읽기 권한만. 실제 Secret 값 불필요 |
| R3 | executor Registry factory | 구현 연결 미완료 / 별도 준비 필요 | worker에서 지연 생성할 인증 경로·모듈 배포. driver dbutils/client 캡처 금지 |
| C1 | `checkpoint` 전체 경로·소유권 | UNKNOWN / REQUIRES_APPROVAL | `/Volumes/<catalog>/<schema>/<volume>/job3/<고정ID>`; `<approved-fixed-id>`는 같은 역할. Gold target namespace와 CP namespace는 독립 승인 |
| C2 | checkpoint volume/parent | UNKNOWN / REQUIRES_APPROVAL | `<approved-checkpoint-volume-parent>`; 존재·READ/WRITE VOLUME·다른 query와 격리·보존 확인 |
| W1 | writer identity/owner | UNKNOWN / REQUIRES_APPROVAL | `writer.owner`; 실제 run-as 주체와 UC/Kafka/Secret 권한, target 소유권 |
| W2 | 단일 writer 통제 | 선언 미입력 / REQUIRES_APPROVAL | `writer.exclusive_target`, max_concurrent_runs=1 + 외부 Job/notebook/backfill 배제·MODIFY 통제 증거. 선언은 lock이 아님 |
| P1 | Phase2 config/선행 수용 | UNKNOWN / 기준 정렬 필요 | `<approved-job2-config-path>`; canonical validated·topic/등록 장비 수용 및 Phase2 main 병합 |
| P2 | Phase3 config 경로 | UNKNOWN / REQUIRES_APPROVAL | `<approved-phase3-config-path>`, `<reviewed-local-config.json>`; 같은 검토 설정의 배포/로컬 경로 역할, hash·보관 위치 |
| D1 | fixture 시각·ID namespace | UNKNOWN / REQUIRES_APPROVAL | `<approved-UTC-minute>`, `<approved-unique-run-namespace>`; 현재 과거 fixture 그대로 발행 금지 |
| D2 | test 장비·입력 격리 | UNKNOWN / REQUIRES_APPROVAL | 등록 store/equipment/metric/unit 매핑, 배경 입력 배제, raw→Phase2 경로. 임의 장비 등록 금지 |
| D3 | 입력 publisher / batch 진행 | 미구현 / 별도 준비 필요 | 8레코드·고유7 ID·중복1을 여러 micro-batch에 전송하고 ACK/partition/offset 관측할 인증 도구 |
| D4 | expected 파일 | 로컬24행 증거 있음 / 실입력 결과 REQUIRES_APPROVAL | `<approved-expected-json-path>`; 시각·장비 이동을 반영한 1분6+5분18과 모든12컬럼 |
| D5 | 실제 W 진전·대기 계획 | UNKNOWN / REQUIRES_APPROVAL | 합법적인 후속 event-time·진행 장비·실행 시간. 가짜 미래 clock/idle로 대체 금지 |
| L1 | 최대 실행 시간 | UNKNOWN / REQUIRES_APPROVAL | `limits.max_runtime_minutes`, Job timeout·독립 종료 감시·실행 책임자 |
| L2 | 비용 상한·포함 항목 | UNKNOWN / REQUIRES_APPROVAL | `limits.max_cost_usd`; 실제 가격·크레딧은 UNKNOWN. DBU/instance/storage/유지·전송 등 포함 범위 |
| F1 | 장애·복구 수용 | DBR hook 미구현 / REQUIRES_APPROVAL | MERGE 전/후 callback 실패, 정상 재시작·동일CP replay, 오류 증빙·중단 기준 |
| X1 | 종료·보존·정리 | UNKNOWN / REQUIRES_APPROVAL | run 취소/compute 종료 확인, 증빙 보존기한, 정확한 테스트 table/CP별 삭제 별도 승인 |
| A1 | 실행 후 run ID | UNKNOWN / 실행 후 생성 | `<record-run-id-after-approved-submission>`, `<recorded-run-id>`; 사전 기본값을 만들지 않음 |

설정의 고정 계약은 UTC, W10분, Append, 1분 tumbling, 5분 길이/1분 slide다.
`gold_format_approved=false`는 현재 유지한다. true로 바꾸어도 배포 gate는 해제되지 않는다.
민감값을 제외한 검토 설정이라도 실제 host/리소스명은 공개 Git에 무분별하게 추가하지 않는다.

## 4. 명령·코드 경로 감사

| 실행 단계 | 현재 코드/명령 | 분류·성공/중단 기준 |
|---|---|---|
| 로컬 설정 검사 | `python -m streaming.tools.phase3_preflight --config ...` | 로컬 가능. 템플릿 exit2/BLOCKED. static PASS도 권한·승인 증거가 아님 |
| 로컬 후보 선택 | `prepare_metric_deployment(..., adapter='uc_managed_delta')` | 로컬 가능. 올바른 로컬 설정 필요. query·Secret·network 시작 안 함 |
| 로컬 회귀 | 기존 pytest 명령 | 로컬 가능. 이번에는 기존 Preflight30+선택5만 재실행. 새 테스트 작성 안 함 |
| Runtime/권한 확인 | `databricks clusters get`, `jobs get`, `jobs list-runs`, UC DESCRIBE/SHOW GRANTS | 승인 후 cloud read. 현재 실행 금지. 실제 주체/build/권한이 다르면 중단 |
| Phase2 config 확인 | `load_runtime_config(...); p2.job.validated_topic` | 로컬 파일 검사 가능. module·rules/config 필요. 이 값 확인만으로 실행 중인 Phase2 수용 증거가 되지 않음 |
| Kafka/Registry 연결 | `read_validated_kafka` + `registry_factory` | 입력 라이브러리 구현 있음. 실제 auth factory 배선·bounded 연결 수용 notebook/entrypoint 미완성. 자동 실행 명령 없음 |
| UC DDL 준비 | runbook의 `ddl = ... USING DELTA` | 문자열 준비는 가능. `spark.sql(ddl)`은 승인 후 cloud write이며 문서에서는 주석 상태. 자동 생성 안 함 |
| UC DDL 수용 | `DESCRIBE ... AS JSON`, schema·NULL·duplicate/merge 검사 | 승인 후 cloud. OSS Delta V2 JSON metadata는 미지원이었음. 실제 DBR에서 거부되면 중단·별도 지원 metadata 방식 검토 |
| Job3 configured entry | `start_configured_metric_aggregation` | **항상 차단**. `require_deployment_ready`가 무조건 예외. 승인 후에도 별도 launcher 검토·구현 필요 |
| Job3 low-level library | `start_metric_aggregation` / `start_metric_query` / `GoldUCDeltaSink.merge_rows` | 동작 라이브러리 있음. 자체 배포 승인 gate는 없음. 승인 없이 직접 호출해 configured gate를 우회하면 안 됨 |
| Job 제출 | `databricks jobs run-now` | CLI는 존재하지만 배포된 Job/task/entrypoint UNKNOWN. 명령 존재 ≠ 완성된 E2E 실행 경로 |
| 입력 준비 | runbook의 offline 시각/ID 변환 Python | 로컬 가능. 현재 expected.json은 미추적 증빙에 의존(이번 checkout엔1개 있음). fresh clone에서 파일 존재·hash 확인 전 중단 |
| 실제 입력 발행 | 결정적 bounded raw publisher | **미구현**. 임의 실행 명령 없음. 랜덤 simulator/validated 직접 발행 대체 금지 |
| W/집계 관측 | `query.recentProgress`, Gold `spark.table`, 정확24행 비교 | 승인 후. 실제 query handle/실행 중 observer 연결은 launcher에서 준비해야 함 |
| offset/checkpoint | progress start/endOffset + ACK + CP identity 대사 | 승인 후. read 권한·보존 확인. consumer group commit만으로 완료 선언 금지 |
| 재시작/장애 | 같은CP restart, MERGE 경계 실패 | 로컬 증거 있음. DBR fault hook 및 정상 stop/restart 관측 절차 미완성. killpg를 DBR에 적용하지 않음 |
| 중단/비용 | `get-run`, `cancel-run --timeout 5m` | 승인 후. API timeout은 Job/compute 종료 보장이 아님. 최종 종료 상태와 부대 리소스 확인 |
| 정리 | 주석 처리된 `DROP TABLE`, `dbutils.fs.rm` | 별도 삭제 승인 필요. wildcard·운영/공유 target/CP 삭제 금지 |
| Git proposal | add/commit/push 명령 예시 | 사용자 수동 검토용. 이번 Codex는 Git 쓰기 실행 안 함 |

runbook의 `settings.require_deployment_ready()`에서 현재 의도적으로 멈춘다. 따라서 여러 셀을
그대로 순서대로 실행하면 뒤의 cloud 셀까지 진행하는 runnable notebook이 아니다.
설정만 읽는 것과 실제 query 시작은 구분하며, 수용용 읽기 절차와 launcher 승인을 별도 작업으로
준비한다. CLI/profile/DDL 예제를 E2E 완성 도구라고 부르지 않는다.

설계의 과거 수치 tolerance 문장은 유한 Python/Spark oracle 비교 이력이다.
생산 Gold 동일 final 판정은 `gold_row_comparison.py`·공통 MERGE의 정확 비교이며
1 ULP 차이도 CONFLICT다. 대상 NOT NULL·NULL/중복키 검사는 기존 read schema 타입 검사만으로
충족되지 않는다. version 컬럼이나 자동 보정을 이번에 추가하지 않았다.

## 5. 실행 순서와 중단/비용 통제

1. Phase2 선행 병합/기준 정렬 및 회귀 계획을 닫는다. 이번에는 Git 변경하지 않는다.
2. G1 형식과 E1~E4 Workspace/DBR/compute/코드 ref, U1~U2 대상·권한·DDL을 승인한다.
3. C1~C2 격리된 고정 checkpoint, W1~W2 identity와 외부 writer 배제 증거를 확보한다.
4. launcher/auth factory·결정적 publisher·DBR 장애 hook 준비를 별도 검토한다.
   승인된 cloud read/쓰기 수용 범위를 나누고 low-level API로 차단을 우회하지 않는다.
5. 승인된 Runtime/권한 및 Kafka/Registry의 네트워크/인증/schema를 확인한다.
   topic·리소스·권한 미확인/불일치면 중단한다. 의도치 않은 전체 topic replay를 하지 않는다.
6. Gold DDL/NOT NULL/MANAGED/provider와 불변 MERGE를 격리 target에서 수용한다.
   다른 값이 UPDATE되거나 conflict가 삼켜지면 실행을 중단한다.
7. 승인된 단일 Job3를 실행하고 승인된 raw 입력을 여러 batch로 전송·ACK/offset을 기록한다.
8. 실제 W와 Append를 관측하고 expected24행, 자연키24개, 1분6/5분18과 모든 값·경계를 대사한다.
9. 같은 checkpoint로 정상/실패 복구 후 Gold 불변을 확인하고 run/offset/history 증빙을 수집한다.
10. 오류/시간·비용 중단점이면 run 취소→최종 run 상태→compute 종료→부대 비용 확인.
    증빙 보존 후 정확한 test table과 checkpoint 각각 삭제 승인을 받아 정리한다.

기준 fixture의 UTC base를 B라 하면 마지막 target window 종료는 B+10분이다.
W=max event-time−10분이므로 마지막 target 전체 확정은 실제 W가 B+10분을 지나고
출력 batch까지 처리되어야 한다. 이를 위해 B+20분을 지난 합법적인 event-time 진행이
필요하며 마지막 target 이벤트(B+5분)나 idle sleep만으로 닫히지 않는다.
실제 대기·compute 수명은 시작 시각/Phase2 late·future 규칙/traffic에 따라 달라진다.
진행 입력은 target 검증 장비와 분리하고 별도 결과·ACK도 설명해야 한다.

예산 승인에는 실행자·감시자, compute 유형/worker 수, 시작·중단 시각, Job timeout,
독립 수동 중단 수단과 storage/전송/maintenance의 포함 범위를 기록한다.
금액 설정은 자동 hard cap이 아니다. 가격·크레딧·청구는 UNKNOWN이며 숫자를 만들지 않았다.
마감까지 W가 부족하면 OPEN_PENDING_EVENT_TIME/미확정으로 보고하고 중단한다.
checkpoint 초기화나 미래 clock·watermark 변경으로 통과시키지 않는다.

## 6. 로컬 DoD와 이번 검증 범위

기존 실행 증거: Unit1019, Phase3 Unit305(전체에 포함), Streaming10, Gold 통합·복구15,
이름 기반 Delta9 passed. UC 후보 manifest26개 파일을 SHA-256으로 재대조해 불일치/누락0.
기존 두 runbook의 placeholder19종을 승인표에 모두 연결했고 누락0을 확인했다.
이전 검증의 Python source/test9개 hash도 현재 파일과 일치했다. Runbook과 기존 증빙은 보존했다.
이번에는 일반 Unit/Streaming/Gold를 재실행하지 않았다. 과거 결과를 이번 새 실행으로 표시하지 않는다.
현재 설치된 로컬 Python3.11.15 / Temurin21.0.13+9 / PySpark4.0.0 / Delta4.0.0을 조회했다.
실제 Workspace·DBR·Kafka·UC는 UNKNOWN이며 과거 타 Phase 보고서를 현재 환경값으로 채우지 않았다.

이번 실제 명령:

```sh
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m pytest tests/unit/phase3/test_phase3_preflight.py tests/unit/phase3/test_phase3_uc_selection.py -q -p no:cacheprovider
# 35 passed
PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m streaming.tools.phase3_preflight --config config/phase3_gold.example.json
# exit2, static BLOCKED, overall BLOCKED
```

생산 코드/테스트/fixture/의존성/기존 증빙은 수정하지 않았다. 새 범용 테스트도 만들지 않았다.
로컬 DoD 충족은 실제 UC/Kafka/DBR E2E·권한·동시 writer·checkpoint 복구의 증명이 아니다.
전체 DoD는 **미충족**이고 main merge는 권고하지 않는다.

## 7. 현재 상태에 맞춘 Git 제안 — 실행하지 않음

기존 구현21파일/문서7파일을 다시 staging할 필요가 없다. 이번 감사 문서만 별도 묶는다.

```sh
git add -- docs/reports/phase-3-e2e-readiness.md docs/reports/phase-3-e2e-approval-checklist.md
git diff --cached --check
git diff --cached --stat
git commit -m 'docs(phase3): audit E2E approval gates and branch prerequisites'
```

위 명령은 사용자 검토 후 수동 실행용이다. 기존 미추적155개 증빙은 별도 검토 후 이전 증빙
Git 제안의 파일별 목록으로 선택한다. ignore된 log는 force-add 하지 않고 민감정보·hash·
문서가 참조하는 expected/JUnit 가용성을 확인한다. `git add .`로 묶지 않는다.

검토한 문서·증빙 commit 뒤 사용자 push 대상은 `origin/feat/phase-3-gold-windows`다.

```sh
git push origin feat/phase-3-gold-windows
```

push는 main merge/E2E 승인이 아니다. 이번 실제 Git 쓰기는0이며 원격 조회/fetch도 하지 않았다.
main merge 전에는 최신 원격 Phase2 병합 확인, 기준 정렬·회귀, Gold 형식 승인,
실제 UC/Kafka/DBR E2E와 동일CP 복구·정확24행·종료/비용·공개 증빙을 모두 확인한다.
