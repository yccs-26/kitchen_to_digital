# Phase 3 Gold Streaming 운영 준비 Runbook

2026-10-23. **준비 문서이며 운영 실행 승인이 아니다.**
기존 `docs/runbook.md`는 Phase 1 절차이므로 변경하지 않고 Phase 3를 분리했다.
최신 판정/실행 명령은 `docs/reports/phase-3-verification.md`의 2026-10-23 부록을 따른다.

## 실행 전 게이트

1. `docs/adr/phase-3-gold-table-format.md`의 형식 결정을 승인한다.
2. 실제 UC catalog/schema/table과 provider/owner/schema/권한을 별도 승인된 조회로 확인한다.
3. 현재 로컬 GoldDeltaSink를 그대로 운영에 배포하지 않는다.
   forPath·로컬 `_delta_log` JSON 검사는 UC managed table adapter를 대체하지 않는다.
   `/Volumes/...`를 Gold 데이터 경로로 넣어 UC table처럼 쓰는 우회도 하지 않는다.
4. Kafka/Registry executor 접근과 DBR Standard serialization/import를 승인된 환경에서 검증한다.
5. 하나의 writer만 실행되도록 task/run 소유권과 UC MODIFY 권한을 제한한다.

## 환경별 확인 템플릿

아래는 값 입력 양식이며 존재하는 리소스 이름이나 실행 가능한 Job 배포 정의가 아니다.
비밀값 자체를 문서·로그·commit에 넣지 않는다.

```yaml
approval_status: PENDING
runtime: DBR_17_3_LTS_CANDIDATE
access_mode: STANDARD_TO_VERIFY
session_timezone: UTC
gold:
  format: USER_DECISION_REQUIRED
  table: <approved_catalog>.<approved_schema>.<approved_gold_table>
  provider_metadata: TO_VERIFY
checkpoint: /Volumes/<approved_catalog>/<approved_schema>/<checkpoint_volume>/job3/<fixed_query_identity>
kafka:
  topic: kitchen.sensor.validated
  bootstrap_servers: INJECT_AT_RUNTIME
  startingOffsets: EXPLICIT_FIRST_START_POLICY
  failOnDataLoss: true
  service_credential_reference: APPROVED_REFERENCE_ONLY
registry:
  url: INJECT_AT_RUNTIME
  authentication_reference: APPROVED_REFERENCE_ONLY
  executor_reachability: TO_VERIFY
job:
  max_concurrent_runs: 1
  other_jobs_and_manual_writers: MUST_BE_EXCLUDED
state:
  provider_and_shuffle_partitions: PIN_AFTER_ACCEPTANCE
  branch_order: [1m_tumbling, 5m_sliding]
  watermark: 10 minutes
  output_mode: append
```

[DBR 17.3 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)는 Spark 4.0.0을 포함한다.
실제 cluster의 build/access mode/API는 미조회·미실행이다. local OSS Delta 4.0.0과
DBR 내장 Delta의 버전·기능을 동일하다고 가정하지 않는다. 운영 DBR에 OSS Delta jar를
임의 설치해 내장 라이브러리를 덮어쓰지 않는다.

UC table 이름을 확인하고 USE CATALOG/USE SCHEMA, SELECT/MODIFY 및 신규 생성 시
CREATE TABLE 권한을 확인한다. checkpoint volume은 READ VOLUME/WRITE VOLUME 등의
실제 권한을 확인한다. 승인된 target에만 적용하며 blanket 권한을 부여하지 않는다.
12컬럼 Gold DDL은 논리상 TIMESTAMP_LTZ 2개, STRING 5개, BIGINT 1개, DOUBLE 4개,
전부 NOT NULL이다. 실제 형식별 지원을 검증하기 전 DDL을 실행하지 않는다.

## Kafka 입력과 실패 처리

- key는 equipment_id의 UTF-8 bytes, value는 기존 sensor_metric_event.avsc 10필드다.
- magic byte 0 + 4-byte big-endian schema ID + Avro body를 기존 Phase 2 decoder로 해석한다.
- registry_factory는 executor에서 생성 가능한 client를 반환해야 한다. driver SparkSession,
  dbutils, native client를 closure에 캡처하지 않는다. 인증값은 승인된 비밀 주입 경로를 쓴다.
- 실제 executor에 repository 모듈·fastavro·confluent-kafka와 Registry 네트워크가 필요하다.
  Python Kafka package 설치가 Spark Kafka connector 존재를 뜻하지 않는다.
- Spark reader는 options를 명시적으로 받는다. 현재 adapter는 failOnDataLoss/startingOffsets를
  강제하지 않으므로 배포 전 양식과 실제 options를 대조한다. 시작 정책을 조용히 기본값에 맡기지 않는다.
- Kafka key 불일치, framing/decode/Registry 조회 오류는 예외로 전파한다.
  consumer 오류나 retention 손실을 빈 DataFrame/성공으로 바꾸지 않는다.
- 시작 offset은 처음 query를 만들 때 적용된다. 재시작 offset은 같은 checkpoint가 결정한다.
  별도 consumer commit이나 batch ID 원장으로 덮어쓰지 않는다.
- 같은 ID의 불변 payload는 bounded dedup으로 처리한다. 변경 payload 탐지나 영구 dedup은 아니다.

## 배포와 single writer

Job의 동시 실행 수를 1로 설정하고, 같은 target을 쓰는 다른 Job/notebook/backfill도 차단한다.
Job 하나의 concurrency=1은 외부 writer까지 잠그지 않는다. 권한·소유자·운영 절차를 함께 적용한다.
동시에 같은 checkpoint를 두 query가 쓰지 않는다. Delta 자연키 UNIQUE를 가정하지 않는다.
[Databricks concurrent run 권고](https://docs.databricks.com/aws/en/jobs/run-now)

현재 callback은 로컬 Delta 경로만 캡처하고 batch.sparkSession으로 sink를 구성한다.
주입형 테스트 sink는 로컬 장애 주입 전용이며 운영 직렬화 보장 경로가 아니다.
cloudpickle 직렬화 통과는 실제 Standard compute 수용 검증을 대체하지 않는다.
[foreachBatch 제약](https://docs.databricks.com/aws/en/structured-streaming/foreach)

## 중단과 재시작

1. 소유 query/run과 승인된 target/checkpoint를 확인하고 기록한다.
2. 기존 단일 query를 정상 중단하고 활성 writer가 없는지 확인한다.
3. 코드·두 branch 순서·grouping·state schema·shuffle/state provider·source와 checkpoint를 유지한다.
4. 동일 checkpoint에서 재시작한다. checkpoint의 commits/offsets/state를 삭제·수정하지 않는다.
5. 최근 progress, lag, target 자연키별 행 수, count/sum/avg와 재시도 batch를 대조한다.

checkpoint 유실·state schema 변경·source retention 만료는 이 절차의 범위가 아니다.
새 checkpoint에 earliest/latest를 넣어 우회하지 말고 별도 복구/재계산 승인을 받는다.

## 장애 대응

| 관측 | 대응 |
|---|---|
| MERGE 전 실패 | Gold 보존, 원인 수정 후 같은 checkpoint 재시작 |
| MERGE 성공 후 callback/checkpoint 실패 | 동일 final 재전달을 정확 비교, 동일 값 NO-OP 확인 |
| 단일 MERGE 중 오류 | commit 여부와 target 상태 확인. 파일 생성만으로 commit 성공을 판정하지 않음 |
| 여러 MERGE 중 일부 성공 | 각 MERGE는 별개 commit. 전체 batch/여러 table의 원자성을 가정하지 않음 |
| CONFLICT | writer 정지, 기존 값 보존, 양쪽 12컬럼·source·W·batch 기록. 자동 UPDATE 금지 |
| 1 ULP 재계산 차이 | 기존 값 보존/실패 유지. epsilon·반올림·강제 재시도 무한 반복으로 숨기지 않음 |
| state 급증 | branch/operator별 rows/memory/removed/late, event-time max와 W, 입력 차원 수 확인 |
| idle tail 미확정 | OPEN_PENDING_EVENT_TIME으로 관측. 벽시계나 운영 heartbeat로 W를 강제 전진하지 않음 |

backfill/정정/누산 방식 변경은 별도 승인 대상이다. current checkpoint를 임의 초기화하지 않는다.

## 운영 수용 테스트와 DoD

확인할 항목: 실제 validated Kafka→Registry→decoder→두 window→선택한 UC sink,
권한/네트워크 실패, actual Standard callback 직렬화, 두 해상도 수치/late/idle,
같은 batch 재실행, 단일 writer 통제, 동일 checkpoint 정상·장애 재시작, state 장기 관측.

로컬 DoD와 운영 DoD는 구분한다. Kafka/DBR E2E·Gold 형식 승인·UC adapter 검증·Phase 2
병합 관계가 닫히기 전 Phase 3 전체 완료나 main merge를 권고하지 않는다.
실제 클라우드 착수 전 리소스·권한·비용/최대 시간·중단/보존 범위를 별도 승인한다.

## 로컬 재현과 증빙

기존 `.venv`에서 `python -m pytest tests/unit -q -p no:cacheprovider`를 실행한다.
Spark는 프로젝트 환경을 바꾸지 않는 별도 venv/JDK를 사용한다.
일반 Streaming 10건과 Delta 통합 15건을 **별도 pytest 프로세스**로 실행한다.
이미 시작된 일반 Spark JVM에 builder 설정만으로 Delta JAR를 추가할 수 있다고 가정하지 않는다.
정확한 env prefix·파일 목록·최종 결과와 최초 실패는 보고서의 2026-10-23 부록에 있다.
확장 검사는 테스트가 생성한 임시 디렉터리와 전용 process group만 종료한다.
운영 환경에서 그 kill harness나 Phase 1의 checkpoint 이동 절차를 그대로 실행하지 않는다.
