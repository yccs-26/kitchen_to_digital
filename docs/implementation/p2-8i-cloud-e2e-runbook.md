> 최신 결과는 **§13 Autonomous Mission**, Cloud 실행 조건은 §12·§13을 따른다. 실물 로컬 복구와 MSK metadata는 확인했지만 전체 Cloud E2E는 BLOCKED다.

# P2-8i — Cloud E2E 실행 준비 패키지 (P2-8j 안전성 보강)

**CLOUD EXECUTION BLOCKED / LOCAL VERIFIED.** 2026-10-23 KST 갱신.
P2-8j에서는 AWS 읽기 전용 관리 조회만 수행했고, Databricks principal 조회는
실패하여 후속 조회를 중단했다. SQL/Kafka/Compute/권한 변경은 실행하지 않았다.
아래의 과거 P2-8i 계획보다 마지막 P2-8j 결과와 현행 코드가 우선한다.
실제 시작 branch는 `feat/phase2-validation-silver`, HEAD는
`e266a545e084c7da6213f39aa4cfb3ddf0b58da6`다. 과거 P2-8g 승인은 이번 E2E의
승인이 아니다. 이 문서의 cloud 명령은 별도 승인 이후에만 실행한다.

## 1. 현재 계약과 실행 경계

현재 코드와 P2-8d/P2-8h 증빙을 확인했다. `platform.md`의 Job 2 미구현·bounded
dedup 표기는 현재 코드와 다르다. 실제 구현은 영속 Silver event_id 비교이며,
late는 처리 시각 기준 분류다. 실제 Spark watermark/stateful dedup을 구현했다고
하지 않는다. Bronze UC Managed Iceberg / Silver UC Managed Delta를 유지한다.

| 항목 | 확인한 값 / 코드 |
|---|---|
| MSK | ap-northeast-2, ktd-msk-serverless, `boot-aersevd2.c2.kafka-serverless.ap-northeast-2.amazonaws.com:9098` |
| raw / output | kitchen.sensor.raw / kitchen.sensor.validated / kitchen.sensor.quarantine |
| 설정 증빙 | P2-8g `20261022T051154Z/topics-final.json`: 두 output 3 partitions, delete, 7일, bytes=-1, RF=3/min ISR=2. 이번 broker 재조회 없음 |
| source 인증 | runtime의 `ktd-msk-consumer` → Spark `databricks.serviceCredential`; Job 2 group ID는 Spark가 생성하며 코드에 고정된 운영 consumer group 없음 |
| producer 인증 | runtime의 `ktd-msk-job2-credential` → 기존 runtime_config의 UDF token signer → Python OAUTHBEARER/SASL_SSL |
| ACK | callback 성공 + message 존재 + bool 제외 int offset >=0. flush()==0 단독으로 완료하지 않음 |
| Registry | Confluent Cloud `https://psrc-1oqld6g.ap-northeast-2.aws.confluent.cloud`, basic scope/key `ktd-job2/schema-registry-basic-auth`; 비밀값 미조회 |
| Avro | 기존 sensor_metric_event.avsc 10필드, raw/validated `*-value` subject, auto.register=false/use.latest=false; schema ID는 실행 전 조회로 확인 |
| production Silver | `ktd.silver.sensor_validated`, 기존 DeltaSilverStorage insert-only MERGE |
| production checkpoint | `/Volumes/ktd/silver/checkpoints/job2/sensor-validation`; 삭제·초기화·테스트 재사용 금지 |
| 기존 launcher | `scripts/run_job2_databricks.py --runtime-config config/job2-runtime.json --available-now` Python script-task 경로. 이 명령은 승인 없이 실행하지 않음 |
| 기존 버전 표기 | 실제 runtime JSON git:b8c6ee2 / example git:79163ad. 사용자 파일을 바꾸지 않음. E2E harness의 processing_version은 manifest의 실제 준비 SHA로 설정 |

이번 도구는 기존 happy-path 한 건 도구를 대체 수정하지 않는다. 기존
`phase1_msk_fixture_producer.produce_fixture/Fixture`로 raw bytes를 발행하고,
Job 2의 **동일 make_batch_handler/_execute_batch**와 기존 signer·Delta/publisher를
재사용한다. 기존 도구에 없던 다중 fixture·offset window·대사만 추가했다.

일반 E2E도 run 전용 checkpoint를 사용한다. Cloud harness는 raw ACK의 정확한
partition/offset만 foreachBatch에 전달한다. 따라서 이 검증은 실제 MSK→기존
Job 2 처리→Delta/output 경로의 격리 수용 검증이며, 운영 query 전체 raw backlog를
처리한 실험은 아니다. filter 없이 기존 launcher를 실행하는 것은 별도 범위다.

## 2. 로컬 파일과 환경 분리

새 파일의 저장소 기준 경로와 절대 경로:

- `scripts/phase2/cloud_e2e.py`:
  `/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/scripts/phase2/cloud_e2e.py`.
  prepare/snapshot/publish/run-job/verify/compare를 담당한다.
- `tests/integration/test_job2_cloud_e2e_tools.py`:
  `/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/tests/integration/test_job2_cloud_e2e_tools.py`.
  네트워크 없는 contract/실패/재처리 검사다. 기존 외부 integration처럼 cloud에
  접속하지 않으므로 기본 pytest 실행에 포함한다.
- 이 문서:
  `/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/docs/implementation/p2-8i-cloud-e2e-runbook.md`.
- 로컬 증빙 prefix:
  `/Users/uchan/Downloads/DE/02_project/kitchen_to_digital/docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/`.
  `manifest-preview.json`은 고정 시각의 **미실행 예시**다. 실제 실행에는 새 manifest를
  생성한다. 별도 tests/fixtures 템플릿을 중복 저장하지 않는다.

| 환경 | 수행 범위 / 인증 |
|---|---|
| Mac `.venv` | prepare/dry-run/compare/도구 테스트. 비밀값과 클라우드 클라이언트 생성 없음 |
| 기존 EC2 | 승인 후 기존 Python 환경에서 publish. IAM default chain은 기존 raw producer role. 새 설치/EC2 시작은 자동 수행하지 않음 |
| 기존 DBR Compute | 승인 후 snapshot/run-job/verify. Spark·기존 UC credential/Registry secret 참조 사용. 별도 output 검증 credential 이름을 CLI로 전달 |
| `/tmp` OSS Spark/Delta | 기존 P2-8h fresh-process recovery 회귀. UC·MSK 증거가 아님 |

기존 tracked 수정 6개 및 미추적 runtime/happy-path/Registry/cloud 증빙은
byte 단위 snapshot으로 보존 확인한다. 로컬 계획서의 Git 제외도 유지한다.

## 3. Fixture manifest와 기대값

`run_id=p2i-<UUID hex 32자리>`, `event_id=<run_id>/<case>`다. duplicate와
conflict는 normal의 ID를 그대로 사용한다. source에는 `e2e:<run_id>`를 넣고
동일 manifest를 재사용할 때 key/value/시간/ID를 변경하지 않는다. source는
기존 string 필드이며 wire 필드를 추가하지 않는다. 모든 key는 fridge-001이다.
따라서 순차 ACK의 partition 동일·offset 증가를 검증하여 canonical 선행을 보장한다.

| 입력 순서 | case | Silver | validated | Quarantine / 오류 | 기준 시각에서 분류 |
|---|---|---|---|---|---|
| 0 | normal, 4.2 | 신규 | O | X | ON_TIME |
| 1 | business_anomaly, 30.0 | 신규 | O | X, 업무 이상값도 계약 유효 | ON_TIME |
| 2 | domain_invalid, unit 불일치 | X | X | UNIT_MISMATCH/unit | 격리 |
| 3 | bad_frame, magic=0xff | X | X | INVALID_CONFLUENT_FRAME/raw_value, event_id=null | 격리 |
| 4 | unknown_schema | X | X | UNKNOWN_SCHEMA_ID/schema_id, event_id=null | 격리 |
| 5 | duplicate, normal과 bytes 동일 | 기존 1행 | O 재발행 | X | ON_TIME |
| 6 | conflict, normal ID + 값9.5 | 기존4.2 보존 | X | EVENT_ID_CONFLICT/metric_value | 격리 |
| 7 | late_within, -1분 | 신규 | O | X | ON_TIME (현재 10분 정책 이내) |
| 8 | late_outside, -20분 | 신규 | O | X | LATE, 필터링하지 않음 |
| 9 | future, +24시간 | X | X | FUTURE_EVENT_TIME/event_time | FUTURE_INVALID |

기대 결과는 **10 raw ACK / 4 canonical ID / 최소 5 validated / 5 Quarantine**다.
정상 완료 최초 시도에서 validated의 normal ID 2건은 계획된 duplicate다.
재시작으로 생긴 추가 출력은 extra_deliveries로 보고하며 canonical 변화·누락은 FAIL이다.
필수 필드 누락은 현재 non-null Avro writer가 먼저 거부하므로 실제 wire fixture는
직렬화 가능한 domain 위반을 선택했다. 로컬 domain의 필드 누락 검사는 기존 unit이다.

known schema ID는 prepare의 명시 인자다. snapshot은 schema 본문, raw/validated
subject 등록, raw subject ID 일치를 확인한다. unknown 후보 기본값 2147483647은
미존재라고 가정하지 않는다. 정확히 HTTP404/error40403일 때만 snapshot READY다.
404 endpoint/인증/timeout 오류는 중단한다. schema 등록·수정은 하지 않는다.

manifest는 config/rules/schema/관련 소스 hash와 준비 시 HEAD SHA를 보관한다.
내용·배포 파일이 바뀌면 실행을 차단한다. DBR에 .git이 없어도 실제 소스 hash를
대조하므로 git 바이너리 없이 실행 가능하다. 준비 당시 dirty 파일이 있었다면
HEAD SHA만으로 해당 artifact를 재현할 수 없으므로 source hash 증거를 함께 보존한다.
사용자 commit 및 배포 후 **새 SHA에서 manifest를 재생성**하는 것을 권장한다.

## 4. 실행 전 gate — 현재 차단 요소

아래 조건의 현재 조회 결과는 마지막 P2-8l 절을 따른다. 미충족 상태에서는
cloud 명령을 실행하지 않는다. `--execute`/run 승인만으로는 부족하며 검토된
`--readiness`가 추가로 필요하다. prepare가 만드는 기본 양식은 BLOCKED다.

1. VPC 접근 가능한 기존 EC2/DBR 상태, runtime 패키지, signer/UC credential 동작.
   EC2 inventory는 Kafka CLI/JAR와 기존 kafka-python 경로 증거이며 이 새 도구가
   import하는 confluent-kafka/fastavro까지 설치됐다는 증거는 아니다. 원격에서
   `python -c 'import kafka, aws_msk_iam_sasl_signer, confluent_kafka, fastavro'`를
   먼저 검사하고 누락 시 중단한다. 설치는 별도 승인 후 수행하며 도구가 자동 설치하지 않는다.
2. output 읽기 권한과 해당 UC Service Credential 이름. 현재 source role은 raw
   ReadData만 기록돼 있어 output 검증에 그대로 사용할 수 없다.
3. UC 검증 principal의 USE CATALOG/SCHEMA + SELECT, 기존 Volume 접근 권한.
4. 장애 시험용 별도 **기존** Delta table. 도구가 CREATE TABLE을 실행하지 않는다.
   없으면 아래 DDL의 별도 승인·실행이 필요하다. 정상·장애 시험 모두 production
   Silver 재사용을 금지한다. 목적지는 `<production_table>_e2e_<UUID hex>`만 허용한다.
5. run identity가 Silver에 있으면 snapshot에서 중단. manifest·출력 파일은
   덮어쓰지 않는다. 기존 run artifact를 지우고 재사용하지 않는다.
6. 실제 단가·현재 누적 지출·크레딧, 자동 종료 및 시간 제한 확인.
7. 생산 Job 2가 동시에 raw를 읽어 같은 fixture를 처리하지 않도록 독립 실행
   시간을 확보한다. harness는 active query가 있는 같은 SparkSession을 거부한다.
   다른 Compute의 기존 writer/Job 상태도 승인 실행 전에 확인해야 한다.
   raw/validated는 공유 topic이므로 Job 1과 다른 downstream consumer도 fixture를
   읽을 수 있다. run_id가 이를 자동 차단해 주지는 않는다. 다른 workload 영향을
   허용하지 않는 실행 시간/대상 소비자 상태를 먼저 확인하고, 중지가 필요하면
   별도 승인받는다. 이 조건이 충족되지 않으면 fault 시험을 진행하지 않는다.

## 5. 승인 후 명령 순서 (이번에는 실행하지 않음)

아래 경로·UUID·known schema ID는 실제 승인 때 확정한다. 공통 인자 파일을
수정할 필요 없이 CLI 인자와 artifact만 생성한다. Databricks Python script-task의
python_file은 배포된 `scripts/phase2/cloud_e2e.py`다. `--runtime-config`는 해당
배포의 **절대 경로**로 전달한다. `--manifest/--before/--acks/--output`은 driver에서
읽고 쓸 수 있는 승인된 Volume 경로로 전달한다. 로컬에서 EC2로 옮긴 파일도
동일 bytes여야 하며 manifest/config/rules와 streaming 모듈을 함께 배포한다.

### 5.1 로컬 준비와 dry-run

Compute 준비가 끝난 뒤 새 시각의 manifest를 만든다. 실제 발행은 prepare 기준
2분 이내여야 한다. 오래된 fixture는 자동 시간 수정 없이 **새 run_id로 재생성**한다.
Late 기대 분류는 준비 시각 기준이며 실행 지연으로 바뀔 수 있다. 실제 batch counts와
progress를 보관하고, 정상 시험을 수 분 안에 끝내어 경계 이동을 최소화한다.
장시간 뒤 replay하면 Late 분류가 달라지고 +24시간 future도 언젠가는 유효해질
수 있다. 이번 같은-manifest 재시작 수용은 짧은 실행 window를 대상으로 하며,
영구적으로 고정된 품질 판정 ledger가 존재한다고 주장하지 않는다.

```bash
# 저장소 루트, 값은 실제 승인 때 입력 (source/manifest 파일을 편집하지 않음)
RUN_ID="p2i-<새 UUID hex>"
KNOWN_SCHEMA_ID="<조회로 확인할 raw schema ID>"
RUN_DIR="<승인된 새 실행 artifact 경로>"
CONFIG="<배포된 config/job2-runtime.json 절대 경로>"
VERIFY_CREDENTIAL="<output+raw 읽기 가능한 UC service credential 이름>"

python scripts/phase2/cloud_e2e.py prepare --runtime-config "$CONFIG" \
  --run-id "$RUN_ID" --schema-id "$KNOWN_SCHEMA_ID" \
  --silver-table "ktd.silver.sensor_validated_e2e_<UUID hex>" \
  --manifest "$RUN_DIR/manifest.json" --readiness "$RUN_DIR/readiness.json"

# 실제 publish·Spark·Registry·AWS 접근 없음
python scripts/phase2/cloud_e2e.py publish --runtime-config "$CONFIG" \
  --manifest "$RUN_DIR/manifest.json" --output "$RUN_DIR/acks.json"
```

필요한 격리 테이블 생성 SQL 초안 (사용자 별도 승인 전 실행 금지):

```sql
CREATE TABLE ktd.silver.sensor_validated_e2e_<UUID_hex> (
  event_id STRING NOT NULL, event_time TIMESTAMP NOT NULL,
  store_id STRING NOT NULL, equipment_id STRING NOT NULL,
  equipment_type STRING NOT NULL, metric_name STRING NOT NULL,
  metric_value DOUBLE NOT NULL, unit STRING NOT NULL,
  schema_version STRING NOT NULL, source STRING NOT NULL
) USING DELTA;
```

새 checkpoint는 원본의 sibling Job 2 경로
`/Volumes/ktd/silver/checkpoints/job2/e2e/<run_id>`다. production checkpoint를
초기화·삭제하거나 원본 config에 덮어쓰지 않는다. 기존 test checkpoint도 재시작 때 유지한다.

### 5.2 DBR snapshot → 기존 EC2 raw 발행 → DBR Job 2 → 대사

아래는 각 환경의 Python script-task parameter와 동일한 CLI다. 이 도구 자체는
Compute/EC2를 시작하지 않는다. EC2가 stopped면 중단하고 별도 시작 승인을 받는다.

```bash
# DBR: metadata + 시작 offsets + Registry + Silver collision read-only 검사
python scripts/phase2/cloud_e2e.py snapshot --runtime-config "$CONFIG" \
  --manifest "$RUN_DIR/manifest.json" --verification-credential "$VERIFY_CREDENTIAL" \
  --output "$RUN_DIR/before.json" --readiness "$RUN_DIR/readiness.json" --execute --approve-run-id "$RUN_ID"

# EC2: manifest/config/rules/소스와 before.json을 동일 bytes로 전달한 뒤 실행
python scripts/phase2/cloud_e2e.py publish --runtime-config "$CONFIG" \
  --manifest "$RUN_DIR/manifest.json" --before "$RUN_DIR/before.json" \
  --output "$RUN_DIR/acks.json" --readiness "$RUN_DIR/readiness.json" --execute --approve-run-id "$RUN_ID"

# DBR: ACK가 있는 정확한 원본 위치만 처리, availableNow, 최대 대기 180초
python scripts/phase2/cloud_e2e.py run-job --runtime-config "$CONFIG" \
  --manifest "$RUN_DIR/manifest.json" --before "$RUN_DIR/before.json" \
  --acks "$RUN_DIR/acks.json" --output "$RUN_DIR/query-first.json" \
  --readiness "$RUN_DIR/readiness.json" --execute --approve-run-id "$RUN_ID"

# DBR: 시작 high offset부터 새 end high offset 미만까지 모두 읽고 대사
python scripts/phase2/cloud_e2e.py verify --runtime-config "$CONFIG" \
  --manifest "$RUN_DIR/manifest.json" --before "$RUN_DIR/before.json" \
  --acks "$RUN_DIR/acks.json" --verification-credential "$VERIFY_CREDENTIAL" \
  --output "$RUN_DIR/report-first.json" --readiness "$RUN_DIR/readiness.json" --execute --approve-run-id "$RUN_ID"
```

Python의 `job_request()`는 API 호출 없이 제출 JSON을 생성한다. 다음 예시는
로컬에서 실제 배포 경로/Volume/credential 이름을 인자로 넣어 파일만 만든다.
실행 전에 request JSON을 검토한다. run-job/verify도 앞 절의 CLI 인자 목록을
같은 함수에 전달한다. [Jobs submit CLI](https://docs.databricks.com/aws/en/dev-tools/cli/reference/jobs-commands#databricks-jobs-submit).

```python
import json
from pathlib import Path
from scripts.phase2.cloud_e2e import job_request

manifest = json.loads(Path("<로컬 manifest.json>").read_text())
readiness = json.loads(Path("<로컬 검토 완료 readiness.json>").read_text())
parameters = [
    "snapshot", "--runtime-config", "<배포 repo 절대 경로>/config/job2-runtime.json",
    "--manifest", "<Volume run 경로>/manifest.json",
    "--output", "<Volume run 경로>/before.json",
    "--readiness", "<Volume run 경로>/readiness.json",
    "--verification-credential", "<확정한 검증 credential>",
    "--execute", "--approve-run-id", manifest["run_id"],
]
request = job_request(
    manifest, "0929-113111-cgu7k91c",
    "/Workspace/<승인한 배포 repo>/scripts/phase2/cloud_e2e.py",
    parameters, readiness,
)
with Path("/tmp/p2i-snapshot-request.json").open("x") as output:
    json.dump(request, output, indent=2)
```

승인 후에만 `databricks jobs submit --json @/tmp/p2i-snapshot-request.json
--profile ktd --no-wait`를 실행한다. 응답 run_id를 저장하고
`databricks jobs get-run <Databricks_RUN_ID> --profile ktd`로 상태를 확인한다.
생성 JSON에는 새 cluster/libraries 설치가 없으며 동일 request는 동일 idempotency
토큰을 사용한다. 의도한 재시작은 새 query output 경로를 사용하므로 request도 달라진다.

각 task는 timeout_seconds=600 이하, max_retries=0으로 제출한다. cluster는 기존
승인된 cluster만 사용한다. SDK/CLI Jobs submit은 실행 승인이므로 지금 제출하지
않았다. Jobs UI/API에서 task state 및 result_state를 저장한다. query 정상 종료는
검증 PASS와 다르며 별도 verify가 PASS여야 수용한다.

### 5.3 Kafka / Silver 대사 계약

- 검증 group=`ktd-p2e2e-<UUID hex>`로 기존 Job 2 그룹과 분리한다.
  auto.commit=false, auto.offset.store=false, auto.offset.reset=error,
  allow.auto.create.topics=false, 수동 assign이며 subscribe/commit을 호출하지 않는다.
- before snapshot에 모든 세 topic/partition low/high를 저장한다. high는 다음
  offset이므로 `[before.high, after.high)`를 읽는다. retention이 before.high를
  넘어가거나 partition 집합이 바뀌면 FAIL이다. 최신만 poll해서 과거 출력을 놓치지 않는다.
- 최대 60초/10,000개 레코드의 bounded read다. 범위를 다 읽지 못하면 중단한다.
  consumer는 실패를 포함해 close하며 JSON에 실제 위치·key/value를 남긴다.
- raw는 ACK topic/partition/offset의 bytes까지 대사한다. validated는 run namespace
  ID·equipment key·UTC 정규화 10필드 canonical을 비교한다. Quarantine은 원본
  lineage + key/hash/schema ID/event ID/error code·field/quarantine_id를 비교한다.
  framing/unknown-schema는 decoded ID가 없으므로 raw ACK lineage로 run에 귀속한다.
- 관련 없는 run의 정상 output은 제외한다. window 내 decode 불가 output은
  보수적으로 FAIL로 보고해 유실을 숨기지 않는다. Registry 시스템 예외도 전파한다.
- Silver는 먼저 MANAGED/delta provider와 canonical 10필드 타입(timestamp LTZ/double/string)을
  확인하고 run의 모든 ID를 실제 Delta adapter로 조회한다. ID별 최대 2행 검사를
  포함하므로 단순 count(*)가 아니다. canonical 전체 필드와 invalid ID 부재를 검사한다.
- 결과 JSON은 PASS/FAIL, issues, extra_deliveries, canonical, ACK, bounds,
  observed_records를 포함한다. 메시지에 비밀값을 넣지 않는 합성 fixture만 사용한다.

## 6. IAM 및 UC 최소 권한 초안

실효 권한은 이번에 조회하지 않았다. 아래 role 관계는 기존 P2-8d 기록이며,
정책 초안이 broker 인증 성공을 뜻하지 않는다.

| 주체 | 기존 후보 / 필요 권한 |
|---|---|
| raw fixture producer | 기존 EC2 `ktd-msk-producer-role`: cluster Connect, raw DescribeTopic/WriteData. 이번 생성 권한 불필요; CreateTopic 추가 금지 |
| Job 2 source | `ktd-databricks-msk-consumer-role` / UC ktd-msk-consumer: raw ReadData/DescribeTopic, Connect, Spark 그룹 권한. 검증 group으로 바꾸지 않음 |
| Job 2 producer | `ktd-msk-job2-producer-role` / UC ktd-msk-job2-credential: 두 output DescribeTopic/WriteData + Connect. CreateTopic 없음 유지 |
| output 검증 consumer | 미확정. 기존 consumer role에 일시적으로 아래 exact-topic/group 권한을 추가하고 별도 검증 credential로 연결하는 재사용안 우선 검토. 기존 역할의 다른 의존성·trust·boundary·SCP를 실제 승인 전 확인 |
| UC read-only 검증 사용자/SP | `USE CATALOG ktd`, `USE SCHEMA ktd.silver`, 대상 테이블 `SELECT`, 지정 run artifacts Volume의 READ/WRITE VOLUME. Kafka IAM과 별개 |
| 격리 Job 2 실행 사용자/SP | 기존 SELECT/MODIFY + checkpoint Volume READ/WRITE, Service Credential ACCESS, Registry secret scope READ. 격리 table 최초 생성은 CREATE TABLE 권한 및 별도 승인 필요 |

`ReadData`의 선행 권한은 Connect/DescribeTopic/AlterGroup이며 AlterGroup에는
DescribeGroup이 필요하다. [AWS 공식 action 표](https://docs.aws.amazon.com/msk/latest/developerguide/kafka-actions.html).
아래 검증 role 초안은 실행할 run UUID로 group ARN을 치환하여 wildcard 없이 적용한다.
기존 정책을 제거하거나 덮어쓰지 않는다. IAM 변경은 이번 작업에서 하지 않았다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "kafka-cluster:Connect",
      "Resource": "arn:aws:kafka:ap-northeast-2:048715026902:cluster/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2"
    },
    {
      "Effect": "Allow",
      "Action": ["kafka-cluster:DescribeTopic", "kafka-cluster:ReadData"],
      "Resource": [
        "arn:aws:kafka:ap-northeast-2:048715026902:topic/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2/kitchen.sensor.raw",
        "arn:aws:kafka:ap-northeast-2:048715026902:topic/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2/kitchen.sensor.validated",
        "arn:aws:kafka:ap-northeast-2:048715026902:topic/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2/kitchen.sensor.quarantine"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"],
      "Resource": "arn:aws:kafka:ap-northeast-2:048715026902:group/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2/ktd-p2e2e-<RUN_UUID_HEX>"
    }
  ]
}
```

## 7. 실패·재시작 실행 절차

| 시나리오 | 준비한 방법 / 중단·복구 |
|---|---|
| Registry 실패 | 격리 run-job `--fault registry --approve-fault registry`: 해당 handler의 get_schema만 503 예외. 실제 Registry 설정/네트워크는 변경하지 않음 |
| Delta write 실패 | `--fault delta --approve-fault delta`: 격리 target의 insert_if_absent만 OSError. 신규 ID가 아직 없는 별도 run/table에서 수행 |
| producer timeout | `--fault producer_timeout --approve-fault producer_timeout`: produce 직전 합성 TimeoutError. 실제 broker timeout 검증으로 주장하지 않음 |
| ACK 불확실성 | `--fault ack_uncertain --approve-fault ack_uncertain`: 실제 flush/callback 실행 뒤 합성 TimeoutError. 원본을 재발행하지 않고 같은 checkpoint로 재실행 |
| batch/ACK 후 checkpoint 실패 | `--fault after_ack --approve-fault after_ack` (batch도 같은 경계): handler 성공 뒤 반환 전 예외. 격리된 해당 run의 batch만 실패 |
| 실제 broker/IAM/network 장애 | 구현·실행하지 않음. 다른 workload 영향·범위·복구·별도 승인 필요 |

fault는 production Silver와 같은 target이면 차단한다. 같은 SparkSession의 다른
active query도 차단한다. 모듈 patch는 해당 batch 범위에서 자동 복원한다.
실제 broker 장애·프로세스 kill을 재현한 것으로 표현하지 않는다. 기존 실제 로컬
프로세스 종료·Delta/checkpoint 검증은 P2-8h test를 별도로 재실행한다.

각 fault에는 **새 run_id와 빈 격리 table**을 사용한다. 승인 후 5.2의 run-job에
fault 옵션을 붙이고, 실패 JSON을 `query-fault.json`으로 남긴다. 그 뒤 같은
manifest/ACK/before/checkpoint로 `--fault none` 실행하여 `query-restart.json`을
만든다. 입력 publish는 다시 하지 않는다. verify는 처음 before부터 다시 읽으므로
앞서 ACK된 출력과 재시작 출력 모두 대사하며 extra_deliveries를 기록한다.

정상 재시작 시험도 같은 manifest/ACK/checkpoint로 run-job을 다시 호출한다.
`report-first.json`과 새 `report-restart.json`에 대해 아래 로컬 비교를 실행한다.

```bash
python scripts/phase2/cloud_e2e.py compare --runtime-config "$CONFIG" \
  --manifest "$RUN_DIR/manifest.json" \
  --previous-report "$RUN_DIR/report-first.json" \
  --output "$RUN_DIR/report-restart.json" \
  --query-before "$RUN_DIR/query-first.json" \
  --query-after "$RUN_DIR/query-restart.json"
```

compare는 canonical 전체 불변과, 같은 checkpoint/query ID·다른 run ID 및 최종
query 완료를 함께 검사한다. query 인자를 생략하면 scope=canonical_only이며
Spark checkpoint 복구 증거라고 해석하지 않는다. fault 직후 불완전한 결과는
FAIL이 정상이다. 최종 verify PASS와 query-fault→query-restart 관계를 대사한다.

publish는 manifest 옆 publish.lock 및 case별 fsync journal을 먼저 쓴다. 부분 ACK,
응답 유실, 타입/순서 이상은 UNCONFIRMED로 중단한다. lock을 삭제해 재발행하지 않는다.
같은 manifest의 새 output 파일명으로 우회해도 CLI lock은 공유한다. broker 원본을
읽어 전달 여부를 대사하고 별도 사용자 결정을 받는다. 새 run_id만 만들고 미확인
이전 발행을 무시하는 방식도 금지한다.

## 8. 비용·종료·승인 범위

현재 Compute 후보는 과거 보고의 `ktd-phase1-dev`, cluster ID
`0929-113111-cgu7k91c`, DBR17.3/Spark4다. 현재 상태·node type·worker 수·DBU/h·
자동 종료·크레딧·누적 지출은 **미확인**이다. 신규 Compute/EC2/NAT/VPN을 만들지 않는다.
실행 주체가 Compute API에서 설정을 읽고 실제 승인 대상으로 고정한다.

비용 상한은 프로젝트 Databricks **현금 누적 약 $10** 목표다. 이번 실행 비용
0원으로 가정하지 않는다. 시작 전 아래 값을 채우지 못하면 유료 실행을 보류한다.

```text
H = (기동 + snapshot/발행대기/Job2/verify/종료에 걸리는 분) / 60
Databricks = H × (driver DBU/h + worker수 × worker DBU/h) × 해당 SKU USD/DBU
AWS compute = H × (driver USD/h + worker수 × worker USD/h)
EC2 client = 실제 시작~종료 H × 해당 기존 instance USD/h
추가비용 = EBS + MSK partition/storage/IO + 네트워크 + Registry plan의 증분
예상 총액 = 위 합계 + 지연/재시도 여유분
허용 잔액 = $10 - 확인한 프로젝트 Databricks 현금 누적 지출
승인 조건 = 예상 Databricks 현금액이 허용 잔액 이내, AWS 비용 별도 확인
```

[Databricks 가격](https://www.databricks.com/product/pricing)은 DBU와 클라우드
인프라 비용·크레딧을 구분해야 한다. AWS Billing/Cost Explorer의 실제 지출·크레딧과
Databricks Billing/usage(`system.billing.usage`, 필요 권한 별도)를 확인한다.
실제 billing은 지연될 수 있어 승인 시각의 rate와 실행 시간을 함께 보존한다.
단가/크레딧 미조회 상태에서 금액 추정치를 만들어내지 않는다.
AWS 크레딧은 해당 계정의 적용 대상 EC2/MSK/EBS 등에서 확인하며 Databricks DBU에
자동 적용된다고 가정하지 않는다. Databricks 자체 크레딧의 잔액/만료도 별도 확인한다.

- 기존 cluster의 자동 종료를 확인하고, 변경이 필요하면 사용자에게 별도 승인받는다.
  활성 streaming 동안 idle 종료만 믿지 않는다. [자동 종료 문서](https://docs.databricks.com/aws/en/compute/configure#automatic-termination).
- 도구 query wait는 180초이며 timeout/예외 시 query.stop을 호출한다. Jobs task
  timeout 600초, retry=0, 실행 중 시간 상한을 사람이 감시한다. timeout이
  Compute 자체 종료를 보장하지 않는다는 점에 유의한다.
- 성공/실패 무관: consumer.close 확인 → active query 정지 확인 → 해당 task/run
  cancel/종료 확인 → 사용한 Compute terminate → TERMINATED 재조회 → 시작했다면
  기존 EC2 stop/stopped 확인 → 이번 임시 권한만 회수/재조회.
- 승인 후 수동 중지 명령: `databricks jobs cancel-run <RUN_ID> --profile ktd`,
  `databricks clusters delete 0929-113111-cgu7k91c --profile ktd`,
  `databricks clusters get 0929-113111-cgu7k91c --profile ktd`.
  EC2를 시작했다면 `aws ec2 stop-instances --instance-ids i-08e374efb0c31160d
  --region ap-northeast-2`, 이어 describe-instances로 stopped를 확인한다.
  SDK/CLI 버전의 help를 먼저 확인하고 다른 사용자의 Compute는 종료하지 않는다.
- Delta table/증빙/checkpoint/Kafka 데이터는 자동 삭제하지 않는다. MSK·기존 EBS·
  보관 데이터 비용은 Compute 종료 뒤에도 남을 수 있다. 잔여 리소스와 과금 기간을 기록한다.

요청할 정확한 승인: 사용할 cluster/EC2와 시간·비용 상한, raw 10건 발행,
실제 schema/metadata 조회, 지정 UC 격리 table 생성 및 MODIFY/checkpoint 쓰기,
Job2 output 발행/읽기, 선택한 fault별 주입 및 같은 checkpoint 재개, 읽기 principal과
필요한 임시 IAM/UC credential 변경, query/Compute/EC2 종료·임시 권한 회수다.
기존 production checkpoint 초기화·Topic 변경·기존 데이터 삭제는 승인 범위에서 제외한다.

## 9. 로컬 검증과 증빙

최종 수치와 실행 원문은
`docs/evidence/phase-2/p2-8i-cloud-e2e-preflight/test-results.txt`와
`local-summary.json`을 따른다. 이번 실제 재실행 결과는 전체 unit 751 passed, 새 도구 36 passed,
합산 787 passed / 외부 opt-in 11 skipped, 실제 로컬 Spark/Delta 1 passed(45.28초)다.
기존 751/로컬 1 통과 수치를 재사용하지 않았다. 기존 Authlib deprecation 경고 1개다.

```bash
.venv/bin/python -m pytest -q tests/unit --basetemp=/tmp/ktd-8i-unit-final
.venv/bin/python -m pytest -q tests/integration --basetemp=/tmp/ktd-8i-integration-final
# 기존 /tmp OSS 환경이 있는 경우에만 (클라우드 대체 실행 금지)
JAVA_HOME=/tmp/ktd-8h-java/jdk-21.0.13+9/Contents/Home \
KTD_LOCAL_DELTA_JARS=/tmp/ktd-8h-jars/delta-spark.jar,/tmp/ktd-8h-jars/delta-storage.jar \
KTD_RUN_LOCAL_DELTA=1 /tmp/ktd-8h-spark/bin/python -m pytest -q \
  tests/integration/test_job2_local_delta_recovery.py \
  --basetemp=/tmp/ktd-8i-local-delta
```

포트폴리오 캡처: dry-run의 cloud_access=false, 10/4/5/5 계획, 도구 테스트·전체
unit·실제 로컬 Spark/Delta 통과 화면, 합성 canonical-replay.json의 canonical
불변/extra_deliveries. Kafka/Registry 대역과 실제 MSK를 구분해 표시한다.

Commit 후보는 새 tool, tool test, 이 runbook, p2-8i-cloud-e2e-preflight의 새 증빙과
기존 phase-2-verification.md에 추가한 이번 결과뿐이다. 기존 미커밋 파일은 섞지 않는다.
권장 메시지: `test: prepare approval-gated Phase 2 cloud E2E tooling`.
이 문서와 증빙을 사용자에게 넘기고 멈춘다. 실제 Cloud 실행은 하지 않는다.

## 10. P2-8j — Deployment Readiness & Safety Verification (2026-10-23 KST)

**전체 Cloud 실행: BLOCKED.** 로컬 gate 통과와 실제 환경의 실행 가능 여부를
구분한다. `p2-8i-cloud-e2e-preflight/`의 이전 manifest/request는 당시 소스 hash를
갖는 이력이다. 수정된 코드로 다시 제출하지 말고 새 manifest와 readiness를 준비한다.
이번 작업은 읽기 전용 관리 조회 외 Cloud 동작을 수행하지 않았다.

### 10.1 실제 읽기 전용 조회와 한계

`docs/evidence/phase-2/p2-8j-cloud-readiness/management-preflight.json`에 선택한
metadata와 조회 명령을 기록했다. 총 25회: AWS 24회(23회 성공, cluster policy 미설정 응답 1회), Databricks 1회 실패다.
STS/IAM/MSK Describe/GetBootstrap/EC2 Describe/GetInstanceProfile만 사용했다.
SQL, Kafka metadata/consume/produce, SSM 명령, 역할 변경, Compute 시작·정지는 없다.

| 항목 | 현재 증거 / 판정 |
|---|---|
| AWS 관리 principal | `arn:aws:iam::048715026902:user/chan`, AdministratorAccess 등 attached policy 조회. 관리 API 인증 READY, broker 실행 principal과 구분 |
| MSK | `ktd-msk-serverless`, ACTIVE/SERVERLESS, 기존 ARN/bootstrap 일치. 관리 상태 READY; 실제 topic metadata/ACK는 이번 UNVERIFIED |
| EC2 | `i-08e374efb0c31160d`, t3.micro, stopped. instance profile이 실제 ktd-msk-producer-role을 참조함을 확인. 시작하지 않았으므로 client 실행 BLOCKED |
| raw role | Connect 및 raw CreateTopic/DescribeTopic/WriteData. 기존 CreateTopic은 제거하지 않았고 새 권한도 추가하지 않음 |
| Job 2 producer role | Connect + 두 output DescribeTopic/WriteData, CreateTopic 없음. IAM 정의 확인, 실제 signer/broker는 UNVERIFIED |
| source consumer role | Connect + raw DescribeTopic/ReadData + group 권한. output ReadData가 기존 조회 정책에 없음. output 검증에 그대로 사용 불가, BLOCKED |
| boundary / Deny | 조회한 세 role과 chan user에 PermissionsBoundary 필드 없음. 확인한 policy statement에 명시적 Deny 없음. SCP·session policy·broker 조건까지 실효 권한을 확정한 것은 아님. cluster resource policy는 NOT_CONFIGURED 응답 확인 |
| DBR workspace | 로컬 ktd profile host=`https://dbc-1c3b2d11-7408.cloud.databricks.com`만 확인 |
| DBR principal | `current-user me` 실패. 오류 원문은 저장하지 않았고 안전한 분류에서 정확한 인증/연결 원인은 확정되지 않음. 인증 우회·새 credential·후속 API 재시도 없음 |
| DBR Compute / UC / credential | 현재 상태·run-as·grants·credential binding·writer 목록은 후속 조회를 중단해 UNVERIFIED. 과거 cluster 0929-113111-cgu7k91c / DBR17.3 자료는 후보일 뿐 |
| 비용 | AWS t3.micro 유형만 현재 확인. DBU/VM 단가·누적 현금·크레딧·자동 종료는 UNVERIFIED. Cost Explorer 유료 조회와 billing SQL도 실행하지 않음 |

P2-8j 당시 `credentials get`으로 기록했으나 P2-8k에서 하위 명령 help를
확인하여 `credentials get-credential NAME`으로 정정했다. `credentials get --help`는
그룹 help를 보여주므로 개별 명령 확인 증거로 충분하지 않았다. 현재 auth 조회 실패 뒤 해당 metadata
API를 호출하지 않았다. 이후 인증 복구는 사용자가 정상 경로로 수행해야 하며,
이번 세션에서 토큰을 만들거나 비밀값을 가져오지 않았다.

### 10.2 확인 결함 4개와 수정

4개 실패 테스트를 먼저 실행하여 **4 failed**, 수정 후 **4 passed**를 확인했다.

1. 정상 E2E가 production Silver를 고를 수 있었다. 이제 모든 시험은
   `ktd.silver.sensor_validated_e2e_<run UUID hex>`가 기본이며 허용 대상도 이 이름뿐이다.
   다른 catalog/schema, production 이름, 대소문자 별칭은 목적지로 허용하지 않는다.
2. checkpoint가 production과 같지만 않으면 하위 경로 등 임의 위치를 허용했다.
   이제 정확히 `/Volumes/ktd/silver/checkpoints/job2/e2e/<run_id>`만 허용한다.
3. execute 승인 flag만 있으면 비용·외부 Writer 확인 없이 세션을 만들었다.
   이제 `validate_readiness()`를 통과하기 전 Spark/Consumer/Producer를 생성하지 않는다.
   `job_request()`에도 검토된 readiness가 필요하고 cluster/credential 불일치 시 거부한다.
4. checkpoint나 첫 run ID가 없는 재시작 자료도 비교 PASS가 될 수 있었다.
   두 자료의 non-empty run/manifest/checkpoint/query/run ID가 모두 있어야 한다.

기존 생산 Job 2·Delta MERGE·ACK·Avro 계약은 변경하지 않았다. 기존 테스트의
production 기본값 의존은 새 격리 기본값으로 바꾸고, 명시적 unsafe manifest 거부
검사로 유지했다. 기존 권한 실패 테스트에는 유효한 local readiness 대역을 넣어
실제로 client 실패 분기를 계속 검증한다. 테스트를 삭제하거나 오류를 무시하지 않았다.

추가로 raw publish 직전 default chain의 STS principal을 확인하여 예상 EC2 role과
다르면 발행을 막는다. chan 관리자 인증을 raw producer role로 오인하지 않는다.
caller ARN만 ACK 결과에 저장하며 access key/token은 기록하지 않는다.

### 10.3 정확한 격리와 Writer 제외

- 운영: table=`ktd.silver.sensor_validated`, checkpoint=`.../job2/sensor-validation`.
- 시험: table=`ktd.silver.sensor_validated_e2e_<UUID hex>`,
  checkpoint=`.../job2/e2e/p2i-<UUID hex>`.
- raw source는 before.high에서 시작하고 ACK의 정확한 offset만 기존 handler에 전달한다.
  운영 group/checkpoint를 설정하거나 commit하지 않는다. ACK 10개가 같은 partition에서
  증가하는지 확인해 normal→duplicate→conflict 순서를 보존한다.
- 같은 SparkSession의 active query는 실행 직전에 거부한다. 다른 Compute/Notebook/
  외부 writer와 downstream consumer가 없는 시간은 운영자가 확인하고 증거를 남긴다.
- readiness의 Writer 확인은 최대 5분만 유효하다. 전체 readiness는 최대 15분이다.
  이 확인은 분산 lock이 아니며, 확인 후 다른 writer가 시작하는 TOCTOU를 막지 못한다.
  예약/독점 실행 시간 확보가 없으면 실행하지 않는다. 이번에는 writer 부재 미확인이다.
- 새 table/GRANT는 생성하지 않았다. 기존 table 존재·유일성·canonical·invalid ID 부재를
  snapshot/verify로 읽고, 최초 table 생성은 승인 후 앞 절 DDL로 별도 수행한다.

### 10.4 readiness JSON 계약

prepare에 `--readiness`를 지정하면 동일 run/manifest hash를 가진 **BLOCKED** 양식을
새 파일로 만든다. 검토자가 사실을 확인한 뒤에만 아래 항목을 채워 READY로 바꾼다.
확인할 수 없는 항목을 임의로 READY/0원/빈 writer 목록으로 채우지 않는다.

- run/manifest/environment/table/checkpoint 일치, 실제 IAM 역할 4개 및 DBR run-as/UC reader.
- output 검증 credential 이름과 실제 role 연결. IAM/UC, 배포 hash, 의존성,
  네트워크 검토, cleanup plan이 모두 READY.
- writer NONE_CONFIRMED + 확인 시각/증거 참조, Compute ID/자동 종료 시간/수동 중지 확인.
- 실제 누적 Databricks 현금, 승인한 Databricks/AWS 각각의 상한, 확인한 순현금
  시간당 단가, startup·대기·종료를 포함한 최대 분, rate/credit 검증 증거.
- 예상액은 `최대분/60 × 확인한 시간당 순현금 비용`으로 계산해 각각 승인 상한과 비교한다.
  누적 현금 + Databricks 승인액이 $10을 넘거나, NaN/bool/null/음수 비용이면 거부한다.

이 양식은 operator-reviewed attestation이지 IAM 권한이나 broker 상태를 자동
증명하는 인증서가 아니다. 현재 실제 양식은 BLOCKED이며 양성 local-test 양식을
실제 승인 증빙으로 사용하지 않는다. 새 코드가 old manifest hash를 거부하므로
기존 P2-8i preview는 보존만 하고 재사용하지 않는다.

`job_request(manifest, cluster_id, python_file, parameters, readiness)`로 제출 JSON을
만들 때도 검토가 필요하다. parameters에는 `--readiness <배포한 절대 경로>`를 넣는다.
사용자 승인 전 submit API/CLI는 실행하지 않는다. 이 함수 자체는 API를 호출하지 않는다.

### 10.5 Principal A–E와 최소 정책

| 역할 | 현재 확인 / 추가 확인 |
|---|---|
| A fixture raw producer | 기존 role `arn:aws:iam::048715026902:role/ktd-msk-producer-role` 및 EC2 profile 연결 확인. 실행 직전 STS assumed-role 일치 검사. 기존 raw WriteData 사용 |
| B Job 2 source/producer | AWS source role 및 `ktd-msk-job2-producer-role` 정책 정의 확인. 기존 두 UC credential 연결과 ACCESS는 현재 UNVERIFIED |
| C output 검증 consumer | 실제 principal 미선정. 현 source role은 output ReadData 부족. 승인 후 동일 role 재사용 또는 전용 role/credential을 선택해야 함 |
| D UC Delta 검증 | 실제 사용자/SP 미확인. USE CATALOG/SCHEMA, SELECT, artifact Volume READ/WRITE; writer에는 격리 table MODIFY/checkpoint 쓰기 별도 필요 |
| E 배포/Job 실행 | AWS 관리자는 chan, Databricks 배포/run-as 사용자 미확인. Workspace 파일 배포, CAN_ATTACH_TO 및 Job 실행/중지, 필요 시 Compute CAN_MANAGE를 실제 확인 |

C 정책은 앞 §6의 exact cluster/3 topics/exact run group 초안이다. 새 증빙의
`output-reader-policy-proposal.json`은 실제 ARN과 예시 run group을 갖는 미적용 문서다.
기존 consumer role에 재사용하면 신규 데이터 권한은 output 두 topic으로 제한할 수
있지만, 현재 role에 이미 있는 group `.../*` 권한까지 이 추가 정책이 제한하지는 않는다.
단독 최소 권한이 필요하면 별도 reader role/credential 선택을 승인 단계에서 결정한다.
운영 source group 권한을 임의 축소하거나 Job 2 producer에 CreateTopic을 추가하지 않는다.
IAM simulation은 이번에 새로 실행하지 않았고, 정의 조회를 broker 성공으로 표현하지 않는다.

UC 서비스 credential은 IAM role과 별개로 Databricks ACCESS/trust가 필요하다.
[공식 service credential 문서](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/service-credentials).
이번에는 UC grants·trust external ID 일치와 broker 인증을 확인하지 않았다.

### 10.6 Fixture·대사와 비용·종료

10개 입력의 canonical4/validated최소5/Quarantine5는 실제 Job 2 컴포넌트 로컬
검사로 유지했다. 재시작은 at-least-once이며 distinct Kafka 위치의 중복 출력은
허용하고 extra_deliveries로 표시한다. canonical 내용과 유일성은 유지해야 한다.
보고서에 `canonical_fingerprints`, `validated_lineage_by_event`,
`quarantine_lineage_by_case`, `failure_reasons_by_case`를 추가했다.
기존 raw ACK 및 run_id/manifest hash와 함께 기계 대사할 수 있다. 정상/누적 replay
결과는 canonical-replay.json의 first/restart로 구분한다. 실제 MSK ACK는 미검증이다.

Query wait는 run_job의 timeout 기본 180초이며 1~180만 허용한다. Jobs request의
run/task timeout은 600초, max_retries=0이다. readiness는 최대 총 계획 30분의
예상 비용을 검사하지만 전체 Compute 생존 시간을 강제로 제한하는 장치는 아니다.
Task 종료/Query stop만으로 Compute 종료·$10 상한 준수를 보장하지 않는다.

성공·실패 시 다음 증빙을 모두 남겨야 전체 실행을 완료로 보고한다.

1. 해당 Spark query inactive/query.stop 결과 (다른 query는 중지하지 않음).
2. 검증 Consumer close 결과. publish client도 finally close.
3. 해당 Job Task terminal 상태와 result_state. cancel-run만 요청했다고 완료하지 않음.
4. 이번 승인으로 사용한 Compute만 terminate 후 get으로 TERMINATED 재확인.
5. 이번 추가한 임시 IAM 정책만 회수 후 조회/원본 정책 보존 확인.
6. EC2를 시작했다면 stop 후 stopped 재확인. 이미 stopped였던 EC2는 이번에 변경하지 않음.
7. MSK/EBS/Delta 보관 비용·credit 및 종료 시각. 과금 잔액 미확인 사항을 숨기지 않음.

최소 시나리오는 metadata/snapshot → raw10 ACK → availableNow Job2 → read-only verify다.
장애·재시작은 추가 시간/비용 승인이 필요하다. 단가/credit/현재 현금이 없어 금액 추정은
아직 산출하지 않는다. 임의 예시 단가로 $0 또는 $10 이내를 주장하지 않는다.

### 10.7 로컬 재검증 / 상태 구분

최종 수치·명령 원문은 `p2-8j-cloud-readiness/test-results.txt`와 local-summary.json을
따른다. 새 도구 60 passed, 전체 unit 포함 811 passed/11 skipped, 실제 로컬
Spark/Delta 1 passed(48.32초)이며 기존 Authlib warning 1개다. READY는 확인된 해당 항목 범위만 뜻하며 전체 Cloud 실행을 뜻하지 않는다.

- READY: local dry-run/guard/manifest/격리 설정·대사, AWS 관리 인증·MSK 상태·EC2 profile/role 정의.
- BLOCKED: 전체 Cloud 실행, output ReadData 주체, 현재 stopped EC2의 client 실행,
  미확인 비용/cleanup 권한·readiness 승인 및 미배포 코드.
- UNVERIFIED: DBR principal/Compute/UC table/grants/service credential·원격 패키지,
  다른 writer 부재, 실제 broker metadata/ACK/source checkpoint 복구/SCP.

새 증빙은 preflight/local verification임을 명시한다. 기존 P2-8i 증빙은 덮어쓰지 않았다.
P2-8j 작업 종료 당시 tool/test/runbook은 미커밋이었다. P2-8k 시작 시
사용자가 이 변경을 42dd5c9로 commit한 것을 확인했다. 기존 unrelated 6개 파일을 섞지 않는다. 권장 메시지는
`fix: enforce isolated and reviewed Phase 2 E2E deployment`다.


## 11. P2-8k — Blocker Resolution & Final Execution Readiness (2026-10-23 KST)

이 절이 최신 실행 판단이다. **전체 Cloud E2E: BLOCKED.** 새 CLI는 만들지 않았다.
실제 변경 없이 원인을 좁히고 기존 도구의 raw offset 경계 결함만 수정했다.
증빙은 `docs/evidence/phase-2/p2-8k-cloud-unblock/`의 관리 preflight/local verification이다.

### 11.1 Databricks 장애 원인과 정상 복구 경로

- 실제 시작 HEAD는 `42dd5c9edf92402f10de37540e3dbdf1e0b910fa`다.
- CLI v1.18.0, profile `ktd`, auth_type `databricks-cli`; credential storage는 OS keyring.
  DATABRICKS/AWS 환경변수 override 없음. 현재 `.venv`에 databricks-sdk는 미설치다.
  remote Runtime의 SDK 버전은 조회하지 못했다. 설치하지 않았다.
- host는 Workspace URL `https://dbc-1c3b2d11-7408.cloud.databricks.com`이며
  account console endpoint가 아니다. account_id/workspace_id 필드 존재를 확인했다.
  이 필드만으로 실제 workspace membership이나 권한을 입증하지 않는다.
- `auth describe` text가 **refresh token is invalid**로 새 access token 취득 실패를
  명시한다. `current-user me`도 갱신 단계에서 exit1. Workspace SCIM Me 요청 의도는
  `GET /api/2.0/preview/scim/v2/Me`이며 실제 Workspace HTTP 응답 코드는 관측되지 않았다.
  401/403이나 계정 정지·Compute credit 부족으로 단정하지 않는다.
- Workspace DNS/TLS1.3 및 인증서 검증은 성공했다. 이 시점 host 접속 장애보다
  OAuth refresh 실패가 직접 차단 원인이다. broker/VPC 경로 검증은 아니다.
- JSON auth describe는 exit0/status=error/error={}를 반환했다. exit0만으로 인증 READY로
  판단하면 안 된다. text 기본 출력으로 원인을 확인했고 debug/--sensitive는 쓰지 않았다.

사용자가 직접 정상 재인증할 명령은 아래와 같다. **이번 작업에서는 실행하지 않았다.**
토큰 값 전달·복사나 다른 profile로 우회하지 않는다.

```sh
databricks auth login --profile ktd
```

사용자가 정상 재인증 완료를 알려준 후, 다음 read-only 조회부터 다시 시작한다.
실패하면 원인을 기록하고 해당 경로를 중단한다. raw JSON 전체에는 사용자 정의
설정 값이 있을 수 있으므로 role/owner/state/권한/경로만 선택해 비밀값 없이 저장한다.

```sh
databricks current-user me -p ktd -o json
databricks catalogs get ktd -p ktd -o json
databricks schemas get ktd.silver -p ktd -o json
databricks volumes read ktd.silver.checkpoints -p ktd -o json
databricks grants get-effective catalog ktd -p ktd -o json
databricks grants get-effective schema ktd.silver -p ktd -o json
databricks grants get-effective volume ktd.silver.checkpoints -p ktd -o json
databricks credentials get-credential ktd-msk-consumer -p ktd -o json
databricks credentials get-credential ktd-msk-job2-credential -p ktd -o json
databricks clusters get 0929-113111-cgu7k91c -p ktd -o json
databricks jobs list --expand-tasks -p ktd -o json
databricks jobs list-runs --active-only -p ktd -o json
databricks repos list -p ktd -o json
```

`get-effective`는 상속 권한도 포함한다. principal의 실제 group membership도 함께
대조하고 next_page_token이 있으면 모두 조회한다. repos list는 권한상 보이는 repo만
반환하고 Git CLI repos를 누락할 수 있다. 빈 jobs/runs/repo 목록은 Writer 부재 증거가
아니다. Notebook/다른 Compute/외부 실행을 포함한 운영자 확인과 독점 시간 예약이 필요하다.
SQL, credential validate/temporary credential 발급, Compute start는 위 조회에 없다.

### 11.2 Output 검증 Consumer 주체와 최소 추가 정책 확정안

선정 후보는 기존 `arn:aws:iam::048715026902:role/ktd-databricks-msk-consumer-role`,
기존 UC credential `ktd-msk-consumer`다. actual credential binding/ACCESS는 재인증 후
확인해야 하므로 실행 principal 검증 완료로 표시하지 않는다.

| 역할 | 주체 / 이번 판단 |
|---|---|
| AWS 관리 | user chan. management와 simulation만 실행. raw 발행 주체로 사용하지 않음 |
| Raw fixture | 기존 EC2 role ktd-msk-producer-role. raw WriteData 존재. stopped 유지 |
| Job2 source | ktd-databricks-msk-consumer-role / ktd-msk-consumer. 기존 raw 읽기 및 group 권한 유지 |
| Job2 output producer | ktd-msk-job2-producer-role / ktd-msk-job2-credential. 두 output 쓰기 유지 |
| Output 검증 | 위 기존 source consumer role/credential 재사용 후보 선정. 임시 추가 읽기 정책만 필요 |

EC2 관리 client role은 현재 raw WriteData 중심이며 ReadData가 없다. snapshot/verify는
raw bytes도 읽으므로 이를 재사용하면 raw+output 읽기 권한이 필요하고 현재 UC/DBR
실행 경로에도 맞지 않는다. source consumer 재사용은 신규 role/credential 없이 추가
데이터 권한을 output 두 곳에 한정한다. 운영 source 역할 자체가 더 넓게 읽게 되므로
적용 시간과 정책 이름을 기록하고 후속 정상 검증 종료 시 이번 추가 정책만 회수한다.

정책: `p2-8k-cloud-unblock/output-reader-policy-delta.json`.
cluster ARN은 `arn:aws:kafka:ap-northeast-2:048715026902:cluster/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2`.
추가 topic ARN은 같은 cluster descriptor의 topic 리소스 두 개
`kitchen.sensor.validated`, `kitchen.sensor.quarantine`뿐이다.
추가 Action은 DescribeTopic/ReadData뿐이며 Connect/raw/group은 기존 허용을 재사용한다.
기존 wildcard group 권한을 이 delta가 축소한다고 주장하지 않는다.

read_window는 manual assign/position/poll만 쓰고 subscribe/join/commit을 호출하지 않는다.
enable.auto.commit/enable.auto.offset.store도 false다. 다만 AWS MSK IAM 문서의
ReadData dependent actions에는 Connect/DescribeTopic/AlterGroup, AlterGroup에는
DescribeGroup이 있으므로 group 권한을 불필요하다고 제거하지 않는다.
검증 group은 `ktd-p2e2e-<UUID>`이며 standalone 정책이라면 그 정확한 ARN에만
DescribeGroup/AlterGroup을 넣는다. 기존 role에는 이미 group /*가 있으므로 신규
임시 정책에 중복 group grant를 넣지 않는다.
[MSK Action 의존 관계](https://docs.aws.amazon.com/msk/latest/developerguide/kafka-actions.html)

실제 IAM 시뮬레이션: 현재 Connect1+Group2 allowed, output2×Action2 implicitDeny.
미적용 delta를 추가한 평가에서는 7/7 allowed. baseline/proposal 총14개 실제 ARN
평가를 증빙에 보존했다. 다중 topic 요약의 placeholder ARN은 개별 ARN으로 재평가했다.
정책을 적용하지 않았으며 simulation은 SCP/session/trust/UC/네트워크/broker 성공 증명이 아니다.

### 11.3 UC 격리 리소스·Grant 계약

인증 실패로 현재 catalog/schema/Volume 존재·owner·실효 grants를 조회하지 못했다.
403/404가 나오더라도 권한 때문에 보이지 않는 경우와 실제 부재를 구별한다.
없으면 자동 생성하지 않는다. 다음 값은 **계획이며 생성된 리소스가 아니다.**

- table `ktd.silver.sensor_validated_e2e_<32hex UUID>`.
- checkpoint `/Volumes/ktd/silver/checkpoints/job2/e2e/p2i-<32hex UUID>`.
- Volume 객체는 `ktd.silver.checkpoints`다. job2/e2e는 Volume 내부 디렉터리다.
- Table creator/owner, writer run-as, verifier user/SP는 현재 UNVERIFIED.
  table 생성 주체가 초기 owner가 되며 실제 ownership은 생성 후 조회로 확인해야 한다.

| 주체 | 승인 후 필요한 최소 권한 / 대상 |
|---|---|
| 별도 table 생성 주체 | USE CATALOG on ktd, USE SCHEMA + CREATE TABLE on ktd.silver. 승인한 10필드 Managed Delta만 생성. 장기 writer에 schema-wide CREATE TABLE을 새로 주지 않음 |
| Job2 writer | ktd/ktd.silver 사용, 정확한 run table SELECT+MODIFY(lookup/MERGE), ktd.silver.checkpoints READ VOLUME+WRITE VOLUME, 두 service credential ACCESS, 기존 Registry secret scope READ |
| verifier | ktd/ktd.silver 사용, 정확한 run table SELECT, 검증 credential ACCESS, Registry secret scope READ. table MODIFY 불필요 |
| artifact 배포/수집 | 실제 artifact 위치가 Volume이면 해당 Volume READ/WRITE, Workspace 파일이면 해당 경로 권한. verifier라는 이유만으로 checkpoint WRITE를 추가하지 않음 |

UC Volume grants는 디렉터리별이 아니라 Volume 객체에 적용된다. checkpoint 경로 분리는
데이터 격리 규칙이며 권한 격리 자체가 아니다. checkpoints Volume WRITE가 운영
checkpoint 접근도 허용할 수 있음을 승인 시 명시하고 기존 권한을 확인해 필요한
경우에만 추가한다. 더 강한 경계가 필요하면 별도 Volume은 새로운 승인/범위다.
[Volume 권한](https://docs.databricks.com/aws/en/volumes/privileges),
[UC 권한](https://docs.databricks.com/aws/en/data-governance/unity-catalog/access-control/privileges-reference).

### 11.4 Raw 시작 offset·checkpoint 실행 계약과 로컬 수정

생산 source는 runtime의 earliest/latest를 사용한다. E2E harness는 이 값을 덮어쓰고
snapshot의 각 partition high(next offset)를 JSON startingOffsets로 설정한다.
assign은 raw의 [0,1,2], failOnDataLoss=true, 별도 run checkpoint, availableNow다.
기존 Raw의 모든 데이터가 fixture라고 가정하지 않는다. 세 partition에서 source를
읽지만 handler에는 ACK의 정확한 partition/offset 10개만 전달한다. source 로그는
다른 raw 위치까지 진행할 수 있으며 운영 group offset과는 별개다.

| 상황 | 적용 계약 / 실행 판단 |
|---|---|
| A fixture 전 Job2 시작 | 금지. availableNow는 시작 시점의 범위로 끝나 fixture를 놓칠 수 있고 도구는 ACK 파일이 필요함 |
| B fixture 후 Job2 시작 | 유일한 정상 순서. before snapshot → raw10 ACK → 즉시 availableNow → 누적 대사 |
| C 신규 checkpoint | snapshot high JSON 적용. 예시 p0=50,p1=100,p2=75를 로컬 검사. 실제 값은 승인 후 실측하며 지금 추정하지 않음 |
| D 동일 checkpoint 재시작 | Spark가 checkpoint offset/commit을 사용. 미완료 batch만 replay; 완료된 fixture가 반드시 재발행된다고 기대하지 않음 |
| E checkpoint 존재 | startingOffsets로 rewind할 수 없음. 첫 실행은 새 run 경로 부재를 확인하고 재시작은 같은 manifest/ACK/before를 유지 |
| F 세 partition | 정확히 0/1/2 및 low/high non-bool int>=0. partition/retention 변경은 중단 |

Spark는 재개 시 startingOffsets보다 checkpoint를 사용한다.
[Spark Kafka 계약](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html).
로컬 실제 Delta/file-source 재시작은 이를 보완하지만 MSK source 증거는 아니다.

확정 결함: snapshot READY/세 partition/경계 형식/ACK>=snapshot.high를 검사하지 않고
Spark IO로 진행했다. 먼저 7 failed를 재현하고 validate_input_window 최소 검사를
publish 전/각 ACK 후/run_job의 Spark IO 전에 추가했다. raw ACK가 다른 partition 또는
시작 하한 이전이면 partial UNCONFIRMED journal/lock을 보존하고 다음 건을 보내지 않는다.
운영 Job2/launcher/sink/publisher 코드는 수정하지 않았다.

정상 범위는 `[before.high, after.high)`다. 검증 시 collect_bounds와 validate_bounds가
retention gap/partition 변경을 거부하고 raw ACK bytes/10건을 대사한다. ACK 이후
retention 유실이나 checkpoint가 이미 ACK를 지나간 경우를 성공으로 보고하지 않는다.
첫 정상 실행의 checkpoint 부재·writer 독점 시간·입력 상한은 실제 승인 실행 전 확인한다.

### 11.5 배포·비용·단일 정상 E2E 승인 패키지

현재 HEAD 42dd5c9는 P2-8j commit이다. 이번 로컬 offset 수정은 아직 미커밋이므로
HEAD만 배포 artifact라고 주장하지 않는다. manifest는 streaming 전체/도구/fixture
producer source SHA256, config/rules/schema hash를 포함하고 load_manifest가 재계산한다.
.git이 있으면 HEAD도 확인한다. commit/source 불일치 거부 테스트를 추가했다.
사용자 commit 후 같은 SHA와 실제 파일 hash로 manifest를 새로 만들고 원격에서도 검증한다.
Jobs git_source.git_commit 또는 repo HEAD/실제 source hash를 대조한다. branch 이름이나
로컬 HEAD만으로 원격 배포를 확인하지 않는다. 현재 원격 Job/Repo/Writer는 UNVERIFIED.

최소 시나리오는 **정상10건 E2E 1회**다. fault=none, 재발행/장애 주입/재시작은 제외한다.
현재 2분 fixture 유효기간 때문에 Compute/권한/배포가 준비된 뒤 manifest와 snapshot을
새로 만들고 2분 내 발행한다. 미확인 상태를 readiness READY로 채우지 않는다.

실행 전 (현재 실행 금지):

1. 사용자의 정상 재인증 후 위 read-only 항목, UC owner/grants/credential binding,
   current Compute node/DBU/자동 종료/종료 권한, notebook 포함 다른 Writer를 확인한다.
2. IAM delta·격리 table 생성·필요 UC grants·배포 및 실행/정리 범위와 예산을 승인받는다.
   IAM 적용 전 기존 정책/이름 충돌을 기록하고 기존 정책은 덮어쓰지 않는다.
3. 승인한 Compute 준비 후 새 UUID와 실제 SHA의 manifest/readiness를 만든다.
   새 table은 §5.1의 10필드 DDL로 승인 후 생성하며 기존 table 재사용/replace 금지.
   checkpoint가 이미 있으면 정상 최초 실행을 중단한다. 원본 경로는 조회/변경하지 않는다.
4. Cloud snapshot이 세 topic bounds와 빈 run table을 확인한다. 실측 raw high, manifest,
   readiness와 artifact를 배포해 같은 내용을 사용한다. bootstrap/topic metadata 실패,
   registry 불일치/unknown schema가 존재하면 raw를 보내지 않는다.

실행 중 (별도 승인 후 §5.2의 기존 명령 사용):

5. 승인한 기존 EC2에서 STS raw role 확인 → publish10 → ACK/journal 저장 및 즉시 검토.
   부분/불확실 ACK는 자동 재전송하지 않는다. 기존 EC2 패키지 누락 시 설치하지 않고 중단.
6. ACK 이후 DBR run-job, fault=none, Query<=180초, Task600초/retry0.
   실제 options/StreamingQueryProgress의 offset 범위를 기록한다.
7. verify에서 raw bytes/ACK10, canonical4 fingerprint, validated최소5/Quarantine5 및
   lineage/reasons를 검사한다. ACK만으로 PASS하지 않고 부족/불일치면 중단한다.
8. query report/checkpoint commit·progress 증거를 수집한다. 정상 최초 실행에서는
   checkpoint를 변경하거나 완료 fixture replay를 유도하지 않는다.

실행 후 (성공·실패 공통):

9. Consumer finally close, 해당 query inactive/stop, Task terminal 상태를 확인한다.
10. 이번 승인 Compute만 종료 후 TERMINATED 확인, 시작한 EC2만 stopped 확인,
    이번 IAM delta/임시 UC grant만 회수 후 조회한다. 원래 정책·권한은 유지한다.
11. run table/checkpoint/artifact는 증빙으로 보존하고 자동 삭제하지 않는다. 실험 리소스
    목록/잔여 비용/시작·종료 시각·credit/현금 사용을 기록한다. 정리 실패면 완료로 보고하지 않는다.

비용 최신 관리 inventory는 EC2 3대 stopped, gp3 3개 총46GiB, EIP2개,
NAT Gateway2개 available, MSK1개 ACTIVE다. ap-northeast-2/현재 AWS account 범위일
뿐 다른 region/Databricks 관리 계정 자원이나 SQL warehouse 전체를 조사한 것은 아니다.
NAT/기타 자원이 이 실험 전용인지 미확인이라 변경·삭제하지 않았다.

| 비용 종류 | 실제 확인 / 추정 / 한계 |
|---|---|
| Databricks | 현재 Compute 후보0929-113111-cgu7k91c는 과거 자료. live node/DBU SKU/rate·credit·기지출 미확인. 금액 산출 BLOCKED |
| AWS 지속 | 기존 MSK cluster/partition-hour·저장/traffic, NAT hour/data, EBS, 공인IPv4가 잔여 과금 후보. EC2 stopped가 전체 AWS 비용0을 뜻하지 않음 |
| 단가 기반 일부 추정 | AWS 공인IPv4 list price $0.005/IP-hour. 조회된 EIP2개만 유지 시 $0.01/h, 30일720h $7.20 gross. 할인/credits/세금 및 다른IP 제외. E2E 신규 증가액이 아닌 기존 자원 추정 |
| 최소 E2E | 새 topic/role/Compute 생성 없이 기존 EC2+DBR 사용. startup/snapshot/대기/검증/종료 포함 최대30분을 승인할 계획이나 실제 단가가 없어 전체 승인액은 미정 |
| 프로젝트 목표 | Databricks 현금 누적+승인액 <=약$10. AWS/Confluent 비용은 별도다. AWS credit이 Databricks 계약 청구에 적용된다고 가정하지 않음 |

[Databricks pricing](https://www.databricks.com/product/pricing),
[MSK pricing](https://aws.amazon.com/msk/pricing/),
[EBS pricing](https://aws.amazon.com/ebs/pricing/),
[IPv4/NAT pricing](https://aws.amazon.com/vpc/pricing/).
Seoul 단가로 확인하지 못한 MSK/NAT에 US East 예시 요금을 대입하지 않았다.
계정 Billing/Credits 화면에서 credit eligibility/만료/잔액 및 실지출을 사용자가 확인해야 한다.
요금과 Compute 정보를 확보한 뒤 `(DBU/h×net $/DBU + VM/h)×총시간`에 기존 AWS
지속비용·traffic/storage를 별도 더하고 readiness 양식에 반영한다. 시간 제한은 현금 상한이 아니다.

현재 요청할 수 있는 다음 단계는 **사용자의 ktd 정상 재인증**이다. 그 뒤 read-only
관리 조회로 나머지 UC/배포/Writer/비용 조건을 채우고 위 정상1회 실행을 별도 승인한다.
이번 작업은 IAM 적용/로그인/테이블 생성/Compute·EC2 시작/발행 없이 여기서 멈춘다.


## 12. P2-8l — 인증 복구 이후 실측 실행 준비 (2026-10-23 KST)

**전체 정상 Cloud E2E: BLOCKED.** 이번에는 인증·UC·배포·네트워크 실측으로 이전
미확인 항목을 해소했다. 아래는 관리 metadata와 로컬 검증이며 실제 데이터 E2E가 아니다.
source/launcher/테스트 코드는 변경하지 않았다. 새 증빙은
`docs/evidence/phase-2/p2-8l-cloud-readiness/`다.

### 12.1 Git와 인증 / 실행 주체

- 로컬 HEAD, local origin tracking ref 및 실제 `git ls-remote --heads origin
  feat/phase2-validation-silver` 결과가 모두
  `3d772bdea261de4275f5888599d661b7f888fb73`로 일치한다. push 반영을 서버에서 확인했다.
- P2-8k source/test는0c0b9dc로 commit되었고 선택한3d772bd에 포함된다.
  추적 파일은 시작 시 clean, 미추적 보류4개(개인정보 README와 legacy script3개)는 보존했다.
  ignored `config/job2-runtime.json`도 보존했다. 이 파일의 old processing_version은
  실제 배포 SHA 증거가 아니며 E2E는 manifest SHA로 override한다.
- `databricks auth profiles`에서 ktd valid/default=true,
  `current-user me --profile ktd` 성공. Active 사용자 ID `77582772021430`, direct users/admins.
  개인정보 username은 증빙에서 CURRENT_USER alias로 처리했다. 로그인/토큰 생성·출력 없음.
- Workspace는 `https://dbc-1c3b2d11-7408.cloud.databricks.com`, ID7474648508646287.
  AWS 관리자는 기존 chan. raw 역할은 기존 EC2 ktd-msk-producer-role,
  Job2 source/output-reader 후보는 ktd-databricks-msk-consumer-role,
  output producer는 ktd-msk-job2-producer-role로 유지한다.

### 12.2 UC 실측과 추가 Grant 판단

| 객체 | 실측 존재/형식 | 현재 owner·권한 판단 |
|---|---|---|
| ktd | catalog 존재 | 현재 사용자 owner. USE CATALOG 근거 |
| ktd.silver | schema 존재 | 현재 사용자 owner. USE SCHEMA/CREATE TABLE 근거 |
| ktd.silver.checkpoints | MANAGED Volume 존재 | 현재 사용자 owner. READ/WRITE VOLUME 근거 |
| ktd.silver.sensor_validated | MANAGED/DELTA/10필드 metadata 일치 | 현재 사용자 owner. 운영 table을 시험에 재사용하지 않음 |
| ktd-msk-consumer | SERVICE credential, 실제 consumer role ARN 일치 | 현재 사용자 owner. 추가 ACCESS grant 필요 근거 없음 |
| ktd-msk-job2-credential | SERVICE credential, producer role ARN 일치 | 현재 사용자 owner, ACCESS explicit도 조회됨 |
| Registry 참조 | scope ktd-job2/key schema-registry-basic-auth metadata 존재 | 현재 사용자 scope MANAGE. secret 값은 읽지 않음 |

실제 grants get-effective도 조회했다. owner 권한은 결과의 빈 grant 목록만으로
없다고 판단하지 않는다. 객체 owner는 객체의 모든 privilege를 가진다.
[UC owner 권한](https://docs.databricks.com/aws/en/data-governance/unity-catalog/manage-privileges).
기존 Job run-as도 현재 사용자다. **동일 사용자를 새 E2E 실행 주체로 유지하면 새 UC
GRANT는 기본 실행안에 필요하지 않다.** 다른 SP/group으로 바꾸면 이 판단을 재사용하지 않는다.
실제 SELECT/MERGE/checkpoint IO와 service credential signer는 Compute 미실행이라 미검증이다.

preview 이름 `ktd.silver.sensor_validated_e2e_44444444444444444444444444444444`는
metadata API에서 부재 응답을 확인했다. 실제 실행의 새 UUID에서는 다시 확인한다.
해당 run table 생성만 승인 대상이며 기존 catalog/schema/Volume 생성은 불필요하다.
checkpoint 경로는 `/Volumes/ktd/silver/checkpoints/job2/e2e/<run_id>`다.
Volume 존재와 신규 디렉터리 부재는 별개이며 실제 경로 부재/IO는 아직 미검증이다.
Volume 전체 WRITE 권한을 run 경로 권한 격리라고 표현하지 않는다.

### 12.3 실제 Job·Compute·원격 artifact

| 항목 | 현재 실측 / 실행 판단 |
|---|---|
| Job |50294370651632, task job2-validation-silver. 현재 run-as/creator=현재 사용자, Job IS_OWNER |
| 실행 source | Workspace FILE `/Workspace/Users/<CURRENT_USER>/ktd-job2-b8c6ee2/scripts/run_job2_databricks.py` |
| 실제 Parameters | --runtime-config config/job2-runtime.json --check-kafka-metadata. E2E run-job가 아님 |
| Job/Task timeout | 둘 다0. 새 E2E request의600초와 다름. retries는 현재 명시값 미확인 |
| Git source | Jobs git_source 없음, repos list도 빈 목록. 경로 이름 b8c6ee2를 artifact SHA로 확정하지 않음 |
| 배포 파일 읽기 | launcher/runtime JSON을 AUTO export하여 hash 비교. 두 파일은 local과 일치 |
| E2E 도구 | 동일 배포 root의 scripts/phase2/cloud_e2e.py는 명시적 부재 응답. 전체 현재 SHA 배포 안 됨 |
| Compute |0929-113111-cgu7k91c / ktd-phase1-dev, TERMINATED, DBR17.3.x-scala2.13, USER_ISOLATION, UI source |
| 규모/자동 종료 | driver/worker node type r5d.large, num_workers0, SPOT_WITH_FALLBACK/first_on_demand1, autotermination60분 |
| 종료 권한 | 현재 사용자 CAN_MANAGE 확인. 실제 start/stop은 하지 않음 |
| 현재 Writer | visible active Job runs=[], 조회된 classic Compute는 이1개이고 TERMINATED. 해당 Compute의 live Spark query는 없음. 외부/서버리스 writer까지 부재를 보장하지 않음 |

Workspace API namespace는 `/Users/...`, 실행 filesystem은 `/Workspace/Users/...`다.
FILE의 RAW export는 unsupported였으며 **AUTO export**로 실제 bytes/hash를 확인했다.
처음 namespace/format 오류도 증빙에 남겼다. 실패를 artifact 부재로 오인하지 않았다.
원문 source/설정 secret을 출력하지 않고 hash/허용된 설정 참조만 저장했다.
현재 cluster library-status는 설치 상태 근거를 반환하지 않았다. 기존 Job libraries에는
confluent-kafka2.15.1/fastavro1.12.2/MSK signer1.0.2가 있지만 새 request에 자동 상속된다고
가정하지 않는다. 준비된 Compute의 실제 import와 EC2의 kafka-python/fastavro/confluent/
MSK signer/botocore를 승인 범위에서 확인하고 누락 시 설치 전에 중단한다.

### 12.4 안전한 배포 artifact와 runtime 주입

정확한 배포 source SHA는 **3d772bdea261de4275f5888599d661b7f888fb73**로 선정했다.
로컬 `/tmp/ktd-8l-deployment/`에 git archive로 streaming 전체, Avro10필드 schema,
validation rules, E2E 도구와 기존 raw producer, pyproject/lock만 구성했다(총28파일).
legacy script3개/개인정보 README/다른 Phase/실제 credential 파일은 포함하지 않았다.
source hash23개와 config/rules/schema를 격리 package 안에서 기존 load_manifest로 검증했다.
이 package는 아직 원격에 업로드하지 않았다. package-plan JSON의 파일별 hash가 기준이다.

runtime JSON은 strict load가 허용하는 기존 설정과 **SecretReference 이름만** 가져오고
processing_version을 선정 SHA로 생성했다. 새 증빙 runtime-config-preview.json은 비밀값이
없는 주입 예시다. 실제 Secret은 기존 UC credential/Databricks secret scope에서 실행 시
해석한다. ignored 원본 파일을 force-add하거나 access key/token을 배포 파일에 넣지 않는다.
preview의 schema ID1/오래된 reference_time/UUID4는 Cloud 검증되지 않은 계획이고 실행본이 아니다.

향후 배포 승인 후, 새 경로 부재를 먼저 확인하고 정확한 package만 업로드한다.
아래는 **지금 실행하지 않은 승인 대기 명령**이다. CURRENT_USER는 직접 확인한 사용자
namespace로 치환하고, 기존 b8c6ee2 디렉터리를 덮어쓰지 않는다.

```sh
databricks workspace get-status /Users/<CURRENT_USER>/ktd-job2-3d772bd -p ktd
databricks workspace import-dir /tmp/ktd-8l-deployment /Users/<CURRENT_USER>/ktd-job2-3d772bd -p ktd
```

새 root의 source/runtime/rules/schema를 AUTO export/hash로 다시 비교한 뒤에만
readiness의 deployed_artifact를 READY로 바꾼다. 기존 Job을 수정하지 않고 새 one-off
Jobs submit JSON으로 절차를 수행한다. 기존 metadata 진단 Job을 Run now하지 않는다.

### 12.5 IAM과 실제 네트워크 의존성

현재 IAM 정의와 실제 ARN별 simulation을 다시 조회했다. 기존 consumer의 Connect1/
검증 group DescribeGroup·AlterGroup2는 allowed, output2×DescribeTopic/ReadData4는
**implicitDeny**다. UC credential은 이 실제 역할에 연결됨을 확인했으므로 부족 범위가
명확해졌다. 추가 grant는 기존 §11.2/아래 JSON의 output 두 topic 읽기뿐이다.

`docs/evidence/phase-2/p2-8k-cloud-unblock/output-reader-policy-delta.json`

승인 전 적용하지 않는다. 승인 후에도 원래 role 정책/credential은 유지하고, 새 inline
policy 이름이 기존에 없음을 확인해 이번 정책만 추가·회수한다.

```sh
aws iam get-role-policy --role-name ktd-databricks-msk-consumer-role --policy-name ktd-p2-e2e-output-reader-temporary --region ap-northeast-2
# 위의 정확한 NoSuchEntity 확인·기존 정책 보존·사용자 승인 후에만:
aws iam put-role-policy --role-name ktd-databricks-msk-consumer-role --policy-name ktd-p2-e2e-output-reader-temporary --policy-document file://docs/evidence/phase-2/p2-8k-cloud-unblock/output-reader-policy-delta.json --region ap-northeast-2
```

이 역할의 기존 raw/group wildcard 권한은 축소하지 않는다. simulation은 실제 broker
성공이나 SCP/session/trust 전체 증거가 아니다. 기존 Job2 producer에 CreateTopic을
추가하거나 MSK topic/자동 생성 설정을 바꾸지 않는다.

네트워크 관리 실측:

- MSK ACTIVE/SERVERLESS, IAM enabled, bootstrap 기존 runtime과 일치.
- VPC10.94/16(현재 Workspace ID tag)↔KTD10.20/16 peering active·양방향10.20/10.94 route.
  MSK SG의TCP9098이 현재 Databricks worker SG2개와 기존 EC2 client SG를 참조한다.
  이 설정은 TCP/Registry 실제 연결 성공을 입증하지 않는다.
- EIP eipalloc-02c2bbfa08f1f20da → NAT nat-04acfddc496fd144e,
  VPC vpc-097807d9351692806. main route rtb-0d538aeb6844f75ae가0.0.0.0/0→NAT,
  다른 route table에 명시 연결되지 않은 subnet4개가 이 경로에 의존한다.
- EIP eipalloc-0c8326e202cac064d → NAT nat-04a269200987189f1,
  VPC vpc-0d04681c8530be6fe. main route rtb-01a71a3f6f364aa68가 같은 방식으로
  subnet4개에 적용된다. 이 VPC는 다른 Databricks WorkerEnv tag가 있어 불필요 자원으로
  단정하지 않는다. 정확한 workload 소유/계정 binding은 추가 확인 대상이다.
- 두 EIP는 EC2에 직접 연결된 idle IP가 아니라 NAT ENI에 연결되어 있다.
  삭제 시 NAT outbound·외부 Registry/패키지 접근을 끊을 수 있어 제거하지 않았다.

### 12.6 현재 비용과 정상1회 승인 범위

무료 AWS Price List Query API의 **Seoul 공개 OnDemand 단가**를 조회했다.
청구서/credit-adjusted 현금 비용과 구분한다. 가격 조회6회, 자원/IAM 조회24회다.

| 항목 | 관측/단가 | 추정 범위 |
|---|---|---|
| 기존 EC2 |3대 모두stopped, raw client t3.micro=$0.013/h | E2E 때 기존1대30분 사용 시$0.0065; 실행 전 permission/import 확인 필요 |
| DBR VM |r5d.large=$0.173/h, num_workers0/first_on_demand1 |1 VM30분=$0.0865. DBU·추가 storage/IP·traffic/세금 제외 |
| Databricks DBU |기존 UI Compute, 실제 DBU/h/SKU/rate·credits·기지출 미확인 | Jobs task라서 Jobs Compute 할인 단가로 간주하지 않음. $10 현금 목표 판정은 BLOCKED |
| NAT |2개 available, standard NAT $0.059/h/개 및$0.059/GB |2개 유지 시간당$0.118; 신규 E2E 증가액과 구별 |
| IPv4 |기존 EIP2개, 공개$0.005/IP-hour |기존$0.01/h. EC2에 새 ephemeral public IP가 붙으면 그 비용도 별도 |
| EBS |gp3 46GiB, 공개 저장 단가$0.0912/GB-month |storage 기준$4.1952/month, 추가 IOPS/throughput·세금/credit 제외 |
| MSK |cluster$0.92/h, partition$0.0018/h, in$0.123/GB,out$0.061/GB,storage$0.114/GB-month |실제 현재 partition 수/저장/traffic은 broker 미조회로 미확인. 기존9 partitions 가정은 조건부 추정만 가능 |

현재 관측한 cluster1+NAT2+EIP2만 유지하는 gross subtotal은 **$1.048/h**,
720시간 유지 시 **$754.56**다. partition/EBS/traffic/DBR는 빠진 값이며 실제 청구액이
아니다. Compute를 종료해도 이 기존 비용은 남는다. 실험30분의 VM2대 증가 추정
$0.093과 기존 지속 비용을 혼동하거나 중복 합산하지 않는다.
AWS credits가 어느 비용을 언제까지 덮는지, Databricks 계약/credit/현금 누적은 확인하지
못했다. billing SQL/Cost Explorer는 실행하지 않았다. 사용자의 Billing/Credits 확인 후
누적 DBR 현금+승인 DBR액<=약$10, 별도 AWS 승인액을 채워야 한다.

[무료 가격 API 근거](https://aws.amazon.com/blogs/aws-cloud-financial-management/aws-price-list-gets-a-natural-language-upgrade-introducing-the-aws-pricing-mcp-server/),
[AWS Price List](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/price-changes.html),
[IPv4/NAT](https://aws.amazon.com/vpc/pricing/),
[Databricks calculator](https://www.databricks.com/product/pricing/product-pricing/instance-types).
calculator의 검색 snippet을 계약 단가나 DBU/h 근거로 사용하지 않았다.

**승인할 최소 변경/실행은 다음 정상 E2E1회뿐이다. 현재 실행하지 않았다.**

1. 기존 consumer role에 위 output2 읽기 임시 policy 추가·회수.
2. 새 run table1개를 기존 ktd.silver에10필드 Managed Delta로 생성. 현재 owner/run-as
   동일 조건에서 추가 UC GRANT/Volume 생성은 기본안에서 제외한다.
3. 선정 SHA package28파일을 새 Workspace 경로에 배포, runtime 참조 주입·hash 검증.
4. 기존 EC2 i-08e374efb0c31160d와 Compute0929-113111-cgu7k91c만 시작·종료.
   라이브러리 설치/새 자원 생성은 포함하지 않고 import 누락이면 중단한다.
5. Compute/권한/deploy 준비·Writer 독점 시간을 확인한 다음 fresh UUID·시각·실제 schema ID의
   manifest/readiness를 만든다. package source와 선택 SHA를 고정해 docs HEAD 변경과 구분한다.
   CLI prepare를 다른 HEAD에서 실행해 SHA를 섞지 않고 기존 make_manifest API의
   deployment_sha 인자를 사용해 정확한 package에서 생성/검증할 수 있다.
6. §5.2의 기존 snapshot→publish→run-job→verify 명령을 새 경로/동일 manifest로 사용한다.
   실제 raw0/1/2 high 기록→10 ACK 확인→ACK 위치만 handler 전달→canonical4·validated최소5·
   Quarantine5와 bytes/fingerprint/lineage/offset/reason 대사. publish2분 TTL 초과 시 중단한다.
7. job_request의 one-off 기존 Compute request는 timeout600/retry0/query<=180을 확인해
   `databricks jobs submit --json @<reviewed-request.json> -p ktd`로 승인 후에만 제출한다.
   이 API가 TERMINATED Compute를 시작할 수 있으므로 read-only 단계에서는 호출하지 않는다.
8. 최대 총30분 계획에 startup/대기/검증/종료를 포함한다. 현재 auto termination60분을
   비용 상한으로 쓰지 않고 CAN_MANAGE 주체가 수동 종료·TERMINATED 재확인한다.
   Consumer close/query inactive/task terminal/EC2 stopped/이번 policy만 회수·조회/비용 기록까지
   성공·실패 공통으로 수행한다. 임시 policy 제거 실패나 Compute 종료 실패면 미완료다.

추가 승인에서 제외: 장애 주입·checkpoint 재시작·기존 Kafka offset/checkpoint 변경,
기존 table 변경/재사용, topic 변경, NAT/EIP 제거, 설치/신규 유료 자원, 다른 Phase.
새 run table/checkpoint/artifacts는 증빙으로 보존하고 자동 삭제하지 않는다.
최종 Writer quiet-window와 실제 DBU/credit budget이 없으면 READY로 바꾸지 않는다.

### 12.7 로컬 검증과 남은 실행 gate

이번 전체 unit/integration823 passed/11 skipped/1 warning, 실제 local Spark/Delta
fresh-process 복구1 passed45.54s, package source23개/hash 및 manifest 재검증 PASS,
prepare/dry-run은 cloud_access=false다. 기존 Authlib warning이다. code defect는 발견되지
않아 source/test를 수정하지 않았다. 로컬 결과는 Kafka/Registry/UC runtime 증거가 아니다.

해소: 인증/실제 원격 push, UC 존재·owner·기본 권한, credential role 연결/Registry 참조,
Job run-as·종료 권한, Compute 현 상태·네트워크 설정·NAT/EIP 의존성, 공개 AWS단가.
남음: IAM delta 적용 승인, 새 table 생성·artifact 배포 승인, 실제 DBU/credits 예산,
승인된 runtime import/network 확인과 raw snapshot, 최종 독점 시간/정상1회 실행 승인.
전체 E2E는 BLOCKED, 실제 데이터 연결·실행은 UNVERIFIED로 유지하고 여기서 멈춘다.

## 13. Autonomous Mission — 실물 로컬 대체 검증과 Cloud 미완료 gate (2026-10-23)

§12의 과거 read-only 결과에 추가된 실행 기록이다. **로컬 수용 검증은 완료했지만
Databricks/MSK data E2E 승인·성공으로 바꾸지 않는다.** production SHA는
`3d772bdea261de4275f5888599d661b7f888fb73`이고 실제 origin도 동일했다.

### 13.1 재사용 가능한 실물 로컬 검증

새 테스트 `tests/integration/test_job2_local_kafka_recovery.py`는 별도 process의
`tests/integration/job2_local_kafka_worker.py`를 호출한다. 기존 Kafka/Registry와
기존 Job2 process_batch/Delta storage/publisher를 연결하며 launcher·물리 계약을
재작성하지 않았다. 입력은10건, canonical4, validated5, quarantine5다.

```sh
export JAVA_HOME=/tmp/ktd-8h-java/jdk-21.0.13+9/Contents/Home
export KTD_LOCAL_KAFKA_JARS="$(find /tmp/ktd-mission-jars /tmp/ktd-8h-jars -name '*.jar' | paste -sd, -)"
KTD_RUN_LOCAL_KAFKA=1 /tmp/ktd-8h-spark/bin/python -m pytest -q \
  tests/integration/test_job2_local_kafka_recovery.py \
  --basetemp=/tmp/ktd-local-<새로운-고유-run>
```

이는 이 Mac의 기존 환경 경로다. Kafka/Registry9092/8081 준비와 Java/Python/JAR
경로를 먼저 확인한다. 전용 fault container용19092/18081 포트도 비어 있어야 한다.
외부 source/dependency를 추측해 설치하지 않는다. dependency 버전·hash는 증빙을
참조한다. pytest basetemp는 기존 경로를 정리할 수 있으므로 반드시 새 경로를 쓴다.

최종 **8 passed,310.97s**. 실제 broker ACK/Registry decode, corrupt framing/unknown
schema/domain/collision/future 분기, 영속 canonical, 오래된 중복, checkpoint 재개,
fresh replay, Delta MERGE 오류, 발행 전 실패, ACK 불확실성, quarantine 실패,
ACK 후 Driver SIGKILL, 별도 Broker/Registry stop/start를 검증했다.
입력 source를 materialize한 뒤 Broker를 중단해 producer delivery failure를 확인했다.
시간 분류는 fixture 기준의 controlled clock이며 native Spark watermark state 실험은 아니다.
Kafka 중복 발행을 허용하는 복구이며 단일 writer/OSS Delta/local RF1 범위다.

별도 실행한 기존 unit/integration은823 passed/19 skipped/1 Authlib warning,
기존 file-source local Delta는1 passed47.94s, Cloud CLI5단계는 dry-run만 exit0다.
최종 테스트 소스와 initial/interrupted 실험은 구분해 보존했다.

### 13.2 이번 MSK metadata 실행과 원상 복구

사용자의 mission 승인 범위에서 기존 EC2만59.12초(시작요청→stopped 확인) 사용했다.
IAM user chan→기존 EC2 role/client 경로로 실제 두 output metadata/config가 조회됐다.
partitions3/RF3/min ISR2/delete/retention604800000ms/bytes=-1로 일치했다.
설치·topic 변경·데이터 발행·Databricks Compute 실행은0건이다.

임시 `ktd-p2-metadata-20261023T061445Z`는 exact2 output ARN의
DescribeTopic/DescribeTopicDynamicConfiguration만 추가했고 최종 NoSuchEntity다.
EC2 stopped/기존 inline 및 attached 정책 동일/cleanup_errors=[]를 확인했다.
완료 증거와 실제 client 명령은 아래 metadata-and-cleanup.json에 있다.
VM/IPv4 단순 추정은 약$0.0003이며 실제 청구·최소 과금·credits는 미확인이다.
$0.05 계획 예산은 billing hard cap이 아니다. 기존 상시 AWS 자원 비용은 별도다.

실제 raw에 영향을 줄 외부 Writer/Consumer의 완전 격리가 입증되지 않아 fixture0건으로
제한했다. output reader role은 이번 IAM simulation에서도 exact2×2 권한이 implicitDeny다.
이 결과를 broker produce/consume 실패 재현 또는 Cloud data E2E 증거로 쓰지 않는다.

### 13.3 비용 gate와 다음 최소 실행

Databricks auth/UC metadata 조회는 성공했고 Compute는 TERMINATED, active runs0이다.
현금0의 credit 적용과 AWS 부대 과금 통제를 입증할 수 없어 **BLOCKED_COST**다.
현재 Job의 과거 b8c6ee2 artifact를 새 HEAD의 배포물로 간주하지 않는다.

다음은 이번 mission에서 실행하지 않은 잔여 gate다.

1. 실제 credits/rate/service 적용·AWS 비용을 확인해 현금0 가능 여부를 증빙한다.
   현금 위험이 있으면 별도 사용자 지시 전 Compute를 실행하지 않는다.
2. output reader exact2 topic DescribeTopic/ReadData 최소 임시 권한과 회수를 검토한다.
3. selected SHA의 검토된 package 배포, 격리 UC Delta table/checkpoint, 원격 dependency/
   Registry 인증·네트워크와 Writer quiet-window를 확보한다.
4. §12.6의 실제 raw offset snapshot→10건 ACK→Job2→canonical/output 대사→모든
   client/query/Compute 종료·policy 회수·비용 기록을 수행한다.
5. 같은 Cloud runtime의 checkpoint 재시작·부분 실패 복구와 Job1 raw retention/lag/
   Bronze lineage 대사를 마감한다. 로컬 증거만으로 main merge하지 않는다.

### 13.4 증빙과 사용자 commit 경계

- `docs/evidence/phase-2/local-kafka-e2e/20261023T060250Z/`
- `docs/evidence/phase-2/local-recovery/20261023T060250Z/`
- `docs/evidence/phase-2/msk-e2e/20261023T061445Z/`

최종 기존 Kafka running/Registry stopped, init·Postgres stopped/exited,
ecommerce-dw untouched, 이번 fault container6개 stopped. container/volume/topic/schema/
checkpoint/Delta data는 삭제하지 않았다. 기존214개 파일 SHA256은 의도한 문서2개
추가 절을 제외하고 보존한다. 실제 runtime/보류 파일은 staging 대상에서 제외한다.

아래는 **실행하지 않은 사용자용 명령**이다. 먼저 기존 P2-8l 변경을 검토하고,
동일 문서의 새 §13/보고서 mission 절은 `git add -p`로 분리한다.

```sh
git add -- tests/integration/job2_local_kafka_worker.py \
  tests/integration/test_job2_local_kafka_recovery.py \
  docs/evidence/phase-2/local-kafka-e2e/20261023T060250Z \
  docs/evidence/phase-2/local-recovery/20261023T060250Z
# 권장: test(phase2): verify real Kafka Delta recovery and checkpoint replay

git add -- docs/evidence/phase-2/msk-e2e/20261023T061445Z
git add -p -- docs/implementation/p2-8i-cloud-e2e-runbook.md \
  docs/reports/phase-2-verification.md
# 권장: docs(phase2): record MSK metadata cleanup and remaining cloud gates
```

포트폴리오 캡처는 real-kafka-suite.log의8 passed와 normal/result.json의 canonical
fingerprint/lineage, recovery query id/run id, MSK 두 topic/config와 cleanup 상태를
보여주면 된다. Compose credentials·토큰·실제 runtime 값은 공유하지 않는다.
