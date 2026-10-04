# Phase 1 — Kafka 원본 적재 운영

## P1-5 격리 failure-state 검증 (실행 전)

`tests/integration/test_bronze_failure_state.py`는 **운영 복구 절차가 아니다**.
sink commit 성공 후 checkpoint commit 전 장애의 영속 상태를 재현한다.
실제 프로세스 crash를 주입했다고 표현하지 않는다. production Job 코드는 그대로 사용한다.

Spark의 동기식 micro-batch 순서는 `offsets/N → sink commit → commits/N`이다.
종료한 테스트 query의 `commits/N`만 격리하면 sink 결과와 `offsets/N`은 남고
최신 완료 batch가 N−1이 되어 동일 batch N을 재시도할 조건이 된다.
[Spark 4.0.0 구현](https:b//github.com/apache/spark/blob/v4.0.0/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/MicroBatchExecution.scala)을
기준으로 하며 DBR layout이 다르면 수정·추측하지 않고 파일 목록과 함께 실패한다.

실행 조건:

- DBR notebook driver에서 저장소 checkout과 `pytest`를 사용할 수 있어야 한다.
  로컬 Connect 실행용 테스트가 아니다. `/Volumes` 파일 접근 권한과 Managed Iceberg 생성 권한이 필요하다.
- MSK IAM bootstrap(:9098)과 consumer Service Credential이 필요하다.
- `kitchen.sensor.raw`의 partition 1 offset 0/1/2가 Kafka에 남아 있고 offset 2가 `0000`이어야 한다.
  테스트는 발행하지 않는다. producer를 정지한 검증 시간대에 실행한다.
- 원본은 최대 1,000행의 소규모 fixture로 제한한다. Kafka에서 직접 expected를 읽는다.
  retention으로 필수 offset이 사라지면 명시적으로 실패하며 Bronze로 대체하지 않는다.
- 해당 Spark session은 실행 중인 query가 없어야 한다. 다른 query는 자동 중단하지 않는다.
  이 테스트에 대한 job 자동 재시도·동시 실행도 사용하지 않는다.

다음은 **향후 수동 실행 명령**이다. notebook Python 셀에서 실행한다.
환경값은 세션에 설정하고 저장소에 비밀값을 넣지 않는다.

```python
import os
import sys
import pytest

os.chdir("/Workspace/<실제 저장소 경로>/kitchen_to_digital")
sys.path.insert(0, os.getcwd())
# KTD_KAFKA_BOOTSTRAP_SERVERS와 KTD_KAFKA_SERVICE_CREDENTIAL은 세션에 미리 설정
os.environ["KTD_RUN_BRONZE_FAILURE_STATE"] = "1"
try:
    result = pytest.main([
        "tests/integration/test_bronze_failure_state.py",
        "-q", "-s", "-p", "no:cacheprovider",
    ])
    assert result == 0, result
finally:
    os.environ.pop("KTD_RUN_BRONZE_FAILURE_STATE", None)
```

일반 `KTD_RUN_BRONZE_INTEGRATION=1`만으로는 실행되지 않는다.
전용 opt-in을 켜면 아래 격리 이동까지 수행하므로 단순 read-only 점검으로 실행하지 않는다.

자원은 UUID hex `<run_id>`로 생성하며 기존 자원은 재사용하지 않는다.

- Table: `ktd.bronze.sensor_raw_failure_test_<run_id>`
- Checkpoint: `/Volumes/ktd/bronze/checkpoints/job1-failure-test/<run_id>/sensor_raw`
- Evidence: 같은 `<run_id>` 아래 `evidence/`

정확한 table/checkpoint 쌍과 run_id를 검사한다. canonical table/checkpoint,
다른 경로, symlink는 거부한다. 모든 자원은 성공·실패 후 보존하며 자동 삭제하지 않는다.

실행 순서:

1. Kafka expected 원본 확보 → 별도 자원으로 AvailableNow 실행.
2. lineage별 count와 bytes 대사 → 최신 offsets/commits N과 N−1, 데이터 batch progress 확인.
3. checkpoint 전체를 `evidence/checkpoint-backup/`에 복사하고 모든 파일 bytes 비교.
4. 종료 상태를 재확인한 뒤 **`sensor_raw/commits/N` 한 파일만**
   `evidence/quarantined/commit-N`으로 이동한다. `offsets/N`과 나머지 파일은 그대로 둔다.
5. 동일 query 설정으로 재시작 → query ID 유지/run ID 변경, batch N의 동일 start/end offset과
   입력 처리 progress, `commits/N` 재생성 확인.
6. before/after-injection/after-restart의 모든 lineage count=1, missing=0,
   unexpected=0, bytes/headers/timestamps 보존을 대사한다. offset 2의 `0000`도 검증한다.
7. 신규 입력 없는 재실행으로 결과 불변을 확인한 뒤에만 `result.json`에 PASS를 기록한다.

마지막 batch가 비어 있거나 progress가 없거나 checkpoint sidecar/버전이 예상과 다르면
성공으로 간주하지 않는다. commit을 옮긴 뒤 실패하면 그대로 증빙을 보존한다.
백업을 자동 복원하거나 다른 commit을 제거하지 말고 실패 지점을 검토한다.
원본·lineage·progress·layout JSON은 evidence에 남으므로 공유 전 민감 정보를 확인한다.

정상 재시작은 동일 checkpoint로 진행을 복구한다. checkpoint 유실 복구는 query identity와
기존 sink 기록의 관계가 달라질 수 있어 별도 대사·복구 설계가 필요하다.
새 checkpoint의 earliest/latest를 정상 재시작 대용으로 사용하지 않는다.
Kafka retention 밖의 원본은 checkpoint만으로 복원되지 않는다.
이 테스트의 commit 격리는 전체 checkpoint 유실 복구를 검증하지 않는다.

Job 1은 `streaming/jobs/raw_ingestion.py`다. Kafka key/value는 BINARY,
headers는 `ARRAY<STRUCT<key: STRING, value: BINARY>>`로 보존한다.
배열을 map으로 바꾸지 않아 header 순서와 중복 이름도 유지한다.
Avro decode와 event_id dedup은 하지 않는다. null value도 보존한다.

## EC2 / MSK Kafka client

현재 `ktd-kafka-client`(t3.micro)는 Phase 1 검증용 client다. Session Manager로 접속하며
SSH key pair나 inbound TCP 22는 사용하지 않는다. Role은 `ktd-msk-producer-role`이고
Session Manager용 `AmazonSSMManagedInstanceCore`를 사용한다.
`ktd-client-public-a` (`10.20.10.0/24`)의 `ktd-client-public-rt`에만
`0.0.0.0/0 → ktd-igw`를 연결한다. MSK SG는 `ktd-kafka-client-sg`에서 오는 TCP 9098을 허용한다.
이는 기존 구성의 확인 기준이며 MSK private subnet의 route를 변경하는 절차가 아니다.

1. Session Manager에서 `aws sts get-caller-identity`로 producer Role을 확인한다.
   결과의 Account ID·전체 ARN은 증빙에서 가린다. `curl`로 outbound HTTPS 접근을 확인한다.
2. 설치된 Kafka CLI의 classpath에 AWS MSK IAM authentication client JAR이 포함됐는지 확인한다.
   `client.properties`의 핵심 설정은 다음과 같다.

   ```properties
   security.protocol=SASL_SSL
   sasl.mechanism=AWS_MSK_IAM
   sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
   sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
   ```

3. MSK IAM bootstrap endpoint(:9098)는 세션 환경변수 `MSK_BOOTSTRAP_SERVERS`에만 설정한다.
   기존 `kitchen.sensor.raw`는 생성 완료됐으므로 재생성하지 않는다. 다음은 현재 상태를 조회하는
   운영 명령 예시이며, 이번 문서 작업에서 실행하지 않았다.

   ```sh
   kafka-topics.sh --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
     --command-config client.properties --describe --topic kitchen.sensor.raw
   ```

`TopicAuthorizationException`이면 요청 도달 이후의 IAM action/resource scope를 먼저 확인한다.
연결 timeout과 구분하며 cluster/topic/group resource에 필요한 권한만 적용한다.
검증된 범위는 IAM Admin 요청과 topic 생성까지다. CLI/JAR 버전·설치 경로는 제공되지 않아
신규 EC2 설치 절차는 아직 기록하지 않는다. SensorMetricEvent produce와 MSK → Bronze는 PENDING이다.

## Job 실행 조건 (MSK 연결 준비 미완료)

아래는 기존 Job의 실행 방식이다. 현재 코드에는 MSK IAM/Service Credential 연결 설정이 없어
bootstrap 주소 변경만으로 MSK 실행 준비가 끝나지 않는다. 성공한 cloud E2E 절차로 해석하지 않는다.

- 기존 `ktd-phase1-dev`와 `.venv-databricks`를 사용한다.
- `DATABRICKS_CONFIG_PROFILE=ktd`, `DATABRICKS_CLUSTER_ID`는 로컬 환경에만 설정한다.
- `KTD_KAFKA_BOOTSTRAP_SERVERS`는 **compute에서 접근 가능한** broker 주소다.
  Docker의 `localhost:9092`는 로컬 Producer 전용이며 원격 compute를 가리키지 않는다.
  bootstrap뿐 아니라 broker가 반환하는 advertised listener도 접근 가능해야 한다.
- `ktd.bronze` schema와 checkpoint용 UC managed volume이 필요하다.
- `KTD_BRONZE_CHECKPOINT=/Volumes/ktd/bronze/checkpoints/job1/sensor_raw`처럼
  Job 1 전용 하위 경로를 지정한다. 기본 table은 `ktd.bronze.sensor_raw`다.
- 초기 위치는 `earliest`, `failOnDataLoss=true`다. 보존 기간이 지난 원본은 복원하지 못한다.

```sh
.venv-databricks/bin/python -m streaming.jobs.raw_ingestion --available-now
# 지속 실행: --available-now 생략
```

인증·cluster ID·실제 환경값은 소스나 commit에 넣지 않는다.
Job은 schema/volume/권한을 자동 생성하지 않는다. table은 없을 때만 만들고,
기존 table의 managed Iceberg 형식과 컬럼 타입을 확인한다.
다른 writer를 같은 table/checkpoint에 동시에 실행하지 않는다.

## 재시작과 복구

정상 중단 후에는 **같은 table, topic, checkpoint**로 재실행한다.
Spark가 checkpoint의 offset으로 재개하며 `startingOffsets`는 최초 실행에만 적용된다.
Kafka consumer manual commit은 없다. native streaming sink를 사용하며
checkpoint와 sink 사이의 장애 시 보장은 실제 실패 주입 검증으로 별도 확인해야 한다.

checkpoint가 유실되면 자동 재시작하지 않는다. 원본 checkpoint 복원 가능성,
Kafka retention과 partition별 잔존 offset 범위, Bronze lineage를 먼저 대조한다.
새 checkpoint의 `earliest`는 재적재 중복, `latest`는 누락을 만들 수 있다.
복구 범위와 중복 처리 정책을 결정하기 전 데이터·offset·checkpoint를 삭제하지 않는다.
Topic/cluster를 재생성하면 `(topic, partition, offset)`가 재사용될 수 있으므로
같은 ingestion 이력으로 취급하지 않는다.

## 실제 통합 검증

```sh
KTD_RUN_BRONZE_INTEGRATION=1 .venv-databricks/bin/python -m pytest \
  tests/integration/test_bronze_ingestion.py -v -s
```

추가 설정: `KTD_BRONZE_TEST_CHECKPOINT_ROOT`는 위 volume 안의
`job1-tests` 같은 별도 하위 경로다. `KTD_BRONZE_TEST_SCHEMA` 기본값은 `ktd.bronze`다.
Producer는 기존 `KAFKA_BOOTSTRAP_SERVERS`/`SCHEMA_REGISTRY_URL`을 사용한다.
서로 다른 주소를 사용해도 원격 source와 Producer는 반드시 같은 Kafka cluster를 바라봐야 한다.

매번 독립적인 `sensor_raw_test_<run_id>` table/checkpoint를 만들고 보존한다.
테스트는 Kafka에 fixture를 발행하므로 명시적 opt-in이 필요하다.
실제 성공한 A/B/C/D 출력과 해당 table의 lineage 조회 결과를
`docs/evidence/phase-1/`에 촬영한다. 미실행 테스트를 성공 증빙으로 사용하지 않는다.
