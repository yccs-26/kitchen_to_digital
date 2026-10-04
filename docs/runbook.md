# Phase 1 — Kafka 원본 적재 운영

## P1-5 격리 failure-state 검증

`tests/integration/test_bronze_failure_state.py`는 **운영 복구 절차가 아니다**.
sink commit 성공 후 checkpoint commit 전 장애의 영속 상태를 재현한다.
실제 프로세스 crash를 주입했다고 표현하지 않는다. production Job 코드는 그대로 사용한다.

최종 clean run `732e5477deb54c4aab4f4e840b184178`은 이 절차로 PASS했다.
실행 결과와 검증 범위는 [Phase 1 검증 기록](reports/phase-1-verification.md)에 남겼다.
종료한 테스트 query의 최신 `commits/N`을 백업 후 이동하고, sink 결과와 `offsets/N`,
query metadata를 유지한 채 재시작한다. DBR checkpoint layout이 harness의 예상과 다르면
임의로 고치지 않고 파일 목록과 함께 실패한다.

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

Databricks Git Folder에서 branch를 pull했거나 Python 코드를 바꿨다면 먼저 별도 셀에서
다음을 실행한다. 이전에 import된 코드와 test module의 opt-in 상태를 재사용하지 않기 위해서다.

```python
%restart_python
```

restart 후에는 cwd와 환경변수를 다시 설정한다. 아래 코드는 notebook Python 셀에서 실행한다.
저장소 경로와 MSK 환경값은 실제 세션 값으로 바꾸고 비밀값을 저장소에 넣지 않는다.
`KTD_RUN_BRONZE_FAILURE_STATE=1`은 test module collection 전에 설정해야 한다.

```python
import os
import sys
from pathlib import Path

import pytest

repo_root = Path("/Workspace/<실제 Git Folder 경로>/kitchen_to_digital")
os.chdir(repo_root)
if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

os.environ["KTD_KAFKA_BOOTSTRAP_SERVERS"] = "<MSK IAM bootstrap endpoints:9098>"
os.environ["KTD_KAFKA_SERVICE_CREDENTIAL"] = "<consumer Service Credential 이름>"
os.environ["KTD_RUN_BRONZE_FAILURE_STATE"] = "1"
try:
    test_file = Path("tests/integration/test_bronze_failure_state.py")
    assert test_file.is_file(), f"Test file not found: {test_file.resolve()}"
    result = pytest.main([
        str(test_file),
        "-q", "-s", "--assert=plain", "-p", "no:cacheprovider",
    ])
    print(f"pytest result: {result}")
    assert result == 0, result
finally:
    os.environ.pop("KTD_RUN_BRONZE_FAILURE_STATE", None)
```

Workspace filesystem에서 pytest assertion rewriting 문제를 피하려고 `--assert=plain`을
사용한다. pytest cache를 쓰지 않도록 `-p no:cacheprovider`도 유지한다.

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
   progress, `commits/N` 재생성 확인.
6. before/after-injection/after-restart의 모든 lineage count=1, missing=0,
   unexpected=0, bytes/headers/timestamps 보존을 대사한다. offset 2의 `0000`도 검증한다.
7. 신규 입력 없는 재실행으로 결과 불변을 확인한 뒤에만 `result.json`에 PASS를 기록한다.

`numInputRows`는 관찰 evidence다. 실제 실행에서는 동일 batch/offset 경계로 복구되고
commit이 재생성됐어도 0이 나왔다. 양수일 필요는 없으며, 필드가 존재하고 0 이상의 정수인지
검사한다. 누락·음수·잘못된 타입은 실패한다. 복구 성공은 동일 batch/offset boundary,
commit 재생성, 최종 lineage의 missing/duplicate/unexpected=0과 원본 보존으로 판단한다.
이 관찰만으로 Managed Iceberg 내부의 replay 처리 방식을 단정하지 않는다.

격리 대상으로 고르는 최초 실행의 마지막 batch가 비어 있거나, 필요한 progress가 없거나,
checkpoint sidecar/버전이 예상과 다르면
성공으로 간주하지 않는다. commit을 옮긴 뒤 실패하면 그대로 증빙을 보존한다.
백업을 자동 복원하거나 다른 commit을 제거하지 말고 실패 지점을 검토한다.
원본·lineage·progress·layout JSON은 evidence에 남으므로 공유 전 민감 정보를 확인한다.

전체 checkpoint 유실은 이 실험의 검증 범위가 아니다. [재시작과 복구](#재시작과-복구)를 따른다.

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
이 CLI 절차에서는 IAM Admin 요청과 topic 생성을 확인했다. 이후 별도 Avro fixture 발행과
Databricks MSK batch read, Job 1의 Bronze 적재도 성공했다. CLI/JAR 버전·설치 경로는
제공되지 않아 신규 EC2 설치 절차는 아직 기록하지 않는다.

## Job 실행 조건

Job 1은 `KTD_KAFKA_SERVICE_CREDENTIAL` 값을 Kafka source의
`databricks.serviceCredential`로 전달한다. MSK IAM 연결과 Bronze 적재는 실제 검증됐으며,
아래 CLI는 로컬 Databricks Connect 진입점이다. 위 failure-state 테스트는 별도로
Databricks notebook driver에서 실행한다.

- 기존 `ktd-phase1-dev`와 `.venv-databricks`를 사용한다.
- `DATABRICKS_CONFIG_PROFILE=ktd`, `DATABRICKS_CLUSTER_ID`는 로컬 환경에만 설정한다.
- `KTD_KAFKA_BOOTSTRAP_SERVERS`는 **compute에서 접근 가능한** broker 주소다.
  Docker의 `localhost:9092`는 로컬 Producer 전용이며 원격 compute를 가리키지 않는다.
  bootstrap뿐 아니라 broker가 반환하는 advertised listener도 접근 가능해야 한다.
- MSK 실행에는 `KTD_KAFKA_SERVICE_CREDENTIAL`에 consumer Service Credential 이름을 설정한다.
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
Spark가 checkpoint의 offset/query state를 이어가며 `startingOffsets`는 최초 실행에만 적용된다.
Kafka consumer manual commit은 없다. native streaming sink를 사용하며
이번 fixture에서는 정상 재시작과 별도 failure-state reproduction에서 lineage 중복·누락이
없었다. 이 결과를 모든 장애 상황의 exactly-once 보장으로 확장하지 않는다.

checkpoint가 유실되면 자동 재시작하지 않는다. 원본 checkpoint 복원 가능성,
Kafka retention과 partition별 잔존 offset 범위, Bronze lineage를 먼저 대조한다.
새 checkpoint의 `earliest`는 재적재 중복, `latest`는 누락을 만들 수 있다.
Kafka retention 밖으로 사라진 원본은 checkpoint만으로 복구할 수 없다.
복구 범위와 중복 처리 정책을 결정하기 전 데이터·offset·checkpoint를 삭제하지 않는다.
Topic/cluster를 재생성하면 `(topic, partition, offset)`가 재사용될 수 있으므로
같은 ingestion 이력으로 취급하지 않는다.

## 별도 Kafka/Connect 통합 테스트

아래는 기존 Producer/Registry와 Connect를 사용하는 별도 harness다. 위 MSK failure-state
clean run의 실행 명령과 다르며, 해당 테스트가 PASS했다고 대신 기록하지 않는다.

```sh
KTD_RUN_BRONZE_INTEGRATION=1 .venv-databricks/bin/python -m pytest \
  tests/integration/test_bronze_ingestion.py -v -s --assert=plain -p no:cacheprovider
```

추가 설정: `KTD_BRONZE_TEST_CHECKPOINT_ROOT`는 위 volume 안의
`job1-tests` 같은 별도 하위 경로다. `KTD_BRONZE_TEST_SCHEMA` 기본값은 `ktd.bronze`다.
Producer는 기존 `KAFKA_BOOTSTRAP_SERVERS`/`SCHEMA_REGISTRY_URL`을 사용한다.
서로 다른 주소를 사용해도 원격 source와 Producer는 반드시 같은 Kafka cluster를 바라봐야 한다.

매번 독립적인 `sensor_raw_test_<run_id>` table/checkpoint를 만들고 보존한다.
테스트는 Kafka에 fixture를 발행하므로 명시적 opt-in이 필요하다.
실제 성공한 A/B/C/D 출력과 해당 table의 lineage 조회 결과를
`docs/evidence/phase-1/`에 촬영한다. 미실행 테스트를 성공 증빙으로 사용하지 않는다.
