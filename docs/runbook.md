# Phase 1 — Kafka 원본 적재 운영

Job 1은 `streaming/jobs/raw_ingestion.py`다. Kafka key/value는 BINARY,
headers는 `ARRAY<STRUCT<key: STRING, value: BINARY>>`로 보존한다.
배열을 map으로 바꾸지 않아 header 순서와 중복 이름도 유지한다.
Avro decode와 event_id dedup은 하지 않는다. null value도 보존한다.

## 실행 조건

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
