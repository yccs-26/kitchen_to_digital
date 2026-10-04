# Databricks + AWS Setup

Phase 1은 기존 `ktd-phase1-dev`의 Standard access mode,
DBR 17.3 LTS / Spark 4.0.0 / Scala 2.13과
Python 3.12 / Databricks Connect 17.3.14를 사용한다.

## Storage와 catalog

`ktd` Unity Catalog의 managed storage `s3://ktd-bychan/managed/`를 사용한다.
Bronze는 `CREATE TABLE ... USING ICEBERG`로 생성하는 managed table이다.
외부 LOCATION, GlueCatalog 직접 연결, 외부 Iceberg JAR,
custom `spark.sql.catalog.*` 및 Iceberg extension 설정은 사용하지 않는다.
외부 JAR·extension 제거 후에도 Managed Iceberg CREATE·INSERT·SELECT roundtrip이
성공했다. Bronze table은 `ktd.bronze.sensor_raw`다.

초기 `days(ingested_at)` 계획은 적용하지 않는다.
[Managed Iceberg 공식 제약](https://docs.databricks.com/aws/en/iceberg/)상
expression partition transform은 지원되지 않으므로 초기 table은 partition 없이 생성한다.
소규모 기능 검증에서는 단순성을 택하고 날짜별 pruning 최적화는 보류한다.
별도 날짜 컬럼이나 path partitioning으로 이를 흉내 내지 않는다.

Checkpoint는 Job 1 전용 UC volume 하위 경로에 둔다.
[공식 checkpoint 지침](https://docs.databricks.com/aws/en/structured-streaming/checkpoints)에
따라 query별 경로를 분리한다. Public DBFS root는 사용하거나 활성화하지 않는다.

## 로컬 연결

기존 `.venv-databricks`에서 인증 profile과 cluster ID를 환경변수로 전달한다.
테스트 의존성은 다음처럼 기존 Connect 환경에 추가할 수 있다.

```sh
uv pip install --python .venv-databricks/bin/python \
  pytest 'confluent-kafka[avro,schemaregistry]>=2.15.1' \
  'python-dotenv>=1.2.3' 'fastavro>=1.12.2'
```

Connect 연결 성공은 원격 Kafka 접근이나 streaming sink 성공을 의미하지 않는다.
`localhost:9092` advertised listener는 원격 compute에서 로컬 Docker로 연결되지 않는다.
Cloud 검증 경로는 MSK Serverless로 전환했다. IAM 연결과 Databricks MSK batch read,
Job 1의 MSK → Bronze 적재를 확인했다. [실행 결과](reports/phase-1-verification.md)에
fixture lineage와 정상 재시작·failure-state recovery 결과를 기록했다.
실행 및 복구는 [runbook](runbook.md)을 따른다.

## MSK 접근 구성

`ktd-msk-serverless`는 KTD VPC의 두 private subnet에
생성됐다. Databricks VPC와의 peering·양방향 route 및 worker security groups에서
MSK로의 TCP 9098 허용을 구성했다. 전체 인터넷에 broker를 공개하지 않고 private 접근을 사용한다.

UC Service Credential `ktd-msk-consumer`와 AWS IAM Role
`ktd-databricks-msk-consumer-role`을 연결했다. Databricks가 발급한 External ID를
trust policy에 반영하고 self-assume을 구성한 뒤 Service Credential Validate가 성공했다.

Role에 연결한 consumer policy는 `kitchen.sensor.raw` 읽기에 필요한
`kafka-cluster:Connect`, `kafka-cluster:DescribeTopic`, `kafka-cluster:ReadData`,
`kafka-cluster:DescribeGroup`, `kafka-cluster:AlterGroup` 권한을 부여한다.
Producer와 권한을 분리하기 위해 `CreateTopic`과 `WriteData`는 부여하지 않는다.
Job 1은 `KTD_KAFKA_SERVICE_CREDENTIAL`을 Kafka source의
`databricks.serviceCredential`로 전달한다. Service Credential은 Validate뿐 아니라
실제 consumer 연결에 사용됐고, MSK batch read와 Structured Streaming 적재가 성공했다.
MSK IAM bootstrap(:9098)은 `KTD_KAFKA_BOOTSTRAP_SERVERS`로 전달한다.

이 연결 검증으로 cloud Schema Registry 운영이나 장기 Producer 환경까지 확정한 것은 아니다.
EC2 client는 Phase 1 fixture 검증에 사용했으며 장기 실행·배포 방식은 별도로 결정한다.
