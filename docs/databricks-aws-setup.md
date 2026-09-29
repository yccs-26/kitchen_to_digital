# Databricks + AWS Setup

Phase 1은 기존 `ktd-phase1-dev`의 Standard access mode,
DBR 17.3 LTS / Spark 4.0.0 / Scala 2.13과
Python 3.12 / Databricks Connect 17.3.14를 사용한다.

## Storage와 catalog

`ktd` Unity Catalog의 managed storage `s3://ktd-bychan/managed/`를 사용한다.
Bronze는 `CREATE TABLE ... USING ICEBERG`로 생성하는 managed table이다.
외부 LOCATION, GlueCatalog 직접 연결, 외부 Iceberg JAR,
custom `spark.sql.catalog.*` 및 Iceberg extension 설정은 사용하지 않는다.

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
네트워크 경로 변경이 필요한 경우 IAM 확대·공개 broker 노출·새 cloud 자원 생성으로
자동 우회하지 않는다. 실행 및 복구는 [runbook](runbook.md)을 따른다.
