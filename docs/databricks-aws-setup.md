# Databricks + AWS Setup

## Runtime 선택

- Databricks Runtime: 17.3 LTS
- Spark: 4.0.0
- Scala: 2.13

LTS Runtime을 사용해 프로젝트 기간 동안 실행 환경을 고정하고,
Spark 4.0 기반 Structured Streaming 및 Iceberg 연동을 검증하기 위해 선택했다.

## Iceberg 의존성

사용 예정:

`org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:<version>`

Spark 4.0 / Scala 2.13 조합과 맞는 runtime artifact를 사용한다.
Iceberg 버전은 Databricks 환경에서 실제 preflight 테스트 후 고정한다.

## Spark 설정

```text
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions