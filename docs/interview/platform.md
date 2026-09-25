# 실행 환경과 전체 데이터 흐름 — 면접 준비

설계 의도를 설명하는 개인 메모다. 구현·검증이 끝나기 전에는 완료 실적으로 표현하지 않는다.

[설계 기준](../architecture/platform.md)

## AWS + Iceberg + Databricks 역할

“Databricks는 Spark 실행 환경, S3는 데이터 저장, Iceberg는 테이블 관리, Glue는 catalog로 역할을 나누었습니다.”
