# ADR-0001 : Local-first Architecture

## Status
Accepted

## Context
초기 프로젝트 단계에서 바로 AWS 서비스 도입하면 비용, 권한 설정, 배포 복잡도 때문에
스트리밍 처리 로직 검증 및 데이터 신뢰성 검증에 집중하기 어렵다.

## Decision
로컬 Docker Compose 환경에서 Kafka, Spark, Airflow, PostgreSQL, MinIO, Grafana를 우선 구성한다.

AWS IoT Core, Kinesis, S3, DynamoDB, MWAA는 로컬 MVP 검증 후에 동일한 논리 구조 유지하며 단계적으로 전환한다.

## Consequences
- 장점 : 빠른 반복 개발, 낮은 비용, 재현 가능한 개발 환경
- 단점 : AWS IAM, Network, 관리형 서비스 운영 경험은 후속 단계에서 보강 필요

