# ADR-0001 : Local-first Architecture

## Status

Accepted — 초기 결정 기록. 현재 적용 범위는 아래와 구분한다.

## Context

초기 프로젝트 단계에서 바로 AWS 서비스 도입하면 비용, 권한 설정, 배포 복잡도 때문에
스트리밍 처리 로직 검증 및 데이터 신뢰성 검증에 집중하기 어렵다.

## Decision

로컬 Docker Compose 환경에서 Kafka, Spark, Airflow, PostgreSQL, MinIO, Grafana를 우선 구성한다.

AWS IoT Core, Kinesis, S3, DynamoDB, MWAA는 로컬 MVP 검증 후에 동일한 논리 구조 유지하며 단계적으로 전환한다.

## Alternatives

초기부터 AWS 서비스를 도입하는 대안을 검토했다. IAM·네트워크·관리형 서비스 운영을 일찍 다룰 수 있지만,
비용과 환경 준비 부담이 스트리밍 로직·데이터 신뢰성 검증보다 먼저 발생하므로 로컬 우선을 선택했다.

## Consequences

- 장점 : 빠른 반복 개발, 낮은 비용, 재현 가능한 개발 환경
- 단점 : AWS IAM, Network, 관리형 서비스 운영 경험은 후속 단계에서 보강 필요

## 현재 적용 범위

위 Decision은 초기 전체 로컬 MVP 구상이다. 현재 Phase 0 범위는 로컬 Kafka·Registry·Simulator이며,
Phase 1은 MSK Serverless + Databricks + Unity Catalog Managed Iceberg로 구현·검증했다. DynamoDB 현재 상태는 후속 설계다. 전체 로컬 Spark·MinIO 등을 선행 필수로 해석하지 않는다.
IoT Core/Kinesis 역시 현재 필수 수집 경로가 아니다. [현재 플랫폼 설계](../architecture/platform.md)를 따른다.
