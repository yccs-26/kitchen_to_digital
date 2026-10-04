# Phase 1 검증 상태

**부분 구현·검증 완료. 로컬 Kafka → Databricks는 BLOCKED, MSK → Bronze E2E는 PENDING.**
Phase 1 DoD 완료, PR, main 병합, 완료 tag는 아직 진행하지 않았다.
아래 테스트 결과는 기존 실행 기록이며 재실행하지 않았다.

| 검증 | 실제 결과 |
|---|---|
| 기존 Producer | 로컬 Docker Kafka `localhost:9092`, raw topic에 Confluent Avro 발행 |
| 로컬 unit + Phase 0 Avro roundtrip | `KTD_RUN_INTEGRATION=1 uv run python -m pytest tests/unit tests/integration/test_avro_roundtrip.py -q`: 25 passed |
| Connect 환경 unit | `.venv-databricks/bin/python -m pytest tests/unit -q`: 24 passed |
| 기본 전체 수집 | `uv run python -m pytest -q`: 24 passed, opt-in integration 9 skipped |
| Connect | Spark 4.0.0, range, catalog 조회 성공 |
| Bronze table | `ktd.bronze.sensor_raw`, MANAGED / iceberg, 지정한 8개 컬럼 생성 성공 |
| Managed Iceberg roundtrip | 외부 Iceberg JAR·extension 제거 후 CREATE·INSERT·SELECT 성공 |
| MSK 인프라 | Serverless cluster 생성, VPC Peering Active·양방향 route·worker SG의 TCP 9098 허용 구성 완료. Databricks의 실제 연결은 PENDING |
| Service Credential | consumer Role·policy 연결, External ID trust·self-assume 구성 후 Validate 성공. 실제 MSK 인증·소비는 PENDING |
| UC volume | `ktd.bronze.checkpoints` 생성 성공 |
| native streaming sink | 파일 fixture로 bytes/null/headers 보존 및 동일 checkpoint 재실행 통과 |
| 기존 로컬 Kafka source | BLOCKED: `describeTopics` timeout. localhost advertised listener와 로컬 Mac으로의 네트워크 경로 부재 |
| Kafka A/B/C/D | 테스트 코드 작성. 실제 Kafka → Bronze 정상·재발행·손상·재시작 결과는 미검증 |
| sink commit 후 checkpoint 완료 전 실패 | 미검증. 정상 재시작 테스트로 대체 판정하지 않음 |

## Sink 검증의 범위

`test_native_iceberg_sink_restart_with_file_fixtures`가 실제 compute에서 통과했다.
최종 검증 table:
`ktd.bronze.sensor_raw_sink_test_27f12f607c164415ba0a38f10b0d1791`.
Checkpoint:
`/Volumes/ktd/bronze/checkpoints/job1-tests/27f12f607c164415ba0a38f10b0d1791/checkpoint`.

첫 실행 4행 → 동일 checkpoint로 신규 1행 → 입력 없는 재실행 후 5행 유지.
query ID 유지/run ID 변경, 기존 행 불변, lineage별 1행을 확인했다.
이 fixture의 offset은 합성값이며 Kafka 수신 증빙이 아니다.
실패·성공 실행의 테스트 table과 checkpoint는 삭제하지 않고 보존했다.

## 남은 조건

- Phase 0 SensorMetricEvent의 실제 MSK produce·Avro roundtrip·key=equipment_id 검증. 기존 Producer의 MSK/IAM 연결 방식과 cloud Schema Registry 사용 방식은 미확정이며 EC2를 장기 실행 환경으로 확정하지 않는다.
- Job·preflight·통합 테스트의 MSK IAM 연결 설정과 Databricks Structured Streaming의 실제 접속·소비 검증. 기존 코드는 bootstrap 주소 변경만으로 준비가 끝난 상태가 아니다.
- MSK → Bronze E2E 적재와 실제 offset/lineage 대조, duplicate event_id·corrupt payload·동일 checkpoint 재시작 검증. 파일 fixture 결과로 대체하지 않는다.
- 실패 주입을 포함한 Phase 1 DoD 확인 후 PR·main 병합·tag 진행.

계획의 `days(ingested_at)`는 Managed Iceberg 제약으로 미적용이며 초기 table은 무분할이다.

## EC2 → MSK Producer client 검증

아래는 2026-10-02 문서 갱신 시 사용자가 제공한 실제 실행 결과다. 이번 작업에서 AWS에
접속하거나 명령·테스트를 재실행하지 않았으며, 원본 명령 출력과 실행 시각은 별도 제공되지 않았다.

| 검증 | 실제 결과 |
|---|---|
| EC2 접속·identity | `ktd-kafka-client` Session Manager 접속 및 `aws sts get-caller-identity` 성공, `ktd-msk-producer-role` 사용 확인 |
| Outbound internet | `curl` 성공. EC2 → public client subnet → Internet Gateway → Internet 경로 확인 |
| Kafka IAM client | Kafka CLI와 MSK IAM authentication client 구성. SASL_SSL / AWS_MSK_IAM으로 private endpoint(:9098)에 Admin 요청 도달 |
| 최초 topic 생성 | `kitchen.sensor.raw`, 3 partitions 생성 시 `TopicAuthorizationException: Authorization failed.` 발생. 네트워크 연결 실패가 아닌 CreateTopic resource-level authorization 거부 |
| IAM scope 수정 후 재시도 | 해당 KTD MSK cluster의 topic resource 범위를 허용하도록 producer policy를 수정한 뒤 동일 생성 명령 성공 |
| 생성 결과 | `kitchen.sensor.raw`, 3 partitions. Phase 1 기능/E2E 검증 초기값이며 성능 최적값 검증은 아님 |

따라서 EC2 → MSK private endpoint의 네트워크·IAM 인증 및 topic 생성 권한까지 동작했다.
WriteData 정책 구성은 메시지 발행 성공의 증거가 아니며, Databricks의 MSK 접속·consume과
MSK → Bronze E2E는 여전히 PENDING이다. 실제 topic/partition/offset과 Bronze lineage 비교,
duplicate event_id·corrupt payload·real Kafka checkpoint/restart 및 sink 저장 후 checkpoint 완료 전
실패 검증도 미완료다. Phase 1 DoD·PR·main 병합·tag는 완료 처리하지 않는다.

증빙 후보는 최초 CreateTopic authorization 실패와 IAM resource scope 수정 후 생성 성공 화면이다.
현재 저장소에 `docs/evidence/phase-1/`와 해당 이미지가 없어 저장된 증빙으로 표시하지 않는다.
향후 핵심 증빙은 MSK message → Databricks Structured Streaming → Bronze row에서
topic/partition/offset/raw bytes가 대응하는 화면이다. 별도 보관 시 민감 값을 가린다.

## P1-5 failure-state reproduction 준비

상태: **PENDING — 실제 MSK/Databricks failure-state 테스트 실행 전**.
`tests/integration/test_bronze_failure_state.py`와
`scripts/phase1_failure_state.py`에 별도 opt-in 절차를 구현했다.
실행 방법과 제한은 [runbook](../runbook.md#p1-5-격리-failure-state-검증-실행-전)을 따른다.

run_id별 테스트 table/checkpoint에서 native sink 적재 후 최신 데이터 batch의
`commits/N`만 전체 백업·검증 후 격리하고 동일 checkpoint로 재시작한다.
운영 복구나 실제 crash 주입이 아닌 sink commit 후 checkpoint commit 전 **상태 재현**이다.
canonical 자원을 사용하지 않으며 offsets 파일과 query identity를 유지한다.

판정에는 동일 batch/start/end offset의 재시도 progress, commit 재생성,
Kafka expected lineage와 before/after count·missing·duplicate 비교,
partition 1 offset 2의 `0000` 보존이 모두 필요하다. 단순 row count로 PASS하지 않는다.
DBR checkpoint layout 차이와 Kafka retention으로 입력이 없는 경우 실패로 기록한다.
실제 실행 결과·PASS 증빙은 아직 없으며 development plan DoD도 완료 처리하지 않는다.
