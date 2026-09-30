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
| MSK 인프라 | Serverless cluster 생성, VPC Peering Active·양방향 route·worker SG의 TCP 9098 허용 구성 완료. 실제 연결은 PENDING |
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

- MSK의 `kitchen.sensor.raw` topic 생성, Producer IAM Role과 KTD VPC 내부 실행 환경 구성 및 실제 SensorEvent 발행. EC2 Kafka client는 검토 중이며 아직 생성하지 않았다.
- Job·preflight·통합 테스트의 MSK IAM 연결 설정과 Databricks Structured Streaming의 실제 접속·소비 검증. 기존 코드는 bootstrap 주소 변경만으로 준비가 끝난 상태가 아니다.
- MSK → Bronze E2E 적재와 실제 offset/lineage 대조, duplicate event_id·corrupt payload·동일 checkpoint 재시작 검증. 파일 fixture 결과로 대체하지 않는다.
- 실패 주입을 포함한 Phase 1 DoD 확인 후 PR·main 병합·tag 진행.

계획의 `days(ingested_at)`는 Managed Iceberg 제약으로 미적용이며 초기 table은 무분할이다.
