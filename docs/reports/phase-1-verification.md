# Phase 1 검증 상태 — 2026-09-30

**부분 구현·검증 완료. Kafka → Bronze E2E는 네트워크 blocker로 미검증.**
Phase 1 완료 판정, main 병합, 완료 tag는 하지 않았다.

| 검증 | 실제 결과 |
|---|---|
| 기존 Producer | 로컬 Docker Kafka `localhost:9092`, raw topic에 Confluent Avro 발행 |
| 로컬 unit + Phase 0 Avro roundtrip | `KTD_RUN_INTEGRATION=1 uv run python -m pytest tests/unit tests/integration/test_avro_roundtrip.py -q`: 25 passed |
| Connect 환경 unit | `.venv-databricks/bin/python -m pytest tests/unit -q`: 24 passed |
| 기본 전체 수집 | `uv run python -m pytest -q`: 24 passed, opt-in integration 9 skipped |
| Connect | Spark 4.0.0, range, catalog 조회 성공 |
| Bronze table | `ktd.bronze.sensor_raw`, MANAGED / iceberg, 지정한 8개 컬럼 생성 성공 |
| UC volume | `ktd.bronze.checkpoints` 생성 성공 |
| native streaming sink | 파일 fixture로 bytes/null/headers 보존 및 동일 checkpoint 재실행 통과 |
| 원격 Kafka source | `describeTopics` timeout. 현재 localhost advertised listener에 원격 접근 불가 |
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

Databricks compute와 로컬 Producer가 **같은 Kafka cluster**에 접근하도록
승인된 네트워크 경로와 advertised listener가 필요하다.
새 인프라, 공개 broker 노출, IAM/보안 정책 변경은 자동 수행하지 않았다.
연결 확보 후 `scripts/phase1_preflight.py`와 [runbook](../runbook.md)의 E2E 테스트를 실행한다.
계획의 `days(ingested_at)`는 Managed Iceberg 제약으로 미적용이며 초기 table은 무분할이다.
