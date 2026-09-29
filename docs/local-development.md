# 로컬 Simulator 환경과 실행

Phase 0은 Docker Compose의 Kafka/Schema Registry와 호스트 Python Simulator를 사용한다.
로컬에서 계약과 전송 실패를 먼저 검증해 클라우드 비용·권한·네트워크 설정과 분리한다.
검증 당시 Python 3.11.15, Kafka 4.1.0, Registry 8.1.5, confluent-kafka 2.15.1을 사용했다.
Python 요구사항은 3.11 이상이며 `uv.lock`으로 의존성을 재현한다.

## 필수 설정

| 설정 | 기본값·의미 |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | 호스트 접속 `localhost:9092` |
| `SCHEMA_REGISTRY_URL` | `http://localhost:8081` |
| `KAFKA_TOPIC_SENSOR_RAW` | `kitchen.sensor.raw` |
| Avro subject | `kitchen.sensor.raw-value`, 로컬 기본 스키마 등록 필요 |
| serializer | 자동 등록과 latest 선택을 끄고 로컬 스키마에 대응하는 ID 조회 |

호스트 변수는 루트 `.env.example`, Compose PostgreSQL 변수는 `infra/docker/.env.example`을 참고한다.
기본 주소를 쓰면 루트 `.env`는 필요하지 않다. 기존 환경 파일을 덮어쓰지 않는다.
기본 스키마와 호환성 실험용 v2의 차이는 [데이터 계약](data/data-contract.md)에 있다.

## 최소 실행

저장소 루트에서 실행한다. 중지된 기존 컨테이너를 시작하는 절차이며 새 환경의 스키마 등록은 자동화되지 않았다.

```bash
uv sync --locked
docker compose -f infra/docker/compose.yml start kafka schema-registry
curl --fail --silent --show-error http://localhost:8081/subjects
uv run python -m simulator.main --count 1
```

Registry가 준비된 뒤 발행한다. `--count 1`은 수치형 장비마다 1건, 현재 총 4건을 추가한다.
생략하면 장비마다 1초 간격으로 계속 발행한다. 매 실행은 새 `event_id`를 사용하며 오류 주입 기본값은 0이다.
boolean/categorical 장비 설정은 남아 있지만 이 수치 telemetry 경로에서는 발행하지 않는다.

콘솔은 JSON으로 표시하지만 Kafka value는 Avro bytes다. Ctrl+C 종료 시 flush하며,
message timeout은 10초, flush 대기는 최대 15초다. 실패 callback이나 미전송이 남으면 예외로 끝난다.
실패 이벤트의 영속 outbox·자동 재발행은 구현되지 않았다.
기존 `streaming/consumer.py`는 JSON 전용이므로 Avro 확인에는 아래 통합 테스트를 사용한다.

## 검증 명령

```bash
# 외부 서비스 없이 실행
uv run python -m pytest -q

# P0-6: Avro 발행·역직렬화
KTD_RUN_INTEGRATION=1 uv run python -m pytest -q -s tests/integration/test_avro_roundtrip.py

# P0-7: 호환성 및 잘못된 입력
KTD_RUN_INTEGRATION=1 uv run python -m pytest -q -s tests/integration/test_schema_evolution.py

# P0-7: SIGINT와 서비스 중단·재시작
KTD_RUN_INTEGRATION=1 KTD_RUN_RECOVERY=1 uv run python -m pytest -q -s tests/integration/test_local_recovery.py
```

통합 검사는 실행 중인 Kafka/Registry와 등록 스키마가 필요하며, 호환성 검사는 기존 v1/v2를 사용한다.
기본 pytest는 외부 서비스 검사를 skip한다. 통합 검사를 활성화한 뒤 서비스·설정이 맞지 않으면 실패한다.
roundtrip은 raw에 4건을 추가하며 임시 group/manual assign으로 기존 group offset을 변경하지 않는다.
검사 방법과 실제 결과는 [호환성·장애 검증](schema-evolution-test.md), [Phase 0 보고서](reports/phase-0-verification.md)에 있다.

## 장애 검사 주의사항과 복구

중단 검사는 이 저장소의 Compose, localhost:9092/8081과 raw 토픽으로 제한된다.
다른 Producer를 종료하고 단독 실행한다. 동시 발행은 offset 비교에 영향을 준다.
기존 컨테이너를 stop/start하며 시험 레코드는 보존한다. 토픽·볼륨·offset을 초기화하지 않는다.

검사 실패 시 `docker compose -f infra/docker/compose.yml ps -a`로 서비스 상태를 확인한다.
중지됐다면 위 `start kafka schema-registry` 명령으로 복구하고 Registry 준비 상태를 확인한다.
일반 예외에서는 테스트의 `finally`가 서비스를 시작하지만 SIGKILL에서는 실행되지 않는다.

빈 환경의 전체 구축, 컨테이너 재생성·볼륨 유실 복구는 기존 검증 범위에 포함되지 않는다.
