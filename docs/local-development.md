# 로컬 Simulator 실행

저장소 루트에서 실행한다. 호스트 Python 환경 변수 예시는 루트 `.env.example`,
Compose의 PostgreSQL 변수 예시는 `infra/docker/.env.example`에 있다.
기본 주소로 실행할 때 루트 `.env` 생성은 필요하지 않다. 기존 `.env`를 덮어쓰지 않는다.

```bash
uv sync --locked
docker compose -f infra/docker/compose.yml ps -a
uv run python -m simulator.main --count 1
```

Kafka와 Registry가 실행 중이고 `kitchen.sensor.raw-value`에 기본 Avro가 등록돼 있어야 한다.
중지된 기존 컨테이너는 `docker compose -f infra/docker/compose.yml start kafka schema-registry`로
시작할 수 있다. 기동 직후에는 Registry `/subjects`의 HTTP 200 응답을 확인하고 발행한다.
새 환경의 스키마 등록 절차는 아직 자동화하지 않았다.

`--count 1`은 **수치형 장비마다 1건**, 현재 총 4건을 발행하고 종료한다.
생략하면 장비마다 1초 간격으로 계속 발행한다. Ctrl+C로 종료 시 flush한다.
실행할 때마다 새 event_id로 실제 raw 레코드가 추가된다.
boolean/categorical 장비 설정은 보존하지만 이 수치 telemetry 경로에서는 발행하지 않는다.
기본 오류 주입 확률은 0이다. 단위 테스트는 `uv run python -m pytest -q`로 실행한다.

## 직렬화와 전송 확인

```text
수치형 장비 → SensorEvent → dict → 로컬 Avro로 직렬화 → Kafka raw
                                      ↑                    ↓
                           Registry에서 schema ID 조회   delivery callback
```

`auto.register.schemas=False`, `use.latest.version=False`, topic subject naming을 명시했다.
등록된 로컬 스키마를 조회하고 테스트용 version 2를 자동 채택하지 않는다.
schema ID 1은 현재 로컬 Registry에서 관측한 값이며 코드에 고정하지 않았다.
공식 API 설명: https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html

콘솔 payload는 사람이 읽기 위한 JSON 출력이며 Kafka value는 Avro bytes다.
`[KAFKA DELIVERED]`는 broker 전달 callback이고 `[KAFKA FLUSH]`는 누적 성공·실패·미전송 개수다.
기본 message timeout은 10초, flush 대기는 최대 15초다. 실패 callback이나 미전송이 있으면
flush에서 예외를 발생시켜 성공처럼 끝내지 않는다. 이는 전체 장애 복구 검증 완료를 뜻하지 않는다.

기존 `streaming/consumer.py`는 JSON 전용이라 Avro 레코드를 읽으면 실패할 수 있다.
Avro 역직렬화 검증 도구는 P0-6에서 구현한다. 기존 토픽·offset을 초기화하지 않는다.
