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
Avro 확인에는 아래 유한한 통합 테스트를 사용한다. 기존 토픽·offset을 초기화하지 않는다.

## Avro roundtrip 검증 — P0-6

Kafka/Registry와 기본 스키마가 준비된 상태에서 저장소 루트에서 실행한다.

```bash
KTD_RUN_INTEGRATION=1 uv run python -m pytest -q -s tests/integration/test_avro_roundtrip.py
```

실제 `kitchen.sensor.raw`에 4건이 추가된다. 기본 pytest 실행에서는 skip하며
`KTD_RUN_INTEGRATION=1`을 명시했을 때만 실행한다. 활성화 후 서비스가 없거나
설정이 맞지 않으면 skip하지 않고 실패한다. raw 이외의 토픽 설정도 실패한다.

```text
partition별 끝 offset 저장 → 고유 key로 4건 발행 → 저장한 offset부터 읽기
  → 이번 실행 key만 선택 → Registry writer schema로 decode → 원본 전체 dict 비교
```

고정 값 `2.5`, `-18.75`, `0.0`, `175.125`와 한글 source를 사용한다.
UUID event_id와 `p0-6-<run UUID>-<번호>` equipment_id로 실행을 구분한다.
이는 직렬화 검증용 fixture이며 실장비 ID·온도 범위의 업무 유효성 검사는 아니다.
각 메시지의 schema ID가 로컬 스키마의 등록 ID와 일치하는지 확인하고,
event_id를 포함한 10개 필드, key=equipment_id, metric_value의 float 타입을 비교한다.
테스트용 Registry version 2를 latest reader로 자동 선택하지 않는다.

수신 루프는 최대 15초이고 누락 event_id가 있으면 실패한다. metadata/Registry 요청과
Producer flush에는 별도 timeout이 있으므로 테스트 전체가 15초라는 뜻은 아니다.
임시 group과 manual assign을 쓰며 auto commit/offset store를 끄고, 종료 시 Consumer를 닫는다.
기존 group offset을 변경하지 않는다. 검증 레코드는 raw에 보존한다.
`ROUNDTRIP OK` 4줄과 pytest 통과 결과를 함께 촬영하면 복원 증빙으로 사용할 수 있다.
