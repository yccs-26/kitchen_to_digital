# Phase 0 검증 기록

## P0-6 유한한 Avro roundtrip 검증

- 환경: 기존 Python 3.11.15 / confluent-kafka 2.15.1 / Kafka 4.1.0 / Registry 8.1.5.
- 결과: **P0-6 통과. Phase 0 전체는 미완료**

### 변경 이유와 구현

기존 빈 `tests/integration/test_avro_roundtrip.py`에 실제 Producer/Registry/Kafka를
사용하는 유한한 검증을 구현했다. `pyproject.toml`에는 integration marker를 등록했다.
일반 pytest는 통합 테스트를 skip하며 `KTD_RUN_INTEGRATION=1`로 명시한 경우만 발행한다.

발행 전 partition별 끝 offset을 읽고 그 위치에 직접 assign한다. UUID 장비 key로
이번 실행 메시지만 골라 기존 JSON·문자열 및 다른 Producer의 데이터를 decode하지 않는다.
별도 Registry client의 AvroDeserializer가 메시지의 writer schema ID로 스키마를 조회한다.
로컬 reader schema나 테스트용 latest version 2는 주입하지 않는다.
원본 dict 전체 일치, event_id, key, schema ID, metric_value의 float 타입을 assert한다.

fixture 값은 `2.5 / -18.75 / 0.0 / 175.125`, source는 `simulator-왕복검증`이다.
event_id와 equipment_id는 실행별 UUID, event_time은 현재 UTC다. 수치값은 고정이므로
random seed가 필요 없다. 테스트용 장비·매장이며 업무 도메인 검증은 이 테스트의 범위가 아니다.

### 실제 실행·결과

| 실행 명령 | 결과 |
|---|---|
| `.venv/bin/python -m pytest -q` | 8 passed, 1 skipped |
| `KTD_RUN_INTEGRATION=1 .venv/bin/python -m pytest -q -s` | 9 passed (통합 1 + 단위 8) |
| `git diff --check` | 통과 |

Authlib httpx deprecation warning 1건은 기존과 동일하다.
`-s` 전체 실행 중 나타난 모의 delivery failure/remaining 출력은 단위 테스트의 기대한
실패 시나리오이며 실제 통합 발행 실패가 아니다.

```text
[ROUNDTRIP START] offsets={0: 221, 1: 323, 2: 304}
[KAFKA FLUSH] delivered=4 failed=0 remaining=0
```

| event_id | raw partition:offset | metric_value |
|---|---|---:|
| 0f93091b-317a-464a-b71e-b6ac24660f8d | 0:221 | 2.5 |
| a2aa9707-7398-4ef5-8259-1bfbbc95f761 | 1:323 | -18.75 |
| 6beddba5-0aa5-413c-85ea-e1a854a8daf1 | 1:324 | 0.0 |
| 616e012e-c4ab-4748-a66c-bf2e24562bbb | 0:222 | 175.125 |

위 순서의 equipment_id/key는 `p0-6-b77b5de034cc47c2ac2ed106c84b4aa4-0`부터 `-3`이다.
4건 모두 schema ID 1, `fields_equal=True value_type=float`으로 복원됐다.
고정 수치와 한글 source를 포함한 10개 필드 모두 원본과 일치했다.

수신 루프는 15초로 제한하며 실패 시 누락 event_id를 보고한다. 네트워크 호출에는 별도
timeout이 있다. auto commit/offset store를 끄고 임시 group/manual assign을 사용했다.
consumer.close()는 finally에서 실행한다. 기존 group offset commit, topic/volume 삭제,
offset reset은 수행하지 않았고 테스트 레코드는 raw에 남겼다.

### 재현·증빙과 남은 범위

실행 방법은 [로컬 개발 안내](../local-development.md)의 P0-6 절에 있다.
통합 테스트만 `-s`로 실행해 `ROUNDTRIP OK` 4줄과 통과 결과를 촬영하면
event_id/transport 위치/필드 복원 증빙이 된다.

기존 JSON `streaming/consumer.py`의 미커밋 수정은 보존했다. 이 통합 테스트가 P0-6의
검증 Consumer 역할이며 상시 ingestion이나 업무 validation으로 확장하지 않았다.
실제 호환/비호환 변경, 잘못된 Avro 타입·필수값 누락, 서비스 장애·재시작은 P0-7에 남아 있다.

## P0-5 Producer Avro 전환

- 결과: **P0-5 통과. Phase 0 전체는 미완료**
- version 2의 nullable `firmware_version`은 사용자 확인에 따라 호환성 테스트용으로 기록한다.
  Producer는 기존 로컬 기본 스키마를 사용하며 Registry의 테스트 버전을 변경하지 않는다.

### 구현

- `simulator/producer.py`: AvroSerializer와 UTF-8 key, topic subject naming을 적용했다.
  자동 등록/latest 선택을 끄고 로컬 스키마를 Registry에서 조회한다.
  key=payload equipment_id를 검사하고 boolean/string metric_value는 발행 전에 거부한다.
  기존 BufferError 1회 재시도를 유지한다. flush는 15초로 제한하고 실패·미전송 시 예외를 낸다.
- `simulator/main.py`, `simulator/models.py`: 정상 경로를 수치형 장비로 제한하고
  오류 주입 기본값을 0으로 변경했다. 기존 장비 정의와 fault injection 함수는 보존했다.
  `--count`는 장비별 발행 횟수이며 생략하면 연속 실행한다.
- `pyproject.toml`, `uv.lock`: 공식 Avro/Registry extras를 추가해 certifi 누락을 해결했다.
- `simulator/test_buffer_full.py`: 패키지 import와 현재 send 인터페이스를 맞췄다.
  이 수동 스크립트가 실제 버퍼 포화를 증명하는 것은 아니다. 재시도 분기는 단위 테스트로 확인했다.
- `tests/unit/test_avro_producer.py`: 실제 AvroSerializer와 모의 Registry/broker로
  schema ID 선택, key, 타입 거부, BufferError 재시도, flush 실패, 유한 main 실행을 검증한다.
- `.env.example`, `docs/local-development.md`: 호스트 설정과 실행 방법을 기록했다.

### 검증 결과

`uv sync` 성공. confluent-kafka 2.15.1과 기존 fastavro를 유지하고 Registry 의존성을 설치했다.
`.venv/bin/python -m pytest -q`: **8 passed**, Authlib의 httpx deprecation warning 1건.
초기 테스트 fixture의 RegisteredSchema guid와 subject strategy 설정을 수정한 후 통과했다.
단위 테스트의 delivery failure는 모의 오류이며 실제 네트워크 장애 주입은 아니다.

실제 Kafka 검증은 임시 Consumer로 발행 전 각 partition 끝 offset을 읽고,
`asyncio.run(main(count=1))`으로 4건 발행한 뒤 해당 offset부터 최대 15초 동안 읽었다.
임시 UUID group, auto commit/offset store 비활성화, 수동 assign을 사용했고 commit하지 않았다.
이번에는 magic byte와 schema ID, key를 확인했으며 payload 전체 역직렬화는 P0-6 범위다.

```text
START_OFFSETS {0: 221, 1: 319, 2: 300}
[KAFKA FLUSH] delivered=4 failed=0 remaining=0
```

| raw partition:offset | key | schema ID |
|---|---|---|
| 1:319 | fridge-001 | 1 |
| 1:320 | freezer-001 | 1 |
| 2:300 | fryer-001 | 1 |
| 2:301 | hood-001 | 1 |

4건 모두 magic byte 0, schema ID 1이었다. Registry 버전 목록은 `[1, 2]`로 유지됐다.
입력은 현재 UTC 시각·UUID·무작위 수치이며 고정 seed는 사용하지 않았다.
토픽/볼륨/offset 삭제나 초기화는 수행하지 않았다.

### 직접 재현·증빙과 남은 범위

`uv run python -m simulator.main --count 1`로 같은 발행 경로를 실행할 수 있다.
매 실행마다 새 4건이 추가되므로 위 offset과 UUID는 반복되지 않는다.
payload 출력과 4개 delivery callback, `delivered=4 failed=0 remaining=0`을 함께 촬영하면
발행 성공을 보여준다. 

기존 JSON Consumer는 아직 Avro 대응 전이다. 전체 필드 roundtrip, 필수값/Avro 타입 오류,
호환·비호환 변경 재현, 서비스 장애·재시작은 남아 있다. 다음 작업은 P0-6이다.

## P0-4 Avro 등록 상태와 BACKWARD 확인

- 환경: Python 3.11.15, 기존 fastavro, Schema Registry 8.1.5.
- 결과: **P0-4 통과. Phase 0 전체는 미완료.** 아래 이전 절은 당시 상태다.

### 실제 결과와 변경 이유

| 검사 | 결과 |
|---|---|
| 로컬 Avro `parse_schema` | 성공 |
| subject 버전 목록 | `[1, 2]` |
| version 1 / schema ID 1 | 로컬 기본 스키마와 JSON 내용 일치 |
| version 2 / schema ID 2 | nullable `firmware_version`, default null 추가 |
| `GET /config/kitchen.sensor.raw-value` | HTTP 200, `{"compatibilityLevel":"BACKWARD"}` |

기존 등록과 subject 설정이 요구를 만족하므로 재등록하거나 설정을 변경하지 않았다.
version 2도 그대로 보존했다. 기본 스키마 파일은 JSON 내용·필드 순서를 보존하고
들여쓰기와 끝 개행만 정리했다. 데이터 계약 문서는 검증된 물리 스키마와 과거 제안을 구분했다.

subject는 버전·호환성을 관리하는 이름, version은 해당 subject 내의 버전,
schema ID는 직렬화 스키마 식별자다. payload의 string `schema_version`은 별도 필드다.
BACKWARD의 의도는 새 reader가 이전 writer의 데이터를 읽는 것이다.
설정 확인과 실제 호환/비호환 변경 실험은 구분한다.

### 재현 명령

저장소 루트에서 실행한다. GET만 사용하므로 Registry 데이터를 변경하지 않는다.
스키마 정리 전후 모두 파싱·비교 검증을 통과했다.

```bash
.venv/bin/python - <<'PY'
import json
import urllib.request
from pathlib import Path
from fastavro import parse_schema

base = 'http://localhost:8081'
subject = 'kitchen.sensor.raw-value'

def get(path):
    with urllib.request.urlopen(base + path, timeout=10) as response:
        return json.load(response)

local = json.loads(Path('schemas/avro/sensor_metric_event.avsc').read_text())
parse_schema(local)
matched = []
for version in get(f'/subjects/{subject}/versions'):
    registered = get(f'/subjects/{subject}/versions/{version}')
    matches = json.loads(registered['schema']) == local
    print(f'version={version} schema_id={registered["id"]} matches_local={matches}')
    if matches:
        matched.append(version)
assert matched, 'Local schema is not registered'
config = get(f'/config/{subject}')
assert config['compatibilityLevel'] == 'BACKWARD', config
print('PASS: local schema registered at', matched, '; subject config:', config)
PY
```

실제 결과:

```text
version=1 schema_id=1 matches_local=True
version=2 schema_id=2 matches_local=False
PASS: local schema registered at [1] ; subject config: {'compatibilityLevel': 'BACKWARD'}
```

### 남은 범위

Producer/Consumer Avro 전환, roundtrip, 잘못된 타입·필수값 실패, 실제 호환/비호환 검사,
발행 실패·재시작 검증은 남아 있다. 기존 pytest의 certifi 누락은 이번에 수정하거나
재실행하지 않았고 HTTP 조회와 Avro 파싱으로 이번 범위를 검증했다.
`git diff --check`는 통과했다. 다음 작업은 P0-5이며 이번에는 시작하지 않았다.

## P0-3 Schema Registry 기동과 Kafka 연결

- 브랜치: `feat/phase0-foundation`
- 환경: 기존 Docker/Compose/Kafka 환경 유지, Registry 이미지 및 기동 로그 버전 8.1.5.

### 동작과 확인 근거

```text
호스트의 HTTP 요청 → localhost:8081 → Schema Registry
                                      ↓ Kafka 내부 listener
                                  kafka:29092 → _schemas
```

호스트는 공개된 8081 포트로 접근한다. Registry는 Compose 네트워크 안에서
`PLAINTEXT://kafka:29092`로 broker에 연결한다. 기동 로그에 이 주소와
`KafkaStore: Reached offset at 8`이 나타났고, HTTP 조회에서 기존 subject가 반환됐다.
이를 통해 단순 컨테이너 실행뿐 아니라 Kafka 저장 내용의 로딩과 HTTP 접근을 확인했다.

`_schemas` 조회 결과는 partition 1, RF 1, `cleanup.policy=compact`,
leader/replicas/ISR 모두 broker 1이다. raw 이벤트 토픽과 별도의 스키마 저장 토픽이다.
로그에는 복수 SLF4J binding 및 Jersey provider 경고가 있었으나 기동과 `/subjects`
조회는 성공했다. 다른 API 전체의 정상 동작까지 검증한 것은 아니다.

### 실제 실행과 결과

모든 명령은 저장소 루트에서 실행했다. 기존 컨테이너를 `start`했으며 재생성하지 않았다.

| 명령 | 결과 |
|---|---|
| `docker compose -f infra/docker/compose.yml config --quiet` | 정리 전후 종료 코드 0 |
| `docker compose -f infra/docker/compose.yml start schema-registry` | 기존 컨테이너 시작 성공 |
| `docker compose -f infra/docker/compose.yml ps -a` | Kafka·Registry Up, PostgreSQL 중지 유지 |
| `curl --fail --silent --show-error --max-time 10 -w '\nHTTP %{http_code}\n' http://localhost:8081/subjects` | HTTP 200, `["kitchen.sensor.raw-value"]` |
| `docker exec ktd-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:29092 --describe --topic _schemas` | partition 1 / RF 1 / compact 확인 |
| `docker exec ktd-kafka /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka:29092 --topic kitchen.sensor.raw` | 끝 offset 221 / 319 / 300, P0-2 결과와 동일 |
| `git diff --check` | 통과. P0-2에서 기록한 Compose 공백 오류도 해소 |

이번 기동의 핵심 로그 재확인 명령:

```bash
docker logs --since 2026-09-28T11:34:00Z ktd-schema-registry 2>&1 \
  | rg 'kafkastore.bootstrap.servers =|Reached offset|Schema Registry version:|Server started|ERROR'
```

실제 주요 결과:

```text
kafkastore.bootstrap.servers = [PLAINTEXT://kafka:29092]
Reached offset at 8
Schema Registry version: 8.1.5
Server started, listening for requests...
```

기존 subject를 발견했지만 이번에는 스키마를 등록하거나 compatibility 설정을
변경하지 않았다. **등록된 스키마와 로컬 Avro의 일치 여부, subject BACKWARD 확인은
P0-4에 남아 있다.** Producer 발행, roundtrip, 장애 복구 검증도 여전히 미완료다.
P0-2의 pytest 수집 오류는 이번 범위에서 수정하거나 재실행하지 않았다.
기존 토픽·볼륨·offset 삭제/초기화 및 Git staging/commit/push는 수행하지 않았다.
Kafka와 Registry는 실행 상태로 두었다.


## P0-2 Kafka 기동과 토픽 초기화

- 브랜치: `feat/phase0-foundation`
- 환경: macOS arm64, Docker 28.3.3, Compose v2.39.2-desktop.1,
  Kafka 이미지 `apache/kafka:4.1.0`, Python 3.11.15.
- 범위: 기존 컨테이너 시작, broker 접속, raw 토픽 설정, 초기화 재실행 검증.
- 결과: P0-2 검증 통과

### 변경 이유와 동작

`infra/docker/kafka/init-topics.sh`는 기존에 Kafka CLI 실패 시 무한히 재시도했다.
`KAFKA_READY_MAX_ATTEMPTS`(기본 30)를 추가해 시도 횟수를 제한했다.
양의 정수가 아니면 즉시 종료 코드 1로 실패한다. 마지막 접속 시도도 실패하면
토픽 생성으로 넘어가지 않고 종료 코드 1을 반환한다. 시도 사이에는 2초를 기다린다.
이는 **시도 횟수 제한**이며 총 실행시간 60초 보장이 아니다. 각 CLI 호출의 대기시간이
추가된다. 실제 네트워크 장애의 wall-clock timeout 검증은 이번에 수행하지 않았다.

```text
설정 검사 → Kafka 토픽 목록 조회 → 성공 → 없을 때만 raw 생성 → 설정 조회
                         └ 실패 → 횟수 남음: 2초 후 재시도
                                └ 소진: exit 1
```

`--if-not-exists`를 유지해 기존 토픽의 설정·데이터를 재설정하지 않는다.
이미 존재하는 토픽의 partition/RF가 다를 때 자동 교정하는 기능은 없다.
개발 설정은 partition 3 / replication factor 1 / min.insync.replicas 1이며
성능 최적값이나 다중 broker 장애 내성의 증거가 아니다.

### 실제 실행과 결과

명령은 저장소 루트에서 실행했다. Compose는 `infra/docker/.env`를 자동 로드했으며
비밀값이 출력될 수 있는 전체 `config` 대신 `config --quiet`를 사용했다.
처음 루트 `.env`를 명시한 명령은 해당 파일이 없어 실패했다.
환경 변수 예시는 기존 `infra/docker/.env.example`을 사용한다.

| 명령/검사 | 결과 |
|---|---|
| `docker compose -f infra/docker/compose.yml config --quiet` | 종료 코드 0 |
| `bash -n infra/docker/kafka/init-topics.sh` | 종료 코드 0 |
| `KAFKA_READY_MAX_ATTEMPTS=0 bash infra/docker/kafka/init-topics.sh` | 기대한 종료 코드 1, positive integer 오류 |
| `KAFKA_READY_MAX_ATTEMPTS=1 bash infra/docker/kafka/init-topics.sh` | 호스트에 `/opt/kafka/bin/kafka-topics.sh`가 없는 조건에서 CLI 실패 경로 검증: 종료 코드 1, 1회 후 중단 |
| `docker compose -f infra/docker/compose.yml start kafka` | 기존 컨테이너 시작, 재생성 없음 |
| `docker compose -f infra/docker/compose.yml start kafka-init` | 수정 스크립트가 기존 bind mount로 실행됨; 종료 코드 0 |
| `docker logs --tail 12 ktd-kafka-init` | Kafka is ready, Kafka topics initialized, partition 3 / RF 1 확인 |
| `docker compose -f infra/docker/compose.yml ps -a` | Kafka Up, kafka-init Exited (0); Registry/PostgreSQL은 중지 상태 유지 |
| `uv run pytest -q` | 수집 실패: 기존 `simulator/test_buffer_full.py`에서 Producer import 중 `ModuleNotFoundError: certifi` |

CLI 실패 테스트의 반환 코드는 Python `subprocess.run`으로 1임을 assert했다.
이는 실제 broker 장애 주입 시험을 대신하지 않는다.

초기화 실행 전후 아래 명령의 결과가 같았다. 최초 기동 직후 일시적인 연결 경고가
출력됐지만 broker 준비 후 조회에 성공했다.

```bash
docker exec ktd-kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server kafka:29092 --topic kitchen.sensor.raw
```

```text
kitchen.sensor.raw:0:221
kitchen.sensor.raw:1:319
kitchen.sensor.raw:2:300
```

위 값은 partition별 end offset이며 보관 레코드 수를 뜻하지 않는다.
초기화 재실행의 offset 보존을 확인했으며 모든 payload의 동일성이나
Kafka/Registry 동시 재시작 복구를 입증한 것은 아니다. 데이터 발행·삭제,
topic/volume 재생성, consumer offset reset을 수행하지 않았다.

호스트 listener도 아래 명령으로 확인했다.

```bash
.venv/bin/python - <<'PY'
from confluent_kafka.admin import AdminClient
metadata = AdminClient({"bootstrap.servers": "localhost:9092"}).list_topics(timeout=10)
topic = metadata.topics["kitchen.sensor.raw"]
assert topic.error is None, topic.error
print(topic.topic, "partitions=", len(topic.partitions))
print({p: {"replicas": v.replicas, "isrs": v.isrs}
       for p, v in topic.partitions.items()})
PY
```

결과: partition 3개, 각 partition의 replicas와 isrs는 `[1]`.
이 검사는 metadata 접속이며 Producer 발행 성공 증거가 아니다.

### Phase 0 완료 조건 대비 현재 상태

| 항목 | 직접 확인한 상태 |
|---|---|
| 브랜치·기존 경로 확인 | 완료. 중복 Compose/Producer 없음 |
| Kafka/Registry·호스트 연결 | Kafka metadata 접속 완료. Registry 중지, 연결 미검증 |
| raw 개발 설정 기록 | 완료: partition 3, RF 1, min ISR 1 |
| Avro 등록·subject BACKWARD | 스키마 파일 존재. Registry 등록·설정 미검증 |
| 발행 callback·수신 | 코드 존재, 이번 실행에서 발행/수신 미검증 |
| key·Avro roundtrip | main의 key는 equipment_id. Producer는 JSON, Consumer도 JSON. 테스트 파일은 0바이트 |
| 호환/비호환 변경 | fixture 파일 2개 존재, 실제 Registry 검사 미실행 |
| Avro 타입/필수값 실패 | 미실행 |
| 실패·종료·재시작 | 초기화 CLI 실패 종료만 확인. Producer 미전송·Registry metadata 복구 미검증 |
| 실행 문서·버전 | 이번 범위 기록 완료. Avro 실행 설정·의존성 정리는 남음 |
| 증빙·commit·PR·merge | 이 보고서에 실행 결과 기록. 스크린샷 미촬영, Git 변경 작업 미수행 |

기존 미커밋 작업: `.gitignore`, `infra/docker/compose.yml`, `simulator/main.py`,
`simulator/producer.py`, `streaming/consumer.py`, `schemas/`, `tests/`를 보존했다.
`docs/plans/` Git 제외 설정도 유지했다.

최종 `git diff --check`는 기존 미커밋 `infra/docker/compose.yml:71`의
trailing whitespace 1건을 보고했다. 이번 작업의 스크립트 변경에는 공백 오류가 없다.
해당 Compose 변경은 이번 commit 대상에서 제외한다.

후속 P0-3은 Registry 기동·Kafka 연결 확인이다. Producer Avro 전환 시에는
미사용 Avro import로 인한 의존성 오류, boolean/categorical 장비와 double 계약의
차이, `unit=None`, 기본 fault injection을 함께 검토해야 한다.
구형 ADR의 전체 로컬 환경 및 과거 계약 제안보다 최신 로컬 계획서의
Kafka/Registry 로컬 범위와 수치 telemetry 계약을 이번 판단에 적용했다.
