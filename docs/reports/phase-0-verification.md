# Phase 0 검증 기록

## 2026-09-28 — P0-2 Kafka 기동과 토픽 초기화

- 브랜치: `feat/phase0-foundation`
- 기준 HEAD: `05756c602a7dc481e532b3b70ab9430f0c7f43d1` (미커밋 변경 포함)
- 환경: macOS arm64, Docker 28.3.3, Compose v2.39.2-desktop.1,
  Kafka 이미지 `apache/kafka:4.1.0`, Python 3.11.15.
- 범위: 기존 컨테이너 시작, broker 접속, raw 토픽 설정, 초기화 재실행 검증.
- 결과: P0-2 검증 통과. **Phase 0 전체는 미완료.**

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

### 사용자가 확인할 증빙

`docker compose -f infra/docker/compose.yml ps -a`와
`docker logs --tail 12 ktd-kafka-init` 화면을 촬영하면 Kafka 실행과 초기화 성공,
raw 설정을 보여줄 수 있다. 출력에 비밀값이 없는지 확인한다.
기존 Git 제외 폴더 `screenshots/`에 보관할 수 있다.
Kafka는 검증 후 실행 상태로 두었다.
