# Avro 호환성과 로컬 장애 검증

P0-7 실행 결과. version 2는 호환성 실험용이며 Producer 기본 계약은 version 1이다.

## 스키마 호환성

`tests/integration/test_schema_evolution.py`는 기존 등록 버전을 조회한 뒤
Registry의 compatibility 검사 API를 호출한다. 새 버전을 등록하거나 기존 버전을 삭제하지 않는다.

| 후보 | 비교 writer | BACKWARD 결과 | 이유 |
|---|---|---|---|
| v2: nullable firmware_version, default null 추가 | 등록 v1 | True | 새 reader가 이전 레코드의 없는 필드를 default로 채울 수 있음 |
| v3: 필수 manufacturer, default 없음 추가 | 등록 v2 | False | 이전 레코드에 새 필수 필드가 없고 기본값도 없음 |

nullable이라는 타입만으로 충분하다고 일반화하지 않는다. 이 fixture에는 **default null**도 있다.
BACKWARD는 새 reader가 이전 writer의 데이터를 읽는 방향이며 반대 방향이나 모든 과거 버전의
호환성까지 증명한 것은 아니다. 등록된 버전 목록은 `[1, 2]`로 유지됐다.

필수 `source` 누락은 ValueError, string `unit`에 정수 입력은 TypeError로 실패했다.
string `metric_value`는 Producer의 수치형 입력 검사에서 ValueError로 거부한다.
Registry의 스키마 호환성, Avro 직렬화 타입 검사, 장비/단위/값 범위의 업무 검증은 서로 다르다.
`event_time`이 Avro string이라는 사실만으로 올바른 timestamp임을 보장하지 않는다.

## 종료·실패·재시작

`tests/integration/test_local_recovery.py`는 로컬 Compose 전용이며 두 환경 변수를
모두 켰을 때만 실행된다. 다른 simulator/Producer를 종료한 상태에서 순차 실행한다.
동시 발행이 있으면 offset 비교가 실패할 수 있다. 테스트 자체가 외부 프로세스를 종료하지는 않는다.

```text
SIGINT → finally의 flush → 성공 callback과 미전송 여부 확인

표본 발행·원본 bytes/offset/schema 저장
  → 기존 Kafka/Registry stop → broker 실패·Registry 접속 실패 확인
  → finally에서 start → 준비 대기
  → 장애 중 메시지 최종 결과 집계
  → 기존 레코드와 metadata 보존 확인 → 새 Producer 발행·decode
```

최종 실행에서 SIGINT 시 4건 전달, 실패 0, remaining 0이었다. subprocess 종료 코드 -2는
SIGINT에 따른 종료다. broker 중단 시 메시지 1건은 timeout 실패로 확정됐고, 새 Registry client는
직렬화 단계에서 연결 오류를 내며 `produce()`를 호출하지 않았다. 기존 직렬화기는 schema를
캐시할 수 있으므로 broker 검사는 warm serializer, Registry 검사는 cold serializer로 나눴다.

최초 테스트는 flush timeout을 반드시 실패 callback으로 끝난다고 가정해 실패했다.
그 가정을 제거하고 미확정 callback도 추적하도록 수정했다. 최종 성공 실행에서는
`failed=1`, `delivered_after_restart=0`, `unresolved_callbacks=0`이었다.
큐에 남은 레코드가 복구 후 성공하는 분기는 코드에서 처리하지만 이번 최종 실행에서는 관측되지 않았다.

또한 `len(Producer)`에는 Kafka 프로토콜 요청도 포함되므로 순수 이벤트 개수로 해석하지 않는다.
Registry 실패는 `produce()` 호출을 감시하고, 장애 이벤트는 delivery callback으로 최종 결과를
집계하도록 고쳤다. [공식 Producer API](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#producer)

## 검증의 한계

- stop/start 전후 동일 컨테이너와 표본 1건의 bytes·위치, raw low/high offset, 등록 schema·ID·BACKWARD를 비교했다.
- 실패 callback은 기록되지만 디스크 outbox나 실패 이벤트 자동 재발행은 구현돼 있지 않다.
- 컨테이너 삭제/재생성, 볼륨 유실, 강제 전원 종료, 전체 데이터 checksum 검증은 수행하지 않았다.
- broker/Registry를 정상적으로 재시작한 로컬 결과이며 end-to-end exactly-once 증거가 아니다.
- 테스트는 Kafka raw에 정상 fixture를 추가하며 topic/volume/offset을 삭제·초기화하지 않는다.
- `finally`는 일반적인 예외 경로에서 서비스를 복구하지만 테스트 프로세스가 SIGKILL되면 실행되지 않는다.

실행 명령은 [로컬 개발 안내](local-development.md), 실제 결과와 스크린샷 경로는
[Phase 0 보고서](reports/phase-0-verification.md)에 기록했다.
