# P2-7 Job 2 연결 계약

이번 구현은 오프라인 컴포넌트 연결과 Spark API 호출 구조까지다.
Kafka, Registry, Databricks, UC Managed Delta insert-only MERGE, checkpoint 재시작은 실행하지 않았다.

## 처리와 완료 기준

`streaming/jobs/sensor_validation_job.py`는 Job 1의 Kafka source를 재사용하여
raw를 독립 소비한다. Bronze 적재 완료를 전제하지 않는다.

1. Kafka topic/partition/offset/key/value를 받고 BINARY의 bytearray를 bytes로 복사한다.
2. 기존 Avro decoder에서 해석·UTC 정규화를 수행한다.
3. 기존 domain validator의 오류 전체를 단일 QuarantineRecord에 넣는다.
4. 정상 이벤트는 기존 event-time classifier와 `deliver_validated_event`를 호출한다.
5. INSERTED와 DUPLICATE_NOOP는 validated ACK 뒤에만 완료한다.

corrupt Avro는 event_id 없이도 lineage와 raw value SHA-256을 보존한다.
null value는 해시도 null이며 빈 bytes와 구분한다. 해시만으로 원본을 복원할 수
없으므로 Kafka retention과 Job 1 Bronze lineage 대사는 runtime 검증 사항이다.

`SensorDecodeError`는 ValueError 하위 타입이다. 프레임 오류, 알 수 없는 schema ID,
해석 불가능한 Avro, 시각 정규화 오류만 Job의 Quarantine 경계가 잡는다.
Registry 인증/가용성/연결 실패, Registry 응답 JSON/스키마 오류, 예상 밖 decoder
시스템 오류는 전파한다. 기존 정상 decode 및 UTC 로직을 재사용한다.

`RecordResult.completed=True`는 validated 또는 Quarantine ACK 완료만 뜻한다.
CONFLICT는 기존 canonical을 유지하고 validated를 발행하지 않는다. 기존
Quarantine에 `EVENT_ID_CONFLICT` 오류를 differing_fields 순서대로 하나씩 담아
원본 레코드당 한 번 발행한다. `errors[].field`가 differing_fields를 보존하며
lineage, event_id, schema_id, raw key, raw value 해시, processing_version도 유지한다.
반환 결과는 QUARANTINED이고 SilverWriteResult의 CONFLICT와 differing_fields도
보존한다. Quarantine ACK 확인 후에만 completed=True다. 격리 실패는 예외로
전파하여 배치를 실패시킨다. 격리는 충돌 해결이나 canonical 수정이 아니다.

forward plan P2-3의 Quarantine ACK와 P2-4의 canonical 보존을 결합한 정책이다.
영구 충돌을 무조건 배치 실패로 두면 재시작마다 같은 충돌에 걸려 checkpoint가
진행되지 않는다. ACK된 격리를 완료로 인정하여 후속 레코드 처리를 허용한다.
격리 ACK 후 checkpoint 실패 시 재격리될 수 있으며 같은 입력·충돌 필드의
quarantine_id는 유지된다. 일반 DLQ/Retry/Reprocess는 Phase 6 범위로 남긴다.

모든 레코드 완료 후에만 foreachBatch가 정상 반환한다. Spark가 checkpoint를
관리하며 별도 offset commit, checkpoint 파일 쓰기, batch ID 성공 캐시는 없다.
후반 레코드 실패 시 앞서 완료한 레코드도 다시 처리될 수 있다. Silver 중복에서도
발행을 반복하므로 Kafka 중복은 가능하다. downstream은 event_id 또는
quarantine_id로 식별해야 한다. Silver와 Kafka 전체의 exactly-once 보장은 없다.

## 시각과 실행 경계

배치마다 UTC 처리 시각 하나를 domain/time policy에 동일하게 전달한다.
순수 분류 테스트에는 고정 시각을 주입한다. 배치 재시작 시 기준 시각은 달라질
수 있어 late/future 결과의 재시작 간 동일성을 보장하지 않는다.

FUTURE_EVENT_TIME은 domain 단계에서 Quarantine으로 간다. 정상 시각의 LATE는
관측 분류이며 Silver/validated 경로를 허용한다. 이 분류는 Spark watermark 밖의
이벤트 판정이 아니다. 기존 운영 문서의 watermark 초과 실시간 제외 정책은
이번 처리 시각 기준 LATE로 대체하지 않았다. 실제 watermark/state/제외 계수는
후속 runtime 범위이며 이번 query에 withWatermark/dropDuplicates를 추가하지 않았다.

배치 행은 topic/partition/offset 순으로 순차 처리한다. 단일 query·단일 Silver
writer 전제이며 분산 executor에서 SparkSession으로 SQL을 호출하지 않는다.
`toLocalIterator`도 최대 파티션 크기만큼 메모리가 필요하다. 행마다 SQL/ACK를
기다리는 구현이므로 처리량 달성이나 분산 성능을 주장하지 않는다.

외부 클라이언트는 foreachBatch 실행 위치에서 생성한다. SparkSession은
`batch.sparkSession`을 사용하고 client/session 객체를 closure로 운반하지 않는다.
Registry HTTP session은 배치 종료 시 닫는다. producer는 배치 전용이며 각 발행이
flush/ACK를 확인한다. timeout으로 남은 큐의 취소나 나중 전달 여부는 보장하지 않는다.

## 설정과 진입점

기존 `raw_ingestion.py`와 같이 환경변수를 사용한다. 새 설정 프레임워크는 없다.
`IngestionConfig.table`은 재사용한 source 설정의 기존 필드이며 Job 2가 Bronze
테이블을 생성하거나 저장하는 데 사용하지 않는다. Silver 대상은 별도 설정이다.

| 환경변수 | 의미 |
|---|---|
| KTD_KAFKA_BOOTSTRAP_SERVERS | Spark Kafka source broker |
| KTD_VALIDATION_CHECKPOINT | 전용 `/Volumes/<catalog>/<schema>/<volume>/job2/...` 경로 |
| KTD_SILVER_TABLE | 기존 canonical 10필드 Silver 테이블 |
| KTD_PROCESSING_VERSION | Quarantine 생성 코드 버전 |
| KTD_VALIDATION_RULES_PATH | 아래 구조의 JSON 규칙 파일 |
| KTD_SCHEMA_REGISTRY_CONFIG_JSON | Registry client 설정 JSON. url 및 필요한 인증 설정 |
| KTD_PRODUCER_CONFIG_JSON | confluent producer 설정 JSON. bootstrap.servers 및 필요한 인증 설정 |
| KTD_RAW_TOPIC / KTD_QUARANTINE_TOPIC / KTD_VALIDATED_TOPIC | 기본 kitchen.sensor.raw / quarantine / validated |
| KTD_ALLOWED_LATENESS_SECONDS | 처리 시각 기준 late 분류, 기본 600초 |
| KTD_MAX_FUTURE_SKEW_SECONDS | 미래 허용 범위, 기본 300초 |
| KTD_STARTING_OFFSETS / KTD_MAX_OFFSETS | 기본 earliest / 1000 |
| KTD_KAFKA_SERVICE_CREDENTIAL | 기존 Spark source용 선택 설정 |

Spark service credential이 confluent producer 인증을 자동 설정하지 않는다.
인증 설정은 로그로 출력하지 않는다. producer는 acks=all, 성공 callback 활성화,
자동 topic 생성 비활성화를 강제하며 transactional.id를 거부한다.
기존 sensor_metric_event.avsc와 topic_subject_name_strategy를 쓰며 자동 schema 등록은
하지 않는다. validated topic의 subject와 호환 스키마를 실행 전에 준비해야 한다.

JSON 규칙 구조 예시이며 전체 운영 장비 목록이 아니다:

```json
{
  "supported_schema_versions": ["1.0.0"],
  "equipment_registry": [
    {"equipment_id": "fridge-001", "store_id": "store-001", "equipment_type": "refrigerator"}
  ],
  "metric_units": {"refrigerator": {"temperature_celsius": "celsius"}}
}
```

중복 equipment_id 설정은 거부한다. 이 검사는 전체 실제 장비의 전역 유일성 증빙을
대신하지 않는다. 규칙은 명시적으로 주입하며 simulator의 목록을 운영 설정으로
자동 채택하지 않는다.

진입점은 `python -m streaming.jobs.sensor_validation_job --available-now`다.
이번 작업에서 이 명령을 클라우드에 실행하지 않았다. SparkSession은 main에서만
생성하고 종료 시 자신이 시작한 query만 중단한다. 테이블·topic·checkpoint는
생성/삭제/초기화하지 않으며 Spark가 실행 중 자체 checkpoint를 기록한다.

P2-8에서는 패키지/설정 전달, callback 실행 위치의 Registry·Kafka 연결과 인증,
UC Managed Delta insert-only MERGE·단일 writer, query별 checkpoint 분리, 재시작 및 부분 실패,
장비 ID 유일성, input lineage와 각 sink 계수를 실제로 대조해야 한다.
계수는 성공한 배치 시도별 received/validated/quarantine/conflict/duplicate/late이며
재시도 간 고유 누계가 아니다. 실패한 배치의 성공 계수는 출력하지 않는다.

참고: [Spark foreachBatch](https://spark.apache.org/docs/3.5.8/structured-streaming-programming-guide.html#using-foreach-and-foreachbatch),
[toLocalIterator 메모리 계약](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.toLocalIterator.html).

conflict 계수는 Quarantine ACK를 받은 충돌 수로 quarantine 계수의 부분집합이다.

Silver는 `streaming/sinks/delta_silver.py`의 `DeltaSilverStorage`를 사용한다.
P2의 영속 멱등성과 canonical 보존을 위해 Delta target에 공식 지원되는 MERGE를
선택했다. Bronze는 기존 UC Managed Iceberg를 유지한다. 이 선택과 로컬 회귀는
실제 Delta 테이블 생성·MERGE·재시작 검증 완료를 뜻하지 않는다.
