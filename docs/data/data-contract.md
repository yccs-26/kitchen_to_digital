# 이벤트 계약과 Schema Evolution

## 현재 물리 계약

[Avro 스키마](../../schemas/avro/sensor_metric_event.avsc)의 `ktd.sensor.SensorMetricEvent`는 수치 telemetry 계약이다.
모든 필드는 필수이며 null union이나 default는 없다. 현재 Producer는 이 로컬 스키마로 직렬화한다.

| 필드 | Avro 타입 | 의미·현재 생성 방식 |
|---|---|---|
| `event_id` | string | 측정 식별자. Simulator는 새 측정마다 UUID 생성 |
| `event_time` | string | 측정 발생 시각. Simulator는 UTC ISO 8601 문자열 생성 |
| `store_id` | string | 매장 식별자 |
| `equipment_id` | string | 장비 식별자이자 Kafka key |
| `equipment_type` | string | 장비 종류 |
| `metric_name` | string | 측정 항목 |
| `metric_value` | double | 수치 측정값. Producer는 boolean/string 입력 거부 |
| `unit` | string | 측정 단위 |
| `schema_version` | string | 논리 계약 버전. Simulator 생성값은 `1.0.0` |
| `source` | string | 발생 원천. Simulator 생성값은 `simulator` |

`event_time`이 string이라는 사실은 timestamp 유효성을 보장하지 않는다.
장비·metric·unit 조합, 값 범위, 참조 유효성은 후속 Job 2의 [업무 품질 검증](validation.md) 책임이다.
Avro는 구조를 검증하며 장비의 실제 상태나 업무적 타당성을 대신 판정하지 않는다.

## Identity와 처리 metadata

Kafka key는 UTF-8 `equipment_id`이며 Producer가 payload와의 일치를 검사한다.
`event_id`는 같은 측정의 재전송·재처리에서 유지하는 설계다. 새 측정과 재시도를 구분해야 한다.
매장 간 장비 ID의 전역 유일성은 아직 확인해야 한다.

`event_time`은 payload의 발생 시각이다. Job 1은 Kafka `timestamp`를 `kafka_timestamp`로
보존하고 Bronze 행을 만들 때 Spark `current_timestamp()`로 `ingested_at`을 부여한다.
`ingested_at`, `processed_at`과 Kafka topic/partition/offset/timestamp는 Avro payload의 10개 필드에 포함되지 않는다.
`processed_at`은 현재 Bronze 컬럼이 아니며 후속 처리·재처리 시각의 계약은 별도로 정한다.

현재 Bronze는 raw key/value bytes, headers와 topic/partition/offset을 보존한다.
Avro schema ID를 별도 컬럼으로 추출하지 않고 framing을 포함한 원본 value를 그대로 저장한다.
이 경로는 Phase 1에서 구현·검증했다. payload를 해석하지 않으므로 corrupt bytes도 보존한다.

## 버전과 호환성 정책

Avro + Schema Registry는 구조적 계약과 업무 의미를 분리하고 reader/writer 변경을 관리하기 위한 선택이다.
subject는 버전·호환성 관리 단위, Registry schema ID는 직렬화 스키마 식별자,
payload의 `schema_version`은 논리 계약 버전이다.

Producer는 topic subject naming을 쓰며 기본 raw 토픽의 subject는 `kitchen.sensor.raw-value`다.
`auto.register.schemas=False`, `use.latest.version=False`로 등록된 로컬 스키마를 조회한다.
로컬 환경에서 관측한 ID를 코드에 고정하거나 호환성 실험의 latest를 자동 채택하지 않는다.

기본 호환성 방향은 BACKWARD다. 새 reader가 이전 writer 데이터를 읽는 방향이며,
구형 reader의 새 데이터 읽기나 모든 과거 버전 replay까지 자동 보장하지 않는다.

- optional/default 필드 추가도 호환성 검사, 혼합 버전 처리와 backfill 검증 후 적용한다.
- rename·타입 변경·required 추가·의미 변경은 breaking change로 다루고 새 계약/topic 또는 명시적 major migration으로 분리한다.
- 필드 삭제 승인 기준은 미정이다. optional 필드라는 이유만으로 자동 허용하지 않는다.
- 배포 순서와 과거 버전 replay 범위를 별도 검증한다. 이벤트 schema와 Iceberg 저장 schema 진화도 구분한다.

## 검증 범위와 미결정 사항

기존 Phase 0 기록에는 기본 v1 발행·전체 필드 roundtrip, subject BACKWARD,
nullable `firmware_version`과 default null을 추가한 실험용 v2의 호환성 결과가 있다.
v2는 현재 Producer 계약에 채택된 필드가 아니다. 상세 결과와 한계는
[호환성 검증](../schema-evolution-test.md), [검증 보고서](../reports/phase-0-verification.md)를 따른다.
Registry 등록 상태는 해당 실행 시점의 관측이다.

이전 공통 numeric/string/boolean 모델에서 현재 수치형 계약으로 범위가 좁아졌다.
`source → producer_id`, `schema_version: string → int`, logical timestamp 전환은 적용되지 않은 제안이다.
상태·문자 이벤트는 별도 입력 계약이 필요하며 현재 수치형 payload만으로 제공된다고 가정하지 않는다.
미지원 버전 매핑, 구형 원본 변환, fault injection 입력의 품질 처리도 후속 설계 대상이다.

수치형 공통 모델은 단순하지만 상태 이벤트를 별도로 설계해야 한다는 트레이드오프가 있다.
