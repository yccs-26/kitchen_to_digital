# 이벤트 계약과 Schema Evolution

상태: Avro 초기 도입 채택 / 구체 필드의 최신 변경안은 제안 · 2026-09-24

## 채택된 정책

사용자가 Avro + Schema Registry를 처음부터 사용하도록 명시했다. schema ID는 직렬화 스키마 식별자, payload의 schema_version은 논리 계약 버전으로 구분해 유지한다. BACKWARD를 기본 호환성 방향으로 두고 optional/default 필드 추가 중심으로 진화시킨다. breaking change는 새 계약/topic 또는 명시적 major migration으로 분리한다.

BACKWARD의 의도는 새 reader로 이전 데이터를 읽는 것이다. Producer 선배포와 구형 reader가 새 데이터를 읽는 안전성까지 자동으로 승인한 것이 아니다. 배포 순서, mixed-version 테스트와 과거 모든 버전 replay의 범위는 별도 검증한다.

## 이전안과 마지막 제안의 차이

| 항목 | 이전 Work 기준 | 마지막 답변의 제안 |
|---|---|---|
| 이벤트 범위 | 수치·문자·boolean 공통 Metric Event | raw는 수치 telemetry SensorMetricEvent |
| 값 | numeric_value/string_value/boolean_value 중 하나 | metric_value: double |
| 상태 이벤트 | 같은 이벤트 모델 | 요구 시 별도 EquipmentEvent 계약 |
| 종류·metric·단위 | 문자열 및 품질 규칙 | string 유지, Job 2 참조 규칙으로 조합 검증 |
| schema_version | string 예시 1.0 | int 논리 버전 예시 |
| 발생 원천 | source | producer_id |
| ingested_at | 공통 이벤트 필드로 제시 | Spark가 붙이는 처리 metadata |

마지막 제안 이후 사용자의 채택 응답은 확인되지 않았다. **numeric-only, 버전 타입, source/producer_id 전환을 확정 계약으로 간주하지 않는다.** 마지막 설계 방향을 기록하되 코드·Avro schema를 변경하기 전에 이 경계를 닫아야 한다. 기존 상태형 집계와 FSM에 필요한 입력을 수치형 계약만으로 이미 제공한다고 가정하지 않는다.

## 최신 논리 스키마 제안

| 필드 | 타입·책임 |
|---|---|
| event_id | string, 같은 측정의 재전송·재처리에서 유지 |
| event_time | timestamp, 실제 측정 시각 |
| store_id, equipment_id, equipment_type | string |
| metric_name, unit | string, 허용 조합은 Job 2가 검사 |
| metric_value | double, 수치형 telemetry만 |
| producer_id | string, 발생 producer 식별 제안 |
| schema_version | int 제안, 기존 string에서 전환 정책 미정 |

Kafka topic/partition/offset/timestamp 및 ingested_at/processed_at은 운송·처리 metadata로 분리한다. 원본 bytes와 schema 식별 정보는 Bronze에 보존한다. Avro logical timestamp 단위, null/default, subject naming, 미지원 버전 매핑은 아직 물리 계약으로 확정하지 않았다.

## 변경 정책과 검증

- optional/default 추가: 호환성 검사와 V1/V2 혼합 처리·backfill 테스트 후 적용.
- 필드 rename, 타입 변경, required 필드 추가, 의미 변경: 현재 계약에서 허용하지 않는 방향.
- 필드 삭제: 앞선 논의는 breaking으로 분류했고 마지막 답변은 optional 제거를 신중 허용하자고 제안했다. 삭제 승인 기준은 미정이며 자동 허용하지 않는다.
- 계약 오류와 domain 오류는 [검증](validation.md)에서 분류한다.
- 이벤트 schema와 Iceberg 저장 schema 진화는 별개로 검증한다.

## 현재 코드와의 관계

확인한 기존 models.py는 metric_value: float, schema_version: str='1.0', 제한된 equipment_type을 사용한다. 수치형 값이 유사해도 Avro 전환과 새 논리 계약이 완료된 것은 아니다. simulated_fault, 구형 원본 변환, 단위·범위 사전도 전환 설계 대상이다.

**이유:** 구조적 계약과 업무 의미를 분리한다. **트레이드오프:** 범용 수치 모델은 단순하지만 상태·문자 이벤트를 별도로 설계해야 한다. **면접 포인트:** 호환 변경 수용과 breaking 변경 차단을 실제 테스트 결과로 증명한다.
