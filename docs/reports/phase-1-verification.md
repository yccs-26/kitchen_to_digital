# Phase 1 검증 기록

MSK에 발행한 실제 Avro fixture가 Job 1을 거쳐 Bronze에 저장됐고, 동일 checkpoint의
정상 재시작과 격리된 failure-state recovery까지 확인했다. 이 문서는 실제
Databricks 실행 결과와 최종 로컬 검증 결과를 기록한다. 아래 cloud 실행과 로컬 테스트는 각 검증 시점의 기록이다.

## MSK에서 Bronze까지

처음에는 로컬 Docker Kafka를 Databricks에서 읽으려 했지만 `describeTopics`가 timeout으로
끝났다. broker가 알리는 `localhost:9092`와 로컬 Mac으로 원격 compute가 접근할 경로가
없었다. 이후 MSK Serverless의 private endpoint와 VPC Peering, IAM 인증으로 검증 경로를
정했다. EC2 client의 topic 생성 과정에서는 `TopicAuthorizationException`이 발생했고,
producer IAM policy의 topic resource scope를 수정한 뒤 생성에 성공했다.

Databricks에서는 외부 Iceberg JAR·extension 없이 Managed Iceberg CREATE/INSERT/SELECT를
확인했다. 이어 MSK Serverless IAM 연결과 batch read가 성공했고, 실제 Avro fixture를
MSK에 발행해 Job 1으로 Bronze에 저장했다. 앞서 수행한 파일 fixture sink 테스트와 달리,
이번에는 Kafka의 실제 offset과 Bronze의 `(topic, partition, offset)`을 대조했다.

`kitchen.sensor.raw`에서 사용한 fixture는 다음 세 건이다.

| Kafka 위치 | 입력 | Bronze에서 확인한 결과 |
|---|---|---|
| partition 1, offset 0 | 정상 Avro | 해당 lineage의 원본 보존 |
| partition 1, offset 1 | offset 0과 동일 event_id를 재발행한 정상 Avro | 별도 offset의 원본으로 보존 |
| partition 1, offset 2 | corrupt payload `0x0000` | Job 중단 없이 raw bytes `0000` 보존 |

Bronze는 Avro를 decode하거나 event_id로 dedup하지 않는다. 같은 event_id여도 offset이
다르면 각각 남았고, 손상된 payload도 원본 그대로 저장됐다. 기존 checkpoint를 그대로
사용한 정상 재시작에서는 기존 offset의 중복 적재가 없었다.

초기 table은 무분할이다. Managed Iceberg 제약 때문에 `days(ingested_at)`은 적용하지 않았다.

## 저장은 끝났지만 checkpoint commit이 없는 상태

Bronze에 데이터가 저장된 뒤 checkpoint 기록이 끝나지 않은 상황을 재현해봤다.
실제 프로세스를 특정 시점에 kill한 것은 아니다. 격리된 test checkpoint 전체를 백업하고
파일 내용을 비교한 뒤, sink 저장이 완료된 최신 data batch의 `commits/N`만 별도 경로로
옮겼다. sink 결과와 `offsets/N`, query metadata는 남겨둔 채 같은 table/checkpoint/query
설정으로 다시 시작했다.

최종 clean run은 `732e5477deb54c4aab4f4e840b184178`이다.

- Table: `ktd.bronze.sensor_raw_failure_test_732e5477deb54c4aab4f4e840b184178`
- Checkpoint: `/Volumes/ktd/bronze/checkpoints/job1-failure-test/732e5477deb54c4aab4f4e840b184178/sensor_raw`
- Harness evidence: `/Volumes/ktd/bronze/checkpoints/job1-failure-test/732e5477deb54c4aab4f4e840b184178/evidence/`

최종 Databricks 실행 결과:

```text
[PASS] isolated failure-state reproduction
1 passed in 43.36s
pytest result: 0
```

최종 harness는 재시작 전후 query ID가 같고 run ID는 다른지, 같은 batch N이 같은
startOffset/endOffset으로 복구되는지 확인했다. `commits/N`이 다시 생성됐고 Kafka에서
직접 읽은 expected lineage와 비교해 missing=0, duplicate=0, unexpected=0이었다.
raw bytes·headers·timestamp도 유지됐으며, 신규 입력 없는 재실행 이후에도 결과가 같았다.

### numInputRows에 대한 가정을 바꾼 이유

처음에는 recovery 시 `numInputRows > 0`이어야 한다고 가정했다. 이전 관찰 run
`b3429e0538db42009f3cb09287f3168d`에서는 같은 batch N=2와 같은 start/end offset으로
복구됐지만 `numInputRows`는 0이었다. query ID는 유지되고 run ID는 바뀌었으며,
`commits/2`도 다시 생성됐다. 수동으로 Bronze를 조회하니 partition 1의 offset 0/1/2가
각각 한 행씩 남아 있었고, duplicate·missing lineage는 없었다. offset 2의
`value_hex=0000`, `value_bytes=2`도 확인했다.

그래서 입력 행 수가 양수라는 조건을 제거하고, checkpoint 복구와 최종 lineage의
중복·누락을 직접 비교하도록 테스트를 바꿨다. `assert_progress_boundaries()`는 유지했다.
`numInputRows`는 progress와 최종 result evidence에 실제 값으로 남기며, 0 이상의 정수인지
검사한다. 필드 누락, 음수, bool을 포함한 잘못된 타입은 실패한다. 수정한 harness로 새 UUID를
사용해 실행한 결과가 위 최종 clean run이다. `numInputRows=0`의 상세 관찰은 이전 run의
기록이며, 각 run의 실제 값은 해당 progress와 result evidence에서 확인한다.

이 결과만으로 Databricks Managed Iceberg가 어떤 metadata나 transaction mechanism으로
replay를 처리했는지는 알 수 없다. 확인한 범위는 이번 fixture와 failure-state 조건에서
`(topic, partition, offset)` 중복·누락이 없었다는 것이다. 일반적인 exactly-once 보장이나
throughput/latency 수치를 검증한 결과는 아니다.

## Harness 로컬 검증

최종 로컬 검증 기록은 다음과 같다. 외부 연동 opt-in을 끈 일반 테스트 결과이며,
위 Databricks clean run 결과와 구분한다.

```text
uv run --offline python -m pytest tests/unit tests/integration
124 passed, 10 skipped
기존 dependency warning 1건
```

수정 파일 Ruff와 `git diff --check`도 통과했다. 회귀 테스트는 `numInputRows=0` evidence
보존, 누락·음수·잘못된 타입 거부, 0이어도 batch/offset 불일치를 거부하는 동작을 확인한다.

## 남은 정리와 증빙

정상 재시작은 기존 checkpoint의 offset/query state를 이어가는 절차다. 이번 commit 격리는
전체 checkpoint 유실 복구 실험이 아니다. 유실 시에는 Kafka retention과 Bronze lineage를
먼저 대조해야 하며, retention 밖으로 사라진 원본은 checkpoint만으로 복구할 수 없다.
구체적인 실행·복구 절차는 [runbook](../runbook.md#재시작과-복구)에 남겼다.

PENDING으로 남은 것은 증빙 파일 정리와 Phase 1 DoD 최종 검토 후 PR·main 병합이다.
MSK 연결·Bronze 적재·정상 재시작·failure-state clean run은 더 이상 PENDING으로 두지 않는다.
장기 Producer 운영 환경과 cloud Schema Registry 운영 방식은 이번 fixture 검증으로 확정하지 않았다.

아래 이미지는 문서 갱신 시 로컬 저장소에 없어 **TODO — 증빙 후보 경로**로 남긴다.

- `docs/evidence/phase-1/checkpoint-recovery-result.png`: 최종 run의 PASS 출력과 식별 정보
- `docs/evidence/phase-1/checkpoint-recovery-lineage-no-duplicates.png`: lineage별 한 행과 중복·누락 대사
- `docs/evidence/phase-1/checkpoint-recovery-corrupt-bytes-preserved.png`: offset 2의 `0000`과 2 bytes

UC Volume의 harness evidence와 위 저장소 이미지 경로는 별개다. 이미지를 저장할 때는
원본 실행과 연결되는 run ID를 확인하고 비밀값을 가린다.
