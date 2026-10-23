# Phase 3 스트리밍 설계 보완 및 구현 준비

작성·보완일: 2026-10-17 (Asia/Seoul). 상태: 사용자 검토 대기, 구현 전.
범위: numeric metric의 중복 방어·window·Gold 저장과 로컬 검증 계획.

## 1. 결정 요약과 현재 기준

**최종 권장안은 원장 없는 단일 query: bounded event_id dedup → 1분 window →
Append 최종 출력 → foreachBatch Gold Delta MERGE다.**
기존 원장/두 query 기본안을 변경하여 테이블·stream 소비·checkpoint·운영 경계를 줄인다.
추가 현금 비용 $10은 설계 목표이며 cloud 실행 승인이나 실측 비용이 아니다.

이 선택은 **Phase 2가 canonical로 발행한 동일 ID의 payload/event_time이 불변**이라는
기존 validated 계약에 의존한다. Phase 3가 임의 producer의 충돌 payload까지 전 기간에 걸쳐
독립 재검증하는 보장은 제공하지 않는다. 이 보장이 필수로 추가되면 영속 원장이 필요하다.
두 방식이 동등하다고 표현하지 않으며, 이 보장 경계 변경도 사용자 검토 대상이다.

직접 확인한 현재 상태:

- branch `feat/phase-3-gold-windows`, HEAD `74f76a48695243e8f4dcbb5db3a00b3962e807dd`.
  작업 시작 시 유일한 변경은 기존 미추적 설계 파일인 이 문서다.
- `windows.py`: UTC `[start,end)` 1분, 자연키
  `(window_start, window_type, store_id, equipment_id, metric_name)`.
- `minute_aggregation.py`: 한 호출 전체의 ID/9필드 비교, 동일 payload는 1회,
  충돌은 `EventConflictError(event_id, differing_fields)`로 전체 호출 실패. 호출 간 상태 없음.
- `validated_publisher.py`: INSERTED/DUPLICATE_NOOP 모두 발행, CONFLICT는 발행하지 않는다.
  Silver 성공 후 ACK 불확실/timeout, validated ACK 후 checkpoint 실패, 재처리로 중복될 수 있다.
- 원본 checkout의 `docs/plans/ktd-forward-execution-plan-2026-10-06.md` 실행 묶음 C를
  재확인했다. UTC 경계, Job 2/3 독립 watermark, idle 검증, 출력 정책 선결정을 따른다.
- README의 전체 793 passed는 이전 실행 기록이다. 이번 검사 결과는 §11에 따로 기록한다.

validated 필수 Avro 10필드는 유지한다. event_id가 identity이고 Kafka equipment_id key는
분배용이다. 비교 payload 9필드는 event_time, store_id, equipment_id, equipment_type,
metric_name, metric_value, unit, schema_version, source다. UTC instant로 시각을 정규화한다.
transport lineage, query ID, batch ID를 wire payload에 추가하지 않는다.

## 2. 방식 비교와 권장 아키텍처

| 방식 | 중복·재시작 보장 | 복잡도·비용과 한계 | 선택 |
|---|---|---|---|
| 원장 + watermark dedup + window + Gold | 원장 보존 ID 영속 판정, Spark state 복구 | 원장이 고유 입력을 제공하면 native dedup은 중복 역할. 테이블·query·state 증가 | 초기 제외 |
| 원장 + window + Gold | 보존 전 기간 9필드 충돌 판정, 동일 ID 재발행 no-op | 원장 single writer, 두 checkpoint, source retention·소비 추적 필요 | 엄격 충돌 검증이 필요할 때 대안 |
| 원장 없이 watermark dedup + window + Gold | canonical 불변 입력의 bounded 중복 방어, 동일 checkpoint restart, sink retry 수렴 | 전 기간 충돌 탐지·checkpoint 유실 복구는 별도 | **권장** |
| raw foreachBatch에서 Python 집계 후 Gold에 더하기 | 한 batch 내부 중복만 제거 | 배치 간 중복·late·누적 상태·재시도 이중 합산을 직접 구현해야 함 | 금지 |
| foreachBatch 원장 쓰기 후 영향 window 전체 재계산 | 순서·재계산으로 복구 가능 | 신규 행만 재계산하면 누락. durable pending 범위·canonical scan 필요 | 초기 제외 |

```text
validated Kafka
  → 기존 Avro decode / UTC timestamp / numeric·unit·입력 방어
  → withWatermark(event_time, 10 minutes)
  → dropDuplicatesWithinWatermark([event_id])
  → window(event_time, 1 minute), dimension별 count/sum/avg/min/max
  → Append: streaming 정책상 닫힌 window의 전체 결과
  → foreachBatch: 결과 전량 소비 → Gold Delta 자연키 MERGE
```

논리 Job 3에는 우선 query 하나, 영속 checkpoint 하나, Gold sink 하나만 둔다.
dedup은 입력 중복, MERGE는 결과 쓰기 재실행을 각각 방어한다.
Gold 키가 1개여도 count에 중복 이벤트가 반영되었다면 정확한 결과가 아니다.

이 query에는 dedup·window 두 stateful operator가 있다. Spark 4.0 문서는 Update/Complete의
다중 stateful 연결을 제한하므로 Append를 사용한다. Append라고 모든 연결의 정확성이
자동 보장되는 것은 아니다. **dedup→집계의 watermark 전달·late 처리·실제 plan 실행**을
가장 이른 로컬 Spark 검증으로 둔다. correctness check를 끄거나 watermark를 임의 재설정해
통과시키지 않는다. 실패하면 검증 완료로 표시하지 않고 query 분리 필요성을 재검토한다.
[Spark 4.0 지원 제약](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html#unsupported-operations)

## 3. 비선택 원장 방식의 retention·완료·복구 계약

원장과 Gold는 독립된 Delta 테이블이며 checkpoint까지 포함한 원자성은 없다.
원장 성공은 canonical 입력의 내구성만 뜻한다. Gold 처리 완료 플래그로 쓰지 않는다.
원장 방식에서도 Phase 2 Silver/validated를 변경하지 않는다.

대안 구성: Q3A validated→insert-only 원장, Q3B 원장 append stream→window→Gold.
원장이 고유하므로 Q3B native dedup은 생략한다. ID별 canonical 10필드와 최초 lineage를
보존한다. batch 전체와 기존 원장의 충돌을 쓰기 전에 검사하고 differing_fields를 남긴다.
single writer, 기존 중복 키 거부가 전제이며 PK 선언이 유일성을 강제한다고 가정하지 않는다.

| 완료 단계 | 기준 | 실패·복구 |
|---|---|---|
| 원장 수용 완료 | insert/no-op 확인 후 Q3A batch checkpoint 완료 | commit 후 checkpoint 실패는 동일 payload 재시도 no-op |
| 집계 입력 소비 완료 | Q3B checkpoint가 원장 commit의 입력 소비를 완료 | 원장 stream 재독과 window state 복원 |
| window Gold 완료 | 닫힌 window 전체 결과 MERGE 성공 + Q3B batch checkpoint 완료 | Gold 실패는 Q3B batch 실패, 같은 입력 재실행 |
| late 제외 / 미확정 | late 제외 또는 열린 window 상태가 관측됨 | Gold 반영 완료로 기록하지 않음. 대사/향후 backfill 또는 event-time 진행 대기 |

원장 성공/Gold 실패 때 Q3A의 '이번에 신규였던 행'만 Gold로 보내면 안 된다.
재시도에서 원장은 no-op이므로 누락될 수 있다. Q3B가 원장 commit을 독립 소비하게 한다.
Gold 성공/Q3B checkpoint 실패는 같은 final MERGE no-op으로 수렴한다.
원장·Gold를 한 callback에서 순차 저장해도 공동 트랜잭션이 되지 않는다.
insert-only MERGE 결과를 Q3B가 append stream으로 읽는 동작도 실제 runtime 검증 대상이다.

원장 대안의 초기 보관 정책은 **자동 TTL 없음**이다. 현재 최대 재발행·backfill·장애 복구
기간이 정해지지 않아 안전한 삭제 일수를 계산할 수 없다. window가 닫혀도 ID를 삭제하지 않는다.
향후 기간 제한은 upstream 최대 재발행 기간·장애 복구 기간·backfill 범위 중 최대값과 안전 여유,
archive lookup 및 보장 종료 시점을 함께 정해야 한다. 삭제 후 전 기간 ID 판정을 주장하지 않는다.
Delta log/file retention도 Q3B 최대 지연·복구 기간을 포함해야 한다. 소비 전에 필요한
version/file이 만료되면 fail closed 후 staging rebuild한다. compaction은 ID 삭제가 아니다.

원장 없는 권장안에서는 원장 보관 기간과 원장→Gold 완료 플래그가 **해당 없음**이다.
source 재독 가능 기간, checkpoint 보존, Gold commit을 복구의 세 경계로 관리한다.

## 4. Bounded dedup·watermark·late 정책

- 초기 delay는 10분이며 운영 최적값이 아니다. Job 2의 처리 시각 LATE 분류와 분리한다.
  `now - event_time > 10분` 필터로 대체하지 않는다.
- native API는 streaming DataFrame의 watermark가 필요하다. ID state는 checkpoint로
  복원되고 event-time 진행에 따라 제거된다. 처리 시각 10분 TTL이나 영구 원장이 아니다.
- ID dedup은 payload 충돌을 탐지하지 않는다. 동일 ID의 event_time을 발행 시각으로
  바꾸는 것도 기존 계약 위반이다. 이 위반까지 탐지한다고 주장하지 않는다.
- 같은 canonical payload는 state가 살아 있으면 dedup된다. W가 충분히 진행해 ID state가
  제거된 뒤에도 원래 timestamp의 너무 늦은 입력은 dedup operator의 late 판정으로 제외되어
  과거 window를 다시 열지 않아야 한다. 실제 W/eviction을 관측하고 테스트한다.
- watermark 밖을 wall-clock으로 판단하지 않는다. API의 late 제거와 집계 admission을
  구분한다. 단지 delay보다 늦었다는 이유로 집계가 항상 버린다고 일반화하지 않는다.
- 신규 ID의 late는 수용 가능한 window에만 반영한다. 제외 입력은 Kafka/Bronze/Silver의
  보존·대사를 통한 향후 historical 복구 대상이다. 별도 late topic은 도입하지 않는다.
  validated에 raw lineage가 없으므로 Bronze 대사가 자동 연결된다고 가정하지 않는다.
- 미래 허용 편차 초과·NULL/naive/파싱 실패·nonfinite 값·unit 불일치는 watermark 이전에 실패한다.
  기존 규칙과 검증 기준을 주입한다. 허용 미래 편차도 다른 장비의 W에 영향을 주므로 시험한다.
- checkpoint 삭제/교체는 정상 재시작이 아니다. 과거 timestamp가 새 query의 최초 입력이면
  새 W/state에서 다시 수용될 수 있다. 새 checkpoint의 live Gold replay는 금지하고
  staging rebuild/비교/전환을 별도 설계한다. 이번 구현 범위 밖이다.

근거: [Spark dedup API](https://spark.apache.org/docs/4.0.0/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html),
[Spark watermark 보장](https://spark.apache.org/docs/4.0.0/streaming/apis-on-dataframes-and-datasets.html#semantic-guarantees-of-aggregation-with-watermarking).

## 5. Window 출력과 idle 종료

초기 출력은 **Append final**이다. 잠정 조회 테이블·Update query를 추가하지 않는다.
UTC TIMESTAMP(LTZ), session timezone UTC, epoch 기준 1분 `[start,end)`를 유지한다.
자연키는 §1과 같고 window_type은 `1m_tumbling`, unit은 속성이다. 빈 window는 생성하지 않는다.

- W가 window 종료를 충분히 넘어가고 후속 trigger가 처리되어야 확정된다.
  등호/±1 microsecond 경계는 실제 Spark 테스트로 확인한다.
- 입력 중단 시 벽시계로 W가 전진하지 않아 마지막 window는 무기한 미확정일 수 있다.
  '입력 N초 뒤 모든 Gold 완료' SLA를 약속하지 않는다. 이를 누락/완료와 혼동하지 않고
  `OPEN_PENDING_EVENT_TIME`으로 진단한다. 별도 상태 테이블을 뜻하지 않는다.
- AvailableNow/empty batch가 tail window를 강제로 닫는다고 가정하지 않는다.
  production heartbeat 센서·가짜 미래 행으로 W를 밀지 않는다.
- 로컬 확정 테스트는 별도 정상 장비의 시간 진행 fixture를 넣고 그 장비 기대값도 포함한다.
  idle 테스트는 그 행 없이 미확정을 기대한다. 벽시계 sleep으로 final을 기다리지 않는다.
- 유한 데모의 모든 마지막 window 결과는 보존된 전체 fixture를 기존 Python으로 별도 batch
  비교한다. streaming final로 표시하거나 live Gold에 임의 주입하지 않는다.
  idle에서도 즉시 결과가 필수면 Update/잠정 출력 또는 유한 batch 종료 계약을 재설계해야 한다.
- 향후 5분 sliding은 duration 5분/slide 1분을 검증 시작값으로 삼는다. 같은 고유 이벤트에서
  직접 계산하고 1분 avg를 단순 평균하지 않는다. 이벤트 하나는 5개 window에 기여한다.
  5분 도입은 새 plan/checkpoint 호환성을 별도 검토한다.

[Databricks output mode](https://docs.databricks.com/aws/en/structured-streaming/output-mode).
Spark DOUBLE 누산은 Python 정렬+math.fsum과 bitwise 동일하지 않을 수 있다.
count/key/unit/경계는 exact, 정상 크기 수치는 rel_tol=1e-12/abs_tol=1e-12를 테스트 시작 기준으로
삼는다. 상쇄·극단값은 별도 검증하고 nonfinite 최종 결과는 sink 전에 실패한다.

## 6. foreachBatch와 Gold 멱등 저장

foreachBatch는 **window 집계 뒤 sink adapter로만 사용**한다. 내부 DataFrame은 유한 결과다.
그 안에서 withWatermark/aggregate_minutes를 호출해도 source의 배치 간 상태가 생기지 않는다.
직접 pending 원장을 만드는 대신 Spark state/checkpoint를 활용한다.

후보는 Delta `ktd.gold.fct_metric_window`, 초기 single writer·무분할이다.
count/sum/avg/min/max는 전체 window 절대값이며 다음 계약을 따른다.

1. source/target 자연키 NULL·중복을 거부하고 source는 키당 1행으로 제한한다.
2. 신규 키 INSERT, 같은 키·같은 final 값은 no-op. unit/window_end/policy version 차이 또는
   허용 수치 비교를 벗어난 final은 기존 행을 보존하고 실패한다.
3. `target.count += source.count`, `target.sum += source.sum`은 금지한다.
   재시도로 바뀌는 now() 값을 비교 identity에 넣지 않고 최초 metadata를 유지한다.
4. MERGE 실패는 전파한다. 예외를 삼키거나 수동 offset commit하지 않는다.
5. stateful 결과를 전량 소비하고 empty batch를 지원한다. show(2) 같은 부분 소비로 끝내지 않는다.
   여러 action이 필요하면 persist/unpersist 수명을 실패 경로에서도 정리한다.
6. Gold 성공 후 checkpoint 실패는 같은 checkpoint로 재시작하여 같은 final MERGE를 no-op 처리한다.
   batch ID는 진단용이며 다른 query epoch의 전역 순서가 아니다.

권장안의 batch 완료 기준은 출력된 final 전체의 MERGE 성공 후 Spark batch checkpoint 완료다.
출력이 없는 batch도 window state/offset을 checkpoint할 수 있지만 이는 해당 이벤트의
Gold 완료가 아니다. 입력 소비 완료·열린 window·late 제외·Gold final 완료를 구분한다.

closure에 DBUtils/SparkSession/secret/native client를 캡처하지 않는다. primitive 설정을 넘기고
batch DataFrame의 SparkSession을 사용한다.
[foreachBatch at-least-once·전량 소비·직렬화 제약](https://docs.databricks.com/aws/en/structured-streaming/foreach)

향후 backfill은 승인 범위의 전체 canonical 결과를 절대값으로 교체하는 별도 write 계약이다.
동일 범위 live writer를 통제하고 오래된 live 값이 정정 결과를 덮어쓰지 못하게 한다.
이번에 backfill/새 checkpoint cutover/동시 writer는 구현하지 않는다.

## 7. Delta 지원과 트랜잭션·멱등성 범위

기존 목표 runtime은 DBR 17.3 LTS / Spark 4.0.0 / Standard다. 공식 release note도
[Spark 4.0.0 포함](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts)을 명시한다.
cluster 상태·권한·UC table·실제 callback 동작은 이번에 조회/실행하지 않았다.

| 항목 | 문서상 확인 | 보장하지 않는 것 |
|---|---|---|
| Delta MERGE | Databricks SQL/runtime 지원 | 현재 UC 권한·DDL·선택 table의 실행 성공 |
| Delta 단일 table commit | optimistic concurrency/ACID 경계 | Kafka offset·Spark checkpoint·별도 원장과 공동 원자성 |
| 자연키 no-op MERGE | 같은 결과 쓰기의 수렴을 애플리케이션이 설계 | 입력 ID dedup, payload 충돌 탐지, 영구 PK enforcement |
| foreachBatch | at-least-once callback | 자동 end-to-end exactly-once |
| txnAppId/txnVersion | 지원되는 Delta DataFrame write의 중복 batch 식별 | 임의 MERGE의 자동 멱등화, 새 checkpoint/replay 업무 중복 제거 |
| Iceberg | 기존 Bronze managed 경로와 Gold upsert는 별도 | OSS Iceberg MERGE를 DBR managed 지원으로 치환 |

근거: [Databricks MERGE reference](https://docs.databricks.com/aws/en/sql/language-manual/delta-merge-into),
[Delta concurrency](https://docs.delta.io/concurrency-control/),
[Delta streaming 재시도·transaction ID](https://docs.databricks.com/aws/en/structured-streaming/delta-lake).
현재 SQL MERGE reference는 Delta 전용으로 명시한다. Gold Delta는 기존 Iceberg 목표의 물리 형식
변경 제안이며 사용자 검토 대상이다. Bronze/Phase 2 table에는 영향이 없다.
외부 JAR·GlueCatalog·runtime upgrade를 숨은 전제로 도입하지 않는다.

완료 표현은 '보존된 source와 동일 checkpoint, canonical 불변 입력, single writer 전제에서
중복 집계와 sink retry가 수렴함'이다. checkpoint 유실·source retention 만료·계약 위반까지
포함한 무조건 exactly-once를 주장하지 않는다.

## 8. 필수 장애 시뮬레이션과 수용 기준

아래는 신규 테스트 **설계**이며 구현/실행하지 않았다. A=원장→window→Gold, B=권장안이다.
원장+native dedup 조합도 A의 장애를 반복하여 불필요한 dedup이 결과를 바꾸지 않는지 비교한다.

| 장애/입력 | A: 원장 방식 | B: 원장 없는 권장안 |
|---|---|---|
| 다른 micro-batch에 동일 ID | 원장 no-op | 살아 있는 ID state로 제거 |
| 원장 성공 후 Gold 실패 | Q3A commit 유지, Q3B 원장 stream 재시도 | 원장 경계 N/A. 집계 state 계산 후 Gold 전 강제 실패로 대응 검증 |
| Gold 성공 후 checkpoint 실패 | 같은 Q3B final MERGE no-op | 같은 checkpoint restart, 같은 결과 재실행 후 Gold 1행 |
| process restart 후 동일 ID 새 offset | 원장 no-op | ID/window state 복원, count 증가 없음 |
| W 전진·ID eviction 후 과거 ID 재전달 | 원장 no-op | 원 timestamp와 실제 W 확인, late 제거되어 Gold 불변 |
| 새 checkpoint로 과거 재입력 | 원장은 유지되어도 Q3B 재독 설계 필요 | 정상 restart 보장 밖. live 쓰기 차단, staging rebuild |
| 같은 ID의 시각/값/차원 충돌 | canonical 보존, differing_fields와 batch 실패 | 전 기간 탐지 보장 없음. 계약 위반 fixture로 한계 명시, 탐지 통과로 보고하지 않음 |

명확한 fixture: 같은 UTC minute에 batch0 a=2,b=4, batch1 a=2,c=6 →
고유 count=3,sum=12,avg=4,min=2,max=6. 다른 ID의 같은 값은 별도 샘플이다.

추가 필수 검증:

- Spark 4.0 dedup→window plan의 실제 Append 실행. correctness check 기본값 유지.
- 열린 window restart 뒤 새 정상 이벤트도 추가하여 복원된 누적값까지 확인.
- Gold MERGE 전 실패/commit 후 응답 실패/checkpoint 전 실패를 failure hook으로 분리.
  같은 batch 재실행과 다른 batch의 같은 ID 재전달도 분리.
- event_time/W/window_end ±1 microsecond, 자정·timezone 동등성, 미래 편차·다른 장비의 W 영향.
- batch별 source event-time max·실제 W·operator별 state row/제거/drop 지표·Gold 결과 기록.
  drop 지표를 정확한 고유 late ID 수로 단정하지 않음.
- source/target 중복·NULL 키, unit/정책 version/다른 final은 기존 Gold 보존 후 실패.
- 빈 결과, 전량 소비, closure 직렬화, nonfinite 집계 거부.
- idle tail 미확정, 실제 event-time 진행 후 후속 trigger에서 확정. AvailableNow만으로 tail final 기대 금지.
- 향후 5m/1m: 12:04:30Z는 시작 12:00~12:04Z의 5개 window에 기여.
  단순 평균과 가중 평균이 달라지는 수치 포함.

failure hook은 로컬에서 한 번 실패 후 같은 checkpoint restart를 허용한다. process kill도 로컬
subprocess만 대상으로 한다. source retention 밖 offset은 조용히 latest로 건너뛰지 않고 실패한다.
file source는 한 파일씩 원자적으로 추가하고 batch 완료·W·Gold 조건을 bounded wait로 확인한다.
fixture batch 순서·lineage·failure·기대값은 payload 외부 envelope다. schema ID를 고정하지 않는다.

## 9. 신규 파일·정확한 경로와 첫 구현 단위

표의 모든 경로는 다음 절대 루트 기준이다. 이번에는 생성하지 않는다.
`/Users/uchan/Downloads/DE/02_project/ktd-worktrees/phase-3-gold-windows/`

| 루트 아래 신규 경로 | 역할 |
|---|---|
| `streaming/aggregations/streaming_policy.py` | immutable 10분 watermark/UTC 1분/Append 정책·검증 |
| `streaming/aggregations/spark_windows.py` | 주입된 Spark 입력의 native dedup→window plan |
| `streaming/sinks/gold_metric_sink.py` | 주입형 session/table, 자연키 검사·절대 결과 MERGE |
| `streaming/jobs/metric_aggregation.py` | 기존 decoder 연결, 단일 query/checkpoint lifecycle |
| `tests/fixtures/phase3/streaming_batches.json` | batch·lineage·failure·기대값 envelope |
| `tests/unit/phase3/test_streaming_policy.py` | 순수 정책·fixture 계약 검증 |
| `tests/integration/phase3/test_spark_windows.py` | 로컬 파일 source, state/window/idle/restart |
| `tests/integration/phase3/test_gold_recovery.py` | 로컬 Delta MERGE·commit/checkpoint 실패 경계 |

기존 제안 `streaming/sinks/phase3_event_ledger.py`는 초기 목록에서 제거한다.
실제 table/checkpoint는 설정 주입, 공통 Phase 2 config와 기존 순수 Python 코드는 보존한다.
후속 Python은 PEP 8을 따른다.

**첫 구현 단위는 streaming_policy.py + streaming_batches.json + test_streaming_policy.py다.**
1분 UTC/10분 watermark/Append, canonical 불변, batch별 기대값과 정상 restart/새 checkpoint/idle
구별만 검증한다. Spark Job/Delta/원장 구현은 포함하지 않는다. 기존 aggregate_minutes를 유한
canonical 입력의 oracle로 재사용한다. 완료 조건은 기존 79개 회귀 유지와 새 정책·fixture 검증
통과이며, 이를 실제 Spark state 검증으로 표현하지 않는다.

다음 단계는 **로컬 Spark의 dedup→window Append plan과 multi-batch count 3 검증**이다.
이 조합이 통과하기 전에 Gold adapter/cloud launcher를 만들지 않는다. 이후 로컬 Delta sink
재시도와 동일 checkpoint process restart를 검증한다. 로컬 결과는 DBR/UC/Kafka E2E가 아니다.
현재 pyproject에는 PySpark/Delta가 없다. 호환 로컬 runtime 준비는 후속 단계이며 이번에 설치·
dependency/lock 변경을 하지 않는다.

## 10. $10 비용 목표

- 이번 작업은 문서·로컬 검사·공식 문서 조회만 한다. Compute 실행·AWS 인프라 변경·cloud Job·
  table 생성은 하지 않는다. 준비/장애 시뮬레이션은 로컬 파일 source/checkpoint/Delta가 우선이다.
- 원장 없는 query 1개/sink 1개로 운영 경계를 줄인다. 이는 설계 판단이지 실제 달러 절감 실측이 아니다.
- $10은 향후 승인될 추가 유료 검증의 전체 목표다. 기존 서비스 계속 발생 비용의 포함 여부,
  compute 최소 과금·VM/DBU·스토리지·요청·네트워크·크레딧을 확인해야 한다.
  요율/사용량이 미확인이라 $10 이내 실행이나 최대 실행 시간을 보장하지 않는다.
- 미래 유료 검증은 로컬 통과 후 대상·최대 실행 시간·예상 총액·중단 방법을 준비해 별도 승인받는다.
  budget 알림은 강제 지출 한도가 아니다. 예산 내 원격 검증이 안 되면 E2E는 미검증으로 남긴다.

## 11. 이번 보완의 변경·검증 기록

1. 원장/두 query 기본안을 원장 없는 bounded dedup/단일 query로 변경.
2. canonical 불변 전제와 전 기간 충돌 재검증 제외를 명시.
3. 원장 대안의 수용·소비·Gold 완료 기준, TTL 없음, 부분 실패 복구를 구체화.
4. idle tail 미확정과 Delta commit/checkpoint 독립 경계를 명시.
5. 필수 5종 장애와 새 checkpoint·계약 위반 테스트를 분리.
6. $10 목표를 로컬 우선·비용 미확정으로 정의하고 첫 구현 단위를 축소.

이번 실제 검사 결과:

- 기존 `tests/unit/phase3`: `79 passed in 0.05s`.
  `PYTHONDONTWRITEBYTECODE=1`, pytest cache 비활성화, `/private/tmp` 전용 basetemp로 실행했다.
- 추적 파일 123개 SHA-256 대조: 변경 0개. 기존 집계·validated·공통 파일을 보존했다.
- 미추적 문서까지 포함한 `git diff --no-index --check /dev/null` 검사 통과.
- 전체 793개 회귀는 이번에 재실행하지 않았다. 새 Spark/Delta·장애 시뮬레이션은 설계만 했다.
- Compute 실행·AWS 변경·commit/push/merge 없음. 사용자 검토 전 구현을 시작하지 않는다.

## 12. 로컬 통합 구현 확인 — 2026-10-22

사용자의 Stage A–F 통합 구현 요청에 따라 생산 Row Comparison, 로컬 Delta Sink,
Job 3 plan 및 checkpoint 복구 검증을 구현했다. 최신 실행 근거는
`docs/reports/phase-3-verification.md`와 `docs/evidence/phase-3/`에 있다.
과거의 '미구현/검토 대기' 서술은 당시 이력이며 현재 상태는 새 보고서를 따른다.

단일 query/checkpoint를 유지하며 두 native window aggregation branch를 union한다.
물리 plan은 branch별 dedup과 aggregation을 가져 총 stateful operator 4개였다.
공통 DataFrame 변수를 사용해도 dedup state가 하나로 공유되지는 않았다.
1분 출력을 5분 종료까지 지연시키거나 query/checkpoint를 추가하는 대안은 채택하지 않았다.
Window 종류·branch 순서·grouping·stateful 구조는 동일 checkpoint에서 바꾸지 않는다.
기존 1분 실험 checkpoint를 이 구조에 재사용하지 않았고 테스트용 새 checkpoint만 만들었다.

로컬 Spark 4.0.0/Delta 4.0.0에서 Append-final → Mapper → 단일 MERGE가 실행됐다.
source 자연키 중복은 MERGE 전에 거부하고, 동일 target key의 값 차이는 MERGE 내부
raise_error로 트랜잭션을 중단한다. SQL WHEN MATCHED UPDATE 구문은 오류 발생용이며
상이한 값을 자동 UPDATE하는 경로가 아니다. 동일 값은 action 없이 NO-OP이다.

Sink는 초기 소규모 final batch를 driver에 materialize하고 단일 writer를 전제로 한다.
동시 writer의 자연키 유일성, 대량 부하, log retention 뒤 metadata 검사,
실제 Kafka/Registry/Databricks 및 강제 프로세스 종료는 미검증이다.
정상적인 별도 Python/JVM 프로세스 재시작은 동일 checkpoint에서 확인했다.
Kafka adapter는 기존 Confluent Avro 10필드/decoder를 재사용하지만 실제 broker 연결은 하지 않았다.
로컬 Delta 선택을 운영 catalog/권한/배포 검증으로 해석하지 않는다.
