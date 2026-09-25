# 성능·신뢰성 실험 계획

상태: 계획, 측정 결과 없음 · 2026-09-24

사용자가 Silver 쓰기 전략 비교를 향후 포트폴리오·자기소개서 근거로 남기도록 요청했다. 이 문서는 그 실험 명세이며 성능 리포트가 아니다.

## 우선 실험: Silver persistent idempotency

A: 매 micro-batch event_id MERGE. B: append + 주기적 명시적 dedup 및 compaction. 같은 dataset/seed, 부하, runtime, 리소스, checkpoint 시작점과 논리 결과를 비교한다.

측정: input/processing throughput, p50/p95/p99 latency, commit duration, batch duration, 파일 수·평균 크기, write amplification, 정리 비용, 정리 전후 duplicate key, validated 재전달, Gold 정합성.

실패 주입: 동일 event_id 중복, sink 성공 직후 재시작, checkpoint 복구, 장기 replay, 수정 payload 재처리, Silver/validated 중 한쪽 실패, 정리 작업 중단, backfill 동시 실행.

판정: 속도뿐 아니라 canonical 데이터 중복 노출 시점·범위와 누락 여부를 함께 기록한다. compaction이 중복 제거를 대신한다고 가정하지 않는다. B가 소비자에게 중복을 노출하면 추가 보호 비용까지 포함한다. 최종 채택은 실측 후 ADR로 남긴다.

## 나머지 실험

| 실험 | 후보 | 주요 관측 |
|---|---|---|
| Kafka partitions | 3 / 6 / 12 | throughput, lag, p95, key skew |
| 일반 consumer 병렬성 | 1 / 2 / 3 / 6 / 8 | 할당·idle, 처리량. Spark task 병렬성과 동일시하지 않음 |
| watermark | 5 / 10 / 20분 | delay 분포, late 비율, state 크기, 지연 |
| 5분 sliding 간격 | 30초 / 1분 / 5분 | state 크기, 계산 비용, 탐지 유용성 |
| Iceberg partition | day / day+bucket | scan량, pruning, 파일 수·크기, query latency |
| compaction | 전후, 파일 크기·주기 변경 | 비용, file count, query latency, writer 충돌 |
| reliability | RF=3, min ISR=2, acks=all | broker/consumer 중단, 쓰기 거부, 복구·유실·중복 |
| schema evolution | optional/default V2, breaking 타입 변경 | 등록 차단, mixed-version, V1/V2 backfill |

## 결과 기록 양식

| 항목 | 값 |
|---|---|
| 실행 날짜 / commit / runtime / connector | 미측정 |
| 하드웨어·cluster·비용 기준 | 미측정 |
| dataset / seed / 발생률 / 기간 / 반복 수 | 미측정 |
| warmup·측정 구간 / 지연 정의 | 미측정 |
| 대조안과 변경 변수 | 미측정 |
| 정상·중복·late·오류 입력 수 | 미측정 |
| 결과 / 편차 / 병목 / 실패 기록 | 미측정 |
| 정합성 충족 여부 / 한계 | 미측정 |
| 최종 선택 및 tradeoff | 실험 후 결정 |

포트폴리오 문장은 문제 → 대조 실험 → 실제 결과 → 비용·정확성 tradeoff → 선택 순서로 작성한다. 개선 퍼센트·500 events/s·p95 3초 등의 예시를 달성값으로 기입하지 않는다.
