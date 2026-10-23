# Local dual-sink recovery acceptance

最終実行: `8 passed in 310.97s`. 日本語? no: 모든 결과는 실제 로컬 Kafka/Registry,
Spark4.0.1/Delta4.0이며 MSK/UC 증거가 아니다.

각 실패 실행은 batch0 checkpoint commit 부재를 assert한 뒤 같은 checkpoint로
새 Spark 프로세스를 실행했다. query id는 같고 run id는 달랐다. 새 checkpoint의
별도 replay도 canonical payload/fingerprint와 출력 lineage 대사를 통과했다.

| 경계 | 결과 | 복구 후 추가 전달 | fresh replay 후 총 추가 전달 |
|---|---|---:|---:|
| before_merge | PASS | 0 | 10 |
| before_publish | PASS | 0 | 10 |
| broker | PASS | 0 | 10 |
| kill_after_ack | PASS | 10 | 20 |
| quarantine | PASS | 2 | 12 |
| registry | PASS | 0 | 10 |
| uncertain_ack | PASS | 1 | 11 |

추가 전달은 최초 기대 validated5/quarantine5 대비의 Kafka 중복이다. 유실로
바꾸어 해석하지 않으며, 전체 exactly-once 보장으로 표현하지 않는다.

- before_merge: 실제 invalid MERGE의 Spark/Delta AnalysisException. 저장 IO 장애
  전체를 재현한 것은 아니다. 그 뒤 정상 MERGE로 수렴했다.
- before_publish: Silver commit 뒤 validated produce 호출 직전 통제된 TimeoutError.
- uncertain_ack: 실제 broker ACK는 journal에 보이나 application callback 전달을
  억제했다. ACK 불확실성 timeout이 전파된 뒤 중복 전달을 허용하며 복구했다.
- quarantine: 실제 validated ACK 뒤 quarantine produce 직전 실패했다.
- kill_after_ack: 실제 sink ACK 후 checkpoint commit 전에 소유한 Driver process
  group만 SIGKILL했다. 새 프로세스가 같은 checkpoint에서 재처리했다.
- broker: 별도 Kafka container를 stop/start해 실제 producer delivery failure를
  만들었다. 입력은 먼저 materialize해 lazy source fetch 대기와 구분했다.
- registry: 별도 Registry container 중단으로 실제 HTTP 연결 실패를 만들었다.
  품질 Quarantine으로 바꾸지 않고 query가 실패했다. 정상 복구 후만 품질5건이다.

worker log의 예외, deliveries journal의 실제 ACK, persistent-state의 Delta log/
checkpoint를 함께 확인한다. 실험 이전 실패 로그는 experiment-history에 보존했다.
최종 값은 fixture와 모든 출력 bytes/원본 lineage/reason/event payload를 대사한 값이다.
