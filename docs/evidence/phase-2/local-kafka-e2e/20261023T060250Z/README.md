# 실제 로컬 Kafka / Registry / Spark / Delta 검증

범위: LOCAL. MSK/Databricks/UC 증거가 아니다. production source SHA는
`3d772bdea261de4275f5888599d661b7f888fb73`이며 추가 테스트의 hash는 각
`provenance.json`에 있다. fixture 시간은 manifest 기준 통제된 clock이다.

## 재실행

Docker의 기존 KTD Kafka/Registry가 localhost:9092/8081에서 준비되어야 한다.
Broker/Registry 장애 사례는 캐시된 image로 별도 KTD 컨테이너를 만들고
127.0.0.1:19092/18081을 사용한다. 이 포트가 사용 중이면 먼저 확인한다.
새 container/topic/schema/checkpoint/data는 종료 후에도 보존한다.
다른 프로젝트는 조작하지 않는다. Java21/PySpark4.0.1/Delta4.0 환경을 재사용했다.

```sh
export JAVA_HOME=/tmp/ktd-8h-java/jdk-21.0.13+9/Contents/Home
export KTD_LOCAL_KAFKA_JARS="$(find /tmp/ktd-mission-jars /tmp/ktd-8h-jars -name '*.jar' | paste -sd, -)"
KTD_RUN_LOCAL_KAFKA=1 /tmp/ktd-8h-spark/bin/python -m pytest -q \
  tests/integration/test_job2_local_kafka_recovery.py \
  --basetemp=/tmp/ktd-local-<fresh-unique-run>
```

`/tmp` 경로는 이 Mac의 기존 검증 환경이다. 다른 환경에서는 Java/Python과
Kafka4.0.1 Spark connector/Kafka client3.9.1/pool2/Delta jar 위치를 맞춘다.
기존 pytest basetemp를 재사용하면 pytest가 그 디렉터리를 정리할 수 있으므로
항상 새 경로를 사용한다. 기존 checkpoint 삭제는 금지한다.

## 증빙 읽는 순서

1. `normal/manifest.json`: 실제10개 fixture bytes와 기대 canonical/오류.
2. `normal/raw-acks.json`: 실제 raw partition1 offset0..9 ACK.
3. `normal/result.json`: bytes/lineage/reason 대사, canonical4 fingerprint,
   validated5/quarantine5, replay 결과. count만으로 PASS하지 않는다.
4. `normal/batches.jsonl`: 최초10건, 동일 checkpoint 새 위치 offset10의
   오래된 중복1건(duplicate=late=1), fresh checkpoint11건 replay.
5. `normal/queries.jsonl`, `progress.jsonl`, `persistent-state.json`:
   실제 Kafka source/checkpoint/Delta log와 query UUID.
6. `../../local-recovery/20261023T060250Z/`: 실패와 복구별 증빙.

Spark native stateful watermark 연산을 새로 구현한 것이 아니다. 현재 Job2의
시간 분류 경계를 넘어 도착한 중복도 영속 Silver 조회로 방어함을 확인한다.
단일 writer/OSS Delta/local Kafka RF1 검증이며 다중 writer, RF3 MSK 장애,
executor 원격 Registry 접근, UC 권한/IO/DBR runtime은 별도 미검증이다.

[공식 Spark Kafka connector](https://spark.apache.org/docs/4.0.1/streaming/structured-streaming-kafka-integration.html)

화면 캡처: 최종 pytest PASS와 normal/result.json의 status/네 가지 fingerprint,
recovery의 같은 query id/다른 run id를 함께 캡처하면 실물 복구 사실을 보여준다.
토큰/환경변수/Compose secret은 캡처하지 않는다.
