# P2-8D MSK output topic provisioning 사양과 실행 절차

작성일: 2026-10-17 KST. 기준 branch: `feat/phase2-validation-silver`,
HEAD: `8392b77041aa6324efe20d224e4e1fd30d9a776a`.
상태: **준비 문서. 생성·IAM 변경·EC2 시작·Compute 실행·발행은 미실행.**
기존 EC2 역할에는 output 생성 권한이 없어 실행 전 관리 주체 확정이 필요하다.
이 문서의 생성·발행 명령은 사용자 검토 후 별도로 승인된 실행에서만 사용한다.

## 1. 근거와 이번 확인 범위

- 저장소 `AGENTS.md`, 두 로컬 개발 계획서의 P2-8, `docs/runbook.md`의
  EC2/MSK client 절차, `docs/architecture/kafka-topics.md`,
  `docs/reports/phase-2-verification.md`와 현재 runtime/diagnostic 코드를 대조했다.
- 이번 AWS 읽기 전용 관측: STS 호출 주체, MSK cluster와 bootstrap,
  EC2 상태/profile, SSM 상태, 관련 IAM role의 trust principal·inline/attached
  policy·permissions boundary, 서울 MSK 공개 단가. 관측 시각은
  2026-10-17 12:59 UTC 전후다. role trust의 External ID나 secret 값은 기록하지 않는다.
- 사용자 제공 장애 근거: validated의 `UNKNOWN_TOPIC_OR_PART`(3),
  `topic_exists=false`, partition_count=0, ProduceRequest=0,
  Kafka 인증/metadata 연결 성공. 이번에 broker probe를 다시 실행한 결과는 아니다.
- EC2는 stopped이며 SSM describe-instance-information 결과는 빈 목록이었다.
  원격 명령을 전송하지 않았고 현재 topic 목록·partition·설치 파일은 미확인이다.
  stopped 상태는 CLI 미설치를 의미하지 않는다.
- Databricks `credentials get-credential`로 두 credential 참조를 재조회했지만
  token refresh 오류로 실패했다. Compute 실행이나 credential 재발급은 하지 않았다.
  config 참조와 IAM role 존재만으로 UC 연결을 새로 검증했다고 하지 않는다.

## 2. 확인된 관리 경로

| 항목 | 현재 근거 |
|---|---|
| 리전 / cluster | ap-northeast-2 / ktd-msk-serverless, ACTIVE / SERVERLESS, IAM 인증 활성 |
| cluster ARN | arn:aws:kafka:ap-northeast-2:048715026902:cluster/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2 |
| bootstrap | boot-aersevd2.c2.kafka-serverless.ap-northeast-2.amazonaws.com:9098 |
| 관리 EC2 | ktd-kafka-client / i-08e374efb0c31160d / t3.micro / stopped |
| profile → role | ktd-msk-producer-role → ktd-msk-producer-role, EC2 trust |
| 기존 접근 | Session Manager, Java Kafka CLI + AWS MSK IAM JAR + client.properties |
| 네트워크 | 기존 문서의 VPC 내부 TCP 9098 경로. 이번 SG/route 전체 재감사는 미실행 |
| Job 2 source 참조 | config/job2-runtime.json의 ktd-msk-consumer |
| Job 2 producer 참조 | 같은 파일의 ktd-msk-job2-credential |

로컬 AWS CLI 관리 API와 VPC 내부 Kafka Admin API를 구분한다.
AWS `list-clusters-v2`, `describe-cluster-v2`, `get-bootstrap-brokers`는
cluster를 확인한다. Serverless topic 상태는 VPC 내부 Kafka CLI/metadata로 확인한다.
MSK topic 관리 API는 현재 **Provisioned 전용**이므로 `aws kafka describe-topic`으로
Serverless topic 존재 여부를 판단하지 않는다.
[AWS topic API 지원 범위](https://docs.aws.amazon.com/msk/latest/developerguide/msk-topic-operations-information.html)

기존 Java Admin 인증 기준은 아래와 같다. 실제 EC2 파일/JAR/버전/경로는 접속 후
읽기 전용으로 확인한다. 기존 파일을 덮어쓰거나 JAR을 자동 재설치하지 않는다.

```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
```

Java는 default credential chain을 사용하므로 STS CLI의 주체와 Java의 주체가
반드시 같다고 가정하지 않는다. 환경변수/프로파일/JAAS role override 존재를
비밀값 출력 없이 점검한다. 별도 승인된 provisioning role을 선택한다면 기존
client.properties를 덮어쓰지 말고 별도 파일과 명시적 role/profile을 사용한다.
가정한 role로의 fallback을 막는 `awsAddDefaultProviders=false`도 검토한다.
[AWS IAM client의 credential 선택](https://github.com/aws/aws-msk-iam-auth)

## 3. 두 topic 생성 사양

| 설정 | validated | quarantine |
|---|---|---|
| 이름 | kitchen.sensor.validated | kitchen.sensor.quarantine |
| partitions | 3 | 3 |
| cleanup.policy | delete | delete |
| retention.ms | 604800000 (7일) | 604800000 (7일) |
| retention.bytes | -1 (시간 보존 기준 사용) | -1 (시간 보존 기준 사용) |
| key | equipment_id UTF-8 bytes | quarantine_id UTF-8 bytes |
| value | 기존 Confluent framing + Avro 10필드 | 기존 Quarantine JSON |
| replication factor | 생성 옵션 생략, Serverless 기본값 사용 후 실측 | 동일 |
| min.insync.replicas | 서비스 관리값 유지, 지정/변경하지 않음 | 동일 |
| compression.type | producer 기본 유지 | 동일 |
| max.message.bytes | 서비스 기본 1048588 유지 | 동일 |
| message.timestamp.type | 서비스 기본 CreateTime 유지 | 동일 |

7일은 이번 개발 수용·복구 실험을 위한 제안값이다. 영구 감사 보관이나 장기
downstream 재처리를 보장하지 않는다. retention.bytes=-1은 시간보다 먼저 용량
상한에 걸리는 것을 피하지만 보존 기간 내 데이터량에 따른 저장 비용 상한은 없다.
retention은 비동기 segment 정리 정책이므로 정확히 7일 시점에 삭제된다는 뜻도 아니다.
장기 보관이 필요하면 별도 저장/운영 정책을 정하고 기존 topic의 retention을
자동 변경하지 않는다. 이 문서는 raw retention을 바꾸지 않는다.

두 output은 append/replay 이력이 필요하므로 compact를 선택하지 않는다.
특히 validated key는 event_id가 아니라 equipment_id라 compaction을 dedup으로
사용하면 장비의 여러 이벤트를 잃을 수 있다. Kafka 중복 전달은 허용하고
downstream event_id 방어 및 Silver canonical 계약을 유지한다.

Serverless에서 수정 가능한 topic 속성만 지정한다. cleanup.policy는 생성 시에만
선택할 수 있고, broker 설정·segment.bytes·min ISR을 이 절차에서 변경하지 않는다.
RF는 AZ/subnet 수로 추정하지 않는다. CLI describe의 실측값을 증빙한다.
설계 문서의 validated 6 partitions는 성능 초기 후보이며 이번 명시적 요구 3을 적용한다.
[Serverless 설정 제한](https://docs.aws.amazon.com/msk/latest/developerguide/serverless-config.html),
[RF를 생략하는 공식 Serverless 생성 절차](https://docs.aws.amazon.com/msk/latest/developerguide/msk-serverless-create-topic.html)

## 4. provisioning과 runtime 권한 분리

이번 읽기 전용 IAM 조회 결과:

| 주체 | 확인된 범위 | 판단 |
|---|---|---|
| EC2 ktd-msk-producer-role | cluster Connect, raw만 CreateTopic/DescribeTopic/WriteData, AmazonSSMManagedInstanceCore | 두 output 생성/describe/config 조회 권한 없음 |
| ktd-msk-job2-producer-role | cluster Connect, 두 output DescribeTopic/WriteData, attached policy 없음 | runtime 발행용. CreateTopic 없음 유지 |
| ktd-databricks-msk-consumer-role | cluster Connect, raw DescribeTopic/ReadData, group DescribeGroup/AlterGroup, self-assume | output 읽기 검증 권한 없음 |

세 역할에 permissions boundary는 관측되지 않았다. SCP·session policy·resource
policy·실제 role assumption까지 유효 권한 전체를 증명한 것은 아니다.
현재 STS 호출자는 로컬 IAM user이며 이것을 Kafka CLI provisioning principal로
자동 전용하지 않는다. 별도 전용 관리 role을 권장하지만 새 role은 생성하지 않았다.
`ktd-msk-topic-provisioner-role`은 사용할 경우의 **제안 이름**이며 존재 확인값이 아니다.

관리 주체에 필요한 최소 Kafka action/resource 초안은 아래와 같다.
이는 **적용하지 않은 검토용 정책**이다. EC2에서 해당 role을 assume하려면 별도
STS 권한/trust도 필요하며 현재 EC2 role에는 그 권한을 확인하지 못했다.
정책 적용·trust 변경은 이번 작업에 포함하지 않는다.

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ConnectToTargetCluster",
      "Effect": "Allow",
      "Action": "kafka-cluster:Connect",
      "Resource": "arn:aws:kafka:ap-northeast-2:048715026902:cluster/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2"
    },
    {
      "Sid": "ProvisionAndInspectOnlyJob2Outputs",
      "Effect": "Allow",
      "Action": [
        "kafka-cluster:CreateTopic",
        "kafka-cluster:DescribeTopic",
        "kafka-cluster:DescribeTopicDynamicConfiguration"
      ],
      "Resource": [
        "arn:aws:kafka:ap-northeast-2:048715026902:topic/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2/kitchen.sensor.validated",
        "arn:aws:kafka:ap-northeast-2:048715026902:topic/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2/kitchen.sensor.quarantine"
      ]
    }
  ]
}
```

DeleteTopic, AlterTopic, AlterTopicDynamicConfiguration은 부여하지 않는다.
기존 topic 삭제·partition 증감·설정 변경을 수행하지 않기 때문이다.
DescribeTopicDynamicConfiguration은 kafka-configs 읽기에 필요하다.
Connect는 cluster ARN, 나머지 위 권한은 대상 topic ARN으로 한정한다.
[IAM action/resource 명세](https://docs.aws.amazon.com/msk/latest/developerguide/kafka-actions.html)

관리 API 조회자는 필요에 따라 kafka:ListClustersV2, kafka:DescribeClusterV2,
kafka:GetBootstrapBrokers 및 EC2/SSM/IAM 읽기 권한이 필요하다.
kafka: 관리 API 권한은 kafka-cluster: topic 생성 권한을 대체하지 않는다.
Session Manager 시작 권한과 EC2 agent용 AmazonSSMManagedInstanceCore도 별개다.

Job 2 runtime은 기존 Connect + DescribeTopic/WriteData를 유지한다.
향후 enable.idempotence를 활성화하면 cluster의 WriteDataIdempotently도 필요하지만,
현 코드에서는 명시하지 않으므로 이번에 자동 추가하지 않는다.
output 읽기는 별도 검증 주체에 ReadData와 필요한 group 권한을 부여하는 설계로
구분하고 현재 raw consumer로 읽을 수 있다고 가정하지 않는다.
[AWS client authorization 필요 권한](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control-use-cases.html)

## 5. 읽기 전용 preflight와 실행 전 조건

아래는 향후 실행할 명령이다. 먼저 로컬 관리 단말에서 확인한다.

```bash
export KTD_AWS_REGION=ap-northeast-2
export KTD_CLUSTER_ARN=arn:aws:kafka:ap-northeast-2:048715026902:cluster/ktd-msk-serverless/3a22c4d6-d923-4098-9ac4-a4746abf3b8a-s2
aws sts get-caller-identity
aws kafka describe-cluster-v2 --region "$KTD_AWS_REGION" \
  --cluster-arn "$KTD_CLUSTER_ARN"
aws kafka get-bootstrap-brokers --region "$KTD_AWS_REGION" \
  --cluster-arn "$KTD_CLUSTER_ARN"
aws ec2 describe-instances --region "$KTD_AWS_REGION" \
  --instance-ids i-08e374efb0c31160d \
  --query 'Reservations[].Instances[].{Id:InstanceId,State:State.Name,Profile:IamInstanceProfile.Arn}'
```

cluster/account/type/IAM/bootstrap이 위 대상과 다르면 멈춘다.
EC2 시작은 비용이 발생하는 별도 작업이며 이번에는 시작하지 않았다.
승인된 RUNNING client가 준비된 뒤 Session Manager로 접속한다.
SSH 개방, VPC/SG 변경, Databricks Compute 시작을 자동 대안으로 수행하지 않는다.

EC2의 bash에서 실제 기존 CLI와 properties를 확인해 변수를 설정한다.
properties 절대 경로는 미확인이므로 아래 placeholder를 실제 경로로 바꾼다.
CLI가 PATH에 없다면 기존 설치의 실제 절대 경로를 확인해 지정한다.

```bash
export MSK_BOOTSTRAP_SERVERS=boot-aersevd2.c2.kafka-serverless.ap-northeast-2.amazonaws.com:9098
export KTD_KAFKA_TOPICS="$(command -v kafka-topics.sh)"
export KTD_KAFKA_CONFIGS="$(command -v kafka-configs.sh)"
export KTD_CLIENT_PROPERTIES='/absolute/path/to/existing/client.properties'
test -x "$KTD_KAFKA_TOPICS"
test -x "$KTD_KAFKA_CONFIGS"
test -r "$KTD_CLIENT_PROPERTIES"
"$KTD_KAFKA_TOPICS" --version
aws sts get-caller-identity
```

위 test가 하나라도 실패하면 다음 단계로 진행하지 않는다. Java classpath의 IAM JAR,
인증 설정, 실제 선택되는 관리 주체와 권한을 확인한다. properties·환경변수 전체나
awsDebugCreds 원문 로그를 증빙에 출력하지 않는다. raw Describe는 선택한 관리
주체에 raw 권한도 있는 경우에만 보조 확인으로 사용한다.

대상 topic을 완전 일치 regex로 조회한다. timeout·권한 오류를 부재로 취급하지 않는다.

```bash
"$KTD_KAFKA_TOPICS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" \
  --describe --topic '^kitchen[.]sensor[.]validated$'
"$KTD_KAFKA_TOPICS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" \
  --describe --topic '^kitchen[.]sensor[.]quarantine$'
```

- 존재하면 partition 수·leader·replica·ISR을 기록하고 configs 조회로 진행한다.
  기대값과 다르면 멈춘다. 재생성하거나 alter하지 않는다.
- 명시적 UnknownTopic/부재 응답이면 대상 Describe 권한과 cluster 일치 여부를
  확인한 뒤 생성 후보로 둔다. 빈 list 출력만으로 부재를 확정하지 않는다.
- Authorization/SASL/timeout/응답 누락은 UNKNOWN으로 표시하고 생성을 중단한다.

기존 또는 생성 후 topic 설정 조회:

```bash
"$KTD_KAFKA_CONFIGS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" --entity-type topics \
  --entity-name kitchen.sensor.validated --describe --all
"$KTD_KAFKA_CONFIGS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" --entity-type topics \
  --entity-name kitchen.sensor.quarantine --describe --all
```

오래된 CLI가 --all을 지원하지 않으면 미확인 값을 기대값으로 채우지 않는다.
CLI 버전과 일반 describe 결과를 기록하고 유효 설정값 확인을 보류한다.

## 6. 생성 명령 — 승인 후 topic별로 판정

두 명령을 무조건 연속 실행하지 않는다. 각 topic의 부재와 생성 권한·비용을
확인한 경우에만 해당 topic 명령을 한 번 실행한다. 존재하는 topic은 건너뛴다.

```bash
"$KTD_KAFKA_TOPICS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" \
  --create --if-not-exists --topic kitchen.sensor.validated --partitions 3 \
  --config cleanup.policy=delete --config retention.ms=604800000 \
  --config retention.bytes=-1
```

```bash
"$KTD_KAFKA_TOPICS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" \
  --create --if-not-exists --topic kitchen.sensor.quarantine --partitions 3 \
  --config cleanup.policy=delete --config retention.ms=604800000 \
  --config retention.bytes=-1
```

--if-not-exists는 동시 생성 시 재생성을 방지하지만 기존 설정 일치를 증명하지 않는다.
성공 메시지나 exit 0만으로 완료 처리하지 않고 앞 절의 Describe/config를 다시 조회한다.
partition 0/1/2, 유효 leader, replica/ISR, 실측 RF, retention/cleanup을 기록한다.
설정 불일치, 한쪽만 성공, timeout이면 기존 topic을 보존하고 상태를 다시 읽는다.

## 7. metadata → ACK → 읽기 대사 → Job 2 복구 순서

1. 관리 주체로 두 topic의 Describe/config를 완료한다. 한쪽 성공을 양쪽 성공으로
   표시하지 않는다. 생성 시각·principal 참조·cluster·실측 설정을 기록한다.
2. 별도로 승인된 Databricks 실행에서 실제 runtime producer credential로 metadata를
   확인한다. 두 topic 모두 error 없음, 존재 true, partition 수 3, ID 0/1/2가 필요하다.
   현행 `scripts/run_job2_databricks.py`의 `--check-kafka-metadata`와
   `--runtime-config config/job2-runtime.json` 조합은 validated만 확인한다.
   양쪽 확인에는 아래 notebook snippet을 사용한다. Job 2 코드는 수정하지 않는다.
3. 발행은 데이터 변경이므로 별도 승인된 시도에서만 수행한다. validated는 기존
   Avro 10필드·equipment_id key, quarantine은 기존 builder/publisher의 정규 JSON과
   quarantine_id key를 사용한다. console-producer의 임의 문자열을 넣지 않는다.
   validated subject lookup과 실제 serializer 연결도 사전 조건이다.
4. topic별 callback 성공, error 없음, bool이 아닌 정수 offset >= 0,
   topic/partition/offset을 기록한다. acks=all과 자동 생성 비활성화를 유지한다.
   flush=0은 ACK를 대체하지 않는다. timeout은 미배달 확정이 아닌 결과 미확정이므로
   무조건 재발행하지 않는다.
5. 별도 읽기 권한을 가진 검증 주체로 ACK 위치를 읽어 key/payload/identity를 대사한다.
   현재 raw consumer role에는 output ReadData가 없어 기존 검증 스크립트의 consumer
   참조로 읽을 수 있다고 가정하지 않는다. 기존 group/checkpoint를 진행시키지 않는다.
6. 마지막으로 기존 checkpoint에서 Job 2 미완료 batch를 재개해 canonical 한 행,
   validated 누락 없음, 충돌 validated 없음, 같은 lineage/quarantine_id를 확인한다.
   output 생성만으로 과거 실패 batch 복구가 입증되지는 않는다.

현행 `--check-kafka-produce`는 기존 Silver의 event_id
`30008ab5-8a00-43d9-bc4a-94493ceaaea7`를 validated로 재발행하는 **쓰기 작업**이다.
두 topic 공통 probe나 checkpoint 재시작 시험이 아니다. 이번에는 실행하지 않았다.

양쪽 metadata는 후속 승인된 기존 notebook에서 아래 읽기 전용 snippet으로
확인할 수 있다. 새 Compute를 시작하는 명령이 아니며 runtime 파일은 수정하지 않는다.
저장소 import 경로가 준비되고 `spark`가 있는 기존 실행 환경에서만 사용한다.
credential 조회 실패는 별도 오류로 처리하고 설정 dict·원문 예외를 로그에 붙이지 않는다.

```python
import json

from confluent_kafka import Producer
from streaming.jobs import runtime_config as runtime

settings = runtime.load_runtime_config("config/job2-runtime.json")
options = settings.kafka.producer_options(runtime.get_dbutils(spark))
options = runtime.runtime_producer_options(options, spark=spark)
options["log_level"] = 0
assert options["allow.auto.create.topics"] is False
producer = Producer(options)
try:
    for topic in (settings.job.validated_topic, settings.job.quarantine_topic):
        metadata = producer.list_topics(topic=topic, timeout=10)
        detail = metadata.topics.get(topic)
        partitions = detail.partitions if detail is not None else {}
        error = detail.error if detail is not None else None
        result = {
            "topic": topic,
            "metadata_ok": detail is not None and error is None,
            "topic_error_code": error.code() if error is not None else None,
            "partition_ids": sorted(partitions),
            "partition_count": len(partitions),
        }
        print(json.dumps(result))
        assert result["metadata_ok"] and sorted(partitions) == [0, 1, 2]
        assert all(p.error is None and p.leader >= 0
                   for p in partitions.values())
finally:
    del producer
```

이 snippet은 produce/flush/streaming 시작을 호출하지 않는다. 실패한 topic이 있으면
발행 단계로 진행하지 않는다. 관리 CLI의 replica/ISR 확인을 대체하지 않는다.

증빙에는 안전한 metadata, ACK 좌표, schema/identity 대사, batch/checkpoint 결과를
남긴다. token/secret/JAAS credential 값을 기록하지 않는다. 포트폴리오에는 양쪽
3 partitions와 실제 ACK 화면을 촬영하고 미실행 복구 시험과 구분한다.

## 8. 실패 시 중단·재개 기준

| 증상 | 대응 |
|---|---|
| Authorization | principal과 대상 ARN/action 확인. runtime에 CreateTopic 추가 금지 |
| SASL 실패 | JAR/classpath, 시각, credential chain/expiry 확인. topic 부재로 해석 금지 |
| timeout / leader 부재 | VPC/SG/DNS/advertised listener와 서비스 상태 조회. 즉시 재생성 금지 |
| TopicExists / if-not-exists 성공 | Describe/config 재조회. 사양 일치 시에만 완료 |
| partition/cleanup/retention 불일치 | 기존 topic 보존 후 중단. delete/recreate/alter 자동 실행 금지 |
| 한쪽만 생성됨 | 생성된 topic은 재사용. 다른 쪽 원인 해결 후 부재 재확인 |
| UnknownTopic 지속 | cluster/bootstrap/정확한 topic 이름·runtime 권한 확인 후 metadata 재조회 |
| ACK timeout | 미확정 시도 식별자 보관. 읽기 대사 후 재시도 판단 |
| 읽기 권한 없음 | ACK와 읽기 결과를 분리하고 미검증 표시. runtime 권한 확장으로 우회하지 않음 |

## 9. AWS 비용과 기존 크레딧

AWS Price List `pricing get-products`를 service-code=AmazonMSK,
location=Asia Pacific (Seoul), endpoint region=us-east-1로 읽기 조회했다.
전체 결과는 한 페이지이며 Serverless OnDemand 유효일 2026-10-01의 단가다.

```bash
aws pricing get-products --region us-east-1 --service-code AmazonMSK \
  --filters 'Type=TERM_MATCH,Field=location,Value=Asia Pacific (Seoul)' \
  --max-results 100 --no-paginate --output json
```

실행 시 NextToken이 있으면 남은 페이지도 조회한다. 이번 응답에는 없었다.

| 과금 축 | USD 단가 | SKU |
|---|---:|---|
| partition-hour | 0.0018 | 4F5XCJGS3VV44BDP |
| cluster-hour | 0.92 | RRATBJ626ZR2MWQS |
| data-in GB | 0.123 | HCT3RU6BTA73BFPP |
| data-out GB | 0.061 | 4SVK4BMPMQKH5662 |
| storage GB-month | 0.114 | PJDPDU97V2KDJM32 |

두 topic의 6 partitions 증분은 6×0.0018=$0.0108/시간,
24시간=$0.2592, 730시간=$7.884다. 세금·환율·다른 리소스 비용은 제외한다.
빈 topic도 partition-hour 비용이 발생하며 EC2/Job 2를 멈춰도 없어지지 않는다.
기존 cluster $0.92/시간(730시간=$671.60)은 이번 두 topic 생성의 증분과 구분한다.
partition 수에 RF를 곱해 중복 계산하지 않는다. 입출력 GB·보관 GB-month 변동비와
EC2 실행·EBS·public IPv4·전송 비용은 별도다. retention.bytes=-1은 무료 저장 한도가 아니다.
[MSK 과금 체계](https://aws.amazon.com/msk/pricing/)

크레딧은 적용 가능한 청구에 자동 충당되지만 기한·잔액·대상 상품·계정/조직 공유
조건에 따른다. 이번에는 Billing을 조회하지 않아 MSK/EC2 적용 여부와 잔액은 미확인이다.
Billing → Credits의 해당 크레딧 상세에서 Active, Expiration date,
Applicable products, Estimated amount remaining을 확인하고 Bills의 실제 충당과
대사한다. credit ID/코드를 문서에 붙이지 않는다. AWS 크레딧이 Databricks 직접 계약과
Confluent 비용에도 적용된다고 가정하지 않는다. 소진·만료·대상 외 비용은 별도
청구되므로 예산은 크레딧 차감 전 비용으로 판단한다.
[AWS 크레딧 조건과 확인 화면](https://docs.aws.amazon.com/awsaccountbilling/latest/aboutv2/useconsolidatedbilling-credits.html)

## 10. 완료 범위와 다음 논리 작업 하나

완료: 현재 관리 경로·role 권한 차이의 읽기 확인, 생성 사양·CLI·불일치 시 중단·
metadata/ACK/복구 검증 순서·비용 문서화. 제품 코드·설정은 변경하지 않았다.
미완료: provisioning 주체/권한 준비, EC2 내부 CLI/JAR/properties 실제 경로,
broker의 두 topic 최신 상태, Databricks credential 참조 재확인, 크레딧 적용 확인.

다음 한 단위는 사용자가 선택한 관리 주체의 권한과 client를 확인한 뒤 양쪽 topic의
읽기 preflight다. 생성·IAM 변경은 별도 명시 지시를 받은 뒤 수행한다.
로컬 테스트를 cloud provisioning 또는 P2-8 완료 증빙으로 표현하지 않는다.

문서 검증: bash 블록 7개의 `bash -n`, 정책 JSON 파싱·action 범위,
metadata Python snippet 구문, 생성 flag·partition 수, 비용 산식을 확인했다.
`git diff --check`와 새 미추적 문서의 별도 whitespace 검사도 통과했다.
시작 시 존재한 추적·미추적 파일의 SHA-256은 모두 동일했다.
문서만 추가했으므로 전체 unit 회귀는 재실행하지 않았다. 예시의 Kafka/Python
명령은 원격 실행하지 않았으며 구문 검사를 runtime 검증으로 간주하지 않는다.

## 11. P2-8e 읽기 전용 preflight 결과 — 2026-10-17

판정: **로컬 설정·사양 대조 정상, 실제 생성 실행은 차단, 두 topic 존재는 미확인.**
기존 문서에 결과를 기록할 수 있어 별도 P2-8e 파일은 만들지 않았다.
아래는 이번 재조회 결과이며 앞 절의 과거 관측과 구분한다.

### 저장소와 확인 파일

- branch `feat/phase2-validation-silver`, HEAD
  `f2fa1f275e04535ccb85646b63d32543b6f024ef`.
- P2-8d는 위 HEAD에서 커밋됐으며 시작 시 해당 문서 diff는 없었다.
  다른 기존 수정 5개와 미추적 파일은 보존했다.
- AGENTS.md, platform.md, data-contract.md, 두 로컬 계획서의 P2-8을 확인했다.
  platform.md의 Job 2 미구현·복구 미결정 표현은 현재 코드보다 오래된 설명이다.
  이번에는 그 표현을 현재 구현 상태의 증거로 사용하거나 광범위하게 수정하지 않았다.
- `config/job2-runtime.json`과 `config/job2-runtime.example.json`의 broker/region,
  SASL_SSL/AWS_MSK_IAM, 세 topic 이름, source credential `ktd-msk-consumer`,
  producer credential `ktd-msk-job2-credential`이 일치했다.
- `streaming/jobs/runtime_config.py`, `sensor_validation_job.py`,
  `scripts/run_job2_databricks.py`, `tests/unit/test_job2_databricks_entrypoint.py`를
  대조했다. source는 databricks.serviceCredential, Python producer는
  OAUTHBEARER/oauth_cb 경로다. Java Admin의 AWS_MSK_IAM과 API별 표현 차이이며
  자격 증명 이름을 임의 변경할 이유가 아니다. acks=all과 자동 생성 비활성화를 유지한다.
- `infra/docker/compose.yml`, `.env.example`, `kafka/init-topics.sh`를 확인했다.
  init script는 PLAINTEXT 로컬 raw 전용이고 RF=1·생성 명령을 포함한다.
  MSK Admin preflight로 실행하지 않았다. 저장소에서 client.properties 실물은
  발견하지 못했다. 이것은 EC2에도 파일이 없다는 증거가 아니다.

### AWS 읽기 전용 결과와 역할별 권한

AWS 기존 인증은 성공했다. STS의 현재 관리 API 호출 주체는 IAM user `chan`이다.
MSK/IAM 재조회 시작 시각은 2026-10-17 13:51:44 UTC (22:51:44 KST)다.

| 항목 | 이번 결과 |
|---|---|
| cluster | ktd-msk-serverless / ACTIVE / SERVERLESS / ap-northeast-2 |
| ARN | 본문 §2의 cluster ARN과 동일 |
| 인증·bootstrap | IAM 활성, §2의 boot-aersevd2...:9098과 동일 |
| EC2 | i-08e374efb0c31160d / stopped / t3.micro |
| profile 실제 role | ktd-msk-producer-role |
| SSM 관리 인스턴스 조회 | 대상 필터 결과 빈 목록. 현재 원격 CLI 실행 가능성을 확인하지 못함 |
| EC2 role | cluster Connect, raw만 CreateTopic/DescribeTopic/WriteData, AmazonSSMManagedInstanceCore |
| Job 2 producer role | 두 output DescribeTopic/WriteData, cluster Connect. CreateTopic 없음 |
| source consumer role | raw DescribeTopic/ReadData, group DescribeGroup/AlterGroup, self-assume |
| boundary | 위 세 role에서 미관측 |

기존 EC2 role에는 output CreateTopic/DescribeTopic/DescribeTopicDynamicConfiguration이
없다. Job 2 role의 CreateTopic 부재는 의도한 분리이며 runtime 권한 결함으로 보지 않는다.
`ktd-msk-topic-provisioner-role`은 조회된 MSK 관련 역할 목록에 없고 여전히 제안명이다.
현재 관측한 실행 후보는 EC2 profile role이지만 권한이 부족하며, 별도 관리
principal은 선택·검증되지 않았다. 로컬 IAM user를 Kafka 실행 주체로 전용하지 않았다.
SCP·session policy·cluster policy와 실제 Kafka 요청의 유효 권한은 미검증이다.
이는 조회 정책의 범위 판정이며 broker가 반환한 AccessDenied 실행 증빙은 아니다.

UC credential의 이름과 코드 전달 경로는 로컬에서 일치한다. 이번에는 Databricks
API/Compute를 호출하지 않았으므로 credential→IAM role의 실제 연결·토큰 발급은
새로 검증하지 않았다. 기존 §1의 token refresh 실패를 성공으로 덮어쓰지 않는다.

### 사양·실행 가능 조건과 미검증 범위

§3의 partitions=3, cleanup.policy=delete, retention.ms=604800000,
retention.bytes=-1은 사용자 요구와 생성 명령에 일치한다. cleanup/retention은
Serverless 지원 설정이고 RF는 옵션 생략, min ISR은 미지정으로 유지한다.
§4 정책 초안의 CreateTopic은 wildcard 없이 두 정확한 output ARN에만 제한됐다.
정책 초안과 실제 적용 정책은 다르다. 실제 topic 설정/RF/ISR은 조회하지 못했다.
[재확인한 Serverless 설정 제한](https://docs.aws.amazon.com/msk/latest/developerguide/serverless-config.html),
[공식 생성 예시](https://docs.aws.amazon.com/msk/latest/developerguide/msk-serverless-create-topic.html)

실행 변수는 MSK_BOOTSTRAP_SERVERS, KTD_KAFKA_TOPICS, KTD_KAFKA_CONFIGS,
KTD_CLIENT_PROPERTIES이며 IAM JAR이 classpath에 있어야 한다. EC2의 실제 CLI/JAR
버전·절대 경로와 properties 경로는 문서화되지 않았고 §5 placeholder 상태다.
stopped EC2를 시작하거나 SSM 원격 명령을 보내지 않아 그 안을 검사하지 않았다.
개발자 Mac에서 private broker를 직접 probe하지 않았으며 두 output 모두 존재
**미확인**이다. 이전 validated 부재 보고도 이번 최신 metadata 확인으로 대체하지 않았다.
§7 notebook metadata 예시는 토큰 획득에 Spark scalar UDF를 사용하므로 이번
Compute 실행 금지 범위에서는 사용하지 않았다.

생성 전에 필요한 최소 조건은 별도 관리 principal 선택 및 두 output에 한정된
Connect/CreateTopic/DescribeTopic/DescribeTopicDynamicConfiguration 권한 확인,
승인된 가용 VPC client, 실제 CLI/JAR/properties 경로 확인, broker Describe/config
결과 확인이다. 기존 topic이면 재생성하지 않고 불일치 시 멈춘다. 부재를 확인한
topic만 별도 명시적 생성 지시 후 대상이 된다. 이번에는 어떤 조건도 자동 변경하지 않았다.

checkpoint는 두 설정 모두 `/Volumes/ktd/silver/checkpoints/job2/sensor-validation`이다.
Job 2 전용 경로 및 Job 1 재사용 거부 테스트를 확인했다. 설정에 bronze_checkpoint가
없으므로 실제 배포의 Job 1 경로와 중복 여부는 별도 확인이 필요하다. 현재 checkpoint
commit 파일, raw earliest offset/retention은 조회하지 않았다. topic 준비만으로
재시작 누락 방지·부분 실패 복구·기존 원본 재처리 가능성이 증명되지는 않는다.

### 실행 명령과 검증 결과

실행한 AWS 조회는 다음뿐이며 모두 성공했다. 계정/role secret 값은 출력하지 않았다.

- `aws sts get-caller-identity`
- `aws kafka describe-cluster-v2`, `get-bootstrap-brokers` (본 cluster ARN)
- `aws ec2 describe-instances` (본 instance ID)
- `aws ssm describe-instance-information` (본 instance ID 필터)
- `aws iam get-instance-profile`, `list-roles` (MSK 이름 필터)
- 위 세 role의 `get-role`, `list-role-policies`, `get-role-policy`,
  `list-attached-role-policies`, attached policy의 `get-policy`, `get-policy-version`

로컬 검증 명령:

```text
.venv/bin/python -m streaming.jobs.sensor_validation_job --check-config --runtime-config config/job2-runtime.json
.venv/bin/python -m streaming.jobs.sensor_validation_job --check-config --runtime-config config/job2-runtime.example.json
각각 config OK (offline; cloud access not checked), exit 0

.venv/bin/python -m pytest -q tests/unit/test_runtime_config.py::test_job1_checkpoint_cannot_be_reused tests/unit/test_runtime_config.py::test_service_credential_source_matches_job1_options tests/unit/test_runtime_config.py::test_driver_credential_failure_never_starts_query tests/unit/test_job2_databricks_entrypoint.py::test_example_routes_distinct_credentials --basetemp=/tmp/ktd-p2-8e-unit
4 passed, 1 warning in 0.15s
```

각 config 검사와 pytest에서 기존 Authlib/httpx deprecation 경고가 있었다.
정책 JSON/생성 옵션·bash 구문·10필드 스키마 정적 검사와 git diff --check도 통과했다.
기존 문서 본문은 prefix 비교로 보존을 확인했고 다른 기존 파일은 SHA-256이 같았다.
정상은 로컬 참조·계약·설정 검사, 준비 부족은 관리 권한·실경로 미확인,
실제 topic 조회·설정 일치·ACK·재시작은 미검증으로 구분한다.
제품 코드·운영 설정·데이터·offset·checkpoint·AWS 리소스·IAM·다른 Phase를
변경하지 않았다. 문서의 이번 결과만 추가하고 사용자 검토 대기에서 멈춘다.

## 12. P2-8f 실행 경로 결정 — 2026-10-20

**1순위: 기존 EC2를 잠시 사용하고, 기존 EC2 role에 두 output 전용 임시 inline
policy를 추가한 뒤 회수한다.** 정책 적용·EC2 시작/중지·topic 생성은 사용자 승인
후의 별도 작업이다. 이번에는 조회·시뮬레이션·문서 검증만 수행했다.
Job 2 producer role에는 CreateTopic을 추가하지 않는다.

### 12.1 현재 관측과 관리 주체

branch/HEAD는 `feat/phase2-validation-silver` /
`f2fa1f275e04535ccb85646b63d32543b6f024ef`다. 시작 시 이미 있던 P2-8e 문서
추가분과 수정 5개·미추적 파일을 보존한다. 이번 절만 추가한다.
AGENTS.md, P2-8d/e, platform.md, data-contract.md, 계획서 P2-8, infra와
Job 2 runtime/auth 코드를 재확인했다. 구현/설정/checkpoint를 바꾸지 않는다.

2026-10-20 03:14:58 UTC (12:14:58 KST)부터 기존 인증으로 읽기 조회했다.

| 대상 | 이번 관측 |
|---|---|
| 관리 API 호출자 | arn:aws:iam::048715026902:user/chan |
| chan 정책 | AdministratorAccess, IAMUserChangePassword, AmazonS3FullAccess; inline·그룹 없음 |
| EC2 role | ktd-msk-producer-role, EC2 trust, raw 전용 기존 권한과 AmazonSSMManagedInstanceCore |
| Job 2 role | ktd-msk-job2-producer-role, output DescribeTopic/WriteData와 cluster Connect |
| boundary·Deny | 위 user/두 role에 boundary 없음. 조회한 identity 정책에 명시적 Deny 없음 |
| Organizations | describe-organization이 AWSOrganizationsNotInUseException 반환. 이 계정은 조직 미사용으로 관측돼 상속 SCP 대상이 아님 |
| MSK cluster policy | get-cluster-policy는 NotFoundException. topic 부재를 뜻하지 않음 |
| MSK | 기존 ARN/bootstrap 유지, ACTIVE/SERVERLESS, IAM 활성 |
| EC2 | i-08e374efb0c31160d, stopped, Linux/UNIX x86_64 t3.micro |
| 보존 디스크 | vol-07bfdb021de1900f2, gp3 8 GiB, 기존 연결 유지 |

chan은 실제 Kafka 관리 Action도 identity 정책상 허용되지만 AdministratorAccess
주체라 최소 권한 실행안에 적합하지 않다. chan은 승인 후 일회성 IAM/EC2 관리 API
조작 주체로만 사용하고 user key/secret을 EC2에 복사하거나 새 access key를 만들지 않는다.
Kafka Admin 주체는 EC2 instance profile의
`arn:aws:iam::048715026902:role/ktd-msk-producer-role`로 결정한다.
기존 raw 권한은 남으므로 전체 role이 오직 네 Action만 갖는다고 주장하지 않는다.
이번에 추가할 권한 범위만 두 output 관리 최소 집합으로 제한한다.

요청의 네 줄 `kafka-cluster`에는 구체 Action이 없으므로 기존 §4의 Connect,
CreateTopic, DescribeTopic, DescribeTopicDynamicConfiguration으로 해석했다.
네 Action만으로 생성/조회/config 조회를 설계하고 produce/consume 시험은 분리한다.

### 12.2 IAM Policy Simulator 결과

현재 principal 정책은 `simulate-principal-policy`, 초안 단독의 범위는
`simulate-custom-policy`로 평가했다. Connect는 정확한 cluster ARN, 나머지는
각 output topic ARN을 하나씩 지정해 평가했다. 유효 입력 기준 결과는 다음과 같다.

| 평가 주체 | Connect | CreateTopic (양쪽) | DescribeTopic (양쪽) | DescribeTopicDynamicConfiguration (양쪽) |
|---|---|---|---|---|
| chan 현재 | allowed | allowed | allowed | allowed |
| EC2 role 현재 | allowed | implicitDeny | implicitDeny | implicitDeny |
| Job 2 role 현재 | allowed | implicitDeny | allowed | implicitDeny |
| EC2 role + §4 초안 (미적용) | allowed | allowed | allowed | allowed |

principal 4종×7조합=28개, 초안 단독의 3 resource×4 action=12개로 총 40개를
평가했다. MissingContextValues는 모두 빈 목록이었다. 초안 단독은 output 두 곳의
CreateTopic만 허용하고 raw CreateTopic 및 raw/output의 WriteData·DeleteTopic·
AlterTopicDynamicConfiguration을 implicitDeny했다. 실제 EC2 role의 기존 raw
권한까지 이 초안으로 제한되는 것은 아니다.
초기 CLI의 policy-input-list 전달 형식 오류(ValidationError/InvalidInput)는
`--cli-input-json`으로 교정했다. 이를 권한 거부나 정책 적용 결과로 해석하지 않는다.

시뮬레이션은 IAM 평가 근거다. 네트워크·실제 Java credential 선택·broker 인증과
권한 확인의 성공 증거는 아니다. 서비스의 모든 조건을 재현하지 않았고, 요청 시점에
변경된 정책·session 제한 등은 실제 실행 전에 다시 확인한다.
[IAM Simulator의 범위와 한계](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_testing-policies.html)

### 12.3 실행 환경 후보 비교

| 항목 | A. 기존 EC2 잠시 사용 (선정) | B. 기존 가용 VPC 환경 재사용 | C. 로컬 사설 연결 재사용 |
|---|---|---|---|
| 접근 근거 | 같은 KTD VPC의 local route, MSK SG가 client SG의 TCP 9098 허용 | KTD VPC EC2 조회는 stopped client 한 대뿐, SSM Online 없음 | route get 10.20.0.1 결과 default/en0. 사설 경로 미확인 |
| 인증 | EC2 profile의 임시 자격 증명 + Java IAM client | 해당 환경 role/credential 미확인 | chan 자체 권한은 있지만 private 연결·Java credential 준비 미확인 |
| CLI 준비 | 과거 사용 이력 있음. 현재 실제 경로/JAR/버전 미확인 | 재사용 가능한 client를 확인하지 못함 | 로컬 kafka-topics/configs는 PATH에서 발견 못함 |
| 추가 권한 | EC2 role의 output 관리 임시 policy, 운영자의 start/stop/SSM | 발견된 환경에 따라 달라져 미확정 | 기존 경로 없으면 새 VPN/터널이 필요하나 이번 기본안에서 제외 |
| 신규 리소스 | EC2/VPC/NAT/VPN/bastion 신규 생성 없음 | 새 환경을 만들지 않는 조건 | 새 네트워크 자원 만들지 않는 조건 |
| 비용 | 실행 시간 EC2·public IPv4 가능 비용, 기존 EBS 지속 | 이미 실행 중이면 증분이 낮을 수 있으나 해당 환경 미확인 | 경로가 이미 있으면 compute 증분은 작지만 실제 경로 미확인 |
| 종료 후 | EC2 중지로 compute 종료, EBS/MSK/생성 topic 과금 유지 | 원래 환경의 지속 비용과 구분 필요 | 기존 연결 요금이 있다면 지속 |
| 검증 범위 | 승인 후 broker Describe/config/Create가 가능할 설계 | 경로·인증·CLI 확인 전 실물 검증 불가 | 실제 사설 경로 확인 전 실물 검증 불가 |

KTD VPC는 `vpc-0e0dc19b3825259f0`, client SG는 `sg-0ccc0d9fadac1d090`이며
MSK SG `sg-0ce6cb2afcc2cc2ca`의 9098 ingress에 명시돼 있다. client subnet
`subnet-094cde0457fcc97e7`의 route table에는 VPC local과 기존 IGW default route가
있다. 이것만으로 NACL·egress·DNS·SSM outbound·broker listener 도달까지 보장하지 않는다.
로컬 route는 sandbox 안에서 읽기가 거부돼 같은 읽기 확인을 허용된 네트워크
환경에서 수행했다. en0 default만으로 VPN이 전혀 없다고 단정하지 않는다.

B의 조회 범위는 KTD VPC EC2와 계정 SSM 관리 인스턴스다. ECS/Lambda 및 다른
VPC/Databricks 실행 환경 전체를 전수 조사한 것은 아니다. 확인되지 않은 환경을
무료 가용 client로 가정하지 않는다. 현재 확인된 자산 기준으로 A가 변경 종류와
준비 불확실성을 가장 작게 하므로 선정했다. 모든 가능한 환경 중 절대 최저 비용을
증명한 것은 아니다. 별도 provisioning role 생성·trust 설정도 이번 기본안에는 불필요하다.

### 12.4 최소 정책과 사용자 승인 후의 관리 단말 명령

**최소 정책 JSON은 §4의 실제 cluster ARN과 두 정확한 topic ARN을 그대로 사용한다.**
기존 raw policy를 편집하지 않고 아래 새 이름의 임시 inline policy로 별도 적용한다.
output WriteData/ReadData, DeleteTopic, AlterTopic, config 변경 권한은 추가하지 않는다.
Connect는 이미 있지만 JSON에 명시해 §4 초안을 독립적으로 검토 가능하게 유지한다.
권한은 정리 단계에서 회수해야 하며 이번 초안에는 자동 만료 조건이 없다.

다음 블록은 **승인 후** chan의 기존 인증으로 실행할 순서다. 이번에는 실행하지 않았다.
§4 JSON을 관리 단말의 `/tmp/ktd-output-topic-admin.json`에 저장하고 파싱·scope 확인 후
사용한다. 이 파일은 실행 시 만들 임시 파일이며 저장소 새 파일이 아니다.

```bash
export KTD_AWS_REGION=ap-northeast-2
export KTD_CLIENT_INSTANCE=i-08e374efb0c31160d
export KTD_ADMIN_ROLE=ktd-msk-producer-role
export KTD_TEMP_POLICY=ktd-p2-output-topics-admin-temporary
aws sts get-caller-identity --query Arn --output text
aws iam get-role-policy --role-name "$KTD_ADMIN_ROLE" \
  --policy-name "$KTD_TEMP_POLICY"
```

get-role-policy가 **NoSuchEntity**일 때만 새 이름으로 진행한다. 이미 같은 이름이 있으면
중단해 소유/본문을 확인하며 덮어쓰지 않는다. AccessDenied나 연결 실패도 중단한다.
승인 범위가 정책 적용·EC2 시작까지 포함할 때에만 다음을 실행한다.

```bash
aws iam put-role-policy --role-name "$KTD_ADMIN_ROLE" \
  --policy-name "$KTD_TEMP_POLICY" \
  --policy-document file:///tmp/ktd-output-topic-admin.json
aws iam get-role-policy --role-name "$KTD_ADMIN_ROLE" \
  --policy-name "$KTD_TEMP_POLICY"
aws ec2 start-instances --region "$KTD_AWS_REGION" \
  --instance-ids "$KTD_CLIENT_INSTANCE"
aws ec2 wait instance-running --region "$KTD_AWS_REGION" \
  --instance-ids "$KTD_CLIENT_INSTANCE"
aws ec2 describe-instance-status --region "$KTD_AWS_REGION" \
  --instance-ids "$KTD_CLIENT_INSTANCE"
aws ssm describe-instance-information --region "$KTD_AWS_REGION" \
  --filters "Key=InstanceIds,Values=$KTD_CLIENT_INSTANCE"
```

SSM Online과 EC2 status check를 확인할 때까지 client 명령을 실행하지 않는다.
10분 안에 준비되지 않으면 원인과 비용을 기록하고 승인된 정리 절차로 종료한다.
SG/route/NAT를 자동 변경하지 않는다. 접속 방법은 기존 Console Session Manager 또는
아래 명령이다. CLI plugin이 없으면 Console을 사용한다.

```bash
aws ssm start-session --region "$KTD_AWS_REGION" \
  --target "$KTD_CLIENT_INSTANCE"
```

EC2 세션에서는 `aws sts get-caller-identity --query Arn --output text`가 이 계정의
`assumed-role/ktd-msk-producer-role/…`인지 확인한다. Java도 EC2 profile만 선택하도록
환경/JVM 설정/공유 profile의 다른 credential 주입·JAAS override 유무를 점검한다.
token·access key·환경변수 전체를 출력하지 않는다. 기본 chain에 다른 credential이
있으면 멈추며 chan key를 복사해 해결하지 않는다.

### 12.5 CLI와 인증 파일 준비, Describe와 생성

EC2에서 읽기 inventory를 먼저 수행한다. `rg`가 없다면 아래는 기본 도구로 대체하고
없다는 이유로 설치를 자동 실행하지 않는다. 탐색 결과가 여러 개면 버전과 기존 사용
이력을 대조해 실제 바이너리를 한 개 선택한다.

```bash
command -v java
command -v kafka-topics.sh
command -v kafka-configs.sh
rg --files --hidden --no-ignore /opt /usr/local /home/ec2-user 2>/dev/null \
  | rg '/(kafka-topics[.]sh|kafka-configs[.]sh|client[.]properties|aws-msk-iam-auth-.*-all[.]jar)$'
```

CLI/JAR 부재라면 별도의 설치 승인을 받고 Kafka 배포·AWS IAM JAR의 실제 버전과
공식 배포 hash를 고정한다. 설치할 버전은 이번 stopped host에서 확인하지 못해
`<KAFKA_VERSION>` / `<IAM_JAR_VERSION>`으로 남긴다. 임의 최신 버전을 자동 설치하지 않는다.
기존 설치가 있으면 그대로 사용하며 `/opt/kafka`를 EC2 실제 경로로 추측하지 않는다.
[공식 Serverless client 준비](https://docs.aws.amazon.com/msk/latest/developerguide/msk-serverless-create-topic.html)

실제 경로 선택 뒤 변수와 전용 Admin 설정 파일을 준비한다.
기존 properties는 보존하고 별도 새 파일명으로만 작성한다. 아래 properties에는
인증값이 없고 EC2 instance profile credential chain을 사용한다.

```bash
export KTD_KAFKA_TOPICS='<실제 kafka-topics.sh 절대 경로>'
export KTD_KAFKA_CONFIGS='<실제 kafka-configs.sh 절대 경로>'
export KTD_IAM_JAR='<실제 aws-msk-iam-auth-버전-all.jar 절대 경로>'
export KTD_CLIENT_PROPERTIES='<새 Admin properties 절대 경로>'
export MSK_BOOTSTRAP_SERVERS=boot-aersevd2.c2.kafka-serverless.ap-northeast-2.amazonaws.com:9098
export CLASSPATH="$KTD_IAM_JAR${CLASSPATH:+:$CLASSPATH}"
test -x "$KTD_KAFKA_TOPICS"
test -x "$KTD_KAFKA_CONFIGS"
test -r "$KTD_IAM_JAR"
test -r "$KTD_CLIENT_PROPERTIES"
"$KTD_KAFKA_TOPICS" --version
```

```properties
security.protocol=SASL_SSL
sasl.mechanism=AWS_MSK_IAM
sasl.jaas.config=software.amazon.msk.auth.iam.IAMLoginModule required;
sasl.client.callback.handler.class=software.amazon.msk.auth.iam.IAMClientCallbackHandler
request.timeout.ms=10000
default.api.timeout.ms=30000
```

test·인증 설정·CLI 확인이 하나라도 실패하면 Describe로 진행하지 않는다.
실제 경로 placeholder는 승인 후 host 조회로 채운다. Java IAM 설정은
[AWS 공식 client](https://github.com/aws/aws-msk-iam-auth)를 근거로 한다.

Describe/config는 §5의 두 완전 일치 regex와 entity-name 명령을 실행한다.
생성은 §6 명령을 **topic별로** 판단해 사용한다. 모두 이 절의 bootstrap/properties
변수를 사용한다. validated 한 건의 구체적인 순서는 다음과 같다.

```bash
"$KTD_KAFKA_TOPICS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" \
  --describe --topic '^kitchen[.]sensor[.]validated$'
```

존재하면 다음 config 조회로 진행한다. 3 partitions·delete·7일·bytes=-1이 일치하면
생성을 건너뛰고 불일치하면 중단한다. config 조회는 topic이 존재할 때만 수행한다.

```bash
"$KTD_KAFKA_CONFIGS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" --entity-type topics \
  --entity-name kitchen.sensor.validated --describe --all
```

명시적 부재 응답과 대상 Describe 권한, 생성 승인까지 확인한 경우에만 다음을 실행한다.
regex 일치 항목이 없는 빈 응답이나 종료 코드만으로 부재를 판정하지 않는다.
필요하면 대상 하나의 AdminClient metadata로 UnknownTopic 응답인지 확인한다.

```bash
"$KTD_KAFKA_TOPICS" --bootstrap-server "$MSK_BOOTSTRAP_SERVERS" \
  --command-config "$KTD_CLIENT_PROPERTIES" \
  --create --if-not-exists --topic kitchen.sensor.validated --partitions 3 \
  --config cleanup.policy=delete --config retention.ms=604800000 \
  --config retention.bytes=-1
```

quarantine은 §5/6의 **실제 이름 kitchen.sensor.quarantine**에 대한 Describe/config/
create를 같은 순서로 사용한다. 전체 명령을 복사해 무조건 연속 생성하지 않는다.
RF·min ISR을 지정하지 않고 schema·payload·Job 2·기존 topic 설정을 변경하지 않는다.

### 12.6 검증·실패 대응·종료 정리

생성 후 두 topic을 다시 Describe해 partition 0/1/2, 유효 leader, replica/ISR,
실측 RF, cleanup/retention을 기록한다. if-not-exists의 exit 0은 설정 일치 증거가 아니다.
그다음 runtime metadata→ACK→읽기 대사→checkpoint 복구는 §7의 별도 승인 시험이다.
이번 네 Action만으로는 produce/consume할 수 없다. 관리 role에 WriteData를 추가해
ACK를 대신 검증하지 않는다. 두 topic의 현재 존재 여부는 이번에도 미확인이다.

| 실패 상황 | 중단·복구 절차 설계. 이번에는 장애 주입 미실행 |
|---|---|
| IAM AccessDenied | ARN/action·실제 principal 확인. simulator implicitDeny와 broker 거부를 구분. runtime 권한 확장 금지 |
| SASL 실패 | JAR/classpath·시각·credential chain/expiry 확인. topic 부재로 해석 금지 |
| VPC timeout | SG/egress/NACL/DNS/listener/SSM outbound 읽기 확인. 새 NAT/VPN 생성 금지 |
| TopicAlreadyExists | Describe/config 재조회. 일치 시 재사용, 불일치 시 중단. 삭제 금지 |
| CLI/properties 누락 | 실제 경로·버전 기록 후 중단. 설치는 별도 승인 필요 |
| 한쪽만 생성·결과 timeout | 생성된 topic 보존. 다시 조회해 미완료 쪽만 판단. 무조건 재생성 금지 |
| 정리 실패 | EC2 상태와 잔여 policy를 각각 기록. 한쪽 실패와 별개로 다른 정리도 확인하고 비용·권한 잔존 보고 |

실행 승인에는 성공·중간 실패 모두에 대한 **EC2 중지와 이번 추가 권한만의 회수**를
포함한다. 기존 raw policy·AmazonSSMManagedInstanceCore·instance profile은 제거하지 않는다.
topic·기존 EBS·checkpoint·offset도 정리 대상으로 삼지 않는다.

```bash
aws ec2 stop-instances --region ap-northeast-2 \
  --instance-ids i-08e374efb0c31160d
aws ec2 wait instance-stopped --region ap-northeast-2 \
  --instance-ids i-08e374efb0c31160d
aws iam get-role-policy --role-name ktd-msk-producer-role \
  --policy-name ktd-p2-output-topics-admin-temporary
```

이번에 새로 적용한 policy와 내용·소유가 같다는 것을 확인하고 승인된 회수를 진행한다.

```bash
aws iam delete-role-policy --role-name ktd-msk-producer-role \
  --policy-name ktd-p2-output-topics-admin-temporary
aws ec2 describe-instances --region ap-northeast-2 \
  --instance-ids i-08e374efb0c31160d \
  --query 'Reservations[].Instances[].State.Name'
aws iam get-role-policy --role-name ktd-msk-producer-role \
  --policy-name ktd-p2-output-topics-admin-temporary
```

마지막 policy 조회의 NoSuchEntity가 회수 확인이다. 다른 오류를 성공으로 취급하지 않는다.
SSM session을 종료하고 이번에만 만든 임시 properties/JSON은 보관 방침에 따라 정리한다.
EC2는 terminate하지 않고 stopped로 돌린다. 자동 할당 public IPv4 해제는 중지 후
상태에서 확인하며 기존 Elastic IP가 있다면 임의 release하지 않는다.

### 12.7 비용과 남은 조건

AWS Price List에서 **서울** 리전으로 재조회한 단가와 계산이다.

| 항목 | 단가·계산 | 비용 구분 |
|---|---|---|
| Linux t3.micro | $0.013/시간 (SKU NYVHMS44MY8N2F3M) | 실행 중 증분. Linux 초 단위, 최소 60초 |
| public IPv4 한 개 사용 시 | $0.005/시간 | stopped 상태 PublicIp는 null. 시작 후 할당·SSM outbound 미검증 |
| EC2+IPv4 한 개 30분 | (0.013+0.005)×0.5=$0.009 | 세금·전송·CPU credit 초과 등 제외한 예상 |
| 기존 gp3 8 GiB | 8×$0.0912=$0.7296/월 (SKU MTK7D9SGKGYR3JD6) | 기존 비용. 중지 후에도 보존돼 계속 과금 |
| output 6 partitions | §9의 $0.0108/시간, 730시간=$7.884 | 생성 후 지속 증분. EC2 중지로 멈추지 않음 |

EC2/EBS 단가는 이번 API에서 확인했다. MSK 단가는 §9의 10/17 관측값이며 이번에는
재조회하지 않았다. 기존 cluster, topic 입출력·저장, network/log 비용도 남는다.
새 NAT/VPN/bastion/EC2/endpoint는 만들지 않는다. T3 Unlimited의 CPU credit 추가
과금 가능성이 있어 장시간 build·부하 시험은 이 절차에서 제외한다.
[EC2 과금](https://aws.amazon.com/ec2/pricing/on-demand/),
[public IPv4 과금](https://aws.amazon.com/vpc/pricing/)

Billing/크레딧 적용은 미조회다. 기존 AWS 크레딧으로 상쇄된다고 가정하지 않고
차감 전 비용을 제시한다. 중지·임시 권한 회수를 승인된 실험 종료 조건으로 삼되,
계속 과금되는 MSK/EBS/topic 삭제는 별도 작업이므로 이 승인에 포함하지 않는다.

남은 조건은 사용자 실행 승인, 임시 policy 이름의 미사용 확인, EC2 재개 후의
SSM/CLI/Java identity/실제 네트워크 확인, 최신 topic Describe/config다.
현재 policy로는 EC2 단독 관리가 불가능하며 임시 policy 적용 후에도 실제 broker
도달을 실행 전에 단정하지 않는다. 이번 실행 경로 설계와 IAM 평가를 마치고 중단한다.

### 12.8 이번 실행 명령과 문서 검증

실제로 호출한 것은 STS get-caller-identity, IAM user/role/group/managed policy의
읽기 API, Organizations describe-organization, MSK describe-cluster-v2/
get-bootstrap-brokers/get-cluster-policy, EC2 describe-instances/route-tables/
security-groups/volumes, SSM describe-instance-information, IAM의
simulate-principal-policy/simulate-custom-policy, Pricing get-products다.
로컬에서는 route 조회·CLI 경로 조회·Git 읽기·파일 hash·구문 검사만 수행했다.
외부 조회의 오류는 앞 절에 표시했고 Organizations 미사용/cluster policy 미발견을
일반적인 조회 성공이나 topic 부재로 바꾸어 기록하지 않았다.

정책 JSON 파싱과 두 정확한 topic ARN scope, bash 블록 17개 bash -n,
기존 Python 예시 구문, 비용 산식, 유효한 simulator 결과 40개를 확인했다.
초안 단독의 음성 평가는 12개 중 10개 implicitDeny, output CreateTopic 두 개만 allowed다.
본절 이전 문서는 세션 시작 사본과 prefix가 같고, 다른 추적·미추적 파일은 모두
SHA-256이 동일했다. git diff와 git diff --check로 최종 범위를 확인한다.
제품 코드가 바뀌지 않아 pytest 회귀는 반복하지 않았다.
실제 IAM AccessDenied/SASL/네트워크/TopicAlreadyExists 장애 주입과
EC2·Spark 재시작·Kafka 생성·발행은 수행하지 않았다. 실패·재실행 절차는 설계 검토다.

## 13. P2-8g 실제 provisioning 결과 — 2026-10-22

**두 output topic 생성·실측 설정 검증 완료. EC2 stopped 및 임시 권한 회수 확인.**
이는 topic provisioning 완료이며 Job 2 E2E·발행 ACK·Spark 복구 완료를 뜻하지 않는다.

### 13.1 승인 범위와 변경 전 확인

사용자는 이번 채팅에서 EC2 role의 임시 최소 정책 추가·회수, 기존 EC2 시작·중지,
두 output 조회와 명확히 없는 topic만 계획 사양으로 생성, 사후 검증·증빙·필수 정리를
명시적으로 승인했다. 설치·신규 인프라·Kafka 데이터 발행·Databricks 실행은 제외했다.

branch/HEAD는 `feat/phase2-validation-silver` /
`888a59c3acd99989501f487ecd24191a675633af`다. 기존 수정 5개와 미추적 파일은
보존했다. AGENTS.md, 본 실행 문서, 계획서 P2-8, platform/data-contract,
Job 2 runtime의 인증·acks·자동 생성 설정과 기존 SSM 경로를 확인했다.
기존 role의 trust는 EC2, 연결된 instance profile 한 개, 해당 profile의 EC2는
ap-northeast-2의 대상 `i-08e374efb0c31160d` 한 대뿐이었다.
확인한 trust/profile/리전 내 EC2 범위에서 추가 서비스 의존성은 관측되지 않았다.
기존 raw policy와 AmazonSSMManagedInstanceCore 외 추가 정책은 없었다.
관리 API 주체는 `arn:aws:iam::048715026902:user/chan`, cluster는 ACTIVE/SERVERLESS,
bootstrap은 본문과 같고 EC2는 stopped였다.

### 13.2 실제 변경과 클라이언트

임시 policy 이름은 `ktd-p2-output-topics-admin-temporary`다. NoSuchEntity를 확인한
후 §4 JSON을 별도 inline policy로 적용하고 GET 결과와 일치함을 확인했다.
기존 inline policy를 덮어쓰지 않았다. EC2를 시작한 뒤 system/instance status ok와
SSM Online을 확인했다. SSM AWS-RunShellScript로 inventory와 Kafka CLI를 실행했다.

실제 발견한 기존 경로:

| 도구 | 경로·버전 |
|---|---|
| kafka-topics.sh | /home/ssm-user/kafka_2.12-2.8.1/bin/kafka-topics.sh |
| kafka-configs.sh | /home/ssm-user/kafka_2.12-2.8.1/bin/kafka-configs.sh |
| IAM JAR | /home/ssm-user/kafka_2.12-2.8.1/libs/aws-msk-iam-auth-2.3.0-all.jar |
| 기존 인증 파일 | /home/ssm-user/kafka_2.12-2.8.1/bin/client.properties |
| CLI 버전 | 2.8.1 (Commit:839b886f9b732b15) |
| EC2 내부 principal | arn:aws:sts::048715026902:assumed-role/ktd-msk-producer-role/i-08e374efb0c31160d |

기존 properties는 SASL_SSL/AWS_MSK_IAM과 기대한 IAMLoginModule/callback에
일치했다. credential 환경변수·공유 파일·JAAS override는 발견되지 않았다.
기존 파일을 그대로 사용했으며 패키지/JAR 설치나 기존 인증 파일 수정은 없었다.
JVM heap과 classpath는 명령 실행 프로세스의 환경에만 지정했다.

### 13.3 생성 전후 실제 Kafka 결과

두 output 각각에서 생성 전 완전 일치 Describe가 명시적으로
`does not exist as expected`를 반환했다. 정상 --list에는 kitchen.sensor.raw만
있었고 인증/timeout 오류를 output 부재로 분류하지 않았다. 생성 직전에 각각
같은 부재 응답을 다시 확인하고 §6의 --create --if-not-exists 명령을 한 번씩 실행했다.
양쪽 모두 `Created topic ...`와 exit 0을 반환했다. 다른 topic은 생성하지 않았다.

| 실측 항목 | kitchen.sensor.validated | kitchen.sensor.quarantine |
|---|---|---|
| 생성 | 이번 생성, 성공 | 이번 생성, 성공 |
| Describe/config | 모두 exit 0 | 모두 exit 0 |
| partition 수·ID | 3 / 0,1,2 | 3 / 0,1,2 |
| cleanup.policy | delete | delete |
| retention.ms | 604800000 | 604800000 |
| retention.bytes | -1 | -1 |
| 실측 RF | 3 | 3 |
| 실측 min.insync.replicas | 2 (STATIC_BROKER_CONFIG) | 2 (STATIC_BROKER_CONFIG) |
| leader / replicas / ISR | 각 partition 유효 leader, replica 3개·ISR 3개 | 동일 |

RF/min ISR을 지정하거나 변경하지 않고 Serverless 관리값을 읽었다.
사후 --list에는 raw와 두 output이 있었다. 생성·Describe·config 명령과 실제
stdout/stderr, partition별 leader/replica/ISR은 증빙에 저장했다.
config stdout은 대상 설정 관련 행과 전체 원본 출력 SHA-256을 보관했으며 전체
config 텍스트를 보관했다고 하지 않는다. 동적 retention 설정과 broker 기본값을
구분해 유효값을 검사했다.

### 13.4 필수 정리와 보존 확인

성공 후 finally 경로에서 EC2 stop을 요청하고 stopped를 조회했다. 이번 임시
policy의 실제 내용이 적용한 초안과 같음을 확인한 뒤 그 policy만 삭제했다.
재조회 결과는 NoSuchEntity이며 정리 오류는 없었다.

최종 별도 AWS 읽기 확인:

- EC2 stopped, PublicIp=null, 기존 instance profile 유지.
- 기존 EC2 inline policy의 이름·본문, attached policy, role trust 전후 동일.
- `ktd-msk-job2-producer-policy` 전후 동일. Job 2 CreateTopic 추가 없음.
- 임시 policy 부재 재확인. 기존 raw/SSM 권한은 제거하지 않음.
- 기존 데이터·offset·checkpoint·인증 설정·다른 Phase 변경 없음.

### 13.5 실행 시간과 비용

아래는 실제 관리 API 호출·조회 시각이며 청구서의 확정 실행 시간은 아니다.

| 시점 | UTC | KST |
|---|---|---|
| EC2 시작 요청 | 2026-10-22 05:11:57.735 | 14:11:57.735 |
| EC2 중지 요청 | 2026-10-22 05:18:25.858 | 14:18:25.858 |
| stopped 관측 | 2026-10-22 05:18:43.205 | 14:18:43.205 |
| 임시 policy 회수 확인 | 2026-10-22 05:18:46.260 | 14:18:46.260 |

시작 요청~stopped 관측은 405.47초(약 6분 45초)다. §12.7의 10/20 관측 단가로
전체 구간을 과금 시간으로 잡으면 EC2 약 $0.00146, public IPv4 한 개 가정 포함
약 $0.00203이다. 실제 IPv4 할당 시간·청구 내역은 조회하지 않아 확정 비용이 아니다.
실제 RUNNING 과금 구간보다 넓은 요청/관측 구간을 사용한 추정치다.
기존 EBS·MSK cluster 요금은 지속하며 이번 6 partitions는 §9의 단가 기준
$0.0108/시간(730시간 $7.884)과 데이터 저장·입출력 비용이 추가된다.
요금 재조회와 AWS 크레딧 적용 확인은 이번 실행에 포함하지 않았다.

### 13.6 오류·경고와 미검증

기존 raw의 전체 Describe는 TopicAuthorizationException이었다. raw 부재로 처리하지
않았고 기존 raw의 존재는 생성 전후 목록으로 확인했다. EC2 role에는 raw
DescribeTopicDynamicConfiguration이 없고 Kafka 2.8.1 TopicCommand의 전체 Describe는
config도 조회하므로 해당 권한 경계에 부합하는 오류로 판단했다. raw 권한을
넓히지 않았다. raw의 전체 설정·partition 전후 대사는 미검증으로 남긴다.
[Kafka 2.8.1 TopicCommand 구현](https://github.com/apache/kafka/blob/2.8.1/core/src/main/scala/kafka/admin/TopicCommand.scala)

생성 시 점/밑줄 topic 이름의 metric 충돌 가능성에 대한 Kafka 일반 경고가 있었고
원문을 보관했다. 대상의 계획된 이름은 바꾸지 않았다. 두 output의 실제 설정
불일치·생성 실패·정리 실패는 없었다.

다음은 여전히 미검증이다: Job 2 producer credential의 broker metadata,
produce callback ACK, 메시지/offset 대사, Spark checkpoint 재시작·부분 실패 복구.
일반 권한 실패·SASL/네트워크 장애·TopicAlreadyExists·정리 실패 장애 주입은
수행하지 않았다. 이번 provisioning 완료를 Phase 2 전체 완료로 표현하지 않는다.

### 13.7 증빙과 변경 범위

증빙 폴더: `docs/evidence/phase-2/msk-output-topics/20261022T051154Z/`.

- before.json / after.json: 변경 전후 상태와 기존 정책 보존 확인.
- temporary-policy-applied.json / operations.jsonl: 임시 정책·변경 시각.
- client-inventory.json / ready.json: 실제 도구·인증 참조·EC2/SSM 상태.
- topic-preflight.json / topic-list-readonly.json: 생성 전 부재·raw 목록.
- topic-commands.json / topics-final.json / cli-results.txt: 실제 Kafka 명령·설정 결과.
- cleanup.json / run-summary.json: stopped·회수·시간·비용 추정.
- *-request.json / *-result.json: SSM command ID·실행 명령·응답.

cli-results.txt를 열어 양쪽 after-describe/config와 정리 결과 화면을 촬영할 수 있다.
실제 화면 캡처 이미지 파일은 이번에 생성하지 않았다. JSON/CLI 텍스트가 실측 증빙이다.
비밀값은 출력하지 않았고 local evidence 제외 설정도 바꾸지 않는다.
이번 저장소 변경은 본 절과 증빙 폴더뿐이며 Git add/commit/push/merge는 하지 않았다.
Job 2 E2E나 발행 단계로 자동 진행하지 않고 사용자 검토에서 멈춘다.

최종 검증: 증빙 22개 JSON/JSONL 파싱·텍스트 확인과 credential 패턴 검사를
통과했다. 실측 두 topic 설정·leader/ISR·cleanup assertion도 통과했다.
다른 기존 추적·미추적 파일은 세션 시작 SHA-256과 동일했고 git diff --check도
통과했다. 제품 코드 변경이 없어 unit 회귀는 반복하지 않았다.
