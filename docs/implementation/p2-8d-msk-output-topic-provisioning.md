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
