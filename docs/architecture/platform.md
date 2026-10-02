# 실행 환경과 4개 Spark Job

상태: Phase 1의 Managed Iceberg sink 및 EC2 → MSK IAM Admin·topic 생성 검증 완료. MSK → Bronze E2E는 PENDING이며 나머지 Job은 목표 설계다.

[전체 도식](diagrams.md) · [변경 이력](planning-update.md)

## 플랫폼

Phase 1의 cloud E2E 목표는 EC2 Producer → Amazon MSK Serverless (`kitchen.sensor.raw`) → Databricks PySpark Structured Streaming → Unity Catalog Managed Iceberg (`ktd.bronze.sensor_raw`)다. Databricks가 지원하는 managed 경로를 사용해 GlueCatalog 직접 연결과 외부 Iceberg JAR·extension을 두지 않는다. Bronze sink는 검증됐으며 Databricks의 실제 MSK 연결·적재는 아직 미검증이다.

로컬 Docker Kafka의 `localhost:9092`에는 원격 compute가 접근할 수 없어 cloud Kafka로 전환 중이다. `ap-northeast-2`의 KTD VPC 내 두 private subnet에 MSK Serverless를 생성했다. Databricks VPC와의 VPC Peering은 Active이며 양쪽 route를 구성했다. Public internet을 거치지 않도록 하고, MSK security group은 Databricks worker security groups에서 오는 TCP 9098 접근을 허용한다.

MSK는 IAM 인증의 private endpoint를 유지한다. 개발자 Mac의 직접 접속 대신 KTD VPC 내부의 임시 `ktd-kafka-client` EC2를 사용한다. 설치·운영용 outbound internet은 별도 `ktd-client-public-a` subnet과 `ktd-igw`로 제공하고, EC2 → MSK는 VPC 내부 private traffic을 사용한다. Public default route는 client subnet에만 적용하며 기존 MSK private subnet에는 연결하지 않는다. Session Manager로 접속해 SSH key pair와 inbound TCP 22를 사용하지 않는다.

IAM은 cluster/topic/group resource별로 권한을 나누고 역할별 최소 권한을 적용한다. Producer `ktd-msk-producer-role`은 Connect·CreateTopic·DescribeTopic·WriteData를 사용하며 ReadData·consumer group 권한은 부여하지 않는 방향이다. Consumer `ktd-databricks-msk-consumer-role`에는 Connect·DescribeTopic·ReadData·DescribeGroup·AlterGroup만 부여하고 CreateTopic·WriteData는 허용하지 않는다. UC Service Credential `ktd-msk-consumer`의 Validate 성공은 Structured Streaming의 실제 MSK 접속과 구별한다.

EC2의 IAM Admin 요청과 `kitchen.sensor.raw` 생성(3 partitions)까지 검증됐다. 3은 기능/E2E 검증용 초기값이며 성능 최적값이 아니다. Phase 0 로컬 Kafka·Registry·Simulator 범위는 유지한다. 기존 Producer의 MSK/IAM 연결 방식과 cloud Schema Registry 사용 방식은 다음 구현 단계에서 결정하며, EC2를 장기 Producer 환경으로 확정하지 않는다. [검증 범위](../reports/phase-1-verification.md)를 참고한다.

## Job 경계

| Job | 입력 | 책임 | 출력 | 논리 소비 그룹 |
|---|---|---|---|---|
| 1 Raw Ingestion | kitchen.sensor.raw | bytes와 Kafka 메타데이터 원본 보존, 업무 검증·event_id dedup 없음 | Bronze | ktd-raw-ingestion |
| 2 Validation + Dedup + Silver | kitchen.sensor.raw, 명시적 reprocess 경로 | Avro 해석·계약·의미 검증, late 분류, bounded dedup | quarantine / Silver / kitchen.sensor.validated | ktd-validation |
| 3 Metric Aggregation | kitchen.sensor.validated | 1분 tumbling + 5분 sliding 수치 집계 | Gold metric facts | ktd-metric-aggregation |
| 4 State Machine + Alert | kitchen.sensor.validated | 장비별 FSM·health·지속시간 경보 | DynamoDB / state.changes / alerts / 상태·경보 이력 | ktd-state-machine |

**목표 구조에서 Job 1과 Job 2는 raw 전체를 독립 소비한다. Job 2가 Bronze 적재 완료를 기다리는 직렬 경로가 아니다.** 로컬 개발 계획서는 이 입력 방식을 구현 제안으로 표시한다. Phase 2 전에 확정 여부를 확인한다. Bronze 누락 방지를 위해 Job 1의 lag와 Kafka retention 여유를 관측해야 한다. Job 3과 Job 4도 validated를 독립 소비한다.

Spark query마다 독립된 영속 checkpoint를 둔다. 표의 group은 논리적 독립 소비 의도이며 Spark가 자동 생성하는 group ID를 무조건 고정값으로 덮어쓰라는 설정 명세가 아니다.

Silver는 영속 canonical history, validated는 실시간 canonical interface다. Job 2가 둘에 쓰는 것은 분산 원자적 트랜잭션으로 확정되지 않았다. 한쪽 성공 후 장애가 발생했을 때 재전달·대사·누락 복구를 어떻게 할지는 구현 전 해결할 쟁점이다. 다이어그램의 분기는 이 두 출력을 표현한다.

Job 4는 초기에는 validated에서 자체 stateful alert를 계산한다. Job 3 집계를 재사용할 필요가 생기면 metric interface를 추가 검토한다. kitchen.metrics 토픽은 현재 도입하지 않는다.

**이유:** 장애·state·checkpoint·배포 경계를 분리한다. **트레이드오프:** Job 수와 관측 비용, sink 간 일관성 관리가 늘어난다.
