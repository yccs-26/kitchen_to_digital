# P2-8g MSK output provisioning 실측 증빙

사용자가 승인한 임시 IAM 정책 추가·회수, 기존 EC2 시작·중지,
두 output 조회·미존재 topic 생성·사후 검증·필수 정리만 수행했다.
설치·신규 인프라·데이터 발행·Databricks/Job 2 실행은 하지 않았다.

- `before.json`: 변경 전 AWS 주체·cluster·EC2·role 정책·의존성.
- `temporary-policy-applied.json`, `operations.jsonl`: 임시 정책과 변경 시각.
- `ready.json`, `client-inventory.json`: EC2/SSM 및 기존 도구·인증 참조 확인.
- `topic-preflight.json`, `topic-list-readonly.json`: 생성 전 부재와 기존 raw 목록.
- `topic-commands.json`, `topics-final.json`: 실제 생성/Describe/config 결과.
- `cli-results.txt`: 같은 실제 CLI 결과를 읽기 쉽게 펼친 텍스트. 설정 출력은
  대상 속성 관련 행과 원본 전체 출력 hash를 보관하며 전체 config 복사본은 아니다.
- `*-request.json`, `*-result.json`: SSM command ID·실제 명령·응답.
- `cleanup.json`, `after.json`, `run-summary.json`: 정리와 정책 보존, 시간·비용 추정.

정상 결과: 양쪽 3 partitions / RF 3 / min ISR 2 / delete /
retention.ms=604800000 / retention.bytes=-1. 모든 partition의 leader와 ISR 확인.

오류/한계: 기존 raw 전체 Describe는 TopicAuthorizationException이었다.
raw의 존재는 생성 전후 목록으로 확인했지만 전체 config/partition 전후 대사는
미검증이다. raw나 다른 topic의 데이터·설정은 변경하지 않았다.
Kafka CLI의 점/밑줄 metric-name 경고도 원문에 보존했다.
Kafka ACK, 메시지 수/offset, 실제 Job 2 role의 broker metadata,
Spark checkpoint 재시작 복구는 이 작업의 증거에 포함되지 않는다.

`cli-results.txt`의 두 after-describe/config와 정리 결과를 열어 포트폴리오
화면을 촬영할 수 있다. 이 폴더에 실제 화면 캡처 이미지를 생성했다고 주장하지 않는다.
보관된 CLI/JSON이 원본 조회 증빙이며 인증 token·secret 값은 포함하지 않는다.
