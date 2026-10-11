# P2 Job 2 Databricks 실행 준비

2026-10-12: 코드/config/offline 테스트만 수행했다. 아래 업로드, 라이브러리 설치,
읽기 preflight, Job 실행 명령은 사용자가 이후 실행할 절차이며 이번에는 실행하지 않았다.
기존 미커밋 Registry 이관 코드와 Phase 2 보고서/증빙은 수정하지 않았다.

## 실행 경로와 변경 이유

실제 Python script task entrypoint는 **`scripts/run_job2_databricks.py`**다.
자기 파일 위치로 repo root를 `sys.path`에 추가하고 상대 config 경로를 repo 기준으로
해석한다. `streaming.jobs.sensor_validation_job.main()`을 import하고 DBR의
`pyspark.sql.SparkSession` 생성 함수를 주입한다. 코드 본문을 notebook에 복사하지 않는다.
로컬 `python -m streaming.jobs.sensor_validation_job`의 Databricks Connect 경로는 유지한다.

흐름은 `start_configured_validation` → `start_validation` → `foreachBatch` →
`process_record`다. Spark가 raw를 읽고 driver에서 순차 decode/UTC/domain/time 분류,
identity 확인, Delta Silver MERGE, validated Avro 발행 ACK를 처리한다.
Quarantine은 JSON이며 같은 producer credential을 사용한다.
query 하나와 Silver writer 하나가 전제다. executor에서 Python Registry를 호출하는
구조가 아니며 Spark Connect/serverless callback 배포까지 검증한 것은 아니다.

| 대상 | 실제 연결 |
|---|---|
| raw | Spark `databricks.serviceCredential=ktd-msk-consumer` |
| validated / quarantine | `dbutils.credentials.getServiceCredentialsProvider("ktd-msk-job2-producer")` → MSK signer → Python producer `oauth_cb` |
| Registry | 제공된 Confluent HTTPS endpoint + `ktd-job2/schema-registry-basic-auth` secret 참조 |
| Silver | 기존 `ktd.silver.sensor_validated` Managed Delta |

**Spark에 `kafka.security.protocol`을 넣지 않는다.** Python confluent producer는
`databricks.serviceCredential`을 지원하는 Spark sink가 아니므로 그 옵션을 전달하지
않는다. 기존 credential provider 변환과 Python 옵션 `security.protocol=SASL_SSL`,
`sasl.mechanism=OAUTHBEARER`가 필요하다. 두 API의 옵션을 섞지 않는다.

## 1. 로컬 설정 완성

repo root에서 실행한다. 기존 `config/job2-runtime.json`이 있으면 복사로 덮어쓰지 말고
기존 파일을 검토한다.

```bash
cp -n config/job2-runtime.example.json config/job2-runtime.json
```

다음 두 빈 값을 편집한다. 나머지 클라우드 식별자는 예제에 반영되어 있다.

| 필드 | 사용자가 채울 값 |
|---|---|
| `checkpoint` | `/Volumes/<catalog>/<schema>/<existing-volume>/job2/<stable-query-name>` 형태의 전용 경로. Job 1 및 다른 query와 공유 금지 |
| `processing_version` | 실제 배포 snapshot을 식별하는 버전. 미커밋 변경이면 HEAD만으로 표시하지 말고 별도 snapshot 식별자 포함 |

`rules_path=validation_rules.json`은 config 파일 기준 상대 경로다. 현재 규칙에는
store-001의 fridge/freezer/fryer/hood 네 장비만 있다. 실제 raw의 equipment/store/type,
metric/unit/schema version이 이 목록과 맞는지 확인한다. 등록되지 않은 장비는 격리된다.
필요한 운영 목록이 다르면 확인한 값으로 규칙을 보완한 뒤 테스트한다.
`bronze_checkpoint` 선택 필드에 실제 Job 1 경로를 넣으면 동일 문자열을 거부한다.
경로 별칭을 포함한 모든 query의 checkpoint 충돌은 사용자가 별도로 확인한다.

```bash
.venv/bin/python scripts/run_job2_databricks.py \
  --runtime-config config/job2-runtime.json --check-config
```

`config OK (offline; cloud access not checked)`가 나와야 한다. 이 옵션은 Spark 생성,
secret 조회, Registry/Kafka 접속을 하지 않는다. 예제의 빈 값을 그대로 두면 실패한다.
config에 API key, secret 값, AWS key를 넣지 않는다.

## 2. 실행 snapshot을 Workspace 파일로 업로드

Git commit/push 없이도 현재 로컬 코드를 정확히 배포할 수 있는 방식이다.
인증된 Databricks CLI 프로필을 사용한다. 아래 변수에서 사용자 이름과 snapshot 이름을
실제 값으로 바꾼다. 새 전용 디렉터리를 선택하며 기존 파일 overwrite는 하지 않는다.
`RAW` 형식은 `.py`를 notebook으로 바꾸지 않고 모듈 파일로 보존한다.

**아래부터 사용자가 실행하면 실제 Workspace 쓰기가 발생한다.**

```bash
export KTD_JOB2_WORKSPACE='/Users/<user>/ktd-job2-<snapshot>'
.venv/bin/python - <<'PY'
import os
from pathlib import Path, PurePosixPath
import subprocess

root = Path.cwd()
remote = PurePosixPath(os.environ['KTD_JOB2_WORKSPACE'])
assert str(remote).startswith('/Users/') and '<' not in str(remote)
files = sorted((root / 'streaming').rglob('*.py'))
files += [root / name for name in (
    'scripts/run_job2_databricks.py',
    'schemas/avro/sensor_metric_event.avsc',
    'config/job2-runtime.json',
    'config/validation_rules.json',
)]
assert all(path.is_file() for path in files)
for path in files:
    target = remote / path.relative_to(root).as_posix()
    subprocess.run(
        ['databricks', 'workspace', 'mkdirs', str(target.parent)], check=True,
    )
    subprocess.run(
        ['databricks', 'workspace', 'import', str(target),
         '--format', 'RAW', '--file', str(path)], check=True,
    )
print('Uploaded Job 2 snapshot')
PY
```

`rules_path`를 다른 파일로 변경했다면 해당 파일도 같은 상대 위치에 업로드한다.

## 3. Databricks Job 설정

Jobs UI에서 Python script task를 설정한다.

- Source: **Workspace**.
- Path: `/Workspace/Users/<user>/ktd-job2-<snapshot>/scripts/run_job2_databricks.py`.
- Compute: **classic DBR 17.3 LTS**, Unity Catalog 지원, MSK에 연결 가능한 네트워크.
  이 절차는 classic compute용이며 serverless / SQL warehouse용이 아니다.
- Dependent libraries (PyPI, 로컬 검증 당시 직접 의존성 버전):
  `confluent-kafka[avro,schemaregistry]==2.15.1`, `fastavro==1.12.2`,
  `aws-msk-iam-sasl-signer-python==1.0.2`.
  signer의 의존성 botocore도 필요하다. DBR의 PySpark를 사용하며 `pyspark`나
  `databricks-connect`를 추가 설치하지 않는다. DBR에서의 의존성 해석은 미검증이다.
- Maximum concurrent runs: **1**. 첫 실행은 retry 0, 정기 schedule 없음.
  다른 Job/notebook의 동일 Silver writer도 실행 중이 아닌지 확인한다.
- Run as identity: 두 service credential의 `ACCESS`, secret scope 읽기,
  catalog/schema 사용, Silver `SELECT`/`MODIFY`, checkpoint volume 읽기/쓰기가 필요하다.
  MSK IAM role에는 raw 읽기 및 consumer group 사용, producer role에는 validated/quarantine
  쓰기 등의 권한이 필요하다. 실효 권한과 네트워크 연결은 이번에 확인하지 않았다.

첫 Parameters (JSON 문자열 배열):

```json
["--runtime-config", "config/job2-runtime.json", "--check-config"]
```

Run now로 import/config 성공을 확인한다. 이는 클라우드 연결 검증이 아니다.

## 4. 쓰기 전 읽기 확인

동일 compute와 동일 실행 identity의 확인용 notebook에서 다음을 실행할 수 있다.
repo 모듈을 import하는 호출 코드이며 구현 본문을 복사하지 않는다.
Registry lookup은 기존 schema 조회이며 등록하지 않는다. API key나 설정 dict를 출력하지 않는다.

```python
import sys
from pathlib import Path

root = Path('/Workspace/Users/<user>/ktd-job2-<snapshot>')
sys.path.insert(0, str(root))
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from streaming.jobs.runtime_config import load_runtime_config

settings = load_runtime_config(root / 'config/job2-runtime.json')
schema = Schema(
    (root / 'schemas/avro/sensor_metric_event.avsc').read_text(), 'AVRO',
)
with SchemaRegistryClient(settings.registry.resolve(dbutils)) as registry:
    registered = registry.lookup_schema('kitchen.sensor.validated-value', schema)
    print({'validated_schema_lookup': 'OK', 'schema_id': registered.schema_id})

spark.sql('DESCRIBE TABLE ktd.silver.sensor_validated').show(truncate=False)
spark.sql('DESCRIBE DETAIL ktd.silver.sensor_validated').select('format').show()
```

Silver가 Delta이며 canonical 10열, `event_time TIMESTAMP` (LTZ),
`metric_value DOUBLE`, 나머지 STRING인지 확인한다. 테이블을 다시 만들지 않는다.
Registry lookup이 401/403/404 등으로 실패하면 실행을 중단하고 권한/기존 subject를 확인한다.
runtime의 read-only credential로 schema를 자동 등록하는 경로는 없다.
raw에 남은 각 writer schema ID도 같은 Registry에서 올바른 정의를 조회할 수 있어야 한다.
Registry 이관 이력만으로 현재 retention 안의 모든 ID를 확인했다고 판단하지 않는다.

## 5. 실제 유한 Job 2 실행

Parameters를 다음으로 바꾼 뒤 Run now를 누른다. 이때부터 Silver/Kafka/checkpoint에 쓴다.

```json
["--runtime-config", "config/job2-runtime.json", "--available-now"]
```

새 checkpoint에서는 `starting_offsets=earliest`에 따라 현재 보존된 raw를 읽는다.
`max_offsets_per_trigger=1000`은 **배치당 상한이며 전체 처리 건수 상한이 아니다**.
AvailableNow 대상 backlog를 여러 배치로 처리한 뒤 종료한다. 입력 0건으로 성공한 Job은
E2E 증명이 아니다. 정상 재실행에는 같은 checkpoint를 사용한다. starting offsets를
바꿔도 기존 checkpoint의 진행 위치는 되돌아가지 않는다. checkpoint 삭제/offset 초기화를 하지 않는다.

지속 실행 Parameters는 `--available-now`를 제외한다. 유한 실행을 검증한 후 사용한다.

```json
["--runtime-config", "config/job2-runtime.json"]
```

## 6. 실제 E2E에서 대조할 결과

| 입력 | Silver | Kafka / 로그 |
|---|---|---|
| 정상 신규 event_id | canonical 1행 | validated Avro, key=equipment_id, UTC 시각 |
| 같은 ID·같은 payload의 다른 raw offset | 추가 행 없음 | validated 재발행, duplicate 증가 |
| 같은 ID·다른 payload | 기존 canonical 유지 | Quarantine `EVENT_ID_CONFLICT`, field에 차이 보존 |
| corrupt framing/Avro | 저장 없음 | `INVALID_CONFLUENT_FRAME` / `AVRO_DECODE_FAILED` |
| domain invalid | 저장 없음 | errors를 담은 Quarantine JSON |
| 10분을 초과한 late valid | 저장 대상 | validated, late 증가. 버리지 않음 |
| 처리 시각보다 5분을 초과한 미래 | 저장 없음 | `FUTURE_EVENT_TIME` Quarantine |

같은 batch 안에서는 topic/partition/offset 순서다. 충돌 쌍은 canonical이 먼저 존재하거나
동일 partition의 선행 offset에 정상 레코드가 있는지 확인한다. 오래된 future 테스트 입력은
실행 시 future가 아닐 수 있으므로 시각과 fixture 기대값을 실행 시점에 대조한다.
이번에는 fixture를 raw에 새로 쓰지 않았다. 해당 입력이 없는 케이스는 미검증으로 남긴다.

로그의 `Job 2 completed batch_id=... counts=...`는 성공한 batch 시도의 건수이며
재시도 중복을 제외한 전체 누계가 아니다. `received=validated+quarantine`이다.
conflict는 quarantine의 부분집합, duplicate/late는 validated의 부분집합이다.

```sql
SELECT event_id, count(*) AS rows_per_id
FROM ktd.silver.sensor_validated
GROUP BY event_id
HAVING count(*) > 1;
```

중복 행 0건에 더해 실제 fixture event_id별 payload를 대조한다. validated는 Avro decode 후
event_id로, Quarantine은 source_topic/partition/offset·quarantine_id·errors로 대조한다.
출력 topic 검증에는 별도 읽기 권한이 필요하며 producer credential의 읽기 권한을 추정하지 않는다.
Quarantine에는 원본 value가 없으므로 lineage와 hash로 Kafka/Bronze 원본을 대조한다.
Job 1의 lag/retention 및 Bronze 원본 존재도 확인한다.

Silver 성공/발행 실패는 batch 실패로 전달하며 재실행 시 Silver duplicate로 재발행한다.
발행 성공/checkpoint 실패에서도 중복 발행할 수 있다. 전체 sink의 exactly-once가 아니다.
Registry/auth/인프라 예외를 데이터 불량으로 격리하지 않고 Job 실패로 전달한다.
late 분류는 batch 처리 시각 기준이며 Spark watermark 구현의 증명이 아니다.

Job run ID·코드 snapshot·config/rules·입력 offset 범위·건수·Silver 결과·각 topic 출력을
같은 run의 증빙으로 저장한다. 스크린샷은 성공 batch counts, event_id별 Silver,
Quarantine lineage/errors를 찍고 secret이나 해석된 인증 dict는 촬영하지 않는다.

## 검증 경계와 공식 자료

- `.venv/bin/python -m pytest -q tests/unit/test_job2_databricks_entrypoint.py tests/unit/test_runtime_config.py tests/unit/test_sensor_validation_job.py`: **163 passed**.
- `.venv/bin/python -m pytest -q tests/unit`: **669 passed**.
- 두 실행 모두 기존 Authlib/httpx deprecation warning 1개, 실패 없음.
- integration 테스트는 Kafka/Registry 쓰기를 포함하므로 이번 실행 대상에서 제외했다.

offline 테스트는 import, 설정, native session 선택, query lifecycle, 기존 처리/복구 계약을
검증한다. 실제 DBR 의존성, UC credential refresh, 네트워크, 실효 권한, Registry 전체 ID,
Kafka ACK, Silver 쓰기, checkpoint 재시작, 부분 실패 E2E는 이번에 미검증이다.
DLQ/reprocess 추가, cloud 실행, commit/push는 하지 않았다.

- [Python script task](https://docs.databricks.com/aws/en/jobs/tasks/python-script)
- [Workspace 모듈](https://docs.databricks.com/aws/en/files/workspace-modules)
- [Workspace CLI / RAW import](https://docs.databricks.com/aws/en/dev-tools/cli/reference/workspace-commands)
- [Service credential Python API](https://docs.databricks.com/aws/en/connect/unity-catalog/cloud-services/use-service-credentials)
- [MSK Spark authentication](https://docs.databricks.com/aws/en/connect/streaming/kafka/authentication)
