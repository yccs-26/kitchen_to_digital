# P2 Job 2 foreachBatch 초기화 직렬화 수정

## 원인과 재현

2026-10-12, 로컬 Databricks Connect 17.3.14 / Python 3.12 /
`pyspark.cloudpickle` 3.1.1 / confluent-kafka 2.15.1에서 확인했다.
기존 callback은 Producer 인스턴스를 미리 생성하지 않았다. 그러나 중첩 함수의
전역 `Producer` 클래스 참조가 cloudpickle 직렬화 대상에 들어갔다.
이 클래스의 `__module__`은 `cimpl`인데 `sys.modules`에는 해당 이름이 없고,
cloudpickle의 모듈/이름 참조 탐색은 `None`이었다. 따라서 클래스가 값으로 직렬화됐다.

실제 최소 재현:

```bash
.venv-databricks/bin/python - <<'PY'
from pathlib import Path
import subprocess
import sys
from confluent_kafka import Producer
from pyspark import cloudpickle

path = Path('/tmp/ktd-producer-pickle.bin')
path.write_bytes(cloudpickle.dumps(Producer))
result = subprocess.run([
    sys.executable, '-c',
    "from pathlib import Path; from pyspark import cloudpickle; "
    "cloudpickle.loads(Path('/tmp/ktd-producer-pickle.bin').read_bytes())",
], capture_output=True, text=True)
print(result.stderr)
PY
```

새 프로세스에서 실제 `AttributeError: type object 'Producer' has no attribute
'__bool__'`가 발생했다. 같은 프로세스에서는 immutable C type에 `__bool__`을
설정할 수 없다는 TypeError가 발생했다. Kafka/Registry 요청 없이 재현되므로
raw 내용, MSK source 인증, Silver 저장의 오류로 분류하지 않는다.

## 실제 capture와 수정

| 항목 | 수정 전 capture |
|---|---|
| Producer 인스턴스 | 없음 |
| Producer C-extension 클래스 | callback의 전역 참조로 포함됨 — 오류 원인 |
| ValidatedPublisher / QuarantinePublisher 인스턴스 | 없음. 기존 구현은 함수 기반 |
| Registry client / Silver storage 인스턴스 | 없음. 기존에도 배치 실행 시 생성 |
| client config factory | `RuntimeSettings.client_configs` bound method |
| 나머지 closure | config, rules, schema 문자열, registry/producer 설정 dict |

`make_batch_handler`의 callback에서 `_execute_batch`를 **실행 시 import**한다.
실제 처리 본문은 해당 모듈 함수로 이동했다. callback에는 C-extension 클래스나
처리 함수의 전역 객체 그래프가 들어가지 않는다. factory의 self에는 비밀값이 아닌
설정/secret 참조만 있다. runtime의 bound method 경로와 로컬 설정 dict 경로 모두 검사했다.

worker의 `_execute_batch`에서 factory 해석 → Registry/serializer 생성 →
Silver adapter 및 두 Producer 생성 → 기존 `process_batch` 실행 순서를 유지한다.
Producer는 event마다 만들지 않고 **배치당 validated/quarantine 각각 하나**다.
1건/3건 배치 모두 Producer 생성 횟수 2회를 검증했다.
기존 Registry context-manager 종료와 오류 전파를 유지했다.
Silver first, 발행 ACK, 중복/충돌/부분 실패 복구, checkpoint 의미론은 변경하지 않았다.

## 검증

- 실제 Producer를 mock하지 않은 callback을 같은 프로세스 및 새 프로세스에서 복원했다.
- 기본 직렬화와 프로젝트 모듈의 `register_pickle_by_value` 모드를 모두 검사했다.
- 새 worker에서 복원한 callback이 worker가 import한 배치 함수로 전달하는지도 검사했다.
- 일반 unit 환경에는 cloudpickle 3.1.1을 dev dependency로 추가했다.
- Connect 환경에서는 standalone 패키지 대신 `pyspark.cloudpickle`을 사용한다.
- `.venv-databricks/bin/python -m pytest -q tests/unit/test_job2_callback_serialization.py tests/unit/test_sensor_validation_job.py tests/unit/test_runtime_config.py tests/unit/test_validated_publisher.py tests/unit/test_quarantine_publisher.py`: **236 passed**.
- `.venv/bin/python -m pytest -q tests/unit`: **680 passed**.
- 두 실행 모두 기존 Authlib/httpx 경고 1개. `git diff --check` 통과.
- 실제 DBR Job 재실행은 이번에 하지 않았다. worker의 repo import와 cloud 처리는 재실행 검증 대상이다.

## Git 및 Databricks 재배포

이번 수정만 commit할 파일:

```bash
git add streaming/jobs/sensor_validation_job.py \
  tests/unit/test_job2_callback_serialization.py \
  tests/unit/test_sensor_validation_job.py \
  pyproject.toml uv.lock \
  docs/implementation/p2-8c-foreachbatch-serialization.md
git commit -m "fix: isolate Job 2 foreachBatch runtime from cloudpickle"
```

다른 미커밋 fixture/exporter/config/보고서는 섞지 않는다. Codex는 위 명령을 실행하지 않았다.
현재 배포는 Workspace RAW 파일 업로드이므로 push는 실행 전제조건이 아니다.
원격에 보관할 때만 사용자가 `git push origin feat/phase2-validation-silver`를 실행한다.
main merge는 하지 않는다.

사용자 commit 뒤 `git rev-parse --short HEAD` 결과를 `<NEW_SHA>`로 사용한다.
이전 `ktd-job2-dfbe39e`를 수정된 SHA라고 취급하지 않는다.
[기존 RAW 업로드 절차](p2-8b-job2-databricks-run.md)의 업로드 대상만
`/Users/yuchan001016@gmail.com/ktd-job2-<NEW_SHA>`로 지정해 repo 모듈 전체,
기존 `config/job2-runtime.json`, rules, Avro schema, launcher를 함께 배포한다.
runtime에서 달라져야 하는 코드는 `streaming/jobs/sensor_validation_job.py` 하나다.
cloudpickle dev dependency를 DBR에 덮어 설치할 필요는 없다.

Job `50294370651632`의 script path:

```text
/Workspace/Users/yuchan001016@gmail.com/ktd-job2-<NEW_SHA>/scripts/run_job2_databricks.py
```

Parameters는 다음과 같다. `--check-config`만으로는 callback 직렬화를 실행하지 않는다.

```json
["--runtime-config", "config/job2-runtime.json", "--available-now"]
```

기존 checkpoint를 유지하고 초기화하지 않는다. 동일 Silver writer는 하나만 실행한다.
worker 초기화 성공과 실제 batch counts/각 sink 결과를 구분해 증빙한다.

참고: [cloudpickle의 전역 추적 및 클래스 직렬화 구현](https://github.com/cloudpipe/cloudpickle/blob/master/cloudpickle/cloudpickle.py).
