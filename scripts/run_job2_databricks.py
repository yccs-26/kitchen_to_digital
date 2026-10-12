"""Databricks Python script task에서 repo의 Job 2를 실행한다."""

from pathlib import Path
import sys


def resolve_repo_root() -> Path:
    """실행 코드의 filename을 우선 사용하고 실제 repo 파일 구조를 확인한다."""
    # exec(compile(source, filename, "exec"))도 코드 객체에 filename을 남긴다.
    # 공유 globals의 __file__이나 argv[0]은 실행 wrapper를 가리킬 수 있다.
    candidates = (
        resolve_repo_root.__code__.co_filename,
        globals().get("__file__"),
        sys.argv[0] if sys.argv else None,
    )
    for candidate in candidates:
        if not isinstance(candidate, str) or not candidate:
            continue
        path = Path(candidate).resolve()
        if (path.name != "run_job2_databricks.py"
                or path.parent.name != "scripts" or not path.is_file()):
            continue
        root = path.parent.parent
        if (root / "streaming/jobs/sensor_validation_job.py").is_file():
            return root
    raise RuntimeError(
        "Cannot locate Job 2 repository: compile filename or sys.argv[0] "
        "must identify scripts/run_job2_databricks.py in the deployed repo"
    )


REPO_ROOT = resolve_repo_root()


def runtime_session():
    """Databricks Runtime이 제공하는 SparkSession을 사용한다."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def check_kafka_metadata(path, spark):
    """기존 producer 인증으로 metadata만 조회하고 비밀값 없는 결과를 반환한다."""
    from confluent_kafka import KafkaError, KafkaException, Producer
    from streaming.jobs import runtime_config as runtime

    disabled = runtime.LOGGER.disabled
    runtime.LOGGER.disabled = True
    producer = None
    try:
        settings = runtime.load_runtime_config(path)
        options = settings.kafka.producer_options(runtime.get_dbutils(spark))
        options = runtime.runtime_producer_options(options, spark=spark)
        # 진단 전용으로 native broker/auth 원문 로그를 차단한다.
        options['log_level'] = 0
        producer = Producer(options)
        producer.list_topics(timeout=10)
        return {'metadata_lookup': 'succeeded'}
    except Exception as error:
        result = {
            'metadata_lookup': 'failed',
            'error_type': type(error).__name__,
            'code': None, 'name': None,
        }
        if (isinstance(error, KafkaException) and error.args
                and isinstance(error.args[0], KafkaError)):
            code = error.args[0].code()
            result.update(code=code, name=KafkaError(code).name())
        return result
    finally:
        # publish/flush 없이 metadata 전용 producer를 정리한다.
        del producer
        runtime.LOGGER.disabled = disabled


def check_kafka_produce(path, spark):
    """기존 Silver recovery 행 한 건만 정상 validated 계약으로 발행한다."""
    import re

    from confluent_kafka import KafkaError, KafkaException, Producer
    from confluent_kafka.schema_registry import (
        SchemaRegistryClient, topic_subject_name_strategy,
    )
    from confluent_kafka.schema_registry.avro import AvroSerializer
    from streaming.jobs import runtime_config as runtime
    from streaming.sinks.delta_silver import DeltaSilverStorage
    from streaming.sinks.validated_publisher import publish_validated_event

    event_id = '30008ab5-8a00-43d9-bc4a-94493ceaaea7'
    disabled = runtime.LOGGER.disabled
    runtime.LOGGER.disabled = True
    producer = None
    try:
        settings = runtime.load_runtime_config(path)
        if settings.job.validated_topic != 'kitchen.sensor.validated':
            raise ValueError()
        event = DeltaSilverStorage(
            spark=spark, table_name=settings.job.silver_table,
        ).lookup(event_id)
        if event is None or event['event_id'] != event_id:
            raise LookupError()
        resolved = settings.resolve_clients(runtime.get_dbutils(spark))
        options = runtime.runtime_producer_options(resolved.producer, spark=spark)
        options['log_level'] = 0
        schema = (REPO_ROOT / 'schemas/avro/sensor_metric_event.avsc').read_text()
        with SchemaRegistryClient(resolved.registry) as registry:
            serializer = AvroSerializer(registry, schema, conf={
                'auto.register.schemas': False,
                'use.latest.version': False,
                'subject.name.strategy': topic_subject_name_strategy,
            })
            producer = Producer(options)
            message = publish_validated_event(
                event, producer, serializer, topic=settings.job.validated_topic,
            )
        return {
            'produce_probe': 'succeeded', 'topic': settings.job.validated_topic,
            'partition': message.partition(), 'offset': message.offset(),
            'event_id': event_id,
        }
    except Exception as error:
        result = {
            'produce_probe': 'failed', 'error_type': type(error).__name__,
            'code': None, 'name': None,
        }
        if (isinstance(error, KafkaException) and error.args
                and isinstance(error.args[0], KafkaError)):
            code = error.args[0].code()
            result.update(code=code, name=KafkaError(code).name())
        elif isinstance(error, TimeoutError):
            result['timeout'] = True
            state = re.search(
                r'\[callback_received=(True|False); remaining=(\d+|unknown); '
                r'delivery_error=not_observed\]', str(error),
            )
            if state:
                result['callback_received'] = state[1] == 'True'
                result['remaining'] = (
                    int(state[2]) if state[2] != 'unknown' else None
                )
                result['delivery_error'] = 'not_observed'
        return result
    finally:
        del producer
        runtime.LOGGER.disabled = disabled


def main(argv=None) -> None:
    """CWD와 무관하게 repo import와 상대 runtime config 경로를 해석한다."""
    sys.path.insert(0, str(REPO_ROOT))
    from streaming.jobs.sensor_validation_job import main as run_job

    args = list(sys.argv[1:] if argv is None else argv)
    # 실행용 설정은 명시하게 하여 환경 변수 기반 인증 경로 진입을 막는다.
    for index, argument in enumerate(args):
        if argument == "--runtime-config" and index + 1 < len(args):
            path = Path(args[index + 1])
            if not path.is_absolute():
                args[index + 1] = str(REPO_ROOT / path)
            break
        if argument.startswith("--runtime-config="):
            path = Path(argument.split("=", 1)[1])
            if not path.is_absolute():
                args[index] = f"--runtime-config={REPO_ROOT / path}"
            break
    else:
        if "--help" not in args and "-h" not in args:
            raise SystemExit("--runtime-config is required")
    if '--check-kafka-metadata' in args or '--check-kafka-produce' in args:
        import argparse
        import json

        parser = argparse.ArgumentParser()
        parser.add_argument('--runtime-config', required=True)
        probe = parser.add_mutually_exclusive_group(required=True)
        probe.add_argument('--check-kafka-metadata', action='store_true')
        probe.add_argument('--check-kafka-produce', action='store_true')
        parsed = parser.parse_args(args)
        if parsed.check_kafka_produce:
            result = check_kafka_produce(parsed.runtime_config, runtime_session())
        else:
            result = check_kafka_metadata(parsed.runtime_config, runtime_session())
        print(json.dumps(result))
        if result.get('metadata_lookup', result.get('produce_probe')) == 'failed':
            raise SystemExit(1)
        return
    run_job(args, spark_factory=runtime_session)


if __name__ == "__main__":
    main()
