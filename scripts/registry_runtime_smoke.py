"""Databricks driver에서 실행할 읽기 전용 Registry/decode smoke다."""

import base64
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka.schema_registry import SchemaRegistryClient

from scripts.registry_migration import (
    check_definition,
    load_bundle,
    require,
    validate_endpoint,
)
from streaming.jobs.runtime_config import RegistrySettings, SecretReference
from streaming.validation.avro_decoder import (
    SensorDecodeError,
    decode_sensor_event,
    parse_confluent_frame,
)


def decode_summary(value, client):
    """이벤트 본문은 출력하지 않고 ID와 decode 결과만 반환한다."""
    schema_id, _ = parse_confluent_frame(value)
    event = decode_sensor_event(value, client)
    require(event['event_time'].tzinfo is not None,
            'Decoded event_time must be timezone-aware')
    if schema_id == 1:
        require(len(event) == 10, 'ID 1 must decode to canonical 10 fields')
    return {
        'status': 'PASS', 'schema_id': schema_id,
        'field_count': len(event),
        'event_time_is_aware': event['event_time'].tzinfo is not None,
    }


def framing_failure_summary(value, client):
    try:
        decode_sensor_event(value, client)
    except SensorDecodeError as error:
        require(error.code == 'INVALID_CONFLUENT_FRAME',
                'Unexpected error for corrupt framing sample')
        return {
            'status': 'EXPECTED_DECODE_FAILURE', 'code': error.code,
            'quarantine_candidate': True, 'quarantine_published': False,
        }
    raise RuntimeError('Corrupt framing unexpectedly decoded')


def run_smoke(
    spark, dbutils, *, registry_config_path, bundle_directory,
    fixture_path, bootstrap_servers, service_credential='ktd-msk-consumer',
):
    """cloud 생성/이관 이후에만 호출하며 source는 batch read만 수행한다."""
    from pyspark.sql import functions as F

    manifest = load_bundle(bundle_directory)
    config = json.loads(Path(registry_config_path).read_text())['registry']
    require(config['provider'] == 'confluent', 'Confluent provider required')
    require(config['auth_mode'] == 'basic', 'Basic auth required')
    settings = RegistrySettings(
        validate_endpoint(config['url']), 'basic',
        SecretReference.parse(config['credentials']),
    )
    # 비밀값은 실행 위치에서만 해석하며 설정 객체를 출력하지 않는다.
    client = SchemaRegistryClient(settings.resolve(dbutils))
    result = {
        'started_at': datetime.now(timezone.utc).isoformat(),
        'execution_location': 'Databricks driver',
        'executor_registry_access_verified': False,
        'checkpoint_used': False, 'publish_performed': False,
        'sample_scope': 'At most one current ID=1 and one corrupt frame',
    }
    try:
        for entry in manifest['raw']:
            schema = client.get_schema(entry['id'])
            check_definition({
                'schema': schema.schema_str,
                'schemaType': schema.schema_type,
                'references': schema.references or [],
            }, entry)
            result[f"get_schema_{entry['id']}"] = 'PASS'
        fixture = json.loads(Path(fixture_path).read_text())
        value = base64.b64decode(fixture['value_base64'], validate=True)
        require(parse_confluent_frame(value)[0] == 1, 'Fixture ID must be 1')
        require(
            hashlib.sha256(value).hexdigest()
            == manifest['smoke_fixture']['value_sha256'],
            'Fixture differs from inventoried bytes',
        )
        result['fixture_id_1'] = decode_summary(value, client)
        require(result['fixture_id_1']['field_count'] == 10,
                'Fixture is not canonical 10-field v1')
        frame = (
            spark.read.format('kafka')
            .option('kafka.bootstrap.servers', bootstrap_servers)
            .option('databricks.serviceCredential', service_credential)
            .option('kafka.allow.auto.create.topics', 'false')
            .option('subscribe', 'kitchen.sensor.raw')
            .option('startingOffsets', 'earliest')
            .option('endingOffsets', 'latest')
            .option('failOnDataLoss', 'true')
            .option('kafka.request.timeout.ms', '10000')
            .option('kafka.default.api.timeout.ms', '15000')
            .load()
        )
        valid = (
            (F.length('value') >= 5)
            & (F.hex(F.substring('value', 1, 1)) == '00')
        )
        schema_id = F.conv(
            F.hex(F.substring('value', 2, 4)), 16, 10,
        ).cast('long')
        good = frame.where(valid & (schema_id == 1)).select(
            'topic', 'partition', 'offset', 'value',
        ).limit(1).collect()
        corrupt = frame.where(
            F.col('value').isNull() | (F.length('value') < 5)
            | (F.hex(F.substring('value', 1, 1)) != '00')
        ).select('topic', 'partition', 'offset', 'value').limit(1).collect()
        for name, rows, decode in [
            ('current_kafka_id_1', good, decode_summary),
            ('current_kafka_corrupt', corrupt, framing_failure_summary),
        ]:
            if not rows:
                result[name] = {
                    'status': 'NOT_OBSERVED',
                    'reason': 'No matching record in current retention',
                }
                continue
            row = rows[0]
            value = None if row.value is None else bytes(row.value)
            result[name] = {
                **decode(value, client), 'topic': row.topic,
                'partition': row.partition, 'offset': row.offset,
            }
        result['status'] = (
            'PASS' if good and corrupt else 'INCOMPLETE_CURRENT_SAMPLE_MISSING'
        )
        result['finished_at'] = datetime.now(timezone.utc).isoformat()
        return result
    finally:
        # 클라이언트의 인증값을 결과나 Spark closure에 넣지 않는다.
        client.__exit__(None, None, None)
