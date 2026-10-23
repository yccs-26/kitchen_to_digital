"""Injected Phase 2 decode and immutable Job 4 input records; no clients."""

from dataclasses import dataclass
from datetime import datetime

from confluent_kafka.schema_registry.error import SchemaRegistryError

from streaming.state.bindings import ProfileKey
from streaming.state.change_envelope import canonical, digest, read_json, stamp
from streaming.state.health import HealthTimerToken
from streaming.state.job4_contract import InputPosition, ValidatedCommand
from streaming.validation.avro_decoder import (
    SensorDecodeError, decode_sensor_event, parse_confluent_frame,
)


class InputContractError(ValueError):
    """Invalid data/lineage; do not silently skip or change its identity."""


class UnsupportedInputError(InputContractError):
    """No implemented equipment/signal binding is available."""


class RetryableInputError(RuntimeError):
    """An injected decoder dependency failed transiently."""


@dataclass(frozen=True)
class ValidatedKafkaRecord:
    position: InputPosition
    value: bytes
    evaluation_time: datetime

    def __post_init__(self):
        if not isinstance(self.position, InputPosition):
            raise InputContractError('InputPosition required')
        if type(self.value) is not bytes:
            raise InputContractError('wire value bytes required')
        stamp(self.evaluation_time)


@dataclass(frozen=True)
class TimerInput:
    timer_ref: str
    device: tuple[str, str]
    token: HealthTimerToken
    evaluation_time: datetime


@dataclass(frozen=True)
class PreparedInput:
    command_ref: str
    kind: str
    device: tuple[str, str]
    payload: bytes
    origin: bytes
    evaluation_time: datetime

    @property
    def record(self):
        return canonical(dict(
            command_ref=self.command_ref, kind=self.kind, device=self.device,
            payload=read_json(self.payload), origin=read_json(self.origin),
            evaluation_time=self.evaluation_time,
        ))

    @property
    def fingerprint(self):
        data = read_json(self.record)
        data.pop('evaluation_time')
        return digest(canonical(data))

    @property
    def axes(self):
        if self.kind == 'observation' and read_json(self.payload)[
            'equipment_type'
        ] == 'hood':
            return ('OPERATIONAL', 'HEALTH')
        return ('HEALTH',)

    def timer_token(self):
        data = read_json(self.payload)
        data['profile'] = ProfileKey(**data['profile'])
        data['observation_generation'] = tuple(data['observation_generation'])
        data['expected_deadline'] = datetime.fromisoformat(
            data['expected_deadline'],
        )
        return HealthTimerToken(**data)


def restore_input(record):
    data = read_json(record)
    if set(data) != {'command_ref', 'kind', 'device', 'payload', 'origin',
                     'evaluation_time'}:
        raise InputContractError('invalid stored input shape')
    item = PreparedInput(
        data['command_ref'], data['kind'], tuple(data['device']),
        canonical(data['payload']), canonical(data['origin']),
        datetime.fromisoformat(data['evaluation_time']),
    )
    if item.kind not in ('observation', 'tick') or item.record != record:
        raise InputContractError('invalid stored input kind/canonical data')
    if item.kind == 'tick':
        item.timer_token()
    else:
        origin = read_json(item.origin)
        ValidatedCommand(
            item.command_ref,
            InputPosition(origin['topic'], origin['partition'],
                          origin['offset'], bytes.fromhex(origin['key_hex']),
                          origin['writer_schema_id']),
            item.payload, item.evaluation_time,
        )
    return item


def prepare_input(item, *, source_epoch, registry):
    """Freeze canonical data, wire digest and source identity, not batch_id.

    Registry metric/unit/profile/source checks remain in the existing
    Application; the batch layer classifies exceptions from those checks.
    """
    if not isinstance(source_epoch, str) or not source_epoch.strip():
        raise InputContractError('explicit source epoch required')
    if isinstance(item, TimerInput):
        if not isinstance(item.token, HealthTimerToken) or (
            item.device != (item.token.store_id, item.token.equipment_id)
        ) or not isinstance(item.timer_ref, str) or not item.timer_ref.strip():
            raise InputContractError('timer identity/ref mismatch')
        stamp(item.evaluation_time)
        origin = dict(source_epoch=source_epoch, timer_ref=item.timer_ref)
        key = ['job4-tick-v1', source_epoch, item.device, item.timer_ref]
        return PreparedInput('job4-tick:' + digest(canonical(key)), 'tick',
                             item.device, canonical(item.token),
                             canonical(origin), item.evaluation_time)
    if not isinstance(item, ValidatedKafkaRecord):
        raise UnsupportedInputError('validated record or tick required')
    try:
        schema_id, _ = parse_confluent_frame(item.value)
    except ValueError as exc:
        raise InputContractError('invalid wire frame') from exc
    if schema_id != item.position.writer_schema_id:
        raise InputContractError('writer schema ID/frame mismatch')
    try:
        event = decode_sensor_event(item.value, registry)
    except SensorDecodeError as exc:
        raise InputContractError(exc.code) from exc
    except SchemaRegistryError as exc:
        if exc.http_status_code in (408, 429) or exc.http_status_code >= 500:
            raise RetryableInputError('Registry transient failure') from exc
        raise
    except (TimeoutError, ConnectionError) as exc:
        raise RetryableInputError('decoder dependency unavailable') from exc
    origin = dict(source_epoch=source_epoch, topic=item.position.topic,
                  partition=item.position.partition,
                  offset=item.position.offset,
                  writer_schema_id=schema_id, key_hex=item.position.key.hex(),
                  wire_sha256=digest(item.value))
    key = ['job4-observation-v1', source_epoch, item.position.topic,
           item.position.partition, item.position.offset]
    ref = 'job4-input:' + digest(canonical(key))
    try:
        command = ValidatedCommand.from_event(
            event, command_ref=ref, position=item.position,
            reference_time=item.evaluation_time,
        )
    except (ValueError, TypeError, OverflowError) as exc:
        raise InputContractError('invalid canonical validated input') from exc
    return PreparedInput(ref, 'observation', command.device, command.payload,
                         canonical(origin), item.evaluation_time)
