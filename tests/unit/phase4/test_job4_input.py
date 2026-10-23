"""Actual Phase 2 wire decoder with injected Registry, never a service."""

from dataclasses import replace
from datetime import timedelta

from confluent_kafka.schema_registry.error import SchemaRegistryError
import pytest

from streaming.state.change_envelope import read_json
from streaming.state.health import timer_token
from streaming.state.job4_input import (
    InputContractError, RetryableInputError, TimerInput, UnsupportedInputError,
    prepare_input, restore_input,
)
from tests.fixtures.phase4.application_fixture import BASE
from tests.fixtures.phase4.job4_fixture import FakeRegistry, record, resources


def prepare(item):
    return prepare_input(item, source_epoch='fixture-source-v1',
                         registry=FakeRegistry())


def test_phase2_roundtrip_and_source_identity_are_preserved():
    sample = record()
    item = prepare(sample)
    assert item == restore_input(item.record)
    assert item.device == ('store-001', 'hood-001')
    assert read_json(item.payload)['metric_value'] == 800.0
    origin = read_json(item.origin)
    assert origin['offset'] == 10 and origin['writer_schema_id'] == 17
    assert origin['key_hex'] == b'hood-001'.hex()
    assert len(origin['wire_sha256']) == 64
    assert prepare(sample).command_ref == item.command_ref


def test_offset_identity_excludes_value_and_evaluation_but_detects_conflict():
    first = prepare(record())
    conflicting = prepare(record(value=900))
    delayed = prepare(replace(
        record(), evaluation_time=BASE + timedelta(days=1),
    ))
    assert first.command_ref == conflicting.command_ref == delayed.command_ref
    assert first.fingerprint != conflicting.fingerprint
    assert first.fingerprint == delayed.fingerprint


@pytest.mark.parametrize('value', [b'', b'bad', b'\x01' + bytes(8)])
def test_invalid_frame(value):
    with pytest.raises(InputContractError):
        prepare(replace(record(), value=value))


def test_schema_id_and_key_payload_mismatch():
    sample = record()
    with pytest.raises(InputContractError):
        prepare(replace(sample, position=replace(sample.position,
                                                 writer_schema_id=18)))
    with pytest.raises(InputContractError):
        prepare(replace(sample, position=replace(sample.position,
                                                 key=b'fryer-001')))


@pytest.mark.parametrize('status,expected', [
    (404, InputContractError), (429, RetryableInputError),
    (503, RetryableInputError), (403, SchemaRegistryError),
])
def test_dependency_error_classification(status, expected):
    class FailedRegistry:
        def get_schema(self, schema_id):
            raise SchemaRegistryError(status, status * 100, 'fixture error')

    with pytest.raises(expected):
        prepare_input(record(), source_epoch='source',
                      registry=FailedRegistry())


def test_unsupported_signal_type():
    with pytest.raises(UnsupportedInputError):
        prepare({'metric_value': 1})


def test_timer_reuses_native_token_and_roundtrips(tmp_path):
    local, _ = resources(tmp_path, initialize=True)
    try:
        token = timer_token(local.app.load_health('hood-001'))
        sample = TimerInput('deadline-1', ('store-001', 'hood-001'), token,
                            BASE + timedelta(seconds=10))
        item = prepare(sample)
        assert restore_input(item.record).timer_token() == token
        assert item.axes == ('HEALTH',)
        with pytest.raises(InputContractError):
            prepare(replace(sample, device=('other', 'hood-001')))
    finally:
        local.close()
