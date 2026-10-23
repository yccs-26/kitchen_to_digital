"""기존 decoder의 wire 계약 및 새 executor 인증·Registry 경계."""

import pickle
from types import SimpleNamespace

from confluent_kafka.schema_registry.error import SchemaRegistryError
import pytest

from streaming.sources.validated_kafka import (
    ExecutorRegistryFactory, decode_record,
)
from streaming.validation.avro_decoder import SensorDecodeError


@pytest.mark.parametrize("status,code,expected", [
    (404, 40403, SensorDecodeError), (404, 40400, RuntimeError),
    (503, 50001, SchemaRegistryError),
])
def test_registry_error_category_matches_latest_phase2(status, code, expected):
    class Registry:
        def get_schema(self, n):
            raise SchemaRegistryError(status, code, "synthetic service error")

    with pytest.raises(expected):
        decode_record(b"\0\0\0\0\7", b"e", Registry())


def test_executor_secret_missing_and_not_captured(monkeypatch):
    factory = ExecutorRegistryFactory("https://registry.invalid", "TEST_AUTH")
    monkeypatch.delenv("TEST_AUTH", raising=False)
    with pytest.raises(RuntimeError, match="SECRET_UNAVAILABLE"):
        factory()
    monkeypatch.setenv("TEST_AUTH", "test-user:test-secret")
    assert b"test-secret" not in pickle.dumps(factory)
    import confluent_kafka.schema_registry as module

    calls = []
    monkeypatch.setattr(module, "SchemaRegistryClient",
                        lambda opts: calls.append(opts))
    factory()
    assert calls == [{"url": "https://registry.invalid",
                      "basic.auth.user.info": "test-user:test-secret"}]


@pytest.mark.parametrize("url,name", [
    ("http://registry.invalid", "AUTH"),
    ("https://user:pass@registry.invalid", "AUTH"),
    ("https://registry.invalid", "password=value"),
])
def test_invalid_reference_rejected(url, name):
    with pytest.raises(ValueError):
        ExecutorRegistryFactory(url, name)
