import base64
import json
from unittest.mock import Mock

import pytest
from kafka.errors import KafkaTimeoutError
from kafka.sasl.oauth import AbstractTokenProvider

from scripts import phase1_msk_fixture_producer as msk

KEY = b"fridge-001\x00\xff"
VALUE = b"\x00\x00\x00\x00\x01\xff\x80avro\x00"


@pytest.fixture
def fixture_data():
    return {
        "topic": "fixture-selected-topic",
        "equipment_id": "fridge-001",
        "event_id": "fixture-event",
        "schema_version": "1.0.0",
        "key_base64": base64.b64encode(KEY).decode("ascii"),
        "value_base64": base64.b64encode(VALUE).decode("ascii"),
    }


@pytest.fixture
def fixture_path(tmp_path, fixture_data):
    path = tmp_path / "fixture.json"
    path.write_text(json.dumps(fixture_data), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def transport(monkeypatch):
    factory = Mock()
    monkeypatch.setattr(msk, "KafkaProducer", factory)
    signer = Mock(return_value=("test-token", 123456789))
    monkeypatch.setattr(msk.MSKAuthTokenProvider, "generate_auth_token", signer)
    factory.return_value.send.return_value.get.return_value = msk.RecordMetadata(
        "fixture-selected-topic", 2, None, 42, 0, None, len(KEY), len(VALUE), 0
    )
    return factory


def test_load_restores_original_bytes(fixture_path, fixture_data, transport):
    fixture = msk.load_fixture(fixture_path)
    assert fixture.key == KEY
    assert fixture.value == VALUE
    for field in ("topic", "equipment_id", "event_id", "schema_version"):
        assert getattr(fixture, field) == fixture_data[field]
    transport.assert_not_called()


@pytest.mark.parametrize(
    "field",
    [
        "topic",
        "equipment_id",
        "event_id",
        "schema_version",
        "key_base64",
        "value_base64",
    ],
)
def test_missing_field_rejected(fixture_path, fixture_data, field):
    del fixture_data[field]
    fixture_path.write_text(json.dumps(fixture_data))
    with pytest.raises(ValueError, match=f"Missing required field: {field}"):
        msk.load_fixture(fixture_path)


@pytest.mark.parametrize("field", ["key_base64", "value_base64"])
@pytest.mark.parametrize("value", ["%%%", "a", "YQ==\n", "한글"])
def test_invalid_base64_rejected(fixture_path, fixture_data, field, value):
    fixture_data[field] = value
    fixture_path.write_text(json.dumps(fixture_data))
    with pytest.raises(ValueError, match=f"Invalid Base64 in {field}"):
        msk.load_fixture(fixture_path)


@pytest.mark.parametrize("field", ["key_base64", "value_base64"])
def test_empty_bytes_rejected(fixture_path, fixture_data, field):
    fixture_data[field] = base64.b64encode(b"").decode("ascii")
    fixture_path.write_text(json.dumps(fixture_data))
    with pytest.raises(ValueError, match=field):
        msk.load_fixture(fixture_path)


@pytest.mark.parametrize("value", [None, 3, True, " "])
def test_invalid_metadata_rejected(fixture_path, fixture_data, value):
    fixture_data["event_id"] = value
    fixture_path.write_text(json.dumps(fixture_data))
    with pytest.raises(ValueError, match="event_id"):
        msk.load_fixture(fixture_path)


@pytest.mark.parametrize("content", ["[]", "null", "{broken"])
def test_invalid_json_rejected(fixture_path, content):
    fixture_path.write_text(content)
    with pytest.raises(ValueError):
        msk.load_fixture(fixture_path)


def test_send_preserves_bytes_and_uses_ack(fixture_path, transport):
    fixture = msk.load_fixture(fixture_path)
    metadata = msk.produce_fixture(
        fixture, "broker-a:9098, broker-b:9098", "ap-northeast-2", 7
    )
    kafka = transport.return_value
    kafka.send.assert_called_once_with(fixture.topic, key=KEY, value=VALUE)
    assert "partition" not in kafka.send.call_args.kwargs
    assert kafka.send.call_args.kwargs["key"] is fixture.key
    assert kafka.send.call_args.kwargs["value"] is fixture.value
    kafka.send.return_value.get.assert_called_once_with(timeout=7)
    assert (metadata.topic, metadata.partition, metadata.offset) == (
        fixture.topic,
        2,
        42,
    )
    kafka.flush.assert_called_once_with(timeout=7)
    kafka.close.assert_called_once_with(timeout=7)
    config = transport.call_args.kwargs
    provider = config.pop("sasl_oauth_token_provider")
    assert isinstance(provider, AbstractTokenProvider)
    assert provider.region == "ap-northeast-2"
    # Exact config also rules out directly supplied credentials / serializers.
    assert config == {
        "bootstrap_servers": ["broker-a:9098", "broker-b:9098"],
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "OAUTHBEARER",
        "key_serializer": None,
        "value_serializer": None,
        "acks": "all",
        "enable_idempotence": False,
        "retries": 0,
        "allow_auto_create_topics": False,
        "max_block_ms": 7000,
        "request_timeout_ms": 7000,
        "delivery_timeout_ms": 7000,
        "api_version_auto_timeout_ms": 7000,
    }


def test_token_provider_uses_default_credential_chain():
    provider = msk.MSKTokenProvider("ap-northeast-2")
    assert provider.token() == "test-token"
    msk.MSKAuthTokenProvider.generate_auth_token.assert_called_once_with(
        "ap-northeast-2"
    )


def test_cli_reports_broker_metadata(fixture_path, monkeypatch, capsys):
    monkeypatch.setenv("MSK_BOOTSTRAP_SERVERS", "broker:9098")
    monkeypatch.setenv("AWS_REGION", "ap-northeast-2")
    assert msk.main(["--fixture", str(fixture_path)]) == 0
    output = capsys.readouterr().out
    for line in (
        "Produced fixture successfully",
        "topic=fixture-selected-topic",
        "partition=2",
        "offset=42",
        "event_id=fixture-event",
        "equipment_id=fridge-001",
    ):
        assert line in output


@pytest.mark.parametrize("stage", ["init", "send", "ack", "flush", "close"])
def test_failures_are_nonzero_and_never_success(fixture_path, transport, capsys, stage):
    kafka = transport.return_value
    target = {
        "init": transport,
        "send": kafka.send,
        "ack": kafka.send.return_value.get,
        "flush": kafka.flush,
        "close": kafka.close,
    }[stage]
    target.side_effect = KafkaTimeoutError("sensitive error detail")
    assert (
        msk.main(
            [
                "--fixture",
                str(fixture_path),
                "--bootstrap-servers",
                "broker:9098",
                "--timeout",
                "1",
            ]
        )
        == 1
    )
    output = capsys.readouterr()
    assert not output.out
    assert "KafkaTimeoutError" in output.err
    assert "sensitive error detail" not in output.err
    if stage != "init":
        kafka.close.assert_called_once_with(timeout=1)


def test_invalid_fixture_never_constructs_producer(fixture_path, transport, capsys):
    fixture_path.write_text("{}")
    assert (
        msk.main(
            [
                "--fixture",
                str(fixture_path),
                "--bootstrap-servers",
                "broker:9098",
            ]
        )
        == 1
    )
    assert "Missing required field" in capsys.readouterr().err
    transport.assert_not_called()


def test_cli_overrides_environment(fixture_path, monkeypatch, transport):
    monkeypatch.setenv("MSK_BOOTSTRAP_SERVERS", "environment:9098")
    monkeypatch.setenv("AWS_REGION", "us-east-1")
    assert (
        msk.main(
            [
                "--fixture",
                str(fixture_path),
                "--bootstrap-servers",
                "override:9098",
                "--region",
                "ap-northeast-2",
            ]
        )
        == 0
    )
    config = transport.call_args.kwargs
    assert config["bootstrap_servers"] == ["override:9098"]
    assert config["sasl_oauth_token_provider"].region == "ap-northeast-2"


@pytest.mark.parametrize("args", [[], ["--timeout", "0"], ["--region", " "]])
def test_invalid_cli_configuration(fixture_path, monkeypatch, transport, args):
    monkeypatch.delenv("MSK_BOOTSTRAP_SERVERS", raising=False)
    if args:
        args = ["--bootstrap-servers", "broker:9098", *args]
    with pytest.raises(SystemExit) as exc:
        msk.main(["--fixture", str(fixture_path), *args])
    assert exc.value.code == 2
    transport.assert_not_called()
