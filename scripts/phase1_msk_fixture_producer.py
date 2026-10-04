import argparse
import base64
import binascii
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path

from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
from botocore.exceptions import BotoCoreError
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.producer.future import RecordMetadata
from kafka.sasl.oauth import AbstractTokenProvider


@dataclass(frozen=True)
class Fixture:
    topic: str
    equipment_id: str
    event_id: str
    schema_version: str
    key: bytes
    value: bytes


def load_fixture(path: Path) -> Fixture:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        # Invalid file content, rather than a wrongly typed Python argument.
        raise ValueError("Fixture must be a JSON object")  # noqa: TRY004
    fields = (
        "topic",
        "equipment_id",
        "event_id",
        "schema_version",
        "key_base64",
        "value_base64",
    )
    for field in fields:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")
        if not isinstance(data[field], str) or not data[field].strip():
            raise ValueError(f"{field} must be a non-empty string")

    decoded = {}
    for field in ("key_base64", "value_base64"):
        try:
            decoded[field] = base64.b64decode(data[field], validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError(f"Invalid Base64 in {field}") from exc
        if not decoded[field]:
            raise ValueError(f"Decoded {field} must not be empty")
    return Fixture(
        topic=data["topic"],
        equipment_id=data["equipment_id"],
        event_id=data["event_id"],
        schema_version=data["schema_version"],
        key=decoded["key_base64"],
        value=decoded["value_base64"],
    )


class MSKTokenProvider(AbstractTokenProvider):
    def __init__(self, region: str):
        self.region = region

    def token(self) -> str:
        # AWS default credential chain, including the EC2 instance profile.
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


def produce_fixture(
    fixture: Fixture,
    bootstrap_servers: str,
    region: str,
    timeout: int = 30,
) -> RecordMetadata:
    servers = [server.strip() for server in bootstrap_servers.split(",")]
    if not all(servers) or not region.strip() or timeout <= 0:
        raise ValueError(
            "Bootstrap servers, region and a positive timeout are required"
        )
    producer = KafkaProducer(
        bootstrap_servers=servers,
        security_protocol="SASL_SSL",
        sasl_mechanism="OAUTHBEARER",
        sasl_oauth_token_provider=MSKTokenProvider(region),
        key_serializer=None,
        value_serializer=None,
        acks="all",
        enable_idempotence=False,
        retries=0,
        allow_auto_create_topics=False,
        max_block_ms=timeout * 1000,
        request_timeout_ms=timeout * 1000,
        delivery_timeout_ms=timeout * 1000,
        api_version_auto_timeout_ms=timeout * 1000,
    )
    try:
        metadata = producer.send(
            fixture.topic, key=fixture.key, value=fixture.value
        ).get(timeout=timeout)
        producer.flush(timeout=timeout)
        return metadata
    finally:
        producer.close(timeout=timeout)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument(
        "--bootstrap-servers", default=os.getenv("MSK_BOOTSTRAP_SERVERS", "")
    )
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-northeast-2"))
    parser.add_argument("--timeout", type=int, default=30, help="Per-operation seconds")
    args = parser.parse_args(argv)
    if not args.bootstrap_servers.strip():
        parser.error("Set MSK_BOOTSTRAP_SERVERS or --bootstrap-servers")
    if not args.region.strip() or args.timeout <= 0:
        parser.error("Region must not be empty and timeout must be positive")
    try:
        fixture = load_fixture(args.fixture)
    except (OSError, ValueError) as exc:
        print(f"Invalid fixture: {exc}", file=sys.stderr)
        return 1
    try:
        metadata = produce_fixture(
            fixture, args.bootstrap_servers, args.region, args.timeout
        )
    except (KafkaError, BotoCoreError, OSError, ValueError) as exc:
        # Avoid logging signer tokens or credentials embedded in library errors.
        print(
            f"MSK produce failed ({type(exc).__name__}); delivery is unconfirmed. "
            "Check broker evidence before retrying.",
            file=sys.stderr,
        )
        return 1
    print(
        "Produced fixture successfully\n"
        f"topic={metadata.topic}\npartition={metadata.partition}\n"
        f"offset={metadata.offset}\nevent_id={fixture.event_id}\n"
        f"equipment_id={fixture.equipment_id}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
