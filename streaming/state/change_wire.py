"""Local state-change Avro draft and optional explicit Confluent framing.

No Registry client/registration or Kafka producer. Ledger canonical bytes are
authoritative; typed outer fields are checked against those bytes on decode.
"""

from dataclasses import dataclass
from io import BytesIO
import json
from pathlib import Path

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from streaming.state.change_envelope import (
    ChangeEnvelope, OutputConflict, canonical, revision,
)
from streaming.validation.avro_decoder import parse_confluent_frame


WIRE_VERSION = "state-change-avro-v1"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / (
    "schemas/avro/state_change_event.avsc"
)


class ChangeWireError(ValueError):
    pass


def load_change_schema():
    return json.loads(SCHEMA_PATH.read_text())


def _schema_id(value):
    revision(value, 1)
    if value > 2 ** 32 - 1:
        raise ChangeWireError("schema ID exceeds Confluent framing range")
    return value


def _record(change):
    data = change.data
    health = data["axis"] == "HEALTH"
    body = data["body"]
    token = body.get("timer_token")
    return {
        "wire_version": WIRE_VERSION, "axis": data["axis"],
        "state_change_id": None if health else change.identity[1],
        "health_change_id": change.identity[1] if health else None,
        **data["equipment"], "profile_id": data["profile"]["profile_id"],
        "profile_version": data["profile"]["version"],
        "previous_state": body["previous"], "current_state": body["current"],
        "effective_at": data["effective_at"],
        "evaluation_time": data["evaluation_time"],
        "last_seen": body.get("last_seen"),
        "freshness_deadline": body.get("freshness_deadline"),
        "change_revision": data["change_revision"],
        "snapshot_revision": body.get("snapshot_revision_at_change"),
        "policy_fingerprint": data["policy"].get("fingerprint"),
        "timer_provenance": canonical(token) if token is not None else None,
        "canonical_payload": change.payload, "payload_sha256": change.sha256,
    }


@dataclass(frozen=True)
class WireChange:
    key: bytes
    value: bytes
    identity: tuple[str, str]


def encode_change(change, *, canonical_change=None, schema_id=None):
    if not isinstance(change, ChangeEnvelope):
        raise TypeError("ChangeEnvelope required")
    if canonical_change is not None:
        if not isinstance(canonical_change, ChangeEnvelope) or (
            canonical_change.identity != change.identity
            or canonical_change.payload != change.payload
        ):
            raise OutputConflict("wire change conflicts with canonical Ledger")
    stream = BytesIO()
    schemaless_writer(stream, parse_schema(load_change_schema()),
                      _record(change), strict=True)
    value = stream.getvalue()
    if schema_id is not None:
        value = b"\x00" + _schema_id(schema_id).to_bytes(4, "big") + value
    return WireChange(
        change.data["equipment"]["equipment_id"].encode("utf-8"),
        value, change.identity,
    )


def decode_change(key, value, *, schema_id=None):
    if type(key) is not bytes or type(value) is not bytes:
        raise TypeError("key/value must be bytes")
    try:
        if schema_id is not None:
            expected = _schema_id(schema_id)
            actual, value = parse_confluent_frame(value)
            if actual != expected:
                raise ChangeWireError("unexpected writer schema ID")
        stream = BytesIO(value)
        record = schemaless_reader(stream, parse_schema(load_change_schema()))
        if stream.tell() != len(value):
            raise ChangeWireError("trailing Avro bytes")
        change = ChangeEnvelope(record["canonical_payload"])
        if record != _record(change):
            raise ChangeWireError("outer Avro/canonical payload mismatch")
        if key != change.data["equipment"]["equipment_id"].encode("utf-8"):
            raise ChangeWireError("Kafka key/payload identity mismatch")
        return change
    except ChangeWireError:
        raise
    except (ValueError, TypeError, KeyError, EOFError, IndexError,
            OverflowError, UnicodeError) as exc:
        raise ChangeWireError("invalid state-change wire payload") from exc
