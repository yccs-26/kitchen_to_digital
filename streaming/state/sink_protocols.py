"""Injected sink interfaces and explicit local ACK evidence."""

from dataclasses import dataclass
from typing import Protocol

from streaming.state.change_envelope import ChangeEnvelope, CurrentProjection


class RetryableSinkError(RuntimeError):
    """No confirmed completion; retry the exact durable intent."""


@dataclass(frozen=True)
class HistoryAck:
    identity: tuple[str, str]
    payload_sha256: str
    reference: str


@dataclass(frozen=True)
class KafkaAck:
    topic: str
    key: bytes
    partition: int
    offset: int
    payload_sha256: str
    callback_received: bool = True
    error: str | None = None


@dataclass(frozen=True)
class CurrentAck:
    status: str
    projection_id: str
    payload_sha256: str
    generation: int
    revision: int
    canonical_payload: bytes


class HistorySink(Protocol):
    def write(self, change: ChangeEnvelope) -> HistoryAck: ...


class KafkaSink(Protocol):
    def publish(self, change: ChangeEnvelope) -> KafkaAck: ...


class CurrentSink(Protocol):
    def apply(self, projection: CurrentProjection) -> CurrentAck: ...
