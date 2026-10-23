"""Pure offline Job 4 boundary checks, not a Spark or storage adapter.

No source cursor, batch receipt or lineage is persisted here. A runtime must
atomically bind those records to command admission before advancing progress.
Caller evidence is a local assertion, not an authenticated remote receipt.
"""

from dataclasses import dataclass
from datetime import datetime

from streaming.state.application import validated_observation
from streaming.state.change_envelope import (
    canonical, read_json, revision, stamp, text,
)
from streaming.state.publication_store import SINKS, TERMINAL


@dataclass(frozen=True)
class BatchIdentity:
    """Audit identity only; never a native change ID or writer epoch."""

    query_id: str
    checkpoint_epoch: str
    batch_id: int

    def __post_init__(self):
        text(self.query_id)
        text(self.checkpoint_epoch)
        revision(self.batch_id)


@dataclass(frozen=True)
class InputPosition:
    """Explicit decoded validated-topic provenance; no Registry lookup."""

    topic: str
    partition: int
    offset: int
    key: bytes
    writer_schema_id: int

    def __post_init__(self):
        if self.topic != 'kitchen.sensor.validated':
            raise ValueError('validated topic required')
        revision(self.partition)
        revision(self.offset)
        revision(self.writer_schema_id)
        if self.writer_schema_id > 2 ** 32 - 1:
            raise ValueError('writer schema ID exceeds Confluent frame range')
        if type(self.key) is not bytes or not self.key:
            raise ValueError('nonempty wire key bytes required')


@dataclass(frozen=True)
class ValidatedCommand:
    """Immutable numeric command plus lineage and recorded evaluation time.

    Semantic metric/profile/source admission remains in the existing Registry.
    No source or transport identity is invented from event_time or batch_id.
    """

    command_ref: str
    position: InputPosition
    payload: bytes
    reference_time: datetime

    def __post_init__(self):
        text(self.command_ref)
        stamp(self.reference_time)
        if not isinstance(self.position, InputPosition):
            raise ValueError('InputPosition required')
        sample = validated_observation(read_json(self.payload))
        if canonical(sample) != self.payload:
            raise ValueError('canonical normalized validated payload required')
        if self.position.key != sample.equipment_id.encode('utf-8'):
            raise ValueError('Kafka key/payload identity mismatch')

    @classmethod
    def from_event(cls, event, *, command_ref, position, reference_time):
        return cls(command_ref, position,
                   canonical(validated_observation(event)), reference_time)

    @property
    def device(self):
        sample = read_json(self.payload)
        return sample['store_id'], sample['equipment_id']

    def application_arguments(self):
        """Arguments for existing process_validated; lineage stays separate.

        The runtime still needs a durable ingress record. This conversion does
        not imply that current Application storage persists the InputPosition.
        """
        return dict(
            event=read_json(self.payload), command_ref=self.command_ref,
            reference_time=self.reference_time,
        )


def validate_input_order(commands):
    """Check supplied per-device source order within one explicit input tuple.

    Do not sort. Offset gaps can contain other equipment. Cross-batch cursors
    and multi-topic/partition migration require an external authority/cutover.
    """
    if type(commands) is not tuple or any(
        not isinstance(command, ValidatedCommand) for command in commands
    ):
        raise ValueError('tuple of ValidatedCommand required')
    cursors = {}
    for command in commands:
        position = command.position
        cursor = position.topic, position.partition, position.offset
        previous = cursors.get(command.device)
        if previous is not None and (
            cursor[:2] != previous[:2] or cursor[2] <= previous[2]
        ):
            raise ValueError('device source order/cutover mismatch')
        cursors[command.device] = cursor
    return commands


@dataclass(frozen=True)
class CommandEvidence:
    """Caller-supplied status read from one authoritative committed view."""

    device: tuple[str, str]
    command_ref: str
    status: str
    output_ids: tuple[str, ...]

    def __post_init__(self):
        if type(self.device) is not tuple or len(self.device) != 2:
            raise ValueError('store/equipment tuple required')
        for part in self.device:
            text(part)
        text(self.command_ref)
        if self.status not in ('ADMITTED', 'PARTIAL', 'COMPLETE'):
            raise ValueError('unknown command status')
        if type(self.output_ids) is not tuple or len(set(self.output_ids)) != (
            len(self.output_ids)
        ):
            raise ValueError('unique output ID tuple required')
        for output_id in self.output_ids:
            text(output_id)


def checkpoint_ready(commands, deliveries):
    """Conservative synchronous completion predicate; never writes checkpoint.

    Caller must provide the complete expected command manifest and a consistent
    authoritative read. Empty commands means an explicitly empty manifest, not
    successful discovery of missing work. Pending-only scans are insufficient.
    """
    if type(commands) is not tuple or any(
        not isinstance(command, CommandEvidence) for command in commands
    ) or type(deliveries) is not dict:
        raise ValueError('typed command tuple and delivery mapping required')
    keys = [(item.device, item.command_ref) for item in commands]
    if len(set(keys)) != len(keys):
        raise ValueError('duplicate command evidence')
    outputs = [output_id for item in commands for output_id in item.output_ids]
    if len(set(outputs)) != len(outputs):
        raise ValueError('output shared by different command evidence')
    if any(item.status != 'COMPLETE' for item in commands):
        return False
    for output_id in outputs:
        receipt = deliveries.get(output_id)
        if type(receipt) is not dict or set(receipt) != set(SINKS):
            return False
        if any(type(receipt[sink]) is not str or receipt[sink] not in TERMINAL
               for sink in SINKS):
            return False
        if receipt['current'] == 'NOT_REQUIRED' or any(
            receipt[sink] == 'SUPERSEDED' for sink in ('history', 'kafka')
        ) or receipt['kafka'] == 'ALREADY_APPLIED':
            return False
        if (receipt['history'] == 'NOT_REQUIRED') != (
            receipt['kafka'] == 'NOT_REQUIRED'
        ):
            return False
    return True
