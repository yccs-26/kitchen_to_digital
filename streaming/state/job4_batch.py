"""Synchronous explicit-record batches; no Spark or background runtime."""

from dataclasses import dataclass
import sqlite3

from streaming.state.change_envelope import OutputConflict, read_json
from streaming.state.fenced_application import FencedStateApplication
from streaming.state.health_registry import HealthRegistryError
from streaming.state.job4_contract import (
    BatchIdentity, InputPosition, ValidatedCommand, validate_input_order,
)
from streaming.state.job4_input import (
    InputContractError, RetryableInputError, UnsupportedInputError,
    prepare_input,
)
from streaming.state.job4_store import Job4Store
from streaming.state.registry import RegistryError, RegistryErrorCode
from streaming.state.sink_protocols import RetryableSinkError
from streaming.state.writer_fencing import StaleClaim, StaleWriter


@dataclass(frozen=True)
class BatchResult:
    batch_id: str
    command_refs: tuple[str, ...]
    input_committed: bool
    publication_complete: bool
    completion_recorded: bool

    @property
    def checkpoint_allowed(self):
        return (self.input_committed and self.publication_complete
                and self.completion_recorded)


class BatchExecutionError(RuntimeError):
    def __init__(self, category, *, stage, batch_id, failed_ref,
                 completed_refs):
        self.category = category
        self.stage = stage
        self.batch_id = batch_id
        self.failed_ref = failed_ref
        self.completed_refs = tuple(completed_refs)
        super().__init__(f'Job 4 batch incomplete: {category}; stage={stage}')


def failure_category(error):
    if isinstance(error, (StaleWriter, StaleClaim)):
        return 'FENCED'
    if isinstance(error, UnsupportedInputError):
        return 'UNSUPPORTED'
    if isinstance(error, RegistryError) and error.code in (
        RegistryErrorCode.UNKNOWN_EQUIPMENT,
        RegistryErrorCode.HANDLER_NOT_IMPLEMENTED,
        RegistryErrorCode.INPUT_CONTRACT_UNAVAILABLE,
    ):
        return 'UNSUPPORTED'
    if isinstance(error, (InputContractError, HealthRegistryError,
                          RegistryError)):
        return 'CONTRACT'
    if isinstance(error, (RetryableInputError, RetryableSinkError,
                          ConnectionError, TimeoutError)):
        return 'RETRYABLE'
    if isinstance(error, sqlite3.OperationalError) and getattr(
        error, 'sqlite_errorcode', None,
    ) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        return 'RETRYABLE'
    if isinstance(error, (OutputConflict, ValueError)):
        return 'CONFLICT_OR_INTEGRITY'
    return 'SYSTEM_UNCLASSIFIED'


class Job4BatchApplication:
    """Reuse native reducers, admission CAS, codecs and claimed publication.

    apply_inputs returns durable-input evidence only. execute/complete require
    delivered evidence and a durable completion record before reporting ready.
    """

    def __init__(self, application, registry):
        if not isinstance(application, FencedStateApplication) or not (
            isinstance(application.store, Job4Store)
        ):
            raise TypeError('FencedStateApplication with Job4Store required')
        self.application = application
        self.store = application.store
        self.registry = registry

    def _prepare(self, records):
        if not isinstance(records, (tuple, list)):
            raise InputContractError('explicit record sequence required')
        unique = {}
        for record in records:
            item = prepare_input(record, source_epoch=self.store.source_epoch,
                                 registry=self.registry)
            previous = unique.get(item.command_ref)
            if previous is not None and previous.fingerprint != (
                item.fingerprint
            ):
                raise OutputConflict('duplicate offset with conflicting input')
            unique.setdefault(item.command_ref, item)
        observations = []
        for item in unique.values():
            app = self.application.application
            binding = app._binding(item.device[1])
            if binding.store_id != item.device[0]:
                raise InputContractError('store/equipment mismatch')
            policy = app._health_policy(item.device[1])
            if item.kind == 'observation':
                event = read_json(item.payload)
                app.health_registry.resolve(
                    store_id=event['store_id'],
                    equipment_id=event['equipment_id'],
                    equipment_type=event['equipment_type'],
                    profile=policy.profile,
                )
                origin = read_json(item.origin)
                observations.append(ValidatedCommand(
                    item.command_ref,
                    InputPosition(origin['topic'], origin['partition'],
                                  origin['offset'],
                                  bytes.fromhex(origin['key_hex']),
                                  origin['writer_schema_id']),
                    item.payload, item.evaluation_time,
                ))
        validate_input_order(tuple(observations))
        return tuple(unique.values())

    def _writers(self, items, writers):
        for device in dict.fromkeys(item.device for item in items):
            token = writers.get(device)
            if token is None or token.device != device:
                raise StaleWriter('explicit device writer token required')
            self.store._check_writer(token)

    def _fail(self, error, stage, batch_id, ref, completed):
        if batch_id is not None:
            try:
                self.store.attempt(batch_id, stage, ref, completed, error)
            except sqlite3.Error:
                # A broken DB cannot promise audit; never return ready.
                pass
        raise BatchExecutionError(
            failure_category(error), stage=stage, batch_id=batch_id,
            failed_ref=ref, completed_refs=completed,
        ) from error

    def apply_inputs(self, batch, records, *, writers,
                     hook=lambda stage: None):
        batch_id = None
        try:
            if not isinstance(batch, BatchIdentity):
                raise InputContractError('BatchIdentity required')
            items = self._prepare(records)
            with self.store.transaction():
                self._writers(items, writers)
                batch_id = self.store.register(batch, items)
            hook('after_manifest')
            return self.resume_inputs(batch_id, writers=writers, hook=hook)
        except BatchExecutionError:
            raise
        except Exception as exc:
            self._fail(exc, 'preflight_or_manifest', batch_id, None, ())

    def resume_inputs(self, batch_id, *, writers, hook=lambda stage: None):
        """Resume the exact durable manifest without Kafka/Registry access."""
        current, completed = None, []
        stage = 'load_registered'
        try:
            manifest = self.store.manifest(batch_id)
            items = tuple(self.store.input(ref) for ref in manifest['refs'])
            with self.store.transaction():
                self._writers(items, writers)
            for item in items:
                stage = 'command'
                item = self.store.input(item.command_ref)
                current = item.command_ref
                with self.store.input_scope(item):
                    token = writers[item.device]
                    hook('before_command')
                    if item.kind == 'observation':
                        self.application.process_validated(
                            token, read_json(item.payload),
                            command_ref=item.command_ref,
                            reference_time=item.evaluation_time, hook=hook,
                        )
                    else:
                        self.application.evaluate_timer(
                            token, item.device[1], item.timer_token(),
                            command_ref=item.command_ref,
                            reference_time=item.evaluation_time, hook=hook,
                        )
                completed.append(current)
                stage = 'after_command'
                hook('after_command')
            stage = 'input_completion'
            with self.store.transaction():
                self._writers(items, writers)
                self.store.mark(batch_id, hook=hook)
            hook('after_input_complete')
            durable, delivered, _ = self.store.evidence(batch_id)
            return BatchResult(batch_id, tuple(completed), durable, delivered,
                               False)
        except Exception as exc:
            self._fail(exc, stage, batch_id, current, completed)

    def complete(self, batch_id, *, writers, coordinator=None, owner=None,
                 clock=None, lease=None, hook=lambda stage: None):
        completed = []
        try:
            manifest = self.store.manifest(batch_id)
            items = tuple(self.store.input(ref) for ref in manifest['refs'])
            with self.store.transaction():
                self._writers(items, writers)
                for item in items:
                    try:
                        status = self.store.command_status(item.device,
                                                           item.command_ref)
                    except KeyError:
                        continue
                    if status == 'COMPLETE':
                        completed.append(item.command_ref)
            if coordinator is not None:
                if coordinator.store is not self.store:
                    raise InputContractError('publication authority mismatch')
                coordinator.reconcile_claimed(owner=owner, clock=clock,
                                              lease=lease)
            durable, delivered, _ = self.store.evidence(batch_id)
            if not durable or not delivered:
                raise RetryableSinkError('batch publication incomplete')
            with self.store.transaction():
                self._writers(items, writers)
                self.store.mark(batch_id, complete=True, hook=hook)
            hook('after_batch_complete')
            return BatchResult(batch_id, tuple(manifest['refs']), True, True,
                               True)
        except Exception as exc:
            self._fail(exc, 'publication_or_completion', batch_id, None,
                       completed)

    def execute(self, batch, records, *, writers, coordinator=None, owner=None,
                clock=None, lease=None, hook=lambda stage: None):
        result = self.apply_inputs(batch, records, writers=writers, hook=hook)
        return self.complete(
            result.batch_id, writers=writers, coordinator=coordinator,
            owner=owner, clock=clock, lease=lease, hook=hook,
        )

    def recover(self, batch_id, *, writers, coordinator=None, owner=None,
                clock=None, lease=None, hook=lambda stage: None):
        result = self.resume_inputs(batch_id, writers=writers, hook=hook)
        return self.complete(
            result.batch_id, writers=writers, coordinator=coordinator,
            owner=owner, clock=clock, lease=lease, hook=hook,
        )
