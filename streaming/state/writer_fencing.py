"""Opt-in local fencing and admission on NEW SQLite v2 databases only.

Epochs are local control records, separate from domain/monitoring generations.
Callers supply command order, request bytes and a clock. This is not a leader
service, distributed outbox or an external sink fencing mechanism.
"""

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import timedelta
import sqlite3

from streaming.state.change_envelope import (
    OutputConflict, canonical, read_json, stamp, text,
)
from streaming.state.publication_store import PublicationStore, _SCHEMA
from streaming.state.publication import PublicationCoordinator
from streaming.state.sink_protocols import RetryableSinkError


class StaleWriter(OutputConflict):
    """Authority changed; a new authorized token is required."""


class CommandBusy(RetryableSinkError):
    """An earlier device command must be resumed before a later one."""


class RevisionConflict(RetryableSinkError):
    """Re-read state and recompute the same admitted command."""


class ClaimBusy(RetryableSinkError):
    """A live dispatcher claim prevents another dispatcher attempt."""


class StaleClaim(OutputConflict):
    """Expired/replaced claim cannot acknowledge or complete deliveries."""


@dataclass(frozen=True)
class WriterToken:
    device: tuple[str, str]
    owner: str
    epoch: int


@dataclass(frozen=True)
class DispatchClaim:
    output_id: str
    owner: str
    sequence: int
    expires_at: str


def device_key(device):
    if type(device) is not tuple or len(device) != 2:
        raise ValueError('store/equipment tuple required')
    for part in device:
        text(part)
    return canonical(device).decode()


class FencedPublicationStore(PublicationStore):
    """Separate v2 store, no automatic migration and no hidden clock.

    One SQLite transaction checks authority and commits each axis. A durable
    per-device command barrier prevents later commands overtaking partial ones.
    Nested calls use savepoints, including when callers catch inner failures.
    Use a separate connection per thread/process, never share an active scope.
    """

    def __init__(self, path):
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self._writer = None
        self._command = None
        self._dispatch = None
        self._savepoint = 0
        try:
            self.db.execute('PRAGMA foreign_keys=ON')
            self.db.execute('PRAGMA synchronous=FULL')
            with self.transaction():
                version = self.db.execute('PRAGMA user_version').fetchone()[0]
                tables = self.db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'",
                ).fetchall()
                if version == 0 and not tables:
                    for statement in (*_SCHEMA, *_CONTROL_SCHEMA):
                        self.db.execute(statement)
                    self.db.execute('PRAGMA user_version=2')
                elif version != 2:
                    raise ValueError('new v2 database required; no migration')
                else:
                    for table in ('authorities', 'admissions', 'claims'):
                        self.db.execute(f'SELECT * FROM {table} LIMIT 0')
        except BaseException:
            self.db.close()
            raise

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @contextmanager
    def transaction(self):
        if self.db.in_transaction:
            self._savepoint += 1
            name = f'fenced_scope_{self._savepoint}'
            self.db.execute(f'SAVEPOINT {name}')
            try:
                yield
                self.db.execute(f'RELEASE SAVEPOINT {name}')
            except BaseException:
                self.db.execute(f'ROLLBACK TO SAVEPOINT {name}')
                self.db.execute(f'RELEASE SAVEPOINT {name}')
                raise
        else:
            with super().transaction():
                yield

    def acquire_writer(self, device, owner):
        """Explicit LOCAL takeover; production authority remains unresolved."""
        key = device_key(device)
        text(owner)
        with self.transaction():
            row = self.db.execute(
                'SELECT epoch FROM authorities WHERE device=?', (key,),
            ).fetchone()
            if row is None:
                self.db.execute('INSERT INTO authorities VALUES (?,?,1)',
                                (key, owner))
                epoch = 1
            else:
                epoch = row[0] + 1
                updated = self.db.execute(
                    'UPDATE authorities SET owner=?,epoch=? '
                    'WHERE device=? AND epoch=?',
                    (owner, epoch, key, row[0]),
                )
                if updated.rowcount != 1:
                    raise StaleWriter('authority CAS failed')
        return WriterToken(device, owner, epoch)

    def _check_writer(self, token):
        if not isinstance(token, WriterToken):
            raise StaleWriter('writer token required')
        row = self.db.execute(
            'SELECT owner,epoch FROM authorities WHERE device=?',
            (device_key(token.device),),
        ).fetchone()
        if row is None or (row[0], row[1]) != (token.owner, token.epoch):
            raise StaleWriter('writer epoch/owner superseded')

    def command_status(self, device, command_ref):
        row = self.db.execute(
            'SELECT axes,done FROM admissions WHERE device=? AND ref=?',
            (device_key(device), command_ref),
        ).fetchone()
        if row is None:
            raise KeyError(command_ref)
        axes, done = read_json(bytes(row[0])), read_json(bytes(row[1]))
        return ('COMPLETE' if axes == done else
                'PARTIAL' if done else 'ADMITTED')

    def run_command(self, token, command_ref, request, axes, operation):
        """Record a caller-ordered command, then resume its independent axes.

        request must include input, evaluation time and exact configuration.
        A failed/invalid command remains admitted: do not silently skip it.
        The caller must retry the same request; no implicit cancel/migration.
        """
        text(command_ref)
        read_json(request)
        if not axes or len(set(axes)) != len(axes) or any(
            axis not in ('OPERATIONAL', 'HEALTH') for axis in axes
        ):
            raise ValueError('explicit unique axes required')
        if self._writer is not None:
            raise ValueError('nested command scope unsupported')
        if not isinstance(token, WriterToken):
            raise StaleWriter('writer token required')
        key = device_key(token.device)
        encoded_axes = canonical(sorted(axes))
        with self.transaction():
            self._check_writer(token)
            row = self.db.execute(
                'SELECT * FROM admissions WHERE device=? AND ref=?',
                (key, command_ref),
            ).fetchone()
            if row is not None:
                if bytes(row['request']) != request or (
                    bytes(row['axes']) != encoded_axes
                ):
                    raise OutputConflict(
                        'command configuration/input conflict',
                    )
            else:
                previous = self.db.execute(
                    'SELECT ref FROM admissions WHERE device=? AND axes!=done',
                    (key,),
                ).fetchone()
                if previous:
                    raise CommandBusy(
                        'resume incomplete command: ' + previous[0],
                    )
                self.db.execute(
                    'INSERT INTO admissions VALUES (?,?,?,?,?)',
                    (key, command_ref, request, encoded_axes, canonical([])),
                )
        self._writer, self._command = token, command_ref
        try:
            return operation()
        finally:
            self._writer = self._command = None

    def commit(self, command, *, hook=lambda stage: None):
        with self.transaction():
            self._check_writer(self._writer)
            token = self._writer
            if command.axis_key[:2] != token.device or (
                command.command_ref != self._command
            ):
                raise StaleWriter('command identity outside writer scope')
            row = self.db.execute(
                'SELECT axes,done FROM admissions WHERE device=? AND ref=?',
                (device_key(token.device), self._command),
            ).fetchone()
            axes, done = read_json(bytes(row[0])), read_json(bytes(row[1]))
            axis = command.axis_key[2]
            if axis not in axes:
                raise OutputConflict('axis not admitted')
            try:
                self.command_payload(command.axis_key, command.command_ref)
            except KeyError:
                root = self.state(command.axis_key)
                if root.revision != command.expected_revision:
                    raise RevisionConflict('snapshot revision changed')
            output_id = super().commit(command, hook=hook)
            if axis not in done:
                done.append(axis)
                self.db.execute(
                    'UPDATE admissions SET done=? WHERE device=? AND ref=?',
                    (canonical(sorted(done)), device_key(token.device),
                     self._command),
                )
            return output_id

    def _publishable(self, output_id):
        output = self.output(output_id)
        data = output.projection.data
        if self.command_status(output.projection.axis_key[:2],
                               data['command_ref']) != 'COMPLETE':
            raise CommandBusy('publication blocked by incomplete axes')

    def claim(self, output_id, owner, *, evaluation_time, lease):
        text(owner)
        stamp(evaluation_time)
        if not isinstance(lease, timedelta) or lease <= timedelta(0):
            raise ValueError('positive explicit lease required')
        with self.transaction():
            self._publishable(output_id)
            row = self.db.execute(
                'SELECT * FROM claims WHERE output_id=?', (output_id,),
            ).fetchone()
            if row is not None and stamp(evaluation_time) < (
                row['last_checked']
            ):
                raise StaleClaim('dispatcher clock reversed')
            if row is not None and row['active'] and (
                stamp(evaluation_time) < row['expires_at']
            ):
                raise ClaimBusy('dispatcher claim still live')
            sequence = row['sequence'] + 1 if row else 1
            expires = stamp(evaluation_time + lease)
            self.db.execute(
                'INSERT INTO claims VALUES (?,?,?,?,1,?) '
                'ON CONFLICT(output_id) DO UPDATE SET owner=excluded.owner,'
                'sequence=excluded.sequence,expires_at=excluded.expires_at,'
                'active=1,last_checked=excluded.last_checked',
                (output_id, owner, sequence, expires, stamp(evaluation_time)),
            )
        return DispatchClaim(output_id, owner, sequence, expires)

    def _check_claim(self, output_id):
        if self._dispatch is None:
            raise StaleClaim('explicit dispatch scope required')
        claim, clock = self._dispatch
        now = stamp(clock())
        row = self.db.execute(
            'SELECT * FROM claims WHERE output_id=?', (output_id,),
        ).fetchone()
        if row is None or not row['active'] or (
            claim.output_id != output_id
            or (row['owner'], row['sequence'], row['expires_at']) != (
                claim.owner, claim.sequence, claim.expires_at,
            ) or now >= claim.expires_at or now < row['last_checked']
        ):
            raise StaleClaim('dispatcher claim expired or replaced')
        self._publishable(output_id)
        self.db.execute(
            'UPDATE claims SET last_checked=? WHERE output_id=?',
            (now, output_id),
        )

    @contextmanager
    def dispatch_scope(self, claim, clock):
        if self._dispatch is not None:
            raise ValueError('nested dispatch scope unsupported')
        self._dispatch = claim, clock
        try:
            yield
        finally:
            self._dispatch = None

    def release(self, claim):
        with self.transaction():
            self.db.execute(
                'UPDATE claims SET active=0 WHERE output_id=? AND owner=? '
                'AND sequence=?',
                (claim.output_id, claim.owner, claim.sequence),
            )

    def begin_attempt(self, output_id, sink):
        with self.transaction():
            self._check_claim(output_id)
            return super().begin_attempt(output_id, sink)

    def finish_attempt(self, output_id, sink, status, **kwargs):
        with self.transaction():
            self._check_claim(output_id)
            return super().finish_attempt(output_id, sink, status, **kwargs)

    def complete(self, output_id):
        with self.transaction():
            self._check_claim(output_id)
            return super().complete(output_id)


class ClaimedDispatcher(PublicationCoordinator):
    """Lease around the existing ACK coordinator; writes can duplicate."""

    def dispatch_claimed(self, output_id, *, owner, clock, lease,
                         hook=lambda stage: None):
        claim = self.store.claim(output_id, owner, evaluation_time=clock(),
                                 lease=lease)
        try:
            # Process exit leaves the claim live; Python unwinding releases it.
            hook('after_claim')
            with self.store.dispatch_scope(claim, clock):
                return super().dispatch(output_id, hook=hook)
        finally:
            self.store.release(claim)

    def reconcile_claimed(self, *, owner, clock, lease):
        """One bounded scan; report blocked outputs without starving others."""
        results = []
        for output_id in self.store.pending():
            try:
                self.dispatch_claimed(output_id, owner=owner, clock=clock,
                                      lease=lease)
            except (CommandBusy, ClaimBusy):
                results.append((output_id, 'BLOCKED'))
            else:
                results.append((output_id, 'COMPLETE'))
        return tuple(results)


_CONTROL_SCHEMA = (
    'CREATE TABLE authorities (device TEXT PRIMARY KEY, owner TEXT NOT NULL, '
    'epoch INTEGER NOT NULL)',
    'CREATE TABLE admissions (device TEXT NOT NULL, ref TEXT NOT NULL, '
    'request BLOB NOT NULL, axes BLOB NOT NULL, done BLOB NOT NULL, '
    'PRIMARY KEY(device,ref))',
    'CREATE TABLE claims (output_id TEXT PRIMARY KEY REFERENCES outputs(id), '
    'owner TEXT NOT NULL, sequence INTEGER NOT NULL, '
    'expires_at TEXT NOT NULL, active INTEGER NOT NULL, '
    'last_checked TEXT NOT NULL)',
)
