"""Explicit NEW local Job 4 catalog, on the existing SQLite v2 authority.

No migration. The catalog and native admission share one database transaction.
Bootstrap/import of initial snapshots is an explicit caller responsibility.
"""

from contextlib import contextmanager
from pathlib import Path
import sqlite3

from streaming.state.change_envelope import (
    OutputConflict, canonical, digest, read_json, text,
)
from streaming.state.job4_contract import CommandEvidence, checkpoint_ready
from streaming.state.job4_input import restore_input
from streaming.state.publication_store import _SCHEMA, decode_output
from streaming.state.writer_fencing import (
    CommandBusy, FencedPublicationStore, _CONTROL_SCHEMA, device_key,
)


_CATALOG = (
    'CREATE TABLE job4_meta(version INTEGER,source_epoch TEXT)',
    'CREATE TABLE job4_inputs(ref TEXT PRIMARY KEY,record BLOB NOT NULL,'
    'sha TEXT NOT NULL,admitted INTEGER NOT NULL)',
    'CREATE TABLE job4_batches(id TEXT PRIMARY KEY,manifest BLOB NOT NULL,'
    'sha TEXT NOT NULL,input_done INTEGER NOT NULL,complete INTEGER NOT NULL)',
    'CREATE TABLE job4_cursors(device TEXT PRIMARY KEY,topic TEXT NOT NULL,'
    'partition INTEGER NOT NULL,offset INTEGER NOT NULL)',
    'CREATE TABLE job4_attempts(id INTEGER PRIMARY KEY,batch_id TEXT NOT NULL '
    'REFERENCES job4_batches(id),stage TEXT NOT NULL,ref TEXT,error_type TEXT,'
    'completed BLOB NOT NULL)',
)


class Job4Store(FencedPublicationStore):
    """Local catalog version 1 inside native user_version 2."""

    @classmethod
    def create(cls, path, *, source_epoch):
        text(source_epoch)
        path = Path(path)
        # Exclusive create: even an empty existing DB is never upgraded.
        with path.open('xb'):
            pass
        db = sqlite3.connect(str(path), isolation_level=None)
        try:
            db.execute('PRAGMA foreign_keys=ON')
            db.execute('PRAGMA synchronous=FULL')
            db.execute('BEGIN IMMEDIATE')
            for statement in (*_SCHEMA, *_CONTROL_SCHEMA, *_CATALOG):
                db.execute(statement)
            db.execute('INSERT INTO job4_meta VALUES (1,?)', (source_epoch,))
            db.execute('PRAGMA user_version=2')
            db.execute('COMMIT')
        finally:
            db.close()
        return cls(path)

    def __init__(self, path):
        # Inspect ownership read-only before the native constructor touches it.
        probe = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro',
                                uri=True)
        try:
            meta = probe.execute('SELECT * FROM job4_meta').fetchall()
            if len(meta) != 1 or len(meta[0]) != 2 or meta[0][0] != 1:
                raise ValueError('unsupported Job 4 catalog')
            self.source_epoch = text(meta[0][1])
        except sqlite3.Error as exc:
            raise ValueError('explicit Job4Store.create required') from exc
        finally:
            probe.close()
        super().__init__(path)
        self._input = None

    def input(self, ref):
        row = self.db.execute('SELECT * FROM job4_inputs WHERE ref=?',
                              (ref,)).fetchone()
        if row is None:
            raise KeyError(ref)
        record = bytes(row['record'])
        if digest(record) != row['sha']:
            raise ValueError('corrupt Job 4 input')
        item = restore_input(record)
        if item.command_ref != ref:
            raise ValueError('input reference mismatch')
        return item

    def manifest(self, batch_id):
        row = self.db.execute('SELECT * FROM job4_batches WHERE id=?',
                              (batch_id,)).fetchone()
        if row is None:
            raise KeyError(batch_id)
        record = bytes(row['manifest'])
        if digest(record) != row['sha']:
            raise ValueError('corrupt batch manifest')
        return read_json(record)

    def register(self, batch, inputs):
        """Durably freeze expected work, including first evaluation times."""
        refs = list(dict.fromkeys(item.command_ref for item in inputs))
        ranges = {}
        for item in inputs:
            origin = read_json(item.origin)
            if origin['source_epoch'] != self.source_epoch:
                raise OutputConflict('source epoch mismatch')
            if item.kind == 'observation':
                key = origin['topic'], origin['partition']
                low, high = ranges.get(
                    key, (origin['offset'], origin['offset']),
                )
                ranges[key] = min(low, origin['offset']), max(
                    high, origin['offset'],
                )
        data = canonical(dict(batch=batch, source_epoch=self.source_epoch,
                              refs=refs, ranges=[
                                  [*key, low, high + 1]
                                  for key, (low, high)
                                  in sorted(ranges.items())
                              ]))
        batch_id = digest(canonical(batch))
        with self.transaction():
            for item in inputs:
                try:
                    previous = self.input(item.command_ref)
                except KeyError:
                    self._validate_source(item)
                    self.db.execute('INSERT INTO job4_inputs VALUES (?,?,?,0)',
                                    (item.command_ref, item.record,
                                     digest(item.record)))
                else:
                    if previous.fingerprint != item.fingerprint:
                        raise OutputConflict('offset/timer reference conflict')
            previous = self.db.execute(
                'SELECT * FROM job4_batches WHERE id=?', (batch_id,),
            ).fetchone()
            if previous:
                if canonical(self.manifest(batch_id)) != data:
                    raise OutputConflict('batch ID/manifest conflict')
            else:
                self.db.execute('INSERT INTO job4_batches VALUES (?,?,?,0,0)',
                                (batch_id, data, digest(data)))
        return batch_id

    @contextmanager
    def input_scope(self, item):
        if self._input is not None:
            raise ValueError('nested input scope unsupported')
        frozen = self.input(item.command_ref)
        if frozen != item:
            raise OutputConflict('use the original frozen input')
        self._input = item
        try:
            yield
        finally:
            self._input = None

    def run_command(self, token, command_ref, request, axes, operation):
        item = self._input
        if item is None or item.command_ref != command_ref or (
            item.device != token.device
        ):
            raise OutputConflict('Job 4 input scope required')
        data = read_json(request)
        data['job4_ingress'] = read_json(item.record)
        return super().run_command(
            token, command_ref, canonical(data), axes, operation,
            admission_hook=self._bind_ingress,
        )

    def _bind_ingress(self):
        item = self._input
        row = self.db.execute('SELECT admitted FROM job4_inputs WHERE ref=?',
                              (item.command_ref,)).fetchone()
        if row[0]:
            return
        predecessors = self.db.execute(
            'SELECT ref FROM job4_inputs WHERE admitted=0 AND rowid < '
            '(SELECT rowid FROM job4_inputs WHERE ref=?) ORDER BY rowid',
            (item.command_ref,),
        ).fetchall()
        if any(self.input(previous[0]).device == item.device
               for previous in predecessors):
            raise CommandBusy('resume registered device predecessor first')
        self._validate_source(item)
        if item.kind == 'observation':
            origin = read_json(item.origin)
            key = device_key(item.device)
            self.db.execute(
                'INSERT INTO job4_cursors VALUES (?,?,?,?) '
                'ON CONFLICT(device) '
                'DO UPDATE SET topic=excluded.topic,'
                'partition=excluded.partition,'
                'offset=excluded.offset',
                (key, origin['topic'], origin['partition'], origin['offset']),
            )
        self.db.execute('UPDATE job4_inputs SET admitted=1 WHERE ref=?',
                        (item.command_ref,))

    def _validate_source(self, item):
        if item.kind != 'observation':
            return
        origin = read_json(item.origin)
        old = self.db.execute(
            'SELECT * FROM job4_cursors WHERE device=?',
            (device_key(item.device),),
        ).fetchone()
        if old is not None and (
            (old['topic'], old['partition']) != (
                origin['topic'], origin['partition'],
            ) or origin['offset'] <= old['offset']
        ):
            raise OutputConflict('unseen offset/order/cutover mismatch')

    def evidence(self, batch_id):
        commands, deliveries = [], {}
        with self.transaction():
            manifest = self.manifest(batch_id)
            for ref in manifest['refs']:
                item = self.input(ref)
                row = self.db.execute(
                    'SELECT admitted FROM job4_inputs WHERE ref=?', (ref,),
                ).fetchone()
                if not row[0]:
                    return False, False, ()
                admission = self.db.execute(
                    'SELECT request FROM admissions WHERE device=? AND ref=?',
                    (device_key(item.device), ref),
                ).fetchone()
                if admission is None or read_json(bytes(admission[0])).get(
                    'job4_ingress'
                ) != read_json(item.record):
                    raise ValueError('ingress/admission mismatch')
                status = self.command_status(item.device, ref)
                outputs = []
                for axis in item.axes:
                    self.state((*item.device, axis))  # Snapshot digest check.
                    try:
                        saved = self.command_payload((*item.device, axis), ref)
                    except KeyError:
                        if status == 'COMPLETE':
                            raise ValueError('completed command missing axis')
                        continue
                    if saved['output'] is not None:
                        output = decode_output(canonical(saved['output']))
                        if self.output(output.identity) != output:
                            raise ValueError('command/output mismatch')
                        outputs.append(output.identity)
                        deliveries[output.identity] = {
                            sink: self.delivery(output.identity, sink)[
                                'status'
                            ]
                            for sink in ('history', 'kafka', 'current')
                        }
                commands.append(CommandEvidence(item.device, ref, status,
                                                tuple(outputs)))
            durable = all(item.status == 'COMPLETE' for item in commands)
            return durable, checkpoint_ready(tuple(commands), deliveries), (
                tuple(commands)
            )

    def mark(self, batch_id, *, complete=False, hook=lambda stage: None):
        with self.transaction():
            durable, delivered, _ = self.evidence(batch_id)
            if not durable or (complete and not delivered):
                raise OutputConflict('batch completion evidence missing')
            hook('before_batch_complete' if complete
                 else 'before_input_complete')
            self.db.execute(
                'UPDATE job4_batches SET input_done=1,'
                'complete=MAX(complete,?) WHERE id=?',
                (int(complete), batch_id),
            )

    def attempt(self, batch_id, stage, ref, completed, error):
        with self.transaction():
            self.db.execute('INSERT INTO job4_attempts VALUES '
                            '(NULL,?,?,?,?,?)',
                            (batch_id, stage, ref,
                             type(error).__name__ if error else None,
                             canonical(completed)))
