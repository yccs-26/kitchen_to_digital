"""Local SQLite transactional publication store; not a Spark/production outbox.

One command atomically commits opaque snapshot bytes, CAS counters, immutable
output and pending deliveries. No external sink is inside this transaction.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import base64
from datetime import datetime
import sqlite3

from streaming.state.change_envelope import (
    ChangeEnvelope, CurrentProjection, OutputConflict, StateOutput,
    canonical, digest, read_json, revision, stamp, text,
)


SINKS = ("history", "kafka", "current")
TERMINAL = ("ACKED", "ALREADY_APPLIED", "SUPERSEDED", "NOT_REQUIRED")


def encode_output(output):
    return canonical({
        "change": output.change.data if output.change else None,
        "projection": output.projection.data,
    })


def decode_output(payload):
    data = read_json(payload)
    if type(data) is not dict or set(data) != {"change", "projection"}:
        raise ValueError("invalid stored output")
    return StateOutput(
        ChangeEnvelope(canonical(data["change"])) if data["change"] else None,
        CurrentProjection(canonical(data["projection"])),
    )


def axis_id(axis_key):
    if type(axis_key) is not tuple or len(axis_key) != 3:
        raise ValueError("axis key must be store/equipment/axis tuple")
    for value in axis_key:
        text(value)
    if axis_key[2] not in ("OPERATIONAL", "HEALTH"):
        raise ValueError("invalid state axis")
    return canonical(axis_key).decode("utf-8")


def image(payload):
    if type(payload) is not bytes or not payload:
        raise ValueError("nonempty opaque snapshot bytes required")
    return payload


@dataclass(frozen=True)
class StoredState:
    axis_key: tuple[str, str, str]
    generation: int
    revision: int
    snapshot: bytes
    projection_revision: int
    change_revision: int
    last_change_id: str | None


@dataclass(frozen=True)
class PreparedCommand:
    axis_key: tuple[str, str, str]
    command_ref: str
    generation: int
    expected_revision: int
    before_sha256: str
    after_snapshot: bytes
    input_payload: bytes
    decision: str
    output: StateOutput | None = None

    def __post_init__(self):
        axis_id(self.axis_key)
        text(self.command_ref)
        text(self.decision)
        revision(self.generation, 1)
        revision(self.expected_revision)
        if len(text(self.before_sha256)) != 64:
            raise ValueError("invalid before snapshot digest")
        image(self.after_snapshot)
        recorded = read_json(self.input_payload)
        if type(recorded) is not dict or set(recorded) != {
            "input", "evaluation_time",
        } or type(recorded["input"]) is not dict:
            raise ValueError("recorded typed input/evaluation_time required")
        at = recorded["evaluation_time"]
        if type(at) is not str or stamp(datetime.fromisoformat(at)) != at:
            raise ValueError("canonical evaluation time required")
        if self.output is not None:
            if not isinstance(self.output, StateOutput):
                raise TypeError("invalid output")
            data = self.output.projection.data
            if self.output.projection.axis_key != self.axis_key or (
                data["generation"] != self.generation
                or data["command_ref"] != self.command_ref
                or data["current"].get("snapshot_sha256") != digest(
                    self.after_snapshot,
                )
            ):
                raise OutputConflict("command/output/snapshot mismatch")
            if self.output.change is not None and (
                self.output.change.data["evaluation_time"] != at
            ):
                raise OutputConflict("recorded evaluation/output mismatch")

    @property
    def payload(self):
        return canonical({
            "axis_key": self.axis_key, "command_ref": self.command_ref,
            "generation": self.generation,
            "expected_revision": self.expected_revision,
            "before_sha256": self.before_sha256,
            "after_snapshot": base64.b64encode(
                self.after_snapshot,
            ).decode("ascii"),
            "input": read_json(self.input_payload),
            "decision": self.decision,
            "output": (read_json(encode_output(self.output))
                       if self.output else None),
        })


class PublicationStore:
    """SQLite version 1 with explicit ownership; no migration or TTL."""

    def __init__(self, path):
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        try:
            self.db.execute("PRAGMA foreign_keys=ON")
            self.db.execute("PRAGMA synchronous=FULL")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1):
                raise ValueError("unsupported local publication schema")
            if version == 0:
                existing = self.db.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'",
                ).fetchall()
                if existing:
                    raise ValueError("refusing unrelated nonempty database")
                with self.transaction():
                    for statement in _SCHEMA:
                        self.db.execute(statement)
                    self.db.execute("PRAGMA user_version=1")
            else:
                # Required columns must exist; fail closed on corruption.
                self.db.execute("SELECT payload, sha256 FROM outputs LIMIT 0")
        except BaseException:
            self.db.close()
            raise

    def close(self):
        self.db.close()

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def initialize(self, axis_key, generation, snapshot):
        key = axis_id(axis_key)
        revision(generation, 1)
        image(snapshot)
        with self.transaction():
            row = self.db.execute(
                "SELECT * FROM roots WHERE axis=?", (key,),
            ).fetchone()
            if row:
                if row["generation"] != generation or (
                    bytes(row["snapshot"]) != snapshot
                ) or row["revision"] != 0:
                    raise OutputConflict("root already initialized")
                return
            self.db.execute(
                "INSERT INTO roots VALUES (?,?,0,?,?,0,0,NULL,NULL)",
                (key, generation, snapshot, digest(snapshot)),
            )

    def state(self, axis_key):
        row = self.db.execute(
            "SELECT * FROM roots WHERE axis=?", (axis_id(axis_key),),
        ).fetchone()
        if row is None:
            raise KeyError(axis_key)
        snapshot = bytes(row["snapshot"])
        if digest(snapshot) != row["sha256"]:
            raise ValueError("corrupt root snapshot")
        return StoredState(
            axis_key, row["generation"], row["revision"], snapshot,
            row["projection_revision"], row["change_revision"],
            row["last_change_id"],
        )

    def commit(self, command, *, hook=lambda stage: None):
        """Exact command retry is idempotent; CAS losers cannot publish."""
        if not isinstance(command, PreparedCommand):
            raise TypeError("PreparedCommand required")
        key = axis_id(command.axis_key)
        payload = command.payload
        with self.transaction():
            existing = self.db.execute(
                "SELECT * FROM commands WHERE axis=? AND command_ref=?",
                (key, command.command_ref),
            ).fetchone()
            if existing:
                if bytes(existing["payload"]) != payload:
                    raise OutputConflict("same command, different payload")
                return existing["output_id"]
            state = self.state(command.axis_key)
            if state.generation != command.generation or (
                state.revision != command.expected_revision
                or digest(state.snapshot) != command.before_sha256
            ):
                raise OutputConflict("snapshot CAS/generation mismatch")
            output = command.output
            if output is None and command.after_snapshot != state.snapshot:
                raise OutputConflict("changed snapshot requires output intent")
            context = None
            output_id = None
            p_revision = state.projection_revision
            c_revision = state.change_revision
            last_id = state.last_change_id
            if output:
                data = output.projection.data
                context = canonical({name: data[name] for name in (
                    "equipment", "profile", "policy", "generation",
                )})
                root = self.db.execute(
                    "SELECT context FROM roots WHERE axis=?", (key,),
                ).fetchone()
                if root[0] is not None and bytes(root[0]) != context:
                    raise OutputConflict("incompatible root policy/profile")
                if data["projection_revision"] <= p_revision or (
                    data["previous_change_id"] != last_id
                ):
                    raise OutputConflict("projection revision/chain mismatch")
                p_revision = data["projection_revision"]
                expected = c_revision + int(output.change is not None)
                if data["change_revision"] != expected:
                    raise OutputConflict("change revision gap/conflict")
                c_revision = expected
                if output.change:
                    last_id = output.change.identity[1]
                elif data["change_id"] != last_id:
                    raise OutputConflict("Current-only change ID mismatch")
                output_id = output.identity
                saved = self.db.execute(
                    "SELECT payload FROM outputs WHERE id=?", (output_id,),
                ).fetchone()
                encoded = encode_output(output)
                if saved and bytes(saved[0]) != encoded:
                    raise OutputConflict("same output ID, different payload")
                if saved:
                    raise OutputConflict("output reused by another command")
                self.db.execute(
                    "INSERT INTO outputs VALUES (?,?,?,?,0)",
                    (output_id, key, encoded, digest(encoded)),
                )
                for sink in SINKS:
                    status = "PENDING" if output.change or sink == (
                        "current"
                    ) else "NOT_REQUIRED"
                    self.db.execute(
                        "INSERT INTO deliveries VALUES (?,?,?,0,NULL,NULL)",
                        (output_id, sink, status),
                    )
            self.db.execute(
                "UPDATE roots SET revision=?, snapshot=?, sha256=?, "
                "projection_revision=?, change_revision=?, last_change_id=?, "
                "context=COALESCE(?,context) WHERE axis=?",
                (state.revision + 1, command.after_snapshot,
                 digest(command.after_snapshot), p_revision, c_revision,
                 last_id, context, key),
            )
            hook("after_snapshot")
            self.db.execute(
                "INSERT INTO commands VALUES (?,?,?,?,?)",
                (key, command.command_ref, payload, digest(payload),
                 output_id),
            )
            hook("before_commit")
        return output_id

    def output(self, output_id):
        row = self.db.execute(
            "SELECT * FROM outputs WHERE id=?", (output_id,),
        ).fetchone()
        if row is None:
            raise KeyError(output_id)
        payload = bytes(row["payload"])
        if digest(payload) != row["sha256"]:
            raise ValueError("corrupt publication record")
        output = decode_output(payload)
        if output.identity != output_id:
            raise ValueError("publication identity mismatch")
        return output

    def contains_projection(self, projection):
        """Superseding Current ACKs need an exact committed intent witness."""
        rows = self.db.execute(
            "SELECT id FROM outputs WHERE axis=?", (axis_id(
                projection.axis_key,
            ),),
        )
        return any(
            self.output(row[0]).projection == projection for row in rows
        )

    def command_payload(self, axis_key, command_ref):
        row = self.db.execute(
            "SELECT payload,sha256 FROM commands "
            "WHERE axis=? AND command_ref=?", (axis_id(axis_key), command_ref),
        ).fetchone()
        if row is None:
            raise KeyError(command_ref)
        payload = bytes(row[0])
        if digest(payload) != row[1]:
            raise ValueError("corrupt command record")
        return read_json(payload)

    def pending(self):
        return tuple(row[0] for row in self.db.execute(
            "SELECT id FROM outputs WHERE complete=0 ORDER BY rowid",
        ))

    def delivery(self, output_id, sink):
        row = self.db.execute(
            "SELECT * FROM deliveries WHERE output_id=? AND sink=?",
            (output_id, sink),
        ).fetchone()
        if row is None:
            raise KeyError((output_id, sink))
        return dict(row)

    def begin_attempt(self, output_id, sink):
        if sink not in SINKS:
            raise ValueError("unknown sink")
        with self.transaction():
            item = self.delivery(output_id, sink)
            if item["status"] in TERMINAL:
                return False
            if item["status"] == "CONFLICT":
                raise OutputConflict("publication requires conflict review")
            attempt = item["attempts"] + 1
            self.db.execute(
                "UPDATE deliveries SET status='UNKNOWN',attempts=?,"
                "error=NULL WHERE output_id=? AND sink=?",
                (attempt, output_id, sink),
            )
            self.db.execute(
                "INSERT INTO attempts VALUES (?,?,?,'UNKNOWN',NULL,NULL)",
                (output_id, sink, attempt),
            )
        return attempt

    def finish_attempt(self, output_id, sink, status, *, error=None,
                       receipt=None, attempt=None):
        if status not in (*TERMINAL, "FAILED", "CONFLICT"):
            raise ValueError("invalid delivery result")
        encoded = canonical(receipt) if receipt is not None else None
        with self.transaction():
            item = self.delivery(output_id, sink)
            if item["status"] != "UNKNOWN" or (
                attempt is not None and attempt != item["attempts"]
            ):
                raise OutputConflict("no matching outstanding attempt")
            self.db.execute(
                "UPDATE deliveries SET status=?,error=?,receipt=? "
                "WHERE output_id=? AND sink=?",
                (status, error, encoded, output_id, sink),
            )
            self.db.execute(
                "UPDATE attempts SET status=?,error=?,receipt=? "
                "WHERE output_id=? AND sink=? AND attempt=?",
                (status, error, encoded, output_id, sink, item["attempts"]),
            )

    def complete(self, output_id):
        with self.transaction():
            if any(self.delivery(output_id, sink)["status"] not in TERMINAL
                   for sink in SINKS):
                raise OutputConflict("unfinished sink deliveries")
            self.db.execute(
                "UPDATE outputs SET complete=1 WHERE id=?", (output_id,),
            )

    def attempts(self, output_id):
        return tuple(dict(row) for row in self.db.execute(
            "SELECT * FROM attempts WHERE output_id=? ORDER BY rowid",
            (output_id,),
        ))


_SCHEMA = (
    "CREATE TABLE roots (axis TEXT PRIMARY KEY, generation INTEGER NOT NULL, "
    "revision INTEGER NOT NULL, snapshot BLOB NOT NULL, sha256 TEXT NOT NULL, "
    "projection_revision INTEGER NOT NULL, change_revision INTEGER NOT NULL, "
    "last_change_id TEXT, context BLOB)",
    "CREATE TABLE outputs (id TEXT PRIMARY KEY, axis TEXT NOT NULL "
    "REFERENCES roots(axis), payload BLOB NOT NULL, sha256 TEXT NOT NULL, "
    "complete INTEGER NOT NULL)",
    "CREATE TABLE commands (axis TEXT NOT NULL REFERENCES roots(axis), "
    "command_ref TEXT NOT NULL, payload BLOB NOT NULL, sha256 TEXT NOT NULL, "
    "output_id TEXT REFERENCES outputs(id), PRIMARY KEY(axis,command_ref))",
    "CREATE TABLE deliveries (output_id TEXT NOT NULL REFERENCES outputs(id), "
    "sink TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL, "
    "error TEXT, receipt BLOB, PRIMARY KEY(output_id,sink))",
    "CREATE TABLE attempts (output_id TEXT NOT NULL, sink TEXT NOT NULL, "
    "attempt INTEGER NOT NULL, status TEXT NOT NULL, "
    "error TEXT, receipt BLOB, "
    "PRIMARY KEY(output_id,sink,attempt), FOREIGN KEY(output_id,sink) "
    "REFERENCES deliveries(output_id,sink))",
)
