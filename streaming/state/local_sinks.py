"""File-backed doubles, not real Kafka/DynamoDB/Iceberg implementations.

Each database has its own local transaction. Independent files deliberately
have no shared atomic commit with the publication store or other sinks.
"""

from contextlib import contextmanager
import sqlite3

from streaming.state.change_envelope import (
    ChangeEnvelope, CurrentProjection, OutputConflict, canonical, digest,
)
from streaming.state.publication_store import axis_id
from streaming.state.sink_protocols import (
    CurrentAck, HistoryAck, KafkaAck, RetryableSinkError,
)


STATE_TOPIC = "kitchen.state.changes"


def _context(data):
    return canonical({name: data[name] for name in (
        "equipment", "profile", "policy", "generation",
    )})


class _LocalDatabase:
    def __init__(self, path, kind, statements):
        self.db = sqlite3.connect(str(path), isolation_level=None)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA synchronous=FULL")
        try:
            tables = self.db.execute(
                "SELECT name FROM sqlite_master WHERE type='table'",
            ).fetchall()
            if not tables:
                with self.transaction():
                    self.db.execute(
                        "CREATE TABLE local_model "
                        "(kind TEXT, version INTEGER)",
                    )
                    self.db.execute(
                        "INSERT INTO local_model VALUES (?,1)", (kind,),
                    )
                    for statement in statements:
                        self.db.execute(statement)
            else:
                row = self.db.execute("SELECT * FROM local_model").fetchall()
                if len(row) != 1 or tuple(row[0]) != (kind, 1):
                    raise ValueError("wrong local sink/version")
        except BaseException:
            self.db.close()
            raise

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN IMMEDIATE")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def close(self):
        self.db.close()


class LocalHistory(_LocalDatabase):
    def __init__(self, path):
        super().__init__(path, "history", (
            "CREATE TABLE history (identity TEXT PRIMARY KEY, axis TEXT, "
            "generation INTEGER, revision INTEGER, "
            "context BLOB, payload BLOB, "
            "sha256 TEXT, UNIQUE(axis,generation,revision))",
        ))

    def write(self, change):
        if not isinstance(change, ChangeEnvelope):
            raise TypeError("ChangeEnvelope required")
        data = change.data
        key = canonical(change.identity).decode()
        axis = axis_id(change.axis_key)
        with self.transaction():
            previous = self.db.execute(
                "SELECT * FROM history WHERE identity=?", (key,),
            ).fetchone()
            if previous:
                if bytes(previous["payload"]) != change.payload or (
                    previous["sha256"] != change.sha256
                ):
                    raise OutputConflict("History same ID changed payload")
            else:
                latest = self.db.execute(
                    "SELECT * FROM history WHERE axis=? "
                    "ORDER BY revision DESC LIMIT 1", (axis,),
                ).fetchone()
                expected_revision, previous_id = 1, None
                if latest:
                    if bytes(latest["context"]) != _context(data):
                        raise OutputConflict("History context fenced")
                    prior = ChangeEnvelope(bytes(latest["payload"]))
                    expected_revision = latest["revision"] + 1
                    previous_id = prior.identity[1]
                if data["change_revision"] < expected_revision:
                    raise OutputConflict("History same revision changed ID")
                if data["change_revision"] > expected_revision:
                    raise RetryableSinkError("History predecessor missing")
                if data["previous_change_id"] != previous_id:
                    raise OutputConflict("History predecessor ID mismatch")
                self.db.execute(
                    "INSERT INTO history VALUES (?,?,?,?,?,?,?)",
                    (key, axis, data["generation"], data["change_revision"],
                     _context(data), change.payload, change.sha256),
                )
        return HistoryAck(change.identity, change.sha256, key)

    def rows(self):
        rows = self.db.execute("SELECT * FROM history ORDER BY rowid")
        result = []
        for row in rows:
            change = ChangeEnvelope(bytes(row["payload"]))
            if change.sha256 != row["sha256"]:
                raise ValueError("corrupt History")
            result.append(change)
        return tuple(result)


class LocalKafka(_LocalDatabase):
    def __init__(self, path):
        super().__init__(path, "kafka", (
            "CREATE TABLE messages (sequence INTEGER PRIMARY KEY, "
            "topic TEXT, key BLOB, payload BLOB, sha256 TEXT)",
        ))

    def publish(self, change):
        if not isinstance(change, ChangeEnvelope):
            raise TypeError("ChangeEnvelope required")
        key = change.data["equipment"]["equipment_id"].encode("utf-8")
        with self.transaction():
            row = self.db.execute(
                "INSERT INTO messages(topic,key,payload,sha256) "
                "VALUES (?,?,?,?)",
                (STATE_TOPIC, key, change.payload, change.sha256),
            )
            offset = row.lastrowid - 1
        return KafkaAck(STATE_TOPIC, key, 0, offset, change.sha256)

    def messages(self):
        result = []
        for row in self.db.execute("SELECT * FROM messages ORDER BY sequence"):
            payload = bytes(row["payload"])
            if digest(payload) != row["sha256"]:
                raise ValueError("corrupt Kafka model")
            result.append(ChangeEnvelope(payload))
        return tuple(result)


class LocalCurrent(_LocalDatabase):
    def __init__(self, path):
        super().__init__(path, "current", (
            "CREATE TABLE current_state (axis TEXT PRIMARY KEY, "
            "payload BLOB, sha256 TEXT)",
        ))

    def apply(self, projection):
        if not isinstance(projection, CurrentProjection):
            raise TypeError("CurrentProjection required")
        key = axis_id(projection.axis_key)
        data = projection.data
        status = "ACKED"
        canonical_projection = projection
        with self.transaction():
            row = self.db.execute(
                "SELECT * FROM current_state WHERE axis=?", (key,),
            ).fetchone()
            if row:
                previous = CurrentProjection(bytes(row["payload"]))
                if previous.sha256 != row["sha256"]:
                    raise ValueError("corrupt Current model")
                prior = previous.data
                if _context(prior) != _context(data):
                    raise OutputConflict("Current generation/profile fenced")
                if data["projection_revision"] > prior[
                    "projection_revision"
                ] and (
                    data["change_revision"] < prior["change_revision"]
                    or (data["change_revision"] == prior["change_revision"]
                        and (data["change_id"] != prior["change_id"]
                             or data["current"]["state"] != (
                                 prior["current"]["state"]
                             )))
                ):
                    raise OutputConflict("Current change chain regressed")
                old, new = (prior["projection_revision"],
                            data["projection_revision"])
                if old == new:
                    if previous.payload != projection.payload:
                        raise OutputConflict("Current same revision conflict")
                    status = "ALREADY_APPLIED"
                elif old > new:
                    status = "SUPERSEDED"
                    canonical_projection = previous
            if status == "ACKED":
                self.db.execute(
                    "INSERT INTO current_state VALUES (?,?,?) "
                    "ON CONFLICT(axis) DO UPDATE SET "
                    "payload=excluded.payload,sha256=excluded.sha256",
                    (key, projection.payload, projection.sha256),
                )
        final = canonical_projection.data
        return CurrentAck(
            status, canonical_projection.identity, canonical_projection.sha256,
            final["generation"], final["projection_revision"],
            canonical_projection.payload,
        )

    def get(self, axis_key):
        row = self.db.execute(
            "SELECT * FROM current_state WHERE axis=?", (axis_id(axis_key),),
        ).fetchone()
        if row is None:
            return None
        output = CurrentProjection(bytes(row["payload"]))
        if output.sha256 != row["sha256"]:
            raise ValueError("corrupt Current model")
        return output

    def equipment(self, store_id, equipment_id):
        return {axis: record.data for axis in ("OPERATIONAL", "HEALTH")
                if (record := self.get((store_id, equipment_id, axis)))
                is not None}
