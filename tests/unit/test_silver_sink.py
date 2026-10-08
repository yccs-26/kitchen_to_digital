"""Silver 쓰기 계약 테스트. SQLite는 테스트 도구이며 운영 sink가 아니다."""

from collections.abc import Mapping
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import pytest

from streaming.sinks.silver import (
    SilverSink,
    SilverWriteResult,
    SilverWriteStatus,
)
from streaming.validation.event_identity import (
    EventIdentityStatus,
    classify_event_identity,
)


class SqliteTestStorage:
    """임시 파일에 보관하여 sink 및 연결 수명과 영속 상태를 분리한다."""

    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute(
            "CREATE TABLE IF NOT EXISTS canonical "
            "(event_id TEXT PRIMARY KEY, payload TEXT NOT NULL)"
        )
        self.connection.commit()
        self.lookups = 0
        self.inserts = 0

    def lookup(self, event_id: str) -> dict[str, object] | None:
        self.lookups += 1
        row = self.connection.execute(
            "SELECT payload FROM canonical WHERE event_id = ?", (event_id,)
        ).fetchone()
        if row is None:
            return None
        event = json.loads(row[0])
        event["event_time"] = datetime.fromisoformat(event["event_time"])
        return event

    def insert_if_absent(self, event: Mapping[str, object]) -> bool:
        self.inserts += 1
        payload = dict(event)
        payload["event_time"] = payload["event_time"].isoformat()
        with self.connection:
            cursor = self.connection.execute(
                "INSERT INTO canonical VALUES (?, ?) "
                "ON CONFLICT(event_id) DO NOTHING",
                (event["event_id"], json.dumps(payload)),
            )
        return cursor.rowcount == 1

    def count(self) -> int:
        return self.connection.execute(
            "SELECT COUNT(*) FROM canonical"
        ).fetchone()[0]

    def close(self) -> None:
        self.connection.close()


@pytest.fixture
def event() -> dict[str, object]:
    return {
        "event_id": "evt-100",
        "event_time": datetime(2026, 10, 7, 12, tzinfo=timezone.utc),
        "store_id": "store-001", "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius", "metric_value": 4.2,
        "unit": "celsius", "schema_version": "1.0.0", "source": "simulator",
    }


@pytest.fixture
def storage(tmp_path):
    adapter = SqliteTestStorage(tmp_path / "silver.db")
    yield adapter
    adapter.close()


def test_new_event_is_inserted(storage, event) -> None:
    assert SilverSink(storage).write(event) == SilverWriteResult(
        SilverWriteStatus.INSERTED
    )
    assert storage.lookup("evt-100") == event
    assert storage.count() == 1


def test_single_writer_three_deliveries_keep_one_row(storage, event) -> None:
    sink = SilverSink(storage)
    assert sink.write(event).status is SilverWriteStatus.INSERTED
    original = storage.lookup("evt-100")
    for _ in range(2):
        assert sink.write(event) == SilverWriteResult(
            SilverWriteStatus.DUPLICATE_NOOP
        )
    assert storage.lookup("evt-100") == original
    assert storage.count() == 1
    assert storage.inserts == 1


@pytest.mark.parametrize(("field", "value"), [
    ("metric_value", 4.2000001),
    ("event_time", datetime(2026, 10, 7, 12, 1, tzinfo=timezone.utc)),
    ("unit", "fahrenheit"),
    ("equipment_id", "fridge-002"),
    ("schema_version", "1.0.1"),
])
def test_conflict_preserves_canonical(storage, event, field, value) -> None:
    sink = SilverSink(storage)
    sink.write(event)
    incoming = {**event, field: value}
    assert sink.write(incoming) == SilverWriteResult(
        SilverWriteStatus.CONFLICT, (field,)
    )
    assert storage.lookup("evt-100") == event
    assert storage.inserts == 1
    assert storage.count() == 1


def test_multiple_differing_fields_match_identity_helper(storage, event):
    sink = SilverSink(storage)
    sink.write(event)
    incoming = {**event, "metric_value": 8, "unit": "fahrenheit"}
    expected = classify_event_identity(incoming, event)
    assert expected.status is EventIdentityStatus.CONFLICT
    assert sink.write(incoming).differing_fields == expected.differing_fields


def test_transport_changes_are_noop_and_not_stored(storage, event) -> None:
    sink = SilverSink(storage)
    first = {**event, "topic": "raw", "partition": 0, "offset": 1,
             "ingested_at": "2026-10-07T12:00:00Z"}
    sink.write(first)
    second = {**event, "topic": "other", "partition": 2, "offset": 99,
              "ingested_at": "2026-10-08T12:00:00Z"}
    assert sink.write(second).status is SilverWriteStatus.DUPLICATE_NOOP
    assert storage.lookup("evt-100") == event
    assert storage.inserts == 1


def test_reopen_storage_and_new_sink_detect_duplicate(tmp_path, event):
    path = tmp_path / "restart.db"
    first = SqliteTestStorage(path)
    assert SilverSink(first).write(event).status is SilverWriteStatus.INSERTED
    first.close()
    del first
    # 이전 sink나 연결을 넘기지 않고 파일만 다시 열어 재시작 경계를 검증한다.
    second = SqliteTestStorage(path)
    try:
        assert SilverSink(second).write(event).status is (
            SilverWriteStatus.DUPLICATE_NOOP
        )
        assert second.count() == 1
        assert second.inserts == 0
    finally:
        second.close()


def test_existing_sink_sees_external_persistent_insert(storage, event):
    sink = SilverSink(storage)
    storage.insert_if_absent(event)
    assert sink.write(event).status is SilverWriteStatus.DUPLICATE_NOOP
    assert storage.lookups == 1


def test_beyond_watermark_age_does_not_expire_identity(storage, event):
    old = {**event, "event_time": event["event_time"] - timedelta(days=3650)}
    SilverSink(storage).write(old)
    assert SilverSink(storage).write(old).status is (
        SilverWriteStatus.DUPLICATE_NOOP
    )
    assert storage.count() == 1


@pytest.mark.parametrize("conflict", [False, True])
def test_inputs_and_canonical_are_unchanged(storage, event, conflict):
    sink = SilverSink(storage)
    sink.write(event)
    incoming = deepcopy(event)
    if conflict:
        incoming["metric_value"] = 9
    before = deepcopy(incoming)
    first = sink.write(incoming)
    assert sink.write(incoming) == first
    assert incoming == before
    assert storage.lookup("evt-100") == event


def test_new_write_does_not_mutate_input(storage, event):
    before = deepcopy(event)
    SilverSink(storage).write(event)
    assert event == before


def test_different_ids_each_insert(storage, event):
    sink = SilverSink(storage)
    for number in range(3):
        assert sink.write({**event, "event_id": f"evt-{number}"}).status is (
            SilverWriteStatus.INSERTED
        )
    assert storage.count() == 3


@pytest.mark.parametrize("operation", ["lookup", "insert_if_absent"])
def test_storage_failure_propagates(storage, event, monkeypatch, operation):
    failure = OSError("injected storage failure")

    def fail(*args):
        raise failure

    monkeypatch.setattr(storage, operation, fail)
    with pytest.raises(OSError) as raised:
        SilverSink(storage).write(event)
    assert raised.value is failure
    assert storage.count() == 0


@pytest.mark.parametrize("conflict", [False, True])
def test_atomic_insert_loss_rechecks_winner(storage, event, conflict):
    winner = {**event, "metric_value": 9} if conflict else dict(event)

    class InterleavedStorage:
        """단일 writer 계약 밖의 경합도 무조건 삽입으로 처리하지 않는다."""

        def lookup(self, event_id):
            return storage.lookup(event_id)

        def insert_if_absent(self, incoming):
            storage.insert_if_absent(winner)
            return storage.insert_if_absent(incoming)

    result = SilverSink(InterleavedStorage()).write(event)
    assert result == SilverWriteResult(
        SilverWriteStatus.CONFLICT if conflict
        else SilverWriteStatus.DUPLICATE_NOOP,
        ("metric_value",) if conflict else (),
    )
    assert storage.count() == 1
    assert storage.lookup("evt-100") == winner


def test_inconsistent_storage_contract_fails(event):
    class BrokenStorage:
        def lookup(self, event_id):
            return None

        def insert_if_absent(self, incoming):
            return False

    with pytest.raises(RuntimeError, match="row is absent"):
        SilverSink(BrokenStorage()).write(event)


def test_commit_then_error_returns_no_success_and_replay_is_noop(
    storage, event, monkeypatch
):
    original_insert = storage.insert_if_absent

    def uncertain_insert(incoming):
        original_insert(incoming)
        raise OSError("completion acknowledgement lost")

    monkeypatch.setattr(storage, "insert_if_absent", uncertain_insert)
    with pytest.raises(OSError):
        SilverSink(storage).write(event)
    assert SilverSink(storage).write(event).status is (
        SilverWriteStatus.DUPLICATE_NOOP
    )
    assert storage.count() == 1
