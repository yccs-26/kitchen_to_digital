import json
from unittest.mock import Mock

import pytest

from scripts import phase1_failure_state as failure

RUN_ID = "a" * 32
TABLE = f"ktd.bronze.sensor_raw_failure_test_{RUN_ID}"
CHECKPOINT = (
    f"/Volumes/ktd/bronze/checkpoints/job1-failure-test/{RUN_ID}/sensor_raw"
)


def layout():
    def offset(end):
        return f'v1\n{{}}\n{{"kitchen.sensor.raw":{{"1":{end}}}}}'.encode()

    return {
        "metadata": json.dumps({"id": "query-id"}).encode(),
        "offsets/0": offset(1),
        "offsets/1": offset(3),
        "commits/0": b'v1\n{"nextBatchWatermarkMs":0}',
        "commits/1": b'v1\n{"nextBatchWatermarkMs":0}',
        "sources/0/0": b"source metadata",
    }


@pytest.mark.parametrize("as_string", [True, False])
def test_progress_boundaries_accept_json_string_and_dict(as_string):
    _, _, start, end = failure.checkpoint_batch(layout(), "kitchen.sensor.raw")
    source = {
        "startOffset": json.dumps(start, indent=2) if as_string else start,
        "endOffset": json.dumps(end) if as_string else end,
    }
    failure.assert_progress_boundaries(source, start, end)


def test_progress_offset_rejects_malformed_json():
    with pytest.raises(ValueError, match="startOffset: malformed JSON"):
        failure.normalize_progress_offset(
            '{"kitchen.sensor.raw":', "startOffset"
        )


@pytest.mark.parametrize("value", [None, [], 3, "null", "[]", "3"])
def test_progress_offset_rejects_unexpected_type(value):
    with pytest.raises(TypeError, match="endOffset: expected a JSON object"):
        failure.normalize_progress_offset(value, "endOffset")


@pytest.mark.parametrize(
    "value",
    [{}, {"t": []}, {"t": {"1": True}}, '{"t":{"1":"3"}}'],
)
def test_progress_offset_rejects_invalid_mapping(value):
    with pytest.raises(ValueError, match="invalid Kafka offset mapping"):
        failure.normalize_progress_offset(value, "endOffset")


@pytest.mark.parametrize("field", ["startOffset", "endOffset"])
def test_progress_boundaries_reject_different_offset(field):
    _, _, start, end = failure.checkpoint_batch(layout(), "kitchen.sensor.raw")
    source = {"startOffset": json.dumps(start), "endOffset": json.dumps(end)}
    source[field] = '{"kitchen.sensor.raw":{"1":99}}'
    with pytest.raises(
        AssertionError, match=f"{field} differs from checkpoint"
    ):
        failure.assert_progress_boundaries(source, start, end)


@pytest.mark.parametrize(
    "table,path",
    [
        ("ktd.bronze.sensor_raw", CHECKPOINT),
        (TABLE, "/Volumes/ktd/bronze/checkpoints/job1/sensor_raw"),
        (TABLE.upper(), CHECKPOINT),
        (TABLE, CHECKPOINT + "/../sensor_raw"),
        (TABLE, CHECKPOINT.replace(RUN_ID, "b" * 32)),
    ],
)
def test_rejects_non_isolated_resources(table, path):
    with pytest.raises(ValueError, match="Safety guard"):
        failure.guard_resources(table, path, RUN_ID)


def test_accepts_exact_generated_pair():
    failure.guard_resources(TABLE, CHECKPOINT, RUN_ID)


@pytest.mark.parametrize(
    "change",
    [
        {"offsets/1": b"v2\n{}\n{}"},
        {"offsets/1": b'v1\n{}\n{"kitchen.sensor.raw":{"1":1}}'},
        {"commits/2": b'v1\n{"nextBatchWatermarkMs":0}'},
        {"commits/.1.crc": b"unrecognized sidecar"},
        {"state/0": b"unexpected stateful query"},
        {"offsets/1": b'v1\n{}\n{"other.topic":{"1":3}}'},
    ],
)
def test_layout_mismatch_reports_observed_files(change):
    with pytest.raises(ValueError, match="layout mismatch.*observed files"):
        failure.checkpoint_batch(layout() | change, "kitchen.sensor.raw")


def test_selects_latest_data_batch_and_boundaries():
    batch, query, start, end = failure.checkpoint_batch(
        layout(), "kitchen.sensor.raw"
    )
    assert (batch, query) == (1, "query-id")
    assert start == {"kitchen.sensor.raw": {"1": 1}}
    assert end == {"kitchen.sensor.raw": {"1": 3}}


def test_missing_previous_offset_is_not_repaired():
    files = layout()
    del files["offsets/0"]
    with pytest.raises(ValueError, match="layout mismatch"):
        failure.checkpoint_batch(files, "kitchen.sensor.raw")


def test_equal_total_count_does_not_hide_duplicate_and_missing():
    expected = [{"topic": "t", "partition": 1, "offset": n} for n in (0, 1)]
    report = failure.audit_lineage(expected, [expected[0], expected[0]])
    assert report["missing"] == [("t", 1, 1)]
    assert report["duplicates"] == [["t", 1, 0, 2]]
    with pytest.raises(AssertionError, match="reconciliation"):
        failure.assert_audit(report)


def test_canonical_rejected_before_any_copy_or_move(monkeypatch):
    copy = Mock()
    move = Mock()
    monkeypatch.setattr(failure.shutil, "copytree", copy)
    monkeypatch.setattr(failure.shutil, "move", move)
    with pytest.raises(ValueError, match="Safety guard"):
        failure.quarantine_commit(
            "ktd.bronze.sensor_raw",
            CHECKPOINT,
            RUN_ID,
            1,
            layout(),
            lambda: False,
        )
    copy.assert_not_called()
    move.assert_not_called()


def test_active_query_rejected_before_snapshot(monkeypatch):
    read = Mock()
    monkeypatch.setattr(failure, "snapshot_tree", read)
    with pytest.raises(ValueError, match="terminated"):
        failure.quarantine_commit(
            TABLE, CHECKPOINT, RUN_ID, 1, layout(), lambda: True
        )
    read.assert_not_called()


def test_backup_mismatch_prevents_move(monkeypatch, tmp_path):
    # In-memory checkpoint; even temporary commit files are not moved.
    monkeypatch.setattr(failure, "guard_resources", Mock())
    monkeypatch.setattr(
        failure, "snapshot_tree", Mock(side_effect=[layout(), {}])
    )
    monkeypatch.setattr(failure.shutil, "copytree", Mock())
    move = Mock()
    monkeypatch.setattr(failure.shutil, "move", move)
    with pytest.raises(ValueError, match="backup verification"):
        failure.quarantine_commit(
            TABLE,
            tmp_path / "sensor_raw",
            RUN_ID,
            1,
            layout(),
            lambda: False,
        )
    move.assert_not_called()
