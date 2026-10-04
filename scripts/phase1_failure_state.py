import json
import re
import shutil
from collections import Counter
from pathlib import Path


def guard_resources(table, checkpoint, run_id):
    if not __debug__:
        raise RuntimeError(
            "Do not run destructive test helpers with python -O"
        )
    if not re.fullmatch(r"[0-9a-f]{32}", run_id):
        raise ValueError("Expected a generated UUID hex run_id")
    expected_table = f"ktd.bronze.sensor_raw_failure_test_{run_id}"
    expected_path = (
        "/Volumes/ktd/bronze/checkpoints/job1-failure-test/"
        f"{run_id}/sensor_raw"
    )
    if table != expected_table or str(checkpoint) != expected_path:
        raise ValueError(
            "Safety guard: canonical or non-isolated resource rejected"
        )
    path = Path(checkpoint)
    if path.resolve() != path or any(
        p.is_symlink() for p in (path, *path.parents)
    ):
        raise ValueError("Safety guard: checkpoint aliases/symlinks rejected")


def snapshot_tree(root):
    """Read all files; refuse symlinks and special files."""
    result = {}
    for path in sorted(Path(root).rglob("*")):
        if path.is_symlink() or not (path.is_file() or path.is_dir()):
            raise ValueError(f"Unsupported checkpoint entry: {path}")
        if path.is_file():
            result[str(path.relative_to(root))] = path.read_bytes()
    return result


def checkpoint_batch(files, topic):
    try:
        roots = {name.split("/")[0] for name in files}
        assert roots <= {"metadata", "offsets", "commits", "sources"}, roots
        query_id = json.loads(files["metadata"])["id"]
        assert query_id
        ids = {}
        for folder in ("offsets", "commits"):
            names = [
                name.split("/", 1)[1]
                for name in files
                if name.startswith(folder + "/")
            ]
            assert names and all(
                re.fullmatch(r"0|[1-9][0-9]*", n) for n in names
            )
            ids[folder] = {int(n) for n in names}
        batch = max(ids["offsets"])
        assert batch >= 1, "Need a prior batch N-1"
        assert max(ids["commits"]) == batch
        assert batch - 1 in ids["offsets"] & ids["commits"]

        def offsets(number):
            lines = files[f"offsets/{number}"].decode().splitlines()
            assert len(lines) == 3 and lines[0] == "v1"
            json.loads(lines[1])
            value = json.loads(lines[2])
            assert set(value) == {topic} and value[topic]
            assert all(
                str(p).isdigit() and type(o) is int and o >= 0
                for p, o in value[topic].items()
            )
            return value

        start, end = offsets(batch - 1), offsets(batch)
        assert set(start[topic]) == set(end[topic]), "Partition set changed"
        assert all(end[topic][p] >= o for p, o in start[topic].items())
        assert start != end, (
            "Latest batch is empty; do not move an older commit"
        )
        for number in (batch - 1, batch):
            lines = files[f"commits/{number}"].decode().splitlines()
            assert len(lines) == 2 and lines[0] == "v1"
            assert "nextBatchWatermarkMs" in json.loads(lines[1])
        return batch, query_id, start, end
    except (AssertionError, KeyError, ValueError, TypeError) as error:
        raise ValueError(
            f"Databricks checkpoint layout mismatch: {error}; "
            f"observed files={sorted(files)}"
        ) from error


def quarantine_commit(table, checkpoint, run_id, batch, original, is_active):
    guard_resources(table, checkpoint, run_id)
    if is_active():
        raise ValueError("Query must be terminated before checkpoint mutation")
    root = Path(checkpoint)
    evidence = root.parent / "evidence"
    if evidence.is_symlink() or evidence.resolve() != evidence:
        raise ValueError("Evidence path must not be an alias")
    if snapshot_tree(root) != original:
        raise ValueError("Checkpoint changed before backup")
    actual_batch, _, _, _ = checkpoint_batch(original, "kitchen.sensor.raw")
    if batch != actual_batch:
        raise ValueError("Only the latest committed data batch may be moved")
    backup = evidence / "checkpoint-backup"
    backup.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(root, backup, copy_function=shutil.copyfile)
    if snapshot_tree(backup) != original:
        raise ValueError("Full checkpoint backup verification failed")
    destination = evidence / "quarantined" / f"commit-{batch}"
    destination.parent.mkdir(exist_ok=False)
    guard_resources(table, checkpoint, run_id)
    if is_active() or snapshot_tree(root) != original:
        raise ValueError("Query/checkpoint changed before commit quarantine")
    shutil.move(str(root / "commits" / str(batch)), str(destination))
    expected = dict(original)
    moved = expected.pop(f"commits/{batch}")
    if destination.read_bytes() != moved or snapshot_tree(root) != expected:
        raise ValueError("Quarantine mismatch: stop; preserve all evidence")
    return destination


def audit_lineage(expected, actual):
    def key(row):
        return row["topic"], row["partition"], row["offset"]

    expected_keys = {key(row) for row in expected}
    counts = Counter(key(row) for row in actual)
    return {
        "counts": [[*k, n] for k, n in sorted(counts.items())],
        "missing": sorted(expected_keys - counts.keys()),
        "unexpected": sorted(counts.keys() - expected_keys),
        "duplicates": [[*k, n] for k, n in sorted(counts.items()) if n > 1],
    }


def assert_audit(report):
    if report["missing"] or report["unexpected"] or report["duplicates"]:
        raise AssertionError(f"Lineage reconciliation failed: {report}")
