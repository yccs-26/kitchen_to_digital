"""UC 후보의 로컬 단위 계약이며 UC 권한/거버넌스 검증이 아니다."""

from datetime import datetime, timezone
import sys
from types import ModuleType, SimpleNamespace

import pytest

from streaming.aggregations.gold_row_mapper import map_gold_row
from streaming.sinks.gold_delta import DuplicateGoldBatchKey, merge_final_rows
from streaming.sinks.gold_uc_delta import GoldUCDeltaSink


@pytest.fixture
def row():
    return map_gold_row({
        "window_start": datetime(2026, 10, 20, 12, tzinfo=timezone.utc),
        "window_end": datetime(2026, 10, 20, 12, 1, tzinfo=timezone.utc),
        "window_type": "1m_tumbling", "store_id": "s", "equipment_id": "e",
        "metric_name": "m", "unit": "u", "count": 3,
        "sum": 12.0, "min": 2.0, "max": 6.0, "avg": 4.0,
    })


@pytest.fixture
def spark():
    return SimpleNamespace(conf=SimpleNamespace(get=lambda name: "UTC"))


@pytest.mark.parametrize("name", [
    "gold", "schema.gold", "a.b.c.d", "a.b.c;DROP", "a.b.`c`",
    "delta.`/tmp/a`", "a.b.c/path",
])
def test_unvalidated_identifiers_fail_before_spark_access(name):
    with pytest.raises(ValueError):
        GoldUCDeltaSink(None, name, single_writer=True)


def test_exclusive_writer_declaration_required(spark):
    with pytest.raises(ValueError, match="single writer"):
        GoldUCDeltaSink(spark, "a.b.c")


def test_utc_required(spark):
    spark.conf.get = lambda name: "Asia/Seoul"
    with pytest.raises(ValueError, match="UTC"):
        GoldUCDeltaSink(spark, "a.b.c", single_writer=True)


def test_empty_and_duplicate_source_do_not_access_catalog(spark, row):
    sink = GoldUCDeltaSink(spark, "a.b.c", single_writer=True)
    assert sink.merge_rows(iter([])) == 0
    with pytest.raises(DuplicateGoldBatchKey):
        sink.merge_rows([row, row])
    with pytest.raises(ValueError, match="12 Gold"):
        sink.merge_rows([{**row, "extra": 1}])


def test_name_access_schema_and_errors_propagate(spark, row, monkeypatch):
    import streaming.sinks.gold_uc_delta as module

    field = SimpleNamespace(name="test", dataType="double")
    schema = [field]
    calls = []
    target = SimpleNamespace(toDF=lambda: SimpleNamespace(schema=schema))
    tables = ModuleType("delta.tables")
    tables.DeltaTable = SimpleNamespace(forName=lambda session, name: (
        calls.append((session, name)) or target
    ))
    monkeypatch.setitem(sys.modules, "delta", ModuleType("delta"))
    monkeypatch.setitem(sys.modules, "delta.tables", tables)
    monkeypatch.setattr(module, "gold_schema", lambda: schema)
    spark.catalog = SimpleNamespace(getTable=lambda name: SimpleNamespace(
        tableType="MANAGED", isTemporary=False,
    ))
    spark.createDataFrame = lambda rows, schema: rows
    merged = []
    monkeypatch.setattr(module, "merge_final_rows",
                        lambda target, source: merged.extend(source))
    sink = GoldUCDeltaSink(spark, "a.b.c", single_writer=True)
    assert sink.merge_rows([row]) == 1
    assert calls == [(spark, "`a`.`b`.`c`")]
    assert merged == [row]
    spark.catalog.getTable = lambda name: SimpleNamespace(
        tableType="EXTERNAL", isTemporary=False,
    )
    with pytest.raises(ValueError, match="managed"):
        sink.merge_rows([row])
    spark.catalog.getTable = lambda name: SimpleNamespace(
        tableType="MANAGED", isTemporary=False,
    )
    target.toDF = lambda: SimpleNamespace(schema=[])
    with pytest.raises(ValueError, match="schema differs"):
        sink.merge_rows([row])
    target.toDF = lambda: SimpleNamespace(schema=schema)

    def fail(target, source):
        raise RuntimeError("INJECTED_MERGE_FAILURE")

    monkeypatch.setattr(module, "merge_final_rows", fail)
    with pytest.raises(RuntimeError, match="INJECTED_MERGE_FAILURE"):
        sink.merge_rows([row])


def test_shared_merge_has_absolute_insert_and_conflict_trap():
    calls = {}

    class Builder:
        def alias(self, name):
            return self

        def merge(self, source, condition):
            calls["match"] = condition
            return self

        def whenMatchedUpdate(self, **kwargs):
            calls["matched"] = kwargs
            return self

        def whenNotMatchedInsert(self, **kwargs):
            calls["insert"] = kwargs
            return self

        def execute(self):
            calls["executed"] = True

    merge_final_rows(Builder(), Builder())
    assert calls["executed"]
    assert calls["match"].count(" = ") == 5
    assert calls["matched"]["condition"].count("<=>") == 7
    assert "GOLD_FINAL_CONFLICT" in calls["matched"]["set"]["sample_count"]
    assert len(calls["insert"]["values"]) == 12
    assert calls["insert"]["values"]["sum_value"] == "s.`sum_value`"
    assert all("+" not in value
               for value in calls["insert"]["values"].values())
