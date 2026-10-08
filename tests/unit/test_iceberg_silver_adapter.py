"""Spark 없이 SQL 경계와 Silver 계약을 검증하며 runtime 증빙은 아니다."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from streaming.sinks.iceberg_silver import (
    IcebergSilverStorage,
    SilverStorageInvariantError,
)
from streaming.sinks.silver import SilverSink, SilverStorage, SilverWriteStatus
from streaming.validation.event_identity import PAYLOAD_FIELDS


class FakeRow:
    def __init__(self, values):
        self.values = values

    def asDict(self):
        if isinstance(self.values, Exception):
            raise self.values
        return dict(self.values)


class FakeResult:
    def __init__(self, rows):
        self.rows = rows
        self.collected = False

    def collect(self):
        self.collected = True
        if isinstance(self.rows, Exception):
            raise self.rows
        return self.rows


class FakeSpark:
    """응답을 순서대로 반환하며 SQL이나 Iceberg 동작은 재구현하지 않는다."""

    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []
        self.results = []

    def sql(self, query, *, args):
        self.calls.append((query, dict(args)))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        result = response if isinstance(response, FakeResult) else (
            FakeResult(response)
        )
        self.results.append(result)
        return result


@pytest.fixture
def event():
    return {
        "event_id": "evt-100",
        "event_time": datetime(2026, 10, 7, 12, 0, 0, 123456,
                               tzinfo=timezone.utc),
        "store_id": "store-001",
        "equipment_id": "fridge-001",
        "equipment_type": "refrigerator",
        "metric_name": "temperature_celsius",
        "metric_value": 4.2,
        "unit": "celsius",
        "schema_version": "1.0.0",
        "source": "simulator",
    }


def stored_row(event):
    # 실제 SQL의 unix_micros 결과를 정수로 제공한다.
    delta = event["event_time"] - datetime(1970, 1, 1, tzinfo=timezone.utc)
    micros = ((delta.days * 86400 + delta.seconds) * 1_000_000
              + delta.microseconds)
    return FakeRow({**event, "event_time": micros})


def adapter(spark):
    return IcebergSilverStorage(spark=spark, table_name="ktd.silver.sensor")


@pytest.mark.parametrize("name", [
    "", " ", "sensor", "silver.sensor", "a.b.c.d", "a..c",
    "a.b.c; DROP TABLE x", "a.b.`c`", "a.b.c--", "a.b.c d",
])
def test_invalid_identifier_fails_before_io(name):
    spark = FakeSpark()
    with pytest.raises(ValueError):
        IcebergSilverStorage(spark=spark, table_name=name)
    assert spark.calls == []


def test_session_is_injected_without_constructor_io():
    spark = FakeSpark()
    adapter(spark)
    assert spark.calls == []


def test_lookup_missing():
    spark = FakeSpark([])
    assert adapter(spark).lookup("absent") is None
    query, args = spark.calls[0]
    assert "LIMIT 2" in query
    assert "`ktd`.`silver`.`sensor`" in query
    assert "WHERE `event_id` = :event_id" in query
    assert args == {"event_id": "absent"}


def test_lookup_canonical_projection_and_types(event):
    row = stored_row({**event, "offset": 123})
    result = adapter(FakeSpark([row])).lookup(event["event_id"])
    assert result == event
    assert tuple(result) == ("event_id", *PAYLOAD_FIELDS)
    assert result["event_time"].tzinfo is timezone.utc
    assert type(result["metric_value"]) is float
    assert type(result["schema_version"]) is str
    assert type(result["source"]) is str


@pytest.mark.parametrize("micros", [-1, 0, 1, 1791374400123456])
def test_timestamp_utc_microsecond_roundtrip(event, micros):
    event["event_time"] = (
        datetime(1970, 1, 1, tzinfo=timezone.utc)
        + timedelta(microseconds=micros)
    )
    assert adapter(FakeSpark([stored_row(event)])).lookup("evt-100") == event


def test_duplicate_rows_are_invariant_violation(event):
    spark = FakeSpark([stored_row(event), stored_row(event)])
    with pytest.raises(SilverStorageInvariantError, match="Multiple"):
        adapter(spark).lookup("evt-100")


def test_insert_is_conditional_and_preserves_payload(event):
    incoming = {**event, "topic": "raw", "offset": 123}
    before = deepcopy(incoming)
    spark = FakeSpark([], [], [stored_row(event)])
    assert adapter(spark).insert_if_absent(incoming) is True
    query, args = spark.calls[1]
    normalized = " ".join(query.upper().split())
    assert normalized.startswith("MERGE INTO `KTD`.`SILVER`.`SENSOR`")
    assert "ON T.`EVENT_ID` = S.`EVENT_ID`" in normalized
    assert "WHEN NOT MATCHED THEN INSERT" in normalized
    for forbidden in ("UPDATE", "DELETE", "APPEND", "WHEN MATCHED", "CREATE"):
        assert forbidden not in normalized
    assert args == event
    assert tuple(args) == ("event_id", *PAYLOAD_FIELDS)
    assert args["event_time"] is incoming["event_time"]
    assert type(args["metric_value"]) is float
    for field in args:
        assert f":{field} AS `{field}`" in query
        assert f"s.`{field}`" in query
    assert incoming == before
    assert all(result.collected for result in spark.results)
    assert len(spark.calls) == 3


def test_values_are_bound_and_not_interpolated(event):
    event["event_id"] = "x'; DELETE FROM ktd.silver.sensor; --"
    event["source"] = "source'\\value"
    spark = FakeSpark([], [], [stored_row(event)])
    assert adapter(spark).insert_if_absent(event)
    for query, args in spark.calls:
        assert event["event_id"] not in query
        assert event["source"] not in query
        assert args["event_id"] == event["event_id"]


@pytest.mark.parametrize("conflict", [False, True])
def test_existing_row_returns_false_without_write(event, conflict):
    incoming = {**event, "metric_value": 99} if conflict else event
    spark = FakeSpark([stored_row(event)])
    assert adapter(spark).insert_if_absent(incoming) is False
    assert len(spark.calls) == 1


@pytest.mark.parametrize("stage", ["before", "merge", "after"])
@pytest.mark.parametrize("deferred", [False, True])
def test_sql_and_collect_failures_propagate(event, stage, deferred):
    failure = RuntimeError("table not found or Spark failure")
    response = FakeResult(failure) if deferred else failure
    responses = {
        "before": [response],
        "merge": [[], response],
        "after": [[], [], response],
    }
    spark = FakeSpark(*responses[stage])
    with pytest.raises(RuntimeError) as raised:
        adapter(spark).insert_if_absent(event)
    assert raised.value is failure
    assert not spark.responses


@pytest.mark.parametrize("kind", ["missing", "conflict", "duplicate"])
def test_uncertain_merge_does_not_report_success(event, kind):
    rows = {
        "missing": [],
        "conflict": [stored_row({**event, "metric_value": 99})],
        "duplicate": [stored_row(event), stored_row(event)],
    }
    with pytest.raises(SilverStorageInvariantError):
        adapter(FakeSpark([], [], rows[kind])).insert_if_absent(event)


def test_row_conversion_failure_propagates():
    failure = ValueError("broken row")
    with pytest.raises(ValueError) as raised:
        adapter(FakeSpark([FakeRow(failure)])).lookup("evt-100")
    assert raised.value is failure


@pytest.mark.parametrize("value", [None, "123", 1.5, True])
def test_invalid_timestamp_result_fails(event, value):
    with pytest.raises(TypeError):
        adapter(FakeSpark([FakeRow({**event, "event_time": value})])).lookup(
            "evt-100"
        )


def test_missing_payload_field_propagates(event):
    del event["source"]
    with pytest.raises(KeyError):
        adapter(FakeSpark([stored_row(event)])).lookup("evt-100")
    spark = FakeSpark()
    with pytest.raises(KeyError):
        adapter(spark).insert_if_absent(event)
    assert spark.calls == []


def test_silver_boundary_contract_and_repeatability(event):
    row = stored_row(event)
    spark = FakeSpark([], [], [], [row], [row], [row], [row])
    storage: SilverStorage = adapter(spark)
    sink = SilverSink(storage)
    assert sink.write(event).status is SilverWriteStatus.INSERTED
    assert sink.write(event).status is SilverWriteStatus.DUPLICATE_NOOP
    incoming = {**event, "metric_value": 99}
    before = deepcopy(incoming)
    first = sink.write(incoming)
    assert first.status is SilverWriteStatus.CONFLICT
    assert first.differing_fields == ("metric_value",)
    assert sink.write(incoming) == first
    assert incoming == before
    assert sum(query.startswith("MERGE") for query, _ in spark.calls) == 1


def test_same_input_produces_same_sql_and_arguments(event):
    runs = []
    for _ in range(2):
        spark = FakeSpark([], [], [stored_row(event)])
        assert adapter(spark).insert_if_absent(event)
        runs.append(spark.calls)
    assert runs[0] == runs[1]


def test_single_writer_limit_is_documented():
    assert "single writer assumption" in IcebergSilverStorage.__doc__
    assert "concurrent multi-writer behavior not verified" in (
        IcebergSilverStorage.__doc__
    )
