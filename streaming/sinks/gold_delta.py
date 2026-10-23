"""단일 writer용 Gold Delta sink: 불변 확정값의 insert 또는 no-op만 허용한다.

MERGE 한 번이 트랜잭션 경계이며 checkpoint와 원자적이지 않다.
Delta는 자연키 UNIQUE를 강제하지 않으므로 외부 동시 writer는 지원하지 않는다.
"""

from collections.abc import Iterable
import json
from pathlib import Path

from streaming.aggregations.gold_row_comparison import (
    KEY_COLUMNS,
    VALUE_COLUMNS,
    gold_natural_key,
)
from streaming.aggregations.gold_row_mapper import (
    GoldPhysicalRow,
    map_gold_row,
)


COLUMNS = (*KEY_COLUMNS, *VALUE_COLUMNS)
LOGICAL_NAMES = {
    "sample_count": "count", "sum_value": "sum", "min_value": "min",
    "max_value": "max", "avg_value": "avg",
}


class DuplicateGoldBatchKey(ValueError):
    """동일 source batch의 자연키 중복을 쓰기 전에 거부한다."""


def gold_schema():
    """PySpark는 실제 sink 사용 시에만 import한다."""
    from pyspark.sql.types import (
        DoubleType, LongType, StringType, StructField, StructType,
        TimestampType,
    )

    types = {
        "window_start": TimestampType(), "window_end": TimestampType(),
        "sample_count": LongType(),
    }
    return StructType([
        StructField(
            name, types.get(name, DoubleType() if name.endswith("_value")
                            else StringType()), nullable=False,
        ) for name in COLUMNS
    ])


class GoldDeltaSink:
    """로컬 path의 확정 집계를 저장한다. 테이블과 writer 수명은 호출자 책임이다."""

    def __init__(self, spark, path: str | Path):
        self.spark = spark
        self.path = Path(path)
        if not self.path.is_absolute() or "://" in str(path):
            raise ValueError("Gold sink requires an absolute local path")
        if "`" in str(path) or "'" in str(path):
            raise ValueError("Unsupported quote in Delta path")
        if spark.conf.get("spark.sql.session.timeZone") != "UTC":
            raise ValueError("Gold sink requires UTC session timezone")
        self.identifier = f"delta.`{self.path}`"

    def create_table(self) -> None:
        """기존 테이블을 교체하지 않으며 12컬럼 NOT NULL DDL을 적용한다."""
        schema = gold_schema()
        ddl = ", ".join(
            f"`{field.name}` {field.dataType.simpleString()} NOT NULL"
            for field in schema
        )
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {self.identifier} ({ddl}) USING DELTA"
        )
        actual = self.spark.read.format("delta").load(str(self.path)).schema
        expected_types = {f.name: f.dataType for f in schema}
        if {f.name: f.dataType for f in actual} != expected_types:
            raise ValueError("Existing Gold table schema differs")
        # Parquet read schema는 nullable로 완화되므로 Delta metadata로 검사한다.
        from delta.tables import DeltaTable

        detail = DeltaTable.forPath(self.spark, str(self.path))
        metadata = self.spark.read.json(
            str(self.path / "_delta_log" / "*.json")
        ).where("metaData IS NOT NULL").select("metaData.schemaString")
        schemas = [json.loads(row[0]) for row in metadata.collect()]
        if not schemas or any(
            field["nullable"] for schema in schemas
            for field in schema["fields"]
        ):
            raise ValueError("Gold table requires NOT NULL metadata")
        # 실제 Delta table 접근도 확인한다.
        detail.toDF().limit(0).collect()

    def merge_rows(self, rows: Iterable[GoldPhysicalRow]) -> int:
        """모든 source를 검증한 뒤 단일 MERGE로 저장하고 오류를 전파한다.

        초기 소규모 Gold 출력용 driver materialization이다. batch_id로
        충돌 검사를 생략하지 않으며 동일 final 재전달도 값을 대조한다.
        반환값은 입력 행 수이며 신규 insert 수를 의미하지 않는다.
        """
        from delta.tables import DeltaTable

        prepared = prepare_gold_rows(rows)
        if not prepared:
            return 0
        source = self.spark.createDataFrame(prepared, schema=gold_schema())
        target = DeltaTable.forPath(self.spark, str(self.path))
        merge_final_rows(target, source)
        return len(prepared)


def prepare_gold_rows(
    rows: Iterable[GoldPhysicalRow],
) -> list[GoldPhysicalRow]:
    """두 adapter가 동일한 물리 입력 검증과 batch 중복 정책을 사용한다."""
    prepared = []
    seen = set()
    for row in rows:
        if set(row) != set(COLUMNS):
            raise ValueError("Expected exactly 12 Gold physical columns")
        canonical = map_gold_row({
            LOGICAL_NAMES.get(name, name): value
            for name, value in row.items()
        })
        key = gold_natural_key(canonical)
        if key in seen:
            raise DuplicateGoldBatchKey("Duplicate source natural key")
        seen.add(key)
        prepared.append(canonical)
    return prepared


def merge_final_rows(target, source) -> None:
    """하나의 Delta MERGE로 절대값 INSERT 또는 불변 final 충돌을 판정한다."""
    match = " AND ".join(f"t.`{c}` = s.`{c}`" for c in KEY_COLUMNS)
    changed = " OR ".join(
        f"NOT (t.`{c}` <=> s.`{c}`)" for c in VALUE_COLUMNS
    )
    (
        target.alias("t").merge(source.alias("s"), match)
        .whenMatchedUpdate(
            condition=changed,
            set={"sample_count": (
                "CAST(raise_error('GOLD_FINAL_CONFLICT') AS BIGINT)"
            )},
        )
        .whenNotMatchedInsert(values={c: f"s.`{c}`" for c in COLUMNS})
        .execute()
    )
