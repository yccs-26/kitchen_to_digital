"""승인 대기 UC Managed Delta 후보: 이름 기반 접근만 허용한다.

테이블 생성, 권한 부여, LOCATION 지정은 호출하지 않는다. single_writer는
호출자의 배타적 writer 선언이며 실제 UC 권한이나 분산 lock의 증명이 아니다.
"""

from collections.abc import Iterable

from streaming.aggregations.gold_row_mapper import GoldPhysicalRow
from streaming.jobs.raw_ingestion import quoted_table
from streaming.sinks.gold_delta import (
    gold_schema, merge_final_rows, prepare_gold_rows,
)


class GoldUCDeltaSink:
    """검증된 3-part name의 기존 managed Delta에 확정 집계만 저장한다."""

    def __init__(self, spark, table: str, *, single_writer: bool = False):
        self.identifier = quoted_table(table)
        self.table = table
        if single_writer is not True:
            raise ValueError("Exclusive single writer declaration required")
        if spark.conf.get("spark.sql.session.timeZone") != "UTC":
            raise ValueError("Gold sink requires UTC session timezone")
        self.spark = spark

    def merge_rows(self, rows: Iterable[GoldPhysicalRow]) -> int:
        """전체 입력을 검증하고 단일 MERGE한다. 빈 입력도 전량 소비한다.

        반환값은 입력 행 수다. UC NOT NULL DDL은 별도 live 수용 검사 대상이며
        Parquet read schema의 nullable로 NOT NULL을 추정하지 않는다.
        실패는 checkpoint 완료로 처리하지 않고 호출자에게 전파한다.
        """
        prepared = prepare_gold_rows(rows)
        if not prepared:
            return 0
        from delta.tables import DeltaTable

        metadata = self.spark.catalog.getTable(self.identifier)
        if metadata.tableType.upper() != "MANAGED" or metadata.isTemporary:
            raise ValueError("Gold target must be an existing managed table")
        target = DeltaTable.forName(self.spark, self.identifier)
        actual = target.toDF().schema
        expected_types = {f.name: f.dataType for f in gold_schema()}
        if {f.name: f.dataType for f in actual} != expected_types:
            raise ValueError("Existing Gold table schema differs")
        source = self.spark.createDataFrame(prepared, schema=gold_schema())
        merge_final_rows(target, source)
        return len(prepared)
