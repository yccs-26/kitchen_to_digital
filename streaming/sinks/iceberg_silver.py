"""주입된 SparkSession으로 기존 Iceberg Silver 테이블을 읽고 쓴다.

테이블 생성과 세션 설정은 호출자의 책임이다. 대상은 canonical 10개 필드,
event_time TIMESTAMP(LTZ), metric_value DOUBLE 및 나머지 STRING을 전제로 한다.
UC Iceberg에서의 MERGE 지원과 원자적 커밋은 실제 runtime 검증이 필요하다.
지원하지 않는 환경의 오류를 append 등의 대안으로 우회하지 않는다.
"""

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import cast

from streaming.jobs.raw_ingestion import quoted_table
from streaming.validation.event_identity import (
    PAYLOAD_FIELDS,
    EventIdentityStatus,
    classify_event_identity,
)


class SilverStorageInvariantError(RuntimeError):
    """canonical 행의 유일성 또는 쓰기 완료 확인이 깨진 경우다."""


class IcebergSilverStorage:
    """SilverStorage의 단일 writer 구현이며 호출도 순차적으로 수행한다.

    단일 writer 가정(single writer assumption)을 유지한다.
    동시 다중 writer 동작은 미검증(concurrent multi-writer behavior not verified)이다.
    삽입 건수에 대한 runtime별 결과 형식은 사용하지 않는다. 사전 부재,
    MERGE 완료, 사후 동일 payload를 확인했을 때만 True를 반환한다.
    다른 writer가 같은 payload를 삽입하는 경합은 이 방식으로 구별할 수 없다.
    조회는 최신 커밋을 보여야 하며 호출자는 캐시된 대상을 사용하면 안 된다.
    """

    def __init__(self, *, spark, table_name: str) -> None:
        self._table = quoted_table(table_name)
        self._spark = spark

    def lookup(self, event_id: str) -> Mapping[str, object] | None:
        """최대 두 행을 읽어 중복을 검출하고 canonical payload를 복원한다."""
        projection = ", ".join(
            "unix_micros(`event_time`) AS `event_time`"
            if field == "event_time" else f"`{field}`"
            for field in ("event_id", *PAYLOAD_FIELDS)
        )
        rows = self._spark.sql(
            f"SELECT {projection} FROM {self._table} "
            "WHERE `event_id` = :event_id LIMIT 2",
            args={"event_id": event_id},
        ).collect()
        if not rows:
            return None
        if len(rows) > 1:
            raise SilverStorageInvariantError(
                "Multiple canonical Silver rows for one event_id"
            )
        row = rows[0].asDict()
        payload = {
            field: row[field] for field in ("event_id", *PAYLOAD_FIELDS)
        }
        # Spark의 naive datetime 변환과 호스트 시간대 의존성을 피한다.
        micros = payload["event_time"]
        if type(micros) is not int:
            raise TypeError("event_time must be non-null epoch microseconds")
        payload["event_time"] = datetime(
            1970, 1, 1, tzinfo=timezone.utc
        ) + timedelta(microseconds=micros)
        return payload

    def insert_if_absent(self, event: Mapping[str, object]) -> bool:
        """기존 행은 보존하며 조건부 삽입 후 영속 canonical을 확인한다."""
        payload = {
            field: event[field] for field in ("event_id", *PAYLOAD_FIELDS)
        }
        event_id = cast(str, payload["event_id"])
        if self.lookup(event_id) is not None:
            return False

        # 값은 Spark parameter marker로 전달하고 식별자만 검증 후 조합한다.
        fields = tuple(payload)
        source = ", ".join(f":{field} AS `{field}`" for field in fields)
        columns = ", ".join(f"`{field}`" for field in fields)
        values = ", ".join(f"s.`{field}`" for field in fields)
        self._spark.sql(
            f"MERGE INTO {self._table} AS t "
            f"USING (SELECT {source}) AS s "
            "ON t.`event_id` = s.`event_id` "
            f"WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({values})",
            args=payload,
        ).collect()

        stored = self.lookup(event_id)
        if stored is None:
            raise SilverStorageInvariantError(
                "Silver row is absent after MERGE"
            )
        if classify_event_identity(payload, stored).status is not (
            EventIdentityStatus.DUPLICATE
        ):
            # 타입 손실이나 전제 밖의 경합을 성공한 삽입으로 보고하지 않는다.
            raise SilverStorageInvariantError(
                "Silver payload differs after MERGE"
            )
        return True
