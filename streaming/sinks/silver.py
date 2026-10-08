"""Silver canonical 이벤트의 영속 멱등 쓰기 계약.

운영은 단일 writer를 전제로 한다. 저장 어댑터는 별도로 주입하며,
이 모듈에는 특정 저장소 구현이나 프로세스 내부 중복 캐시가 없다.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Protocol, cast

from streaming.validation.event_identity import (
    PAYLOAD_FIELDS,
    EventIdentityStatus,
    classify_event_identity,
)


class SilverStorage(Protocol):
    """재시작 후에도 동일한 canonical 저장소를 조회하는 어댑터 계약.

    event_id당 최대 한 행을 영속 보존하며 기존 행을 수정하거나 삭제하지 않는다.
    조회는 완료된 삽입을 볼 수 있어야 한다. 입력 mapping도 수정하지 않는다.
    단일 writer 전제에서도 조회 후 삽입을 무조건 append로 구현하면 안 된다.
    실제 저장소 어댑터의 원자성은 후속 runtime 검증 대상이다.
    """

    def lookup(self, event_id: str) -> Mapping[str, object] | None:
        """영속 canonical을 반환한다. 부재만 None이며 조회 실패는 예외다."""
        ...

    def insert_if_absent(self, event: Mapping[str, object]) -> bool:
        """ID가 없을 때만 원자적으로 삽입하고 영속 완료 후 True를 반환한다.

        이미 존재하면 변경 없이 False를 반환한다. 실패나 완료 여부가
        불확실하면 예외를 전달한다. overwrite 및 DELETE 후 INSERT는 금지한다.
        """
        ...


class SilverWriteStatus(Enum):
    INSERTED = "INSERTED"
    DUPLICATE_NOOP = "DUPLICATE_NOOP"
    CONFLICT = "CONFLICT"


@dataclass(frozen=True)
class SilverWriteResult:
    status: SilverWriteStatus
    differing_fields: tuple[str, ...] = ()


class SilverSink:
    """검증·UTC 정규화를 마친 이벤트를 단일 writer에서 순차 처리한다.

    매 호출마다 저장소를 조회한다. watermark나 이벤트 나이에 따른 만료는 없다.
    현재 10개 payload 필드만 저장하며 transport metadata는 저장하지 않는다.
    호출자는 저장소 연결과 수명 주기를 관리한다. 재시도·오류 발행은 하지 않는다.
    """

    def __init__(self, storage: SilverStorage) -> None:
        self._storage = storage

    def write(self, event: Mapping[str, object]) -> SilverWriteResult:
        """새 이벤트만 삽입하며 중복·충돌은 기존 canonical을 보존한다."""
        payload = {
            field: event[field] for field in ("event_id", *PAYLOAD_FIELDS)
        }
        event_id = cast(str, payload["event_id"])
        existing = self._storage.lookup(event_id)
        identity = classify_event_identity(payload, existing)
        if identity.status is EventIdentityStatus.NEW:
            if self._storage.insert_if_absent(payload):
                return SilverWriteResult(SilverWriteStatus.INSERTED)
            # 조회 이후 행이 생겼다면 실제 저장된 canonical로 다시 판정한다.
            existing = self._storage.lookup(event_id)
            if existing is None:
                raise RuntimeError("Storage rejected insert but row is absent")
            identity = classify_event_identity(payload, existing)

        status = (
            SilverWriteStatus.DUPLICATE_NOOP
            if identity.status is EventIdentityStatus.DUPLICATE
            else SilverWriteStatus.CONFLICT
        )
        return SilverWriteResult(status, identity.differing_fields)
