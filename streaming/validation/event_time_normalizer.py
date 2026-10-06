from datetime import datetime, timezone

def normalize_event_time(value: str) -> datetime:
    dt = datetime.fromisoformat(value)

    # timezone 없는 경우 실패
    if dt.tzinfo is None:
        raise ValueError("event_time must include timezone information")

    # UTC 변환 후 반환
    return dt.astimezone(timezone.utc)