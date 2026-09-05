"""UTC 时间工具，避免 naive / aware datetime 混用导致延迟判断出错。"""

from datetime import datetime, timezone


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def as_utc(value: datetime) -> datetime:
    """将输入规范为带 UTC 时区的时间。"""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
