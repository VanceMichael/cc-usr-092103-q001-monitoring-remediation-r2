"""时间处理：证据链上所有时刻统一为 UTC、秒级精度的 ISO-8601。

业务发生时刻（occurred_at）由上报方给出，系统记录时刻（recorded_at）
由服务端时钟给出；两者分开保存，补录与迟到校准证书引发的矛盾才
能被如实还原。
"""

from datetime import datetime, timedelta, timezone

_FMT = "%Y-%m-%dT%H:%M:%SZ"


def parse_ts(value) -> datetime:
    """解析 ISO-8601 时间戳；拒绝不带时区的值，避免歧义。"""
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith(("Z", "z")):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(f"无法解析时间戳: {value!r}") from exc
    else:
        raise ValueError(f"不支持的时间类型: {type(value).__name__}")
    if dt.tzinfo is None:
        raise ValueError(f"时间戳缺少时区: {value!r}")
    return dt.astimezone(timezone.utc).replace(microsecond=0)


def format_ts(dt: datetime) -> str:
    """格式化为 UTC 秒级 ISO-8601（Z 结尾）。"""
    return parse_ts(dt).strftime(_FMT)


def utcnow() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def add_days(dt: datetime, days: int) -> datetime:
    return parse_ts(dt) + timedelta(days=days)


def in_window(moment: datetime, start: datetime, end) -> bool:
    """闭区间判断；end 为 None 表示开口（长期有效）。"""
    moment = parse_ts(moment)
    if moment < parse_ts(start):
        return False
    if end is not None and moment > parse_ts(end):
        return False
    return True
