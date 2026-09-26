"""统一时间来源。

业务代码不直接调用 ``datetime.now``，而是依赖 :class:`Clock`，
使"整改逾期"、"事件先后"等判断在测试与重放时完全确定。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional


def parse_ts(value: str) -> datetime:
    """解析 ISO-8601 时间字符串，结果统一为带时区的 UTC 时间。"""
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def fmt_ts(dt: datetime) -> str:
    """格式化为毫秒级 UTC ISO 字符串。"""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


@dataclass
class Clock:
    """固定或实时时钟。``tick`` 保证同一时刻不会产生两条相同时间戳。"""

    _fixed: Optional[datetime] = None
    _ticks: int = 0

    @classmethod
    def fixed(cls, value: str | datetime) -> "Clock":
        dt = value if isinstance(value, datetime) else parse_ts(value)
        return cls(_fixed=dt)

    def freeze(self, value: str | datetime) -> None:
        self._fixed = value if isinstance(value, datetime) else parse_ts(value)
        self._ticks = 0

    def now(self) -> datetime:
        if self._fixed is not None:
            self._ticks += 1
            return self._fixed
        return datetime.now(timezone.utc)

    def advance(self, seconds: float = 0, **kwargs) -> datetime:
        """推进固定时钟（实时时钟不支持推进）。"""
        if self._fixed is None:
            raise RuntimeError("实时时钟不能推进时间")
        from datetime import timedelta

        delta = timedelta(seconds=seconds, **{k: v for k, v in kwargs.items() if k != "seconds"})
        self._fixed += delta
        self._ticks = 0
        return self._fixed
