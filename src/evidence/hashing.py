"""规范化序列化与内容哈希。

所有摘要、链哈希与回执都基于同一套规范化编码：
JSON 键按字典序排序、紧凑分隔符、UTF-8、SHA-256。
同一份事实无论何时计算都得到相同摘要 —— 这是"稳定摘要"的基础。
"""

from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical(obj: Any) -> bytes:
    """把领域对象规范化为字节串。

    与 ``json.dumps(sort_keys=True)`` 的区别：None/布尔不会与同名字符串混淆，
    数字保持 JSON 词法（整数与浮点各自稳定），适合跨语言核验。
    """
    return json.dumps(
        _normalize(obj),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _normalize(obj: Any) -> Any:
    """标签化编码，从根本上消除类型歧义。

    每种 JSON 类型都有不可与用户数据碰撞的标签数组形态：
    None→["~n"]，布尔→["~b",v]，字符串保持裸字符串，
    整数→["~i","词法"]，浮点→["~f","repr"]，
    字典→["~o",[[键,值],…]]（按键排序），数组→["~l",[…]]。
    裸字符串不可能与标签数组混淆。
    """
    if obj is None:
        return ["~n"]
    if obj is True or obj is False:
        return ["~b", obj]
    if isinstance(obj, bool):  # pragma: no cover - 已被上方覆盖
        return ["~b", obj]
    if isinstance(obj, str):
        return obj
    if isinstance(obj, int):
        return ["~i", str(obj)]
    if isinstance(obj, float):
        return ["~f", repr(obj)]
    if isinstance(obj, dict):
        return ["~o", sorted(
            ([str(k), _normalize(v)] for k, v in obj.items()),
            key=lambda kv: kv[0],
        )]
    if isinstance(obj, (list, tuple)):
        return ["~l", [_normalize(v) for v in obj]]
    raise TypeError(f"不可规范化的类型: {type(obj)!r}")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(obj: Any) -> str:
    """对象的稳定内容摘要（十六进制）。"""
    return sha256_hex(canonical(obj))


def digest_concat(*parts: str) -> str:
    """对若干已有摘要做归并哈希（用长度前缀防止拼接歧义）。"""
    h = hashlib.sha256()
    for p in parts:
        h.update(len(p).to_bytes(4, "big"))
        h.update(p.encode("ascii"))
    return h.hexdigest()
