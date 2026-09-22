"""规范化序列化与哈希。

同一逻辑状态必须得到同一字节序列，摘要才"稳定"：
键排序、紧凑分隔符、保留非 ASCII 字符。
"""

import hashlib
import json


def canonical(obj) -> str:
    """把对象序列化为确定性的规范 JSON 字符串。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def sha256_hex(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def hash_obj(obj) -> str:
    """对任意可 JSON 序列化对象计算确定性 SHA-256。"""
    return sha256_hex(canonical(obj))
