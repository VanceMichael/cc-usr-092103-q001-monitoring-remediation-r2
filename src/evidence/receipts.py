"""可核验回执。

每次成功入链的命令都得到一张回执：包含案件、事件哈希、链头与时间，
并由后台以 HMAC-SHA256 签发。执法人员/机构可凭回执号到系统核验，
确认该事件确实入链、入链时的链头是什么。

回执签名只证明"系统在该时刻收录了该事件"，不替代哈希链本身的完整性；
两者结合：链证明事件未被篡改，回执证明收录时间与受理结果。
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from .hashing import canonical

RECEIPT_VERSION = 1


class ReceiptError(Exception):
    pass


@dataclass
class ReceiptService:
    secret: bytes

    def __post_init__(self) -> None:
        if isinstance(self.secret, str):
            self.secret = self.secret.encode("utf-8")
        if len(self.secret) < 16:
            raise ValueError("回执密钥至少 16 字节")

    def issue(self, *, receipt_id: str, case_id: str, event_hash: str,
              case_head: str, global_head: str, recorded_at: str,
              action: str, extra: dict[str, Any] | None = None) -> dict:
        body = {
            "v": RECEIPT_VERSION,
            "receipt_id": receipt_id,
            "case_id": case_id,
            "action": action,
            "event_hash": event_hash,
            "case_head": case_head,
            "global_head": global_head,
            "recorded_at": recorded_at,
        }
        if extra:
            body["extra"] = dict(sorted(extra.items()))
        body = dict(sorted(body.items()))
        body["sig"] = self._sign(body)
        return body

    def verify(self, receipt: dict) -> bool:
        sig = receipt.get("sig")
        if not isinstance(sig, str):
            return False
        expected = self._sign({k: v for k, v in receipt.items() if k != "sig"})
        return hmac.compare_digest(sig, expected)

    def encode(self, receipt: dict) -> str:
        """单行可粘贴的核验串（传输原始回执；签名独立于传输编码）。"""
        return base64.urlsafe_b64encode(
            json.dumps(receipt, ensure_ascii=False, sort_keys=True,
                       separators=(",", ":")).encode("utf-8")
        ).decode("ascii")

    def decode(self, token: str) -> dict:
        try:
            return json.loads(base64.urlsafe_b64decode(token.encode("ascii")))
        except Exception as exc:  # noqa: BLE001
            raise ReceiptError("回执编码无法解析") from exc

    def _sign(self, body: dict) -> str:
        mac = hmac.new(self.secret, canonical(body), hashlib.sha256)
        return base64.urlsafe_b64encode(mac.digest()).decode("ascii").rstrip("=")
