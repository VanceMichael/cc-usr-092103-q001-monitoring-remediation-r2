"""追加式哈希链事件存储。

设计要点：
- 事件只增不改，任何"更正"都是新事件并引用原事件；
- 每个事件的哈希覆盖前一事件哈希（prev_hash），形成全库唯一
  时序的防篡改链，任一历史字节被改动都会使校验失败；
- idempotency_key 唯一约束保证重复上报只产生一条事件，
  重复请求拿到同一张回执；
- 写操作在 BEGIN IMMEDIATE 事务内串行化，配合应用层的
  expected_round 检查实现多人同时操作的乐观并发控制。
"""

from __future__ import annotations

import sqlite3
import threading
from typing import Callable, Iterable, Optional

from .canon import canonical, hash_obj
from .timeutil import format_ts, utcnow

GENESIS_HASH = "0" * 64

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    stream TEXT NOT NULL,
    case_id TEXT,
    type TEXT NOT NULL,
    actor TEXT NOT NULL,
    role TEXT NOT NULL,
    occurred_at TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    idempotency_key TEXT UNIQUE,
    prev_hash TEXT NOT NULL,
    hash TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_events_case ON events(case_id, seq);
CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream, seq);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type, seq);

CREATE TABLE IF NOT EXISTS receipts (
    receipt_id TEXT PRIMARY KEY,
    idempotency_key TEXT UNIQUE,
    event_seq INTEGER NOT NULL REFERENCES events(seq),
    body TEXT NOT NULL
);
"""


class EventStore:
    """线程安全的单链事件存储。"""

    def __init__(self, db_path: str = ":memory:", clock: Callable = utcnow):
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_SCHEMA)
        self._lock = threading.Lock()
        self._clock = clock

    def close(self):
        self._conn.close()

    # ------------------------------------------------------------------ 写入

    def append(
        self,
        *,
        stream: str,
        type: str,
        actor: str,
        role: str,
        occurred_at: str,
        payload: dict,
        case_id: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        recorded_at: Optional[str] = None,
    ) -> tuple[dict, dict, bool]:
        """追加事件，返回 (event, receipt, deduplicated)。

        相同 idempotency_key 重复提交时返回首次的事件与回执，
        deduplicated=True，不产生新事件。
        """
        with self._lock:
            if idempotency_key is not None:
                existing = self._receipt_by_key(idempotency_key)
                if existing is not None:
                    event = self._event_by_seq(existing["event_seq"])
                    return event, existing["body"], True

            recorded_at = format_ts(recorded_at) if recorded_at else format_ts(self._clock())
            occurred_at = format_ts(occurred_at)
            payload = dict(payload)

            self._conn.execute("BEGIN IMMEDIATE")
            try:
                last = self._conn.execute(
                    "SELECT seq, hash FROM events ORDER BY seq DESC LIMIT 1"
                ).fetchone()
                seq = 1 if last is None else last["seq"] + 1
                prev_hash = GENESIS_HASH if last is None else last["hash"]

                body = {
                    "seq": seq,
                    "stream": stream,
                    "case_id": case_id,
                    "type": type,
                    "actor": actor,
                    "role": role,
                    "occurred_at": occurred_at,
                    "recorded_at": recorded_at,
                    "payload": payload,
                    "idempotency_key": idempotency_key,
                    "prev_hash": prev_hash,
                }
                digest = hash_obj(body)
                event = {"event_id": f"evt-{seq:08d}-{digest[:12]}", "hash": digest, **body}
                self._conn.execute(
                    """INSERT INTO events
                       (event_id, stream, case_id, type, actor, role, occurred_at,
                        recorded_at, payload, idempotency_key, prev_hash, hash)
                       VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        event["event_id"], stream, case_id, type, actor, role,
                        occurred_at, recorded_at, canonical(payload),
                        idempotency_key, prev_hash, digest,
                    ),
                )
                receipt = {
                    "receipt_id": f"rcpt-{seq:08d}-{digest[:12]}",
                    "event_id": event["event_id"],
                    "seq": seq,
                    "stream": stream,
                    "case_id": case_id,
                    "type": type,
                    "hash": digest,
                    "prev_hash": prev_hash,
                    "recorded_at": recorded_at,
                }
                self._conn.execute(
                    "INSERT INTO receipts (receipt_id, idempotency_key, event_seq, body)"
                    " VALUES (?,?,?,?)",
                    (receipt["receipt_id"], idempotency_key, seq, canonical(receipt)),
                )
                self._conn.commit()
                return event, receipt, False
            except Exception:
                self._conn.rollback()
                raise

    # ------------------------------------------------------------------ 读取

    def _row_to_event(self, row: sqlite3.Row) -> dict:
        import json

        return {
            "seq": row["seq"],
            "event_id": row["event_id"],
            "stream": row["stream"],
            "case_id": row["case_id"],
            "type": row["type"],
            "actor": row["actor"],
            "role": row["role"],
            "occurred_at": row["occurred_at"],
            "recorded_at": row["recorded_at"],
            "payload": json.loads(row["payload"]),
            "idempotency_key": row["idempotency_key"],
            "prev_hash": row["prev_hash"],
            "hash": row["hash"],
        }

    def _event_by_seq(self, seq: int) -> Optional[dict]:
        row = self._conn.execute("SELECT * FROM events WHERE seq=?", (seq,)).fetchone()
        return self._row_to_event(row) if row else None

    def _receipt_by_key(self, key: str) -> Optional[dict]:
        import json

        row = self._conn.execute(
            "SELECT event_seq, body FROM receipts WHERE idempotency_key=?", (key,)
        ).fetchone()
        if row is None:
            return None
        return {"event_seq": row["event_seq"], "body": json.loads(row["body"])}

    def events(self, *, case_id: Optional[str] = None, stream: Optional[str] = None,
               types: Optional[Iterable[str]] = None) -> list[dict]:
        sql = "SELECT * FROM events WHERE 1=1"
        args: list = []
        if case_id is not None:
            sql += " AND case_id=?"
            args.append(case_id)
        if stream is not None:
            sql += " AND stream=?"
            args.append(stream)
        if types is not None:
            types = list(types)
            sql += f" AND type IN ({','.join('?' * len(types))})"
            args.extend(types)
        sql += " ORDER BY seq"
        return [self._row_to_event(r) for r in self._conn.execute(sql, args)]

    def all_events(self) -> list[dict]:
        return self.events()

    def get_event(self, event_id: str) -> Optional[dict]:
        row = self._conn.execute(
            "SELECT * FROM events WHERE event_id=?", (event_id,)
        ).fetchone()
        return self._row_to_event(row) if row else None

    def get_receipt(self, receipt_id: str) -> Optional[dict]:
        import json

        row = self._conn.execute(
            "SELECT body FROM receipts WHERE receipt_id=?", (receipt_id,)
        ).fetchone()
        return json.loads(row["body"]) if row else None

    def receipt_for_seq(self, seq: int) -> Optional[dict]:
        """取某事件对应的回执。"""
        import json

        row = self._conn.execute(
            "SELECT body FROM receipts WHERE event_seq=?", (seq,)
        ).fetchone()
        return json.loads(row["body"]) if row else None

    def case_ids(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT DISTINCT case_id FROM events WHERE case_id IS NOT NULL ORDER BY case_id"
        ).fetchall()
        return [r["case_id"] for r in rows]

    # ------------------------------------------------------------------ 校验

    @staticmethod
    def hash_of(event: dict) -> str:
        """按写入时的口径重算事件哈希。"""
        return hash_obj({
            "seq": event["seq"],
            "stream": event["stream"],
            "case_id": event["case_id"],
            "type": event["type"],
            "actor": event["actor"],
            "role": event["role"],
            "occurred_at": event["occurred_at"],
            "recorded_at": event["recorded_at"],
            "payload": event["payload"],
            "idempotency_key": event["idempotency_key"],
            "prev_hash": event["prev_hash"],
        })

    def verify_chain(self, *, case_id: Optional[str] = None) -> dict:
        """校验链完整性。

        链的前后链接是全局的（prev_hash 指向全局上一事件），因此按案件
        校验时逐事件重算哈希，并与其全局前驱（seq-1）比对链接——这构成
        该案件事件未被抽换、未被插删的包含性证明。
        """
        events = self.events(case_id=case_id) if case_id else self.all_events()
        problems = []
        for ev in events:
            if self.hash_of(ev) != ev["hash"]:
                problems.append({"seq": ev["seq"], "problem": "hash_mismatch"})
            if ev["seq"] == 1:
                if ev["prev_hash"] != GENESIS_HASH:
                    problems.append({"seq": ev["seq"], "problem": "chain_broken"})
            else:
                prev = self._event_by_seq(ev["seq"] - 1)
                if prev is None or ev["prev_hash"] != prev["hash"]:
                    problems.append({"seq": ev["seq"], "problem": "chain_broken"})
        head = self._event_by_seq(events[-1]["seq"])["hash"] if events else GENESIS_HASH
        return {
            "valid": not problems,
            "checked": len(events),
            "problems": problems,
            "head_hash": head,
        }

    def verify_receipt(self, receipt_id: str) -> dict:
        """核验回执：回执指向的事件真实存在、哈希与链链接无误。"""
        receipt = self.get_receipt(receipt_id)
        if receipt is None:
            return {"valid": False, "receipt_id": receipt_id,
                    "checks": {"receipt_found": False}}
        event = self._event_by_seq(receipt["seq"])
        checks = {"receipt_found": True}
        checks["event_found"] = event is not None
        if event is not None:
            checks["hash_matches"] = self.hash_of(event) == event["hash"] == receipt["hash"]
            checks["receipt_matches_event"] = (
                receipt["event_id"] == event["event_id"]
                and receipt["recorded_at"] == event["recorded_at"]
            )
            if event["seq"] == 1:
                checks["chain_link"] = event["prev_hash"] == GENESIS_HASH
            else:
                prev = self._event_by_seq(event["seq"] - 1)
                checks["chain_link"] = prev is not None and event["prev_hash"] == prev["hash"]
        return {
            "valid": all(checks.values()),
            "receipt_id": receipt_id,
            "receipt": receipt,
            "checks": checks,
        }
