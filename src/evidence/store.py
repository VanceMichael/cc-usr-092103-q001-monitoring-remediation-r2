"""双轨哈希链事件存储。

每条事件同时挂在两条链上：

* 全局链：按入链顺序串联系统内全部事件（含机构/仪器等基准资料事件），
  任何跨案件的增删改都会破坏全局链；
* 案件链：同一 ``case_id`` 的事件独立串联，执法人员沿一条采样记录追查时，
  只需核验该案件链即可重放完整经过。

事件一经入链不可修改、不可删除；更正通过追加"更正事件"实现，原事实保留。
存储采用只追加 JSONL，重放时重建幂等索引并逐条校验哈希。
"""

from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

from .clock import Clock, fmt_ts
from .hashing import digest, digest_concat

GENESIS = "0" * 64
REGISTRY_STREAM = "@registry"


class ChainTampered(Exception):
    """重放校验发现哈希链断裂（事件被增删改）。"""


class ConcurrentModification(Exception):
    """并发写入冲突：expected_case_hash 与案件链当前头部不一致。"""


class IdempotentReplay(Exception):
    """重复请求命中幂等索引，返回首次入链的事件而非新建事件。"""

    def __init__(self, event: "Event"):
        super().__init__(f"幂等命中：{event.event_hash}")
        self.event = event


@dataclass(frozen=True)
class Event:
    seq: int
    case_id: str
    case_seq: int
    event_type: str
    payload: dict
    actor: str
    occurred_at: str          # 业务事实发生时间
    recorded_at: str          # 入链时间
    idempotency_key: Optional[str]
    payload_hash: str
    prev_global_hash: str
    prev_case_hash: str
    event_hash: str

    def to_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_line(cls, line: str) -> "Event":
        return cls(**json.loads(line))


def compute_event_hash(ev: Event) -> str:
    """事件哈希：固定字段 + 载荷摘要 + 两个前驱哈希。"""
    return digest(
        {
            "seq": ev.seq,
            "case_id": ev.case_id,
            "case_seq": ev.case_seq,
            "event_type": ev.event_type,
            "payload_hash": ev.payload_hash,
            "actor": ev.actor,
            "occurred_at": ev.occurred_at,
            "recorded_at": ev.recorded_at,
            "idempotency_key": ev.idempotency_key,
            "prev_global_hash": ev.prev_global_hash,
            "prev_case_hash": ev.prev_case_hash,
        }
    )


@dataclass
class EventStore:
    clock: Clock
    path: Optional[Path] = None
    _events: list[Event] = field(default_factory=list)
    _case_seqs: dict[str, int] = field(default_factory=dict)
    _case_heads: dict[str, str] = field(default_factory=dict)
    _idem: dict[str, str] = field(default_factory=dict)  # key -> event_hash
    _lock: threading.RLock = field(default_factory=threading.RLock)

    def __post_init__(self) -> None:
        if self.path is not None:
            self.path = Path(self.path)
            if self.path.exists():
                self._load()

    # ---------- 写入 ----------

    def append(
        self,
        event_type: str,
        payload: dict,
        *,
        case_id: str,
        actor: str,
        occurred_at: Optional[str] = None,
        idempotency_key: Optional[str] = None,
        expected_case_hash: Optional[str] = None,
    ) -> Event:
        """追加一条事件。

        ``expected_case_hash`` 提供乐观并发控制（用于多人同时复核）：
        若与案件链当前头部不一致，抛出 :class:`ConcurrentModification`，不写入任何内容。
        ``idempotency_key`` 命中时抛出 :class:`IdempotentReplay`，携带首次事件。
        """
        with self._lock:
            if idempotency_key is not None and idempotency_key in self._idem:
                raise IdempotentReplay(self.event_by_hash(self._idem[idempotency_key]))

            current_head = self._case_heads.get(case_id, GENESIS)
            if expected_case_hash is not None and expected_case_hash != current_head:
                raise ConcurrentModification(
                    f"案件 {case_id} 已被他人推进：期望 {expected_case_hash[:12]}，"
                    f"当前 {current_head[:12]}"
                )

            now = fmt_ts(self.clock.now())
            occurred = occurred_at or now
            case_seq = self._case_seqs.get(case_id, 0) + 1
            ev = Event(
                seq=len(self._events) + 1,
                case_id=case_id,
                case_seq=case_seq,
                event_type=event_type,
                payload=payload,
                actor=actor,
                occurred_at=occurred,
                recorded_at=now,
                idempotency_key=idempotency_key,
                payload_hash=digest(payload),
                prev_global_hash=self.global_head,
                prev_case_hash=current_head,
                event_hash="",
            )
            ev = Event(**{**asdict(ev), "event_hash": compute_event_hash(ev)})

            self._events.append(ev)
            self._case_seqs[case_id] = case_seq
            self._case_heads[case_id] = ev.event_hash
            if idempotency_key is not None:
                self._idem[idempotency_key] = ev.event_hash
            self._persist(ev)
            return ev

    # ---------- 读取 ----------

    def append_all(
        self,
        specs: list[tuple[str, dict, Optional[str]]],
        *,
        case_id: str,
        actor: str,
        idempotency_key: Optional[str] = None,
        expected_case_hash: Optional[str] = None,
    ) -> list[Event]:
        """原子追加多条同案件事件（统一加锁、一次 OCC/幂等检查）。

        specs 为 ``(event_type, payload, occurred_at)`` 三元组列表，
        案件链序号依次递增，任一校验失败则整组不写入。
        """
        with self._lock:
            if idempotency_key is not None and idempotency_key in self._idem:
                raise IdempotentReplay(self.event_by_hash(self._idem[idempotency_key]))

            current_head = self._case_heads.get(case_id, GENESIS)
            if expected_case_hash is not None and expected_case_hash != current_head:
                raise ConcurrentModification(
                    f"案件 {case_id} 已被他人推进：期望 {expected_case_hash[:12]}，"
                    f"当前 {current_head[:12]}"
                )

            now = fmt_ts(self.clock.now())
            events: list[Event] = []
            head_global = self.global_head
            head_case = current_head
            next_seq = len(self._events) + 1
            next_case_seq = self._case_seqs.get(case_id, 0) + 1
            for pos, (event_type, payload, occurred_at) in enumerate(specs):
                ev = Event(
                    seq=next_seq,
                    case_id=case_id,
                    case_seq=next_case_seq,
                    event_type=event_type,
                    payload=payload,
                    actor=actor,
                    occurred_at=occurred_at or now,
                    recorded_at=now,
                    # 同一命令组的每条事件都带幂等键，重启后按键即可重组整组
                    idempotency_key=idempotency_key,
                    payload_hash=digest(payload),
                    prev_global_hash=head_global,
                    prev_case_hash=head_case,
                    event_hash="",
                )
                ev = Event(**{**asdict(ev), "event_hash": compute_event_hash(ev)})
                events.append(ev)
                head_global = ev.event_hash
                head_case = ev.event_hash
                next_seq += 1
                next_case_seq += 1

            for ev in events:
                self._events.append(ev)
                self._case_seqs[case_id] = ev.case_seq
                self._case_heads[case_id] = ev.event_hash
                self._persist(ev)
            if idempotency_key is not None:
                self._idem[idempotency_key] = events[0].event_hash
            return events

    @property
    def global_head(self) -> str:
        return self._events[-1].event_hash if self._events else GENESIS

    def case_head(self, case_id: str) -> str:
        return self._case_heads.get(case_id, GENESIS)

    def peek_idempotency(self, key: str) -> Optional[Event]:
        """幂等探测：不写入，返回该 command_id 首次命令组的首条事件。"""
        h = self._idem.get(key)
        return self.event_by_hash(h) if h else None

    def all_events(self) -> list[Event]:
        return list(self._events)

    def case_events(self, case_id: str) -> list[Event]:
        return [e for e in self._events if e.case_id == case_id]

    def event_by_hash(self, event_hash: str) -> Event:
        for ev in self._events:
            if ev.event_hash == event_hash:
                return ev
        raise KeyError(event_hash)

    def head_snapshot(self) -> dict:
        """稳定的链头摘要：跨部门移送时随案移送，供接收方/执法方核验。"""
        return {
            "global_head": self.global_head,
            "global_length": len(self._events),
            "case_heads": dict(sorted(self._case_heads.items())),
        }

    # ---------- 校验 ----------

    def verify(self) -> None:
        """全量重放校验：顺序、载荷哈希、双轨链哈希一致。"""
        prev_global = GENESIS
        case_heads: dict[str, str] = {}
        for i, ev in enumerate(self._events):
            if ev.seq != i + 1:
                raise ChainTampered(f"事件 {ev.seq} 序号不连续")
            if ev.payload_hash != digest(ev.payload):
                raise ChainTampered(f"事件 {ev.seq} 载荷被篡改")
            if ev.prev_global_hash != prev_global:
                raise ChainTampered(f"事件 {ev.seq} 全局链断裂")
            if ev.prev_case_hash != case_heads.get(ev.case_id, GENESIS):
                raise ChainTampered(f"事件 {ev.seq} 案件链断裂（{ev.case_id}）")
            if ev.event_hash != compute_event_hash(ev):
                raise ChainTampered(f"事件 {ev.seq} 事件哈希不匹配")
            prev_global = ev.event_hash
            case_heads[ev.case_id] = ev.event_hash

    def verify_case(self, case_id: str) -> None:
        """只校验单条案件链（执法人员沿采样记录追查时使用）。"""
        prev = GENESIS
        for i, ev in enumerate(self.case_events(case_id), start=1):
            if ev.case_seq != i:
                raise ChainTampered(f"案件 {case_id} 序号不连续")
            if ev.payload_hash != digest(ev.payload):
                raise ChainTampered(f"案件 {case_id} 事件 {i} 载荷被篡改")
            if ev.prev_case_hash != prev:
                raise ChainTampered(f"案件 {case_id} 事件 {i} 案件链断裂")
            if ev.event_hash != compute_event_hash(ev):
                raise ChainTampered(f"案件 {case_id} 事件 {i} 哈希不匹配")
            prev = ev.event_hash

    def merkle_brief(self) -> str:
        """全部事件哈希的归并根，便于一眼比对两个库是否同构。"""
        hashes = [e.event_hash for e in self._events]
        if not hashes:
            return GENESIS
        while len(hashes) > 1:
            nxt = []
            for i in range(0, len(hashes), 2):
                if i + 1 < len(hashes):
                    nxt.append(digest_concat(hashes[i], hashes[i + 1]))
                else:
                    nxt.append(hashes[i])
            hashes = nxt
        return hashes[0]

    # ---------- 持久化 ----------

    def _persist(self, ev: Event) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as f:
            f.write(ev.to_line() + "\n")
            f.flush()

    def _load(self) -> None:
        for raw in self.path.read_text(encoding="utf-8").splitlines():
            raw = raw.strip()
            if not raw:
                continue
            ev = Event.from_line(raw)
            self._events.append(ev)
            self._case_seqs[ev.case_id] = ev.case_seq
            self._case_heads[ev.case_id] = ev.event_hash
            if ev.idempotency_key is not None:
                self._idem.setdefault(ev.idempotency_key, ev.event_hash)
        self.verify()

    def export_case_bundle(self, case_id: str) -> dict:
        """导出一条案件链的完整可核验材料包（跨部门移送用）。"""
        events = [asdict(e) for e in self.case_events(case_id)]
        return {
            "format": "evidence-case-bundle/1",
            "case_id": case_id,
            "case_head": self.case_head(case_id),
            "global_head_at_export": self.global_head,
            "events": events,
        }

    @staticmethod
    def verify_bundle(bundle: dict) -> None:
        """独立核验移送材料包（不依赖本库状态）。"""
        if bundle.get("format") != "evidence-case-bundle/1":
            raise ChainTampered("材料包格式不受支持")
        prev = GENESIS
        for i, raw in enumerate(bundle["events"], start=1):
            ev = Event(**raw)
            if ev.case_id != bundle["case_id"] or ev.case_seq != i:
                raise ChainTampered(f"材料包事件 {i} 归属/序号错误")
            if ev.payload_hash != digest(ev.payload):
                raise ChainTampered(f"材料包事件 {i} 载荷被篡改")
            if ev.prev_case_hash != prev:
                raise ChainTampered(f"材料包事件 {i} 链断裂")
            if ev.event_hash != compute_event_hash(ev):
                raise ChainTampered(f"材料包事件 {i} 哈希不匹配")
            prev = ev.event_hash
        if prev != bundle["case_head"]:
            raise ChainTampered("材料包案件头与事件不一致")
