"""整改取证协同业务服务。

所有命令遵循同一范式：

1. 角色权限校验（:mod:`access`）；
2. 以 **业务发生时刻** 查询时间化基准资料（不是"现在"的状态）；
3. 业务规则校验（重复上报、补录引用、逾期、复核人回避等）；
4. 事件原子入链（幂等键 / 乐观并发），签发可核验回执。

更正与补录永不修改旧事件：原事实留在链上，新事件引用并解释。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Optional

from . import access
from .clock import Clock, fmt_ts, parse_ts
from .errors import (
    CorrectionInvalid,
    DomainError,
    DuplicateReport,
    LateEntryInvalid,
    InstrumentInvalidAtTime,
    NotFound,
    OrderClosed,
    QualificationExited,
    RectificationPerfunctory,
    ReviewerConflict,
    TransferStateError,
    UnauthorizedAtTime,
)
from .receipts import ReceiptService
from .registry import Registry
from .store import (
    GENESIS,
    REGISTRY_STREAM,
    ConcurrentModification,
    Event,
    EventStore,
    IdempotentReplay,
)

RECTIFICATION_PHASE = "rectification"
DEFAULT_RECTIFICATION_DAYS = 30


def digest_id(*parts: str) -> str:
    from .hashing import canonical, sha256_hex
    return sha256_hex(canonical(list(parts)))


# ---------------------------------------------------------------- 案件折叠


@dataclass
class RectificationOrder:
    order_id: str
    opened_event: str
    items: list[dict]
    ordered_at: str
    due_at: str
    status: str = "open"            # open / perfunctory / submitted / verified / rejected
    submissions: list[dict] = field(default_factory=list)
    overdue_marked: bool = False
    review: Optional[dict] = None


@dataclass
class Transfer:
    transfer_id: str
    to_agency: str
    requested_at: str
    status: str = "in_transit"      # in_transit / accepted / filed / returned
    result: Optional[dict] = None


@dataclass
class CaseState:
    case_id: str
    opened: bool = False
    samples: list[dict] = field(default_factory=list)
    phases_reported: dict[str, str] = field(default_factory=dict)   # phase -> event_hash
    late_entries: list[dict] = field(default_factory=list)
    corrections: list[dict] = field(default_factory=list)
    faults: list[dict] = field(default_factory=list)
    orders: dict[str, RectificationOrder] = field(default_factory=dict)
    transfers: dict[str, Transfer] = field(default_factory=dict)
    status: str = "opened"

    def open_order(self) -> Optional[RectificationOrder]:
        for o in self.order_sequence():
            if o.status in ("open", "perfunctory", "submitted", "rejected"):
                return o
        return None

    def order_sequence(self) -> list[RectificationOrder]:
        return [self.orders[k] for k in sorted(self.orders)]


def fold_case(events: list[Event]) -> CaseState:
    """从案件事件流重建当前状态（纯函数，重放同理）。"""
    st: Optional[CaseState] = None
    for ev in events:
        p = ev.payload
        if ev.event_type == "case.opened":
            st = CaseState(case_id=p["case_id"])
            st.opened = True
            continue
        if st is None:
            raise ChainError(f"案件 {ev.case_id} 缺少 case.opened")
        if ev.event_type == "sample.registered":
            st.samples.append({**p, "event_hash": ev.event_hash,
                               "recorded_at": ev.recorded_at})
        elif ev.event_type == "self_check.reported":
            st.phases_reported.setdefault(p["phase"], ev.event_hash)
        elif ev.event_type == "self_check.late_entry":
            st.late_entries.append({**p, "event_hash": ev.event_hash})
        elif ev.event_type == "fact.corrected":
            st.corrections.append({**p, "event_hash": ev.event_hash})
        elif ev.event_type == "reporting_fault.classified":
            st.faults.append({**p, "event_hash": ev.event_hash})
        elif ev.event_type == "rectification.ordered":
            st.orders[p["order_id"]] = RectificationOrder(
                order_id=p["order_id"], opened_event=ev.event_hash,
                items=list(p["items"]), ordered_at=p["ordered_at"], due_at=p["due_at"],
            )
        elif ev.event_type == "rectification.submitted":
            order = st.orders[p["order_id"]]
            order.submissions.append({**p, "event_hash": ev.event_hash})
            if p.get("accepted"):
                order.status = "submitted"
        elif ev.event_type == "rectification.perfunctory_flagged":
            st.orders[p["order_id"]].status = "perfunctory"
        elif ev.event_type == "rectification.overdue_marked":
            st.orders[p["order_id"]].overdue_marked = True
        elif ev.event_type == "rectification.verified":
            order = st.orders[p["order_id"]]
            order.status = "verified" if p["passed"] else "rejected"
            order.review = {**p, "event_hash": ev.event_hash}
        elif ev.event_type == "transfer.requested":
            st.transfers[p["transfer_id"]] = Transfer(
                transfer_id=p["transfer_id"], to_agency=p["to_agency"],
                requested_at=p["requested_at"],
            )
        elif ev.event_type == "transfer.result_recorded":
            t = st.transfers[p["transfer_id"]]
            t.status = p["result"]
            t.result = {**p, "event_hash": ev.event_hash}
        elif ev.event_type == "case.closed":
            st.status = "closed"
    if st is None:
        raise ChainError("案件不存在")
    return st


class ChainError(DomainError):
    pass


# ---------------------------------------------------------------- 服务


@dataclass
class CommandResult:
    events: list[Event]
    receipt: dict
    case_state_before_head: str

    @property
    def event(self) -> Event:
        return self.events[0]


class RemediationService:
    def __init__(self, store: EventStore, clock: Clock, receipts: ReceiptService):
        self.store = store
        self.clock = clock
        self.receipts = receipts
        self.registry = Registry()
        for ev in store.case_events(REGISTRY_STREAM):
            self.registry.apply(ev.event_type, ev.payload, ev.event_hash)
        # 命令在服务层按案件串行化：保证"检查状态→入链"之间不被并发命令穿插
        self._case_locks: dict[str, threading.RLock] = {}
        self._registry_lock = threading.RLock()
        self._locks_guard = threading.Lock()

    def _case_lock(self, case_id: str) -> threading.RLock:
        with self._locks_guard:
            return self._case_locks.setdefault(case_id, threading.RLock())

    # ============================================================ 内部工具

    def _state(self, case_id: str) -> CaseState:
        return fold_case(self.store.case_events(case_id))

    def _issue(self, action: str, events: list[Event], extra: Optional[dict] = None,
               *, status: str = "accepted") -> dict:
        first = events[0]
        receipt = self.receipts.issue(
            receipt_id=f"R-{first.event_hash[:16]}",
            case_id=first.case_id,
            action=action,
            event_hash=first.event_hash,
            case_head=events[-1].event_hash,
            global_head=self.store.global_head,
            recorded_at=first.recorded_at,
            extra={"event_count": len(events), "status": status, **(extra or {})},
        )
        return receipt

    def _rejection_receipt(self, action: str, case_id: str, reason_code: str,
                           detail: str, extra: Optional[dict] = None,
                           *, stable_key: Optional[str] = None) -> dict:
        """被拒绝的命令同样得到稳定、可核验、可重放的回执。

        ``stable_key`` 给出时（如重复上报），回执号与收录时间对同一被拒事实
        保持确定，使重复提交永远拿回同一张回执。
        """
        head = self.store.case_head(case_id)
        now = fmt_ts(self.clock.now())
        tail = stable_key if stable_key is not None else digest_id(head, action, reason_code, detail, now)[:16]
        return self.receipts.issue(
            receipt_id=f"X-{reason_code}-{tail}",
            case_id=case_id, action=action, event_hash=head,
            case_head=head, global_head=self.store.global_head,
            recorded_at=now,
            extra={"status": "rejected", "reason_code": reason_code, "detail": detail,
                   "stable": stable_key is not None, **(extra or {})},
        )

    def _commit(
        self,
        action: str,
        case_id: str,
        actor: str,
        specs: list[tuple[str, dict, Optional[str]]],
        *,
        command_id: Optional[str] = None,
        expected_case_hash: Optional[str] = None,
        extra: Optional[dict] = None,
    ) -> CommandResult:
        try:
            events = self.store.append_all(
                specs, case_id=case_id, actor=actor,
                idempotency_key=command_id, expected_case_hash=expected_case_hash,
            )
        except IdempotentReplay as replay:
            # 重复的 command_id：重放首次的整组事件与回执，不产生新事件
            group = self._command_group(replay.event, command_id)
            receipt = self._issue(action, group, extra={"idempotent_replay": True, **(extra or {})})
            return CommandResult(group, receipt, replay.event.prev_case_hash)
        receipt = self._issue(action, events, extra)
        return CommandResult(events, receipt, events[0].prev_case_hash)

    def _command_group(self, first: Event, idempotency_key: Optional[str]) -> list[Event]:
        """按幂等键重组同一次原子追加的事件组（不依赖时间戳相同）。"""
        if idempotency_key is None:
            return [first]
        return [e for e in self.store.case_events(first.case_id)
                if e.idempotency_key == idempotency_key]

    def _find_event(self, case_id: str, event_hash: str) -> Event:
        for ev in self.store.case_events(case_id):
            if ev.event_hash == event_hash:
                return ev
        raise NotFound(f"案件 {case_id} 中找不到事件 {event_hash[:16]}")

    # ============================================================ 基准资料

    def import_registry_bundle(self, entries: list[dict], *, actor: str) -> CommandResult:
        """批量导入基准资料事件（机构/人员/仪器/期限/规则）。

        每条 entry: {"type": 事件类型, "payload": {...}}，
        载荷中的 ``recorded_at`` 表示资料的收录/登记时间，用于识别事后补录。
        """
        specs: list[tuple[str, dict, Optional[str]]] = []
        for entry in entries:
            etype = entry["type"]
            payload = dict(entry["payload"])
            payload.setdefault("recorded_at", fmt_ts(self.clock.now()))
            specs.append((etype, payload, payload["recorded_at"]))
        with self._registry_lock:
            events = self.store.append_all(specs, case_id=REGISTRY_STREAM, actor=actor)
            for ev in events:
                self.registry.apply(ev.event_type, ev.payload, ev.event_hash)
        receipt = self._issue("registry.import", events)
        return CommandResult(events, receipt, GENESIS)

    def withdraw_qualification(self, org_id: str, *, reason: str, actor: str,
                               role: access.Role, at: Optional[str] = None) -> CommandResult:
        access.require("withdraw.qualification", role)
        when = at or fmt_ts(self.clock.now())
        if self.registry.org_at(org_id, parse_ts(when)) is None:
            raise NotFound(f"机构 {org_id} 不存在")
        specs = [("registry.org_withdrawn",
                  {"org_id": org_id, "at": when, "reason": reason,
                   "recorded_at": fmt_ts(self.clock.now()),
                   "legal_basis": "资质依法退出"}, when)]
        with self._registry_lock:
            result = self._commit("withdraw.qualification", REGISTRY_STREAM, actor, specs)
            for ev in result.events:
                self.registry.apply(ev.event_type, ev.payload, ev.event_hash)
        return result

    # ============================================================ 采样登记

    def register_sample(
        self, *, case_id: str, sample_ref: str, org_id: str, person_id: str,
        instrument_id: str, sampled_at: str, raw_data_ref: str,
        finding: str = "", actor: str, role: access.Role, command_id: str,
    ) -> CommandResult:
        access.require("register.sample", role)
        at = parse_ts(sampled_at)
        with self._case_lock(case_id):
            # 网络重试：相同 command_id 直接重放首次受理结果
            if (probe := self.store.peek_idempotency(command_id)) is not None:
                group = self._command_group(probe, command_id)
                receipt = self._issue(
                    "register.sample", group,
                    extra={"idempotent_replay": True, "sample_ref": sample_ref})
                return CommandResult(group, receipt, probe.prev_case_hash)

            if self.store.case_events(case_id):
                raise DomainError(f"案件 {case_id} 已存在，采样登记只能发生一次")

            # 认知时点 = 登记时刻：只有此刻已收录的资料才算"当时采用"，
            # 事后补录的证书不会被追溯认定为采样时有效
            known_at = self.clock.now()
            org_seg = self.registry.org_at(org_id, at, known_at)
            if org_seg is None or org_seg.status == "withdrawn":
                raise QualificationExited(
                    f"机构 {org_id} 在采样时刻 {sampled_at} 不具备有效资质（未登记或已依法退出）")
            if org_seg.status == "suspended":
                raise QualificationExited(
                    f"机构 {org_id} 在采样时刻处于资质暂停状态：{org_seg.reason}")

            auth = self.registry.person_at(person_id, at, org_id, known_at)
            if auth is None:
                raise UnauthorizedAtTime(
                    f"操作人员 {person_id} 在 {sampled_at} 未获 {org_id} 有效授权")

            inst_tl = self.registry.instruments.get(instrument_id)
            if inst_tl is None:
                raise InstrumentInvalidAtTime(f"仪器 {instrument_id} 未登记")
            cal = inst_tl.at(at, known_at)
            if inst_tl.retired_at is not None and at >= inst_tl.retired_at:
                raise InstrumentInvalidAtTime(
                    f"仪器 {instrument_id} 已于 {fmt_ts(inst_tl.retired_at)} 停用")
            if cal is None:
                raise InstrumentInvalidAtTime(
                    f"仪器 {instrument_id} 在采样时刻 {sampled_at} 无有效校准证书覆盖")

            now = fmt_ts(self.clock.now())
            specs = [
                ("case.opened", {"case_id": case_id, "opened_at": now, "sample_ref": sample_ref}, now),
                ("sample.registered", {
                    "case_id": case_id,
                    "sample_ref": sample_ref,
                    "org_id": org_id,
                    "qualification_no": org_seg.qualification_no,
                    "qualification_event": org_seg.event_hash,
                    "person_id": person_id,
                    "authorization_event": auth.event_hash,
                    "instrument_id": instrument_id,
                    "certificate_no": cal.certificate_no,
                    "calibration_event": cal.event_hash,
                    "sampled_at": sampled_at,
                    "raw_data_ref": raw_data_ref,
                    "finding": finding,
                }, sampled_at),
            ]
            return self._commit("register.sample", case_id, actor, specs, command_id=command_id,
                                extra={"sample_ref": sample_ref})

    # ============================================================ 自查上报

    def report_self_check(self, case_id: str, *, phase: str, content: str,
                          reported_at: Optional[str] = None, actor: str,
                          role: access.Role, command_id: str) -> CommandResult:
        access.require("report.self_check", role)
        with self._case_lock(case_id):
            # 相同 command_id 一律重放首次受理结果（即使首次因重复被拒也稳定重放）
            if (probe := self.store.peek_idempotency(command_id)) is not None:
                group = self._command_group(probe, command_id)
                receipt = self._issue(
                    "report.self_check", group,
                    extra={"idempotent_replay": True, "phase": phase})
                return CommandResult(group, receipt, probe.prev_case_hash)

            st = self._state(case_id)
            if phase in st.phases_reported:
                # 重复上报：不新增事件，但对同一 (案件,阶段) 永远签发同一张稳定回执
                key = digest_id(case_id, "duplicate_phase", phase, st.phases_reported[phase])[:24]
                receipt = self._rejection_receipt(
                    "report.self_check", case_id, "duplicate_report",
                    f"阶段 {phase} 已上报，不得重复上报；如需补充请走补录并引用旧记录",
                    extra={"phase": phase, "first_event_hash": st.phases_reported[phase]},
                    stable_key=key,
                )
                raise DuplicateReport(
                    f"案件 {case_id} 阶段 {phase} 已上报（事件 {st.phases_reported[phase][:12]}）",
                    first_event_hash=st.phases_reported[phase], receipt=receipt,
                )
            when = reported_at or fmt_ts(self.clock.now())
            specs = [("self_check.reported", {
                "case_id": case_id, "phase": phase, "content": content, "reported_at": when,
            }, when)]
            return self._commit("report.self_check", case_id, actor, specs,
                                command_id=command_id, extra={"phase": phase})

    def report_late_entry(self, case_id: str, *, ref_event_hash: str, reason: str,
                          facts: dict, actor: str, role: access.Role,
                          command_id: str) -> CommandResult:
        """自查补录：只能引用既有记录、解释原因，不允许改动被引用记录。"""
        access.require("report.late_entry", role)
        if not reason or not reason.strip():
            raise LateEntryInvalid("补录必须说明原因")
        with self._case_lock(case_id):
            target = self._find_event(case_id, ref_event_hash)
            now = fmt_ts(self.clock.now())
            specs = [("self_check.late_entry", {
                "case_id": case_id,
                "ref_event_hash": target.event_hash,
                "ref_event_type": target.event_type,
                "ref_payload_hash": target.payload_hash,
                "reason": reason.strip(),
                "facts": facts,
                "entered_at": now,
            }, now)]
            return self._commit("report.late_entry", case_id, actor, specs,
                                command_id=command_id)

    def correct_fact(self, case_id: str, *, target_event_hash: str, field_name: str,
                     new_value, reason: str, actor: str, role: access.Role,
                     command_id: str) -> CommandResult:
        """更正事实：原值随更正事件永久保留，原事件不动。"""
        access.require("correct.fact", role)
        if not reason or not reason.strip():
            raise CorrectionInvalid("更正必须说明原因")
        with self._case_lock(case_id):
            target = self._find_event(case_id, target_event_hash)
            if target.event_type in ("case.opened", "fact.corrected"):
                raise CorrectionInvalid(f"事件类型 {target.event_type} 不允许更正")
            old_value = target.payload.get(field_name)
            if old_value == new_value:
                raise CorrectionInvalid("新值与原事实一致，无需更正")
            now = fmt_ts(self.clock.now())
            specs = [("fact.corrected", {
                "case_id": case_id,
                "target_event_hash": target.event_hash,
                "target_event_type": target.event_type,
                "target_payload_hash": target.payload_hash,
                "field_name": field_name,
                "before_value": old_value,
                "after_value": new_value,
                "reason": reason.strip(),
                "corrected_at": now,
            }, now)]
            return self._commit("correct.fact", case_id, actor, specs, command_id=command_id)

    # ============================================================ 错报漏报

    def classify_reporting_fault(self, case_id: str, *, kind: str, detail: str,
                                 basis_event_hash: Optional[str] = None,
                                 actor: str, role: access.Role,
                                 command_id: str) -> CommandResult:
        access.require("classify.reporting_fault", role)
        if kind not in ("misreport", "omission"):
            raise DomainError("fault kind 仅支持 misreport（错报）/ omission（漏报）")
        with self._case_lock(case_id):
            rule = self.registry.rules.get(kind)
            now = fmt_ts(self.clock.now())
            specs = [("reporting_fault.classified", {
                "case_id": case_id, "kind": kind, "detail": detail,
                "basis_event_hash": basis_event_hash,
                "prescribed_action": rule.action if rule else None,
                "prescribed_authority": rule.authority_role if rule else None,
                "classified_at": now,
            }, now)]
            return self._commit("classify.reporting_fault", case_id, actor, specs,
                                command_id=command_id, extra={"kind": kind})

    # ============================================================ 整改

    def issue_rectification_order(self, case_id: str, *, items: list[dict],
                                  actor: str, role: access.Role,
                                  command_id: str) -> CommandResult:
        access.require("issue.rectification_order", role)
        with self._case_lock(case_id):
            st = self._state(case_id)
            if st.open_order() is not None:
                raise OrderClosed("案件存在未闭环的整改通知，不得重复下达")
            days = self.registry.deadline_days(RECTIFICATION_PHASE) or DEFAULT_RECTIFICATION_DAYS
            now = self.clock.now()
            due = now + timedelta(days=days)
            order_id = f"ORD-{len(st.orders) + 1:03d}"
            clean_items = [
                {"code": str(it["code"]), "requirement": str(it["requirement"])}
                for it in items
            ]
            specs = [("rectification.ordered", {
                "case_id": case_id, "order_id": order_id, "items": clean_items,
                "ordered_at": fmt_ts(now), "due_at": fmt_ts(due),
                "deadline_days": days,
            }, fmt_ts(now))]
            return self._commit("issue.rectification_order", case_id, actor, specs,
                                command_id=command_id,
                                extra={"order_id": order_id, "due_at": fmt_ts(due)})

    def submit_rectification(self, case_id: str, *, order_id: str,
                             measures: list[dict], actor: str, role: access.Role,
                             command_id: str) -> CommandResult:
        access.require("submit.rectification", role)
        with self._case_lock(case_id):
            st = self._state(case_id)
            order = st.orders.get(order_id)
            if order is None:
                raise NotFound(f"整改通知 {order_id} 不存在")
            if order.status in ("verified",):
                raise OrderClosed(f"{order_id} 已复核通过并闭环")

            now = self.clock.now()
            overdue = now > parse_ts(order.due_at)
            # 敷衍整改识别：每项整改要求都必须有对应措施且附证据材料
            by_code = {m.get("item_code"): m for m in measures}
            missing = []
            for item in order.items:
                m = by_code.get(item["code"])
                if not m or not str(m.get("action", "")).strip():
                    missing.append({"item_code": item["code"], "reason": "缺少整改措施"})
                elif not m.get("evidence_refs"):
                    missing.append({"item_code": item["code"], "reason": "缺少佐证材料"})

            payload = {
                "case_id": case_id, "order_id": order_id,
                "submitted_at": fmt_ts(now), "due_at": order.due_at,
                "late": overdue, "measures": measures,
                "submitter": actor,
                "accepted": not missing,
            }
            specs: list[tuple[str, dict, Optional[str]]] = [
                ("rectification.submitted", payload, fmt_ts(now))
            ]
            if overdue and not order.overdue_marked:
                specs.append(("rectification.overdue_marked", {
                    "case_id": case_id, "order_id": order_id,
                    "due_at": order.due_at, "marked_at": fmt_ts(now),
                    "overdue_seconds": int((now - parse_ts(order.due_at)).total_seconds()),
                }, fmt_ts(now)))
            if missing:
                specs.append(("rectification.perfunctory_flagged", {
                    "case_id": case_id, "order_id": order_id,
                    "flagged_at": fmt_ts(now), "missing": missing,
                    "note": "整改措施缺项或无佐证，按敷衍整改记录并退回",
                }, fmt_ts(now)))
            result = self._commit("submit.rectification", case_id, actor, specs,
                                  command_id=command_id,
                                  extra={"order_id": order_id, "accepted": not missing, "late": overdue})
            if missing:
                # 事件已入链、回执照常签发，但明确告知未通过受理（退回重做）
                raise RectificationPerfunctory(
                    f"{order_id} 被认定为敷衍整改并已记录退回", missing,
                    receipt=result.receipt,
                )
            return result

    def sweep_overdue(self) -> list[CommandResult]:
        """定时巡检：对所有逾期待整改的通知登记逾期事件（幂等）。"""
        results = []
        now = fmt_ts(self.clock.now())
        for case_id in {e.case_id for e in self.store.all_events() if not e.case_id.startswith("@")}:
            with self._case_lock(case_id):
                st = self._state(case_id)
                for order in st.order_sequence():
                    if order.status in ("open", "perfunctory", "rejected") \
                            and not order.overdue_marked and now > order.due_at:
                        specs = [("rectification.overdue_marked", {
                            "case_id": case_id, "order_id": order.order_id,
                            "due_at": order.due_at, "marked_at": now,
                            "overdue_seconds": int(
                                (parse_ts(now) - parse_ts(order.due_at)).total_seconds()),
                            "marked_by": "system.sweep",
                        }, now)]
                        results.append(self._commit(
                            "sweep.overdue", case_id, "system.sweep", specs,
                            command_id=f"sweep:{order.order_id}:{order.due_at}",
                            extra={"order_id": order.order_id}))
        return results

    def verify_rectification(self, case_id: str, *, order_id: str, passed: bool,
                             comment: str, reviewer: str, role: access.Role,
                             expected_case_hash: Optional[str] = None,
                             command_id: str) -> CommandResult:
        """复核：提交人回避 + 乐观并发，多人同时复核只有一人成功。"""
        access.require("verify.rectification", role)
        with self._case_lock(case_id):
            # OCC 锚点优先：携带的链头一旦过期，立即判定为并发冲突，
            # 不继续按当前状态给出误导性业务错误
            current_head = self.store.case_head(case_id)
            if expected_case_hash is not None and expected_case_hash != current_head:
                raise ConcurrentModification(
                    f"案件已由其他复核人先行复核（当前链头 {current_head[:12]}）")
            st = self._state(case_id)
            order = st.orders.get(order_id)
            if order is None:
                raise NotFound(f"整改通知 {order_id} 不存在")
            if order.status != "submitted":
                raise OrderClosed(f"{order_id} 当前状态 {order.status}，不具备复核条件")
            latest = order.submissions[-1]
            if latest.get("submitter") == reviewer:
                raise ReviewerConflict("复核人不能是整改提交人本人")
            now = fmt_ts(self.clock.now())
            head = expected_case_hash or current_head
            specs = [("rectification.verified", {
                "case_id": case_id, "order_id": order_id, "passed": passed,
                "comment": comment, "reviewer": reviewer, "reviewed_at": now,
                "submission_event": latest["event_hash"],
                "based_on_case_head": head,
            }, now)]
            if not passed:
                specs.append(("rectification.rejected", {
                    "case_id": case_id, "order_id": order_id,
                    "reason": comment, "rejected_at": now,
                    "next_action": "退回机构重新整改，依法升级处置",
                }, now))
            try:
                return self._commit("verify.rectification", case_id, reviewer, specs,
                                    command_id=command_id, expected_case_hash=head,
                                    extra={"order_id": order_id, "passed": passed})
            except ConcurrentModification as exc:
                # 多人同时复核：后来者拿到明确的冲突说明与当前链头，而不是含糊报错
                raise ConcurrentModification(
                    f"案件已由其他复核人先行复核（当前链头 {self.store.case_head(case_id)[:12]}）"
                ) from exc

    # ============================================================ 跨部门移送

    def transfer_suspected_crime(self, case_id: str, *, to_agency: str, basis: str,
                                 actor: str, role: access.Role,
                                 command_id: str) -> CommandResult:
        access.require("transfer.suspected_crime", role)
        with self._case_lock(case_id):
            st = self._state(case_id)
            if not st.faults and not st.corrections:
                raise TransferStateError("无错报漏报或更正事实支撑，不得启动涉嫌犯罪移送")
            if any(t.status == "in_transit" for t in st.transfers.values()):
                raise TransferStateError("本案已有在途移送，须等待接收结果后再行移送")
            now = fmt_ts(self.clock.now())
            transfer_id = f"TRF-{len(st.transfers) + 1:03d}"
            bundle = self.store.export_case_bundle(case_id)
            specs = [("transfer.requested", {
                "case_id": case_id, "transfer_id": transfer_id,
                "to_agency": to_agency, "basis": basis,
                "requested_at": now,
                "case_head": bundle["case_head"],
                "global_head": bundle["global_head_at_export"],
                "event_count": len(bundle["events"]),
                "status": "in_transit",
            }, now)]
            return self._commit("transfer.suspected_crime", case_id, actor, specs,
                                command_id=command_id,
                                extra={"transfer_id": transfer_id, "to_agency": to_agency})

    def record_transfer_result(self, case_id: str, *, transfer_id: str,
                               result: str, docket_no: str = "", note: str = "",
                               actor: str, role: access.Role,
                               command_id: str) -> CommandResult:
        """公检法回流登记真实移送结果（受理/立案/退查）。"""
        access.require("record.transfer_result", role)
        with self._case_lock(case_id):
            st = self._state(case_id)
            tr = st.transfers.get(transfer_id)
            if tr is None:
                raise NotFound(f"移送记录 {transfer_id} 不存在")
            if tr.status != "in_transit":
                raise TransferStateError(f"{transfer_id} 已登记结果：{tr.status}")
            if result not in ("accepted", "filed", "returned"):
                raise DomainError("移送结果仅支持 accepted（受理）/ filed（立案）/ returned（退查）")
            now = fmt_ts(self.clock.now())
            specs = [("transfer.result_recorded", {
                "case_id": case_id, "transfer_id": transfer_id,
                "to_agency": tr.to_agency, "result": result,
                "docket_no": docket_no, "note": note, "recorded_at": now,
                "recorded_by_desk": actor,
            }, now)]
            return self._commit("record.transfer_result", case_id, actor, specs,
                                command_id=command_id,
                                extra={"transfer_id": transfer_id, "result": result})

    def close_case(self, case_id: str, *, actor: str, role: access.Role,
                   command_id: str) -> CommandResult:
        access.require("verify.rectification", role)
        with self._case_lock(case_id):
            st = self._state(case_id)
            pending = [o.order_id for o in st.order_sequence() if o.status != "verified"]
            in_transit = [t.transfer_id for t in st.transfers.values() if t.status == "in_transit"]
            if pending or in_transit:
                raise DomainError(f"案件尚有未闭环整改 {pending} 或在途移送 {in_transit}，不能关闭")
            now = fmt_ts(self.clock.now())
            return self._commit("case.close", case_id, actor,
                                [("case.closed", {"case_id": case_id, "closed_at": now}, now)],
                                command_id=command_id)
