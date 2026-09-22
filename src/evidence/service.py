"""业务服务层：整改取证协同的全部命令与查询。

命令（写）：
- record_sampling        采样记录登记，即时校验资质/授权/校准并留痕
- record_calibration     仪器校准记录登记（可迟到补登，触发矛盾检测）
- qualification_exit     机构资格依法退出
- open_case              依据采样记录开立整改案件
- submit_self_check      自查/整改阶段材料提交（含补录）
- correct_record         更正：新事件引用原事件，原事实保留
- submit_review          复核（乐观并发：expected_round）
- issue_disposition      四类处置按权限矩阵推进（含涉嫌犯罪移送全流程）
- check_overdue          逾期扫描，生成逾期事件与升级建议

查询（读）：
- timeline / summary / replay / verify_chain / verify_receipt
- qualification_at / authorization_at / instrument_at（时间化参考查询）
"""

from __future__ import annotations

import functools
import threading
import uuid
from typing import Optional

from .canon import hash_obj
from .domain import (
    CASE_OPEN,
    Conflict,
    DomainError,
    NotFound,
    PermissionDenied,
    ValidationFailed,
    fold_case,
)
from .reference import ReferenceData, ReferenceView
from .store import EventStore
from .timeutil import add_days, format_ts, parse_ts, utcnow

SYSTEM_ACTOR = "system"
SYSTEM_ROLE = "系统"


def _synchronized(fn):
    """命令级串行化：读状态-校验-写事件作为一个临界区。

    多人同时复核/提交时，后到者在锁内重新读取最新状态，
    乐观并发检查（如 expected_round）因此不会失效。
    """

    @functools.wraps(fn)
    def wrapper(self, *args, **kwargs):
        with self._cmd_lock:
            return fn(self, *args, **kwargs)

    return wrapper

# 机构可提交材料的阶段（复核阶段由复核员推进）
SUBMITTABLE_STAGES = ("self_check", "rectification")
REVIEW_STAGE = "review"


class EvidenceService:
    def __init__(self, store: EventStore, reference: ReferenceData, clock=utcnow):
        self.store = store
        self.ref = reference
        self.clock = clock
        self._cmd_lock = threading.RLock()

    # ============================================================== 内部工具

    def _now(self) -> str:
        return format_ts(self.clock())

    def _view(self, knowledge_at: Optional[str] = None) -> ReferenceView:
        return ReferenceView.build(self.ref, self.store.all_events(), knowledge_at)

    def _case_events(self, case_id: str) -> list[dict]:
        return self.store.events(stream=f"case:{case_id}")

    def _case_state(self, case_id: str):
        events = self._case_events(case_id)
        if not events:
            raise NotFound("案件不存在", case_id=case_id)
        return fold_case(case_id, events)

    def _sampling_event(self, record_id: str) -> dict:
        events = self.store.events(stream=f"sampling:{record_id}", types=["sampling_recorded"])
        if not events:
            raise NotFound("采样记录不存在", record_id=record_id)
        return events[0]

    def _require_open(self, state):
        if state.status != CASE_OPEN:
            raise ValidationFailed("案件已办结或已移送，不能继续提交",
                                   case_id=state.case_id, status=state.status)

    def _stage_due(self, stage: str, from_at: str) -> str:
        stage_def = self.ref.stage_def(stage)
        if stage_def is None:
            raise ValidationFailed("未知阶段", stage=stage)
        return format_ts(add_days(parse_ts(from_at), stage_def["duration_days"]))

    # ============================================================== 参考信息命令

    @_synchronized
    def record_calibration(self, *, instrument_id: str, calibrated_at: str,
                           valid_until: str, certificate_no: str, actor: str,
                           role: str, idempotency_key: Optional[str] = None) -> dict:
        """登记仪器校准记录。

        若证书覆盖某条曾被标记"校准则过期"的采样记录，且系统登记时刻
        晚于采样时刻，则产生 calibration_time_contradiction 事件——
        校准时间矛盾就此留痕，可完整重放。
        """
        if instrument_id not in self.ref.instruments:
            raise NotFound("仪器不存在", instrument_id=instrument_id)
        if parse_ts(calibrated_at) >= parse_ts(valid_until):
            raise ValidationFailed("校准有效期必须晚于校准日期")
        calibration_id = f"cal-{uuid.uuid4().hex[:12]}"
        recorded_at = self._now()
        event, receipt, dedup = self.store.append(
            stream=f"instrument:{instrument_id}",
            type="calibration_recorded",
            actor=actor, role=role,
            occurred_at=calibrated_at,
            payload={
                "calibration_id": calibration_id,
                "instrument_id": instrument_id,
                "calibrated_at": format_ts(calibrated_at),
                "valid_until": format_ts(valid_until),
                "certificate_no": certificate_no,
            },
            idempotency_key=idempotency_key,
            recorded_at=recorded_at,
        )
        receipts = [receipt]
        if not dedup:
            receipts += self._detect_calibration_contradictions(
                instrument_id=instrument_id,
                calibration_id=calibration_id,
                calibrated_at=format_ts(calibrated_at),
                valid_until=format_ts(valid_until),
                recorded_at=recorded_at,
            )
        return {"calibration_id": calibration_id, "receipts": receipts,
                "deduplicated": dedup}

    def _detect_calibration_contradictions(self, *, instrument_id, calibration_id,
                                           calibrated_at, valid_until,
                                           recorded_at) -> list[dict]:
        receipts = []
        lo, hi = parse_ts(calibrated_at), parse_ts(valid_until)
        for ev in self.store.events(types=["sampling_recorded"]):
            p = ev["payload"]
            if p["instrument_id"] != instrument_id:
                continue
            sampled_at = parse_ts(p["sampled_at"])
            if not (lo <= sampled_at <= hi):
                continue
            if parse_ts(recorded_at) <= sampled_at:
                continue  # 证书在采样前已登记，不构成迟到矛盾
            flags = [f["flag"] for f in p.get("flags", [])]
            if "instrument_calibration_expired" not in flags:
                continue
            detail = (
                f"校准证书 {calibration_id} 于 {recorded_at} 补登，"
                f"声称覆盖采样时刻 {p['sampled_at']}；"
                f"采样登记时系统内该仪器校准已过期"
            )
            related = {
                "instrument_id": instrument_id,
                "sampling_record_id": p["record_id"],
                "calibration_id": calibration_id,
                "sampled_at": p["sampled_at"],
            }
            _, receipt, _ = self.store.append(
                stream=f"sampling:{p['record_id']}",
                type="calibration_time_contradiction",
                actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                occurred_at=recorded_at,
                payload={"flag": "calibration_time_contradiction",
                         "detail": detail, "related": related},
            )
            receipts.append(receipt)
            # 同步到由该采样记录开立的案件时间线
            for case_ev in self.store.events(types=["case_opened"]):
                if case_ev["payload"].get("sampling_record_id") == p["record_id"]:
                    case_id = case_ev["case_id"]
                    _, case_receipt, _ = self.store.append(
                        stream=f"case:{case_id}", case_id=case_id,
                        type="calibration_time_contradiction",
                        actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                        occurred_at=recorded_at,
                        payload={"flag": "calibration_time_contradiction",
                                 "detail": detail, "related": related},
                    )
                    receipts.append(case_receipt)
        return receipts

    @_synchronized
    def qualification_exit(self, *, institution_id: str, qualification_id: str,
                           effective_at: str, legal_basis: str, actor: str,
                           role: str, idempotency_key: Optional[str] = None) -> dict:
        """机构资格依法退出：自 effective_at 起该资质不再被认定。"""
        inst = self.ref.institutions.get(institution_id)
        if inst is None:
            raise NotFound("机构不存在", institution_id=institution_id)
        if not any(q.qualification_id == qualification_id for q in inst["qualifications"]):
            raise NotFound("资质不存在", qualification_id=qualification_id)
        event, receipt, dedup = self.store.append(
            stream=f"institution:{institution_id}",
            type="qualification_exited",
            actor=actor, role=role,
            occurred_at=effective_at,
            payload={
                "institution_id": institution_id,
                "qualification_id": qualification_id,
                "effective_at": format_ts(effective_at),
                "legal_basis": legal_basis,
            },
            idempotency_key=idempotency_key,
        )
        receipts = [receipt]
        if not dedup:
            # 在办案件同步标记：资格退出影响案件事实基础
            for case_id in self.store.case_ids():
                state = self._case_state(case_id)
                if state.status == CASE_OPEN and state.institution_id == institution_id:
                    _, r, _ = self.store.append(
                        stream=f"case:{case_id}", case_id=case_id,
                        type="case_flagged",
                        actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                        occurred_at=effective_at,
                        payload={
                            "flag": "institution_qualification_exited",
                            "detail": f"机构资质 {qualification_id} 自 "
                                      f"{format_ts(effective_at)} 起依法退出（{legal_basis}）",
                            "related": {"institution_id": institution_id,
                                        "qualification_id": qualification_id},
                        },
                    )
                    receipts.append(r)
        return {"receipts": receipts, "deduplicated": dedup}

    # ============================================================== 采样登记

    @_synchronized
    def record_sampling(self, *, institution_id: str, operator_id: str,
                        instrument_id: str, sampled_at: str, scope: str,
                        items: list, raw_data_hash: str, actor: str, role: str,
                        record_id: Optional[str] = None,
                        idempotency_key: Optional[str] = None) -> dict:
        """登记采样记录，并按"当时系统已知资料"校验资质、授权与校准。"""
        for kind, ident, table in (("机构", institution_id, self.ref.institutions),
                                   ("操作人员", operator_id, self.ref.operators),
                                   ("仪器", instrument_id, self.ref.instruments)):
            if ident not in table:
                raise NotFound(f"{kind}不存在", id=ident)
        record_id = record_id or f"smp-{uuid.uuid4().hex[:12]}"
        now = self._now()
        view = self._view(knowledge_at=now)

        qual = view.qualification_status(institution_id, sampled_at)
        auth = view.operator_authorization(operator_id, sampled_at, scope=scope)
        inst_status = view.instrument_status(instrument_id, sampled_at)

        flags = []
        if not qual["valid"]:
            flags.append({"flag": "institution_qualification_invalid",
                          "detail": f"采样时刻机构资质无效（{sampled_at}）"})
        if not auth["authorized"]:
            flags.append({"flag": "operator_unauthorized",
                          "detail": f"操作人员未持有效授权（{scope}，{sampled_at}）"})
        if not inst_status["calibrated"]:
            flags.append({"flag": "instrument_calibration_expired",
                          "detail": f"采样时刻仪器校准不在有效期（{sampled_at}）"})

        event, receipt, dedup = self.store.append(
            stream=f"sampling:{record_id}",
            type="sampling_recorded",
            actor=actor, role=role,
            occurred_at=sampled_at,
            payload={
                "record_id": record_id,
                "institution_id": institution_id,
                "operator_id": operator_id,
                "instrument_id": instrument_id,
                "sampled_at": format_ts(sampled_at),
                "scope": scope,
                "items": list(items),
                "raw_data_hash": raw_data_hash,
                "flags": flags,
                "validation": {
                    "knowledge_at": now,
                    "qualification": qual,
                    "authorization": auth,
                    "instrument": inst_status,
                },
            },
            idempotency_key=idempotency_key,
        )
        receipts = [receipt]
        if not dedup:
            for f in flags:
                _, r, _ = self.store.append(
                    stream=f"sampling:{record_id}",
                    type="discrepancy_flagged",
                    actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                    occurred_at=sampled_at,
                    payload={"flag": f["flag"], "detail": f["detail"],
                             "related": {"record_id": record_id}},
                )
                receipts.append(r)
        return {"record_id": record_id, "flags": flags, "receipts": receipts,
                "deduplicated": dedup}

    # ============================================================== 案件开立

    @_synchronized
    def open_case(self, *, sampling_record_id: str, reason: str, actor: str,
                  role: str, case_id: Optional[str] = None,
                  idempotency_key: Optional[str] = None) -> dict:
        """依据采样记录开立整改案件，采样时的校验快照随案保存。"""
        sampling = self._sampling_event(sampling_record_id)
        sp = sampling["payload"]
        case_id = case_id or f"case-{uuid.uuid4().hex[:12]}"
        if self._case_events(case_id):
            raise Conflict("案件编号已存在", case_id=case_id)
        now = self._now()
        first_stage = self.ref.stage_deadlines[0]["stage"]
        _, receipt, dedup = self.store.append(
            stream=f"case:{case_id}", case_id=case_id,
            type="case_opened",
            actor=actor, role=role,
            occurred_at=now,
            payload={
                "case_id": case_id,
                "sampling_record_id": sampling_record_id,
                "institution_id": sp["institution_id"],
                "operator_id": sp["operator_id"],
                "instrument_id": sp["instrument_id"],
                "sampled_at": sp["sampled_at"],
                "reason": reason,
                "sampling_flags": sp.get("flags", []),
                "stage": first_stage,
                "due_at": self._stage_due(first_stage, now),
            },
            idempotency_key=idempotency_key,
        )
        receipts = [receipt]
        if not dedup:
            for f in sp.get("flags", []):
                _, r, _ = self.store.append(
                    stream=f"case:{case_id}", case_id=case_id,
                    type="discrepancy_flagged",
                    actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                    occurred_at=sp["sampled_at"],
                    payload={"flag": f["flag"], "detail": f["detail"],
                             "related": {"record_id": sampling_record_id}},
                )
                receipts.append(r)
        return {"case_id": case_id, "receipts": receipts, "deduplicated": dedup}

    # ============================================================== 自查与补录

    @_synchronized
    def submit_self_check(self, *, case_id: str, stage: str, occurred_at: str,
                          content: dict, actor: str, role: str,
                          is_backfill: bool = False,
                          backfill_reason: Optional[str] = None,
                          references: Optional[list] = None,
                          idempotency_key: Optional[str] = None) -> dict:
        """提交自查/整改材料。

        补录（occurred_at 早于阶段开始，或显式声明 is_backfill）必须：
        1. 说明补录原因；
        2. 引用至少一条已存在的旧记录（事件或采样记录编号）。
        同一阶段同一轮次重复提交相同内容按幂等处理；内容不同须走更正。
        """
        state = self._case_state(case_id)
        occurred = format_ts(occurred_at)

        # 幂等去重优先于阶段校验：重复上报（可能因网络重试迟到，
        # 此时案件已进入后续阶段）直接返回首次回执，不产生新事件。
        for sub in state.submissions:
            if sub["stage"] != stage:
                continue
            if hash_obj({"stage": stage, "round": sub["round"],
                         "content": content}) == sub["content_hash"]:
                event = self.store.get_event(sub["event_id"])
                receipt = self.store.receipt_for_seq(event["seq"])
                return {"receipts": [receipt], "deduplicated": True}

        self._require_open(state)
        cur = state.current_stage()
        if cur is None or cur.stage != stage:
            raise Conflict("当前阶段与提交不符",
                           current_stage=cur.stage if cur else None, submitted=stage)
        if stage not in SUBMITTABLE_STAGES:
            raise ValidationFailed("该阶段不接受机构提交材料", stage=stage)

        backfill = is_backfill or parse_ts(occurred) < parse_ts(cur.started_at)
        references = list(references or [])
        backfill_info = None
        if backfill:
            if not backfill_reason or not backfill_reason.strip():
                raise ValidationFailed("补录必须说明原因")
            if not references:
                raise ValidationFailed("补录必须引用至少一条旧记录")
            resolved = [self._resolve_reference(ref) for ref in references]
            backfill_info = {"reason": backfill_reason, "references": resolved}

        content_hash = hash_obj({"stage": stage, "round": cur.round, "content": content})
        for sub in state.submissions:
            if sub["stage"] == stage and sub["round"] == cur.round:
                raise Conflict("该阶段本轮已有不同内容的提交，请使用更正接口",
                               stage=stage, round=cur.round)

        now = self._now()
        _, receipt, dedup = self.store.append(
            stream=f"case:{case_id}", case_id=case_id,
            type="self_check_submitted",
            actor=actor, role=role,
            occurred_at=occurred,
            payload={
                "stage": stage, "round": cur.round,
                "content": content, "content_hash": content_hash,
                "backfill": backfill_info,
            },
            idempotency_key=idempotency_key,
        )
        if dedup:
            return {"receipts": [receipt], "deduplicated": True}
        receipts = [receipt]
        # 阶段推进：自查→整改→复核
        names = self.ref.stage_names()
        next_stage = names[names.index(stage) + 1]
        _, adv_receipt, _ = self.store.append(
            stream=f"case:{case_id}", case_id=case_id,
            type="stage_advanced",
            actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
            occurred_at=now,
            payload={"from_stage": stage, "to_stage": next_stage,
                     "round": cur.round,
                     "due_at": self._stage_due(next_stage, now)},
        )
        receipts.append(adv_receipt)
        return {"receipts": receipts, "deduplicated": False}

    def _resolve_reference(self, ref: str) -> dict:
        """把补录引用解析为已存在的旧记录，不存在则拒绝。"""
        event = self.store.get_event(ref)
        if event is not None:
            return {"ref": ref, "kind": "event", "type": event["type"],
                    "recorded_at": event["recorded_at"]}
        try:
            sampling = self._sampling_event(ref)
            return {"ref": ref, "kind": "sampling_record",
                    "recorded_at": sampling["recorded_at"]}
        except NotFound:
            pass
        raise ValidationFailed("补录引用的旧记录不存在", ref=ref)

    # ============================================================== 更正

    @_synchronized
    def correct_record(self, *, case_id: str, target_event_id: str, reason: str,
                       correction: dict, actor: str, role: str,
                       idempotency_key: Optional[str] = None) -> dict:
        """更正：以新事件修正原事件，原事实完整保留在链上。"""
        self._case_state(case_id)  # 确认案件存在
        target = self.store.get_event(target_event_id)
        if target is None:
            raise NotFound("被更正的记录不存在", target_event_id=target_event_id)
        if target["case_id"] != case_id:
            raise ValidationFailed("被更正的记录不属于该案件",
                                   target_event_id=target_event_id)
        if not reason or not reason.strip():
            raise ValidationFailed("更正必须说明原因")
        _, receipt, dedup = self.store.append(
            stream=f"case:{case_id}", case_id=case_id,
            type="record_corrected",
            actor=actor, role=role,
            occurred_at=self._now(),
            payload={
                "target_event_id": target_event_id,
                "target_type": target["type"],
                "reason": reason,
                "correction": correction,
            },
            idempotency_key=idempotency_key,
        )
        return {"receipts": [receipt], "deduplicated": dedup}

    # ============================================================== 复核

    @_synchronized
    def submit_review(self, *, case_id: str, decision: str, expected_round: int,
                      actor: str, role: str, comments: str = "",
                      idempotency_key: Optional[str] = None) -> dict:
        """复核结论。多人同时复核时以 expected_round 乐观并发：
        先到者生效，后到者收到 409 与当前轮次，可基于最新状态重提。"""
        if decision not in ("pass", "fail"):
            raise ValidationFailed("复核结论必须为 pass 或 fail")
        reviewer_roles = self.ref.permission_matrix["review_failed"]["roles"]
        if role not in reviewer_roles:
            raise PermissionDenied("该角色无权复核", role=role,
                                   allowed=reviewer_roles)
        state = self._case_state(case_id)
        self._require_open(state)
        cur = state.current_stage()
        if cur is None or cur.stage != REVIEW_STAGE:
            raise Conflict("案件当前不在复核阶段",
                           current_stage=cur.stage if cur else None)
        if state.round != expected_round:
            raise Conflict("复核轮次已变化，请基于最新状态重新提交",
                           current_round=state.round, expected_round=expected_round)

        now = self._now()
        _, receipt, dedup = self.store.append(
            stream=f"case:{case_id}", case_id=case_id,
            type="review_submitted",
            actor=actor, role=role,
            occurred_at=now,
            payload={"decision": decision, "round": state.round,
                     "comments": comments},
            idempotency_key=idempotency_key,
        )
        receipts = [receipt]
        if dedup:
            return {"receipts": receipts, "deduplicated": True}

        if decision == "pass":
            _, r, _ = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="case_closed",
                actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                occurred_at=now,
                payload={"result": "复核通过，案件办结", "round": state.round},
            )
            receipts.append(r)
        else:
            # 复核不通过：依权限矩阵由复核员作出处置，退回重新整改
            matrix = self.ref.permission_matrix["review_failed"]
            _, r, _ = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="disposition_issued",
                actor=actor, role=role,
                occurred_at=now,
                payload={"disposition_type": "review_failed",
                         "label": matrix["label"], "action": matrix["action"],
                         "reason": comments or "复核不通过"},
            )
            receipts.append(r)
            new_round = state.round + 1
            _, r, _ = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="stage_reopened",
                actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                occurred_at=now,
                payload={"stage": "rectification", "round": new_round,
                         "reason": "复核不通过，退回重新整改",
                         "due_at": self._stage_due("rectification", now)},
            )
            receipts.append(r)
        return {"receipts": receipts, "deduplicated": False}

    # ============================================================== 处置与移送

    @_synchronized
    def issue_disposition(self, *, case_id: str, disposition_type: str,
                          actor: str, role: str, reason: str,
                          action: Optional[str] = None,
                          idempotency_key: Optional[str] = None,
                          **extra) -> dict:
        """四类处置统一入口，按权限矩阵校验后推进。

        false_report / perfunctory_rectification：单事件处置。
        suspected_crime：propose → approve（两类角色分别审批、
        审批人互不相同）→ execute（登记移送回执）→ outcome（受理结果）。
        """
        matrix = self.ref.permission_matrix.get(disposition_type)
        if matrix is None:
            raise ValidationFailed("未知处置类型", disposition_type=disposition_type)
        state = self._case_state(case_id)

        if disposition_type != "suspected_crime":
            self._require_open(state)
            if role not in matrix["roles"]:
                raise PermissionDenied("该角色无权作出此处置", role=role,
                                       allowed=matrix["roles"])
            _, receipt, dedup = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="disposition_issued",
                actor=actor, role=role,
                occurred_at=self._now(),
                payload={"disposition_type": disposition_type,
                         "label": matrix["label"], "action": matrix["action"],
                         "reason": reason},
                idempotency_key=idempotency_key,
            )
            return {"receipts": [receipt], "deduplicated": dedup}

        return self._advance_transfer(case_id=case_id, state=state, matrix=matrix,
                                      action=action, actor=actor, role=role,
                                      reason=reason,
                                      idempotency_key=idempotency_key, **extra)

    def _advance_transfer(self, *, case_id, state, matrix, action, actor, role,
                          reason, idempotency_key, **extra) -> dict:
        now = self._now()
        transfer = state.transfer

        if action == "propose":
            self._require_open(state)
            if role not in matrix["propose_roles"]:
                raise PermissionDenied("该角色无权提出移送", role=role,
                                       allowed=matrix["propose_roles"])
            if transfer["proposals"] and transfer["executed"] is None:
                raise Conflict("已有在办移送提案")
            transfer_id = f"trf-{case_id}-{len(transfer['proposals']) + 1}"
            _, receipt, dedup = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="transfer_proposed",
                actor=actor, role=role, occurred_at=now,
                payload={"transfer_id": transfer_id, "reason": reason,
                         "evidence_refs": list(extra.get("evidence_refs", []))},
                idempotency_key=idempotency_key,
            )
            return {"transfer_id": transfer_id, "receipts": [receipt],
                    "deduplicated": dedup}

        proposals = transfer["proposals"]
        if not proposals:
            raise Conflict("尚无移送提案，请先 propose")
        transfer_id = proposals[-1]["transfer_id"]

        if action == "approve":
            if role not in matrix["approve_roles"]:
                raise PermissionDenied("该角色无权审批移送", role=role,
                                       allowed=matrix["approve_roles"])
            approvals = [a for a in transfer["approvals"]
                         if a["transfer_id"] == transfer_id]
            if any(a["approval_role"] == role for a in approvals):
                raise Conflict("该角色已审批，无需重复", transfer_id=transfer_id)
            if any(a["approver"] == actor for a in approvals):
                raise Conflict("审批人必须互不相同", transfer_id=transfer_id)
            if actor == proposals[-1]["proposer"]:
                raise Conflict("提案人不得兼任审批人", transfer_id=transfer_id)
            _, receipt, dedup = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="transfer_approved",
                actor=actor, role=role, occurred_at=now,
                payload={"transfer_id": transfer_id, "approval_role": role,
                         "comment": reason},
                idempotency_key=idempotency_key,
            )
            return {"transfer_id": transfer_id, "receipts": [receipt],
                    "deduplicated": dedup}

        if action == "execute":
            if role not in matrix["execute_roles"]:
                raise PermissionDenied("该角色无权执行移送", role=role,
                                       allowed=matrix["execute_roles"])
            approvals = {a["approval_role"] for a in transfer["approvals"]
                         if a["transfer_id"] == transfer_id}
            missing = [r for r in matrix["approve_roles"] if r not in approvals]
            if missing:
                raise Conflict("移送审批未完成", missing_roles=missing,
                               transfer_id=transfer_id)
            if transfer["executed"] is not None:
                raise Conflict("移送已执行", transfer_id=transfer_id)
            for field_name in ("receiving_organ", "transfer_no", "receipt_no",
                               "received_at"):
                if not extra.get(field_name):
                    raise ValidationFailed("移送执行缺少必要字段",
                                           field=field_name)
            _, receipt, dedup = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="transfer_executed",
                actor=actor, role=role, occurred_at=now,
                payload={"transfer_id": transfer_id,
                         "receiving_organ": extra["receiving_organ"],
                         "transfer_no": extra["transfer_no"],
                         "receipt_no": extra["receipt_no"],
                         "received_at": format_ts(extra["received_at"])},
                idempotency_key=idempotency_key,
            )
            receipts = [receipt]
            if not dedup:
                _, r, _ = self.store.append(
                    stream=f"case:{case_id}", case_id=case_id,
                    type="case_transferred",
                    actor=SYSTEM_ACTOR, role=SYSTEM_ROLE, occurred_at=now,
                    payload={"transfer_id": transfer_id,
                             "receiving_organ": extra["receiving_organ"]},
                )
                receipts.append(r)
            return {"transfer_id": transfer_id, "receipts": receipts,
                    "deduplicated": dedup}

        if action == "outcome":
            if transfer["executed"] is None:
                raise Conflict("移送尚未执行，不能登记受理结果")
            allowed = set(matrix["execute_roles"]) | {"法制审核员"}
            if role not in allowed:
                raise PermissionDenied("该角色无权登记受理结果", role=role,
                                       allowed=sorted(allowed))
            _, receipt, dedup = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="transfer_outcome_recorded",
                actor=actor, role=role, occurred_at=now,
                payload={"transfer_id": transfer_id,
                         "accepted": bool(extra.get("accepted")),
                         "note": reason},
                idempotency_key=idempotency_key,
            )
            return {"transfer_id": transfer_id, "receipts": [receipt],
                    "deduplicated": dedup}

        raise ValidationFailed("未知移送动作", action=action)

    # ============================================================== 逾期扫描

    @_synchronized
    def check_overdue(self, *, now: Optional[str] = None) -> dict:
        """扫描在办案件：当前阶段超过期限即留痕并给出升级建议。

        幂等：同一阶段同一轮次只产生一次 stage_overdue 事件。
        """
        moment = parse_ts(now) if now else self.clock()
        emitted = []
        for case_id in self.store.case_ids():
            state = self._case_state(case_id)
            if state.status != CASE_OPEN:
                continue
            cur = state.current_stage()
            if cur is None or cur.status != "in_progress" or cur.overdue:
                continue
            if parse_ts(cur.due_at) >= moment:
                continue
            overdue_days = (moment - parse_ts(cur.due_at)).days
            _, r1, _ = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="stage_overdue",
                actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                occurred_at=format_ts(moment),
                payload={"stage": cur.stage, "round": cur.round,
                         "due_at": cur.due_at, "overdue_days": overdue_days},
            )
            suggested = ("perfunctory_rectification"
                         if cur.stage == "rectification" else None)
            _, r2, _ = self.store.append(
                stream=f"case:{case_id}", case_id=case_id,
                type="escalation_recommended",
                actor=SYSTEM_ACTOR, role=SYSTEM_ROLE,
                occurred_at=format_ts(moment),
                payload={"stage": cur.stage, "round": cur.round,
                         "suggested_disposition": suggested,
                         "note": "阶段逾期未办结，建议催办"
                                 + ("并评估是否构成敷衍整改" if suggested else "")},
            )
            emitted.append({"case_id": case_id, "stage": cur.stage,
                            "round": cur.round, "overdue_days": overdue_days,
                            "receipts": [r1, r2]})
        return {"checked_at": format_ts(moment), "overdue": emitted}

    # ============================================================== 查询

    def timeline(self, case_id: str, *, at: Optional[str] = None) -> dict:
        events = self._case_events(case_id)
        if not events:
            raise NotFound("案件不存在", case_id=case_id)
        if at is not None:
            cutoff = parse_ts(at)
            events = [e for e in events if parse_ts(e["recorded_at"]) <= cutoff]
        return {"case_id": case_id, "at": at, "events": events}

    def summary(self, case_id: str) -> dict:
        """稳定摘要：同一事件序列必得同一字节与同一摘要哈希。"""
        events = self._case_events(case_id)
        if not events:
            raise NotFound("案件不存在", case_id=case_id)
        state = fold_case(case_id, events)
        cur = state.current_stage()
        body = {
            "case_id": case_id,
            "status": state.status,
            "round": state.round,
            "institution_id": state.institution_id,
            "sampling_record_id": state.sampling_record_id,
            "current_stage": cur.to_dict() if cur else None,
            "stages": [s.to_dict() for s in state.stages],
            "counts": {
                "events": len(events),
                "submissions": len(state.submissions),
                "reviews": len(state.reviews),
                "corrections": len(state.corrections),
                "dispositions": len(state.dispositions),
                "flags": len(state.flags),
            },
            "flags": [{"flag": f["flag"], "type": f["type"]} for f in state.flags],
            "dispositions": [{"type": d["type"], "action": d["action"]}
                             for d in state.dispositions],
            "transfer": {
                "proposals": len(state.transfer["proposals"]),
                "approvals": [a["approval_role"] for a in state.transfer["approvals"]],
                "executed": state.transfer["executed"],
                "outcome": state.transfer["outcome"],
            },
            "head_hash": events[-1]["hash"],
        }
        return {**body, "digest": hash_obj(body)}

    def replay(self, case_id: str, *, at: Optional[str] = None) -> dict:
        """完整重放：以 at（缺省为最新）之前的系统记录重建案件状态。

        校准时间矛盾之类的争议，可由监管、司法、机构三方各自重放
        同一事件序列，得到同一状态与同一摘要哈希。
        """
        events = self._case_events(case_id)
        if not events:
            raise NotFound("案件不存在", case_id=case_id)
        if at is not None:
            cutoff = parse_ts(at)
            events = [e for e in events if parse_ts(e["recorded_at"]) <= cutoff]
        state = fold_case(case_id, events)
        body = {
            "case_id": case_id,
            "at": at or (events[-1]["recorded_at"] if events else None),
            "events_considered": len(events),
            "state": state.to_dict(),
        }
        return {**body, "digest": hash_obj(body)}

    def verify_chain(self, case_id: Optional[str] = None) -> dict:
        return self.store.verify_chain(case_id=case_id)

    def verify_receipt(self, receipt_id: str) -> dict:
        return self.store.verify_receipt(receipt_id)

    # ---------------------------------------------------------- 时间化参考查询

    def qualification_at(self, institution_id: str, at: str,
                         knowledge_at: Optional[str] = None) -> dict:
        return self._view(knowledge_at).qualification_status(institution_id, at)

    def authorization_at(self, operator_id: str, at: str,
                         scope: Optional[str] = None,
                         knowledge_at: Optional[str] = None) -> dict:
        return self._view(knowledge_at).operator_authorization(operator_id, at, scope)

    def instrument_at(self, instrument_id: str, at: str,
                      knowledge_at: Optional[str] = None) -> dict:
        return self._view(knowledge_at).instrument_status(instrument_id, at)

    def sampling_record(self, record_id: str) -> dict:
        event = self._sampling_event(record_id)
        stream_events = self.store.events(stream=f"sampling:{record_id}")
        linked_cases = [
            e["case_id"] for e in self.store.events(types=["case_opened"])
            if e["payload"].get("sampling_record_id") == record_id
        ]
        return {
            "record": event["payload"],
            "recorded_at": event["recorded_at"],
            "event_id": event["event_id"],
            "follow_up_events": [
                {"type": e["type"], "recorded_at": e["recorded_at"],
                 "payload": e["payload"]}
                for e in stream_events if e["type"] != "sampling_recorded"
            ],
            "linked_cases": linked_cases,
        }

    def case_state(self, case_id: str) -> dict:
        return self._case_state(case_id).to_dict()
