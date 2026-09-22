"""领域错误与案件状态折叠。

fold_case 是纯函数：同一事件序列必然折叠出同一状态，
这是"稳定摘要"与"完整重放"的共同基础。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


class DomainError(Exception):
    """业务规则错误；status/code 供 API 层映射。"""

    status = 400
    code = "domain_error"

    def __init__(self, message: str, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self) -> dict:
        return {"error": self.code, "message": self.message, "details": self.details}


class NotFound(DomainError):
    status = 404
    code = "not_found"


class Conflict(DomainError):
    status = 409
    code = "conflict"


class PermissionDenied(DomainError):
    status = 403
    code = "permission_denied"


class ValidationFailed(DomainError):
    status = 422
    code = "validation_failed"


# ---------------------------------------------------------------- 状态折叠

# 阶段实例状态
STAGE_IN_PROGRESS = "in_progress"
STAGE_SUBMITTED = "submitted"
STAGE_COMPLETED = "completed"
STAGE_SUPERSEDED = "superseded"  # 被退回重开后，旧实例封存

# 案件状态
CASE_OPEN = "open"
CASE_CLOSED = "closed"
CASE_TRANSFERRED = "transferred"


@dataclass
class StageInstance:
    stage: str
    round: int
    status: str
    started_at: str
    due_at: str
    submitted_at: Optional[str] = None
    completed_at: Optional[str] = None
    overdue: bool = False

    def to_dict(self) -> dict:
        return {
            "stage": self.stage,
            "round": self.round,
            "status": self.status,
            "started_at": self.started_at,
            "due_at": self.due_at,
            "submitted_at": self.submitted_at,
            "completed_at": self.completed_at,
            "overdue": self.overdue,
        }


@dataclass
class CaseState:
    case_id: str
    status: str = CASE_OPEN
    round: int = 1
    institution_id: Optional[str] = None
    sampling_record_id: Optional[str] = None
    opened_at: Optional[str] = None
    closed_at: Optional[str] = None
    stages: list = field(default_factory=list)
    submissions: list = field(default_factory=list)
    reviews: list = field(default_factory=list)
    corrections: list = field(default_factory=list)
    dispositions: list = field(default_factory=list)
    flags: list = field(default_factory=list)
    transfer: dict = field(default_factory=lambda: {
        "proposals": [], "approvals": [], "executed": None, "outcome": None,
    })

    def current_stage(self) -> Optional[StageInstance]:
        for st in reversed(self.stages):
            if st.status in (STAGE_IN_PROGRESS, STAGE_SUBMITTED):
                return st
        return None

    def to_dict(self) -> dict:
        return {
            "case_id": self.case_id,
            "status": self.status,
            "round": self.round,
            "institution_id": self.institution_id,
            "sampling_record_id": self.sampling_record_id,
            "opened_at": self.opened_at,
            "closed_at": self.closed_at,
            "stages": [s.to_dict() for s in self.stages],
            "submissions": self.submissions,
            "reviews": self.reviews,
            "corrections": self.corrections,
            "dispositions": self.dispositions,
            "flags": self.flags,
            "transfer": self.transfer,
        }


def fold_case(case_id: str, events: list) -> CaseState:
    """把案件相关事件按序折叠为当前状态。"""
    state = CaseState(case_id=case_id)
    for ev in events:
        t = ev["type"]
        p = ev["payload"]
        if t == "case_opened":
            state.status = CASE_OPEN
            state.institution_id = p.get("institution_id")
            state.sampling_record_id = p.get("sampling_record_id")
            state.opened_at = ev["recorded_at"]
            state.stages.append(StageInstance(
                stage=p["stage"], round=state.round, status=STAGE_IN_PROGRESS,
                started_at=ev["recorded_at"], due_at=p["due_at"],
            ))
        elif t == "self_check_submitted":
            cur = state.current_stage()
            if cur and cur.stage == p["stage"] and cur.round == p["round"]:
                cur.status = STAGE_SUBMITTED
                cur.submitted_at = ev["recorded_at"]
            state.submissions.append({
                "stage": p["stage"], "round": p["round"],
                "content_hash": p["content_hash"],
                "occurred_at": ev["occurred_at"],
                "recorded_at": ev["recorded_at"],
                "backfill": p.get("backfill"),
                "event_id": ev["event_id"],
            })
        elif t == "stage_advanced":
            cur = state.current_stage()
            if cur and cur.stage == p["from_stage"]:
                cur.status = STAGE_COMPLETED
                cur.completed_at = ev["recorded_at"]
            state.stages.append(StageInstance(
                stage=p["to_stage"], round=state.round, status=STAGE_IN_PROGRESS,
                started_at=ev["recorded_at"], due_at=p["due_at"],
            ))
        elif t == "review_submitted":
            state.reviews.append({
                "decision": p["decision"], "round": p["round"],
                "comments": p.get("comments", ""),
                "reviewer": ev["actor"], "recorded_at": ev["recorded_at"],
                "event_id": ev["event_id"],
            })
        elif t == "stage_reopened":
            cur = state.current_stage()
            if cur:
                cur.status = STAGE_SUPERSEDED
                cur.completed_at = ev["recorded_at"]
            state.round = p["round"]
            state.stages.append(StageInstance(
                stage=p["stage"], round=p["round"], status=STAGE_IN_PROGRESS,
                started_at=ev["recorded_at"], due_at=p["due_at"],
            ))
        elif t == "disposition_issued":
            state.dispositions.append({
                "type": p["disposition_type"], "label": p["label"],
                "action": p["action"], "reason": p.get("reason", ""),
                "actor": ev["actor"], "role": ev["role"],
                "recorded_at": ev["recorded_at"], "event_id": ev["event_id"],
            })
        elif t == "transfer_proposed":
            state.transfer["proposals"].append({
                "transfer_id": p["transfer_id"], "reason": p.get("reason", ""),
                "proposer": ev["actor"], "recorded_at": ev["recorded_at"],
            })
        elif t == "transfer_approved":
            state.transfer["approvals"].append({
                "transfer_id": p["transfer_id"], "approval_role": p["approval_role"],
                "approver": ev["actor"], "recorded_at": ev["recorded_at"],
            })
        elif t == "transfer_executed":
            state.transfer["executed"] = {
                "transfer_id": p["transfer_id"],
                "receiving_organ": p["receiving_organ"],
                "transfer_no": p["transfer_no"],
                "receipt_no": p["receipt_no"],
                "received_at": p["received_at"],
                "recorded_at": ev["recorded_at"],
            }
        elif t == "transfer_outcome_recorded":
            state.transfer["outcome"] = {
                "transfer_id": p["transfer_id"],
                "accepted": p["accepted"],
                "note": p.get("note", ""),
                "recorded_at": ev["recorded_at"],
            }
            if not p["accepted"] and state.status == CASE_TRANSFERRED:
                # 移送被退回：案件恢复在办，继续整改流程
                state.status = CASE_OPEN
        elif t == "record_corrected":
            state.corrections.append({
                "target_event_id": p["target_event_id"],
                "reason": p["reason"], "correction": p["correction"],
                "actor": ev["actor"], "recorded_at": ev["recorded_at"],
                "event_id": ev["event_id"],
            })
        elif t in ("discrepancy_flagged", "case_flagged", "calibration_time_contradiction"):
            state.flags.append({
                "type": t, "flag": p.get("flag", t),
                "detail": p.get("detail", ""),
                "related": p.get("related", {}),
                "recorded_at": ev["recorded_at"], "event_id": ev["event_id"],
            })
        elif t == "stage_overdue":
            for st in state.stages:
                if st.stage == p["stage"] and st.round == p["round"]:
                    st.overdue = True
        elif t == "case_closed":
            cur = state.current_stage()
            if cur:
                cur.status = STAGE_COMPLETED
                cur.completed_at = ev["recorded_at"]
            state.status = CASE_CLOSED
            state.closed_at = ev["recorded_at"]
        elif t == "case_transferred":
            state.status = CASE_TRANSFERRED
            state.closed_at = ev["recorded_at"]
        # escalation_recommended 等提示性事件不改变状态，仅留在时间线中
    return state
