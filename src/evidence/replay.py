"""执法追查视图：沿一条采样记录完整重放。

输出的时间线把"当时有效"的基准资料快照与案件事件绑定在一起：
执法人员看到的是采样时刻有效的机构资质、人员授权、仪器校准，
随后是原始数据引用、补录（引用旧记录+原因）、更正（原事实保留）、
整改决定、复核结论、跨部门移送与真实回流结果 —— 全部带事件哈希锚点。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .clock import parse_ts
from .hashing import digest
from .registry import Registry
from .service import REGISTRY_STREAM, CaseState, fold_case
from .store import Event, EventStore

# 时间线条目的展示顺序（同 occurred_at 时按案件序号）
_KIND_LABELS = {
    "case.opened": "立案建档",
    "sample.registered": "采样记录登记",
    "self_check.reported": "自查阶段上报",
    "self_check.late_entry": "自查补录（引用旧记录）",
    "fact.corrected": "事实更正（原事实保留）",
    "reporting_fault.classified": "错报/漏报认定",
    "rectification.ordered": "下达整改决定",
    "rectification.submitted": "整改提交",
    "rectification.perfunctory_flagged": "敷衍整改退回",
    "rectification.overdue_marked": "整改逾期标记",
    "rectification.verified": "复核结论",
    "rectification.rejected": "复核不通过退回",
    "transfer.requested": "涉嫌犯罪移送",
    "transfer.result_recorded": "跨部门移送结果回流",
    "case.closed": "案件关闭",
}


@dataclass
class ReplayView:
    case_id: str
    sample_ref: str
    sampled_at: str
    case_head: str
    global_head: str
    chain_valid: bool
    timeline: list[dict]
    original_facts: list[dict]
    corrections: list[dict]
    rectifications: list[dict]
    transfers: list[dict]
    calibration_context: dict
    anomalies: list[dict]
    digest: str

    def to_dict(self) -> dict:
        return {
            "format": "replay-view/1",
            "case_id": self.case_id,
            "sample_ref": self.sample_ref,
            "sampled_at": self.sampled_at,
            "case_head": self.case_head,
            "global_head": self.global_head,
            "chain_valid": self.chain_valid,
            "timeline": self.timeline,
            "original_facts": self.original_facts,
            "corrections": self.corrections,
            "rectifications": self.rectifications,
            "transfers": self.transfers,
            "calibration_context": self.calibration_context,
            "anomalies": self.anomalies,
            "digest": self.digest,
        }


def _validity_snapshot(registry: Registry, p: dict, registry_events: dict[str, Event]) -> dict:
    """采样时刻"当时有效"的三件套快照。

    以采样事件中锚定的事件哈希为准（即当时实际采用的材料），
    再与"今天从资料流重建的 as-of 投影"比对：若事后补录的资质/证书
    改变了投影结果，明确标出 contradiction，执法人员无需在不同副本间猜测。
    """
    sampled_at = p["sampled_at"]
    at = parse_ts(sampled_at)
    org_today = registry.org_at(p["org_id"], at)
    person_today = registry.person_at(p["person_id"], at, p["org_id"])
    inst = registry.instruments.get(p["instrument_id"])
    cal_today = inst.at(at) if inst else None

    anchored_cal = registry_events.get(p.get("calibration_event", ""))
    anchored_auth = registry_events.get(p.get("authorization_event", ""))
    org_tl = registry.orgs.get(p["org_id"])
    anchored_org_seg = next(
        (s for s in (org_tl.segments if org_tl else [])
         if s.event_hash == p.get("qualification_event")), None)
    person_tl = registry.persons.get(p["person_id"])
    anchored_auth_seg = next(
        (s for s in (person_tl.segments if person_tl else [])
         if s.event_hash == p.get("authorization_event")), None)

    return {
        "at": sampled_at,
        "organization": {
            "org_id": p["org_id"],
            "qualification_no": p.get("qualification_no"),
            "status_at_sample": anchored_org_seg.status if anchored_org_seg else None,
            "status_today": org_today.status if org_today else None,
            "scope": list(anchored_org_seg.scope) if anchored_org_seg else [],
            "evidence_event": p.get("qualification_event"),
            "contradiction": bool(org_today and anchored_org_seg and
                                  org_today.event_hash != anchored_org_seg.event_hash),
        },
        "operator": {
            "person_id": p["person_id"],
            "role_title": (anchored_auth_seg.role_title if anchored_auth_seg
                           else (anchored_auth.payload.get("role_title") if anchored_auth else None)),
            "authorized_at_sample": anchored_auth_seg is not None,
            "authorized_today": person_today is not None,
            "evidence_event": p.get("authorization_event"),
        },
        "instrument": {
            "instrument_id": p["instrument_id"],
            "certificate_no": p.get("certificate_no"),
            "anchored_calibration_event": p.get("calibration_event"),
            "certificate_valid_until": (anchored_cal.payload.get("valid_until")
                                        if anchored_cal else None),
            "calibrating_org": (anchored_cal.payload.get("calibrating_org")
                                if anchored_cal else None),
            "certificate_resolves_today": cal_today.certificate_no if cal_today else None,
            # 事后补录的证书把 as-of 投影指向了另一份材料
            "contradiction": (cal_today is not None and anchored_cal is not None
                              and cal_today.event_hash != anchored_cal.event_hash),
        },
    }


def build_replay(store: EventStore, registry: Registry, case_id: str,
                 *, verify: bool = True) -> ReplayView:
    events = store.case_events(case_id)
    if not events:
        raise KeyError(f"案件 {case_id} 不存在")
    chain_ok = True
    if verify:
        try:
            store.verify_case(case_id)
        except Exception:  # noqa: BLE001
            chain_ok = False

    state: CaseState = fold_case(events)
    registry_events = {e.event_hash: e for e in store.case_events(REGISTRY_STREAM)}
    timeline: list[dict] = []
    sample_payload: Optional[dict] = None
    corrections: list[dict] = []
    originals_index: dict[str, Event] = {}

    for ev in events:
        p = ev.payload
        if ev.event_type == "sample.registered":
            sample_payload = p
            originals_index[ev.event_hash] = ev
        entry = {
            "case_seq": ev.case_seq,
            "kind": ev.event_type,
            "label": _KIND_LABELS.get(ev.event_type, ev.event_type),
            "occurred_at": ev.occurred_at,
            "recorded_at": ev.recorded_at,
            "actor": ev.actor,
            "event_hash": ev.event_hash,
            "prev_case_hash": ev.prev_case_hash,
            "payload_hash": ev.payload_hash,
            "summary": _summarize(ev),
        }
        if ev.event_type == "sample.registered":
            entry["validity_snapshot"] = _validity_snapshot(registry, p, registry_events)
        elif ev.event_type == "self_check.late_entry":
            entry["references"] = {
                "ref_event_hash": p["ref_event_hash"],
                "ref_event_type": p["ref_event_type"],
                "ref_payload_hash": p["ref_payload_hash"],
                "reason": p["reason"],
            }
        elif ev.event_type == "fact.corrected":
            corrections.append({
                "event_hash": ev.event_hash,
                "target_event_hash": p["target_event_hash"],
                "field": p["field_name"],
                "before": p["before_value"],
                "after": p["after_value"],
                "reason": p["reason"],
                "corrected_at": p["corrected_at"],
            })
            entry["preserves"] = {
                "target_event_hash": p["target_event_hash"],
                "before_value": p["before_value"],
            }
        timeline.append(entry)

    # 原始事实清单：被更正字段仍以原事件为准列出，并标注已被更正
    correction_targets: dict[str, list[dict]] = {}
    for c in corrections:
        correction_targets.setdefault(c["target_event_hash"], []).append(c)
    original_facts = []
    for ev in events:
        if ev.event_type in ("sample.registered", "self_check.reported",
                             "rectification.submitted"):
            changed = correction_targets.get(ev.event_hash, [])
            original_facts.append({
                "event_hash": ev.event_hash,
                "kind": ev.event_type,
                "payload": ev.payload,
                "payload_hash": ev.payload_hash,
                "corrected_fields": [
                    {"field": c["field"], "before": c["before"], "after": c["after"],
                     "correction_event": c["event_hash"]}
                    for c in changed
                ],
                "superseded": bool(changed),
            })

    # 校准矛盾（含本案件采样时刻）
    anomalies_raw = []
    calibration_context = {}
    if sample_payload is not None:
        inst_id = sample_payload["instrument_id"]
        tl = registry.instruments.get(inst_id)
        anomalies_raw = (tl.anomalies([parse_ts(sample_payload["sampled_at"])])
                         if tl else [])
        calibration_context = _validity_snapshot(registry, sample_payload, registry_events)
    anomalies = [
        {"instrument_id": a.instrument_id, "kind": a.kind, "at": a.at,
         "detail": a.detail, "certificates": list(a.certificates)}
        for a in anomalies_raw
    ]

    rectifications = [
        {
            "order_id": o.order_id,
            "ordered_at": o.ordered_at,
            "due_at": o.due_at,
            "status": o.status,
            "overdue": o.overdue_marked,
            "items": o.items,
            "submission_count": len(o.submissions),
            "review": ({"passed": o.review["passed"], "comment": o.review["comment"],
                        "reviewer": o.review["reviewer"],
                        "event_hash": o.review["event_hash"]}
                       if o.review else None),
            "opened_event": o.opened_event,
        }
        for o in state.order_sequence()
    ]
    transfers = [
        {
            "transfer_id": t.transfer_id,
            "to_agency": t.to_agency,
            "requested_at": t.requested_at,
            "status": t.status,
            "result": ({"result": t.result["result"], "docket_no": t.result.get("docket_no"),
                        "note": t.result.get("note"), "recorded_at": t.result["recorded_at"],
                        "event_hash": t.result["event_hash"]}
                       if t.result else None),
        }
        for t in sorted(state.transfers.values(), key=lambda x: x.transfer_id)
    ]

    view_body = {
        "case_id": case_id,
        "sample_ref": sample_payload["sample_ref"] if sample_payload else None,
        "sampled_at": sample_payload["sampled_at"] if sample_payload else None,
        "case_head": store.case_head(case_id),
        "global_head": store.global_head,
        "chain_valid": chain_ok,
        "timeline": timeline,
        "original_facts": original_facts,
        "corrections": corrections,
        "rectifications": rectifications,
        "transfers": transfers,
        "calibration_context": calibration_context,
        "anomalies": anomalies,
    }
    view_body["digest"] = digest({"replay": {k: v for k, v in view_body.items()
                                             if k not in ("digest",)}})
    return ReplayView(**view_body)


def _summarize(ev: Event) -> str:
    p = ev.payload
    t = ev.event_type
    if t == "sample.registered":
        return f"采样 {p['sample_ref']}｜{p['org_id']}｜证书 {p['certificate_no']}｜原始数据 {p['raw_data_ref']}"
    if t == "self_check.reported":
        return f"阶段「{p['phase']}」自查上报：{p['content']}"
    if t == "self_check.late_entry":
        return f"补录（引用 {p['ref_event_hash'][:12]}）：{p['reason']}"
    if t == "fact.corrected":
        return f"更正 {p['field_name']}：{p['before_value']!r} → {p['after_value']!r}（{p['reason']}）"
    if t == "reporting_fault.classified":
        kind = "错报" if p["kind"] == "misreport" else "漏报"
        return f"认定{kind}：{p['detail']}"
    if t == "rectification.ordered":
        return f"下达 {p['order_id']}，{p['deadline_days']} 日内整改（截止 {p['due_at']}）"
    if t == "rectification.submitted":
        late = "，逾期提交" if p.get("late") else ""
        ok = "受理" if p.get("accepted") else "不受理"
        return f"{p['order_id']} 整改提交（{ok}{late}）"
    if t == "rectification.perfunctory_flagged":
        return f"{p['order_id']} 敷衍整改：{len(p['missing'])} 项缺措施或缺佐证"
    if t == "rectification.overdue_marked":
        return f"{p['order_id']} 已逾期 {p['overdue_seconds']} 秒"
    if t == "rectification.verified":
        return f"{p['order_id']} 复核{'通过' if p['passed'] else '不通过'}（{p['reviewer']}）：{p['comment']}"
    if t == "rectification.rejected":
        return f"{p['order_id']} 退回重改：{p['reason']}"
    if t == "transfer.requested":
        return f"移送 {p['transfer_id']} → {p['to_agency']}（随案 {p['event_count']} 条事件）"
    if t == "transfer.result_recorded":
        label = {"accepted": "已受理", "filed": "已立案", "returned": "退查"}[p["result"]]
        return f"{p['transfer_id']} 移送结果：{label} {p.get('docket_no', '')}".strip()
    if t == "case.opened":
        return f"案件建档（采样 {p.get('sample_ref')}）"
    if t == "case.closed":
        return "案件闭环关闭"
    return t
