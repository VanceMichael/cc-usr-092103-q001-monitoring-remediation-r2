"""时间化参考资料：机构资质、人员授权、仪器校准的"当时有效"判定。

基础资料来自仓库 fixtures（generated_at 视为系统已知起点），
运行期的校准记录、资格退出以事件形式叠加。所有查询都支持两个
时间维度：

- at：业务时刻——"采样那一刻仪器是否在校准有效期内"；
- knowledge_at：系统认知时刻——"以当时系统掌握的资料看，
  该仪器是否有效"。迟到补登的校准证书在 knowledge_at 之前
  不可见，这正是校准时间矛盾可以被完整重放的关键。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from .timeutil import in_window, parse_ts


@dataclass(frozen=True)
class Qualification:
    qualification_id: str
    type: str
    certificate_no: str
    valid_from: str
    valid_to: Optional[str]
    status: str


@dataclass(frozen=True)
class Authorization:
    authorization_id: str
    scope: str
    valid_from: str
    valid_to: Optional[str]


@dataclass(frozen=True)
class Calibration:
    calibration_id: str
    calibrated_at: str
    valid_until: str
    certificate_no: str
    known_at: str  # 系统何时得知该证书（基础资料=generated_at，补登=事件 recorded_at）
    source: str    # "fixture" 或事件 event_id


@dataclass
class ReferenceData:
    """从领域资料解析出的静态参考数据。"""

    generated_at: str
    institutions: dict          # institution_id -> {..., qualifications: [Qualification]}
    operators: dict             # operator_id -> {..., authorizations: [Authorization]}
    instruments: dict           # instrument_id -> {..., calibrations: [Calibration]}
    stage_deadlines: list       # [{stage, name, duration_days}]，有序
    permission_matrix: dict

    @classmethod
    def from_context(cls, ctx: dict) -> "ReferenceData":
        institutions = {}
        for inst in ctx["institutions"]:
            quals = [
                Qualification(
                    qualification_id=q["qualification_id"],
                    type=q["type"],
                    certificate_no=q.get("certificate_no", ""),
                    valid_from=q["valid_from"],
                    valid_to=q.get("valid_to"),
                    status=q["status"],
                )
                for q in inst["qualifications"]
            ]
            institutions[inst["institution_id"]] = {
                "institution_id": inst["institution_id"],
                "name": inst["name"],
                "credit_code": inst.get("credit_code", ""),
                "qualifications": quals,
            }
        operators = {}
        for op in ctx["operators"]:
            auths = [
                Authorization(
                    authorization_id=a["authorization_id"],
                    scope=a["scope"],
                    valid_from=a["valid_from"],
                    valid_to=a.get("valid_to"),
                )
                for a in op["authorizations"]
            ]
            operators[op["operator_id"]] = {
                "operator_id": op["operator_id"],
                "name": op["name"],
                "institution_id": op["institution_id"],
                "authorizations": auths,
            }
        instruments = {}
        for ins in ctx["instruments"]:
            cals = [
                Calibration(
                    calibration_id=c["calibration_id"],
                    calibrated_at=c["calibrated_at"],
                    valid_until=c["valid_until"],
                    certificate_no=c["certificate_no"],
                    known_at=ctx["generated_at"],
                    source="fixture",
                )
                for c in ins.get("calibrations", [])
            ]
            instruments[ins["instrument_id"]] = {
                "instrument_id": ins["instrument_id"],
                "name": ins["name"],
                "institution_id": ins["institution_id"],
                "calibrations": cals,
            }
        return cls(
            generated_at=ctx["generated_at"],
            institutions=institutions,
            operators=operators,
            instruments=instruments,
            stage_deadlines=list(ctx["stage_deadlines"]),
            permission_matrix=dict(ctx["permission_matrix"]),
        )

    def stage_names(self) -> list[str]:
        return [s["stage"] for s in self.stage_deadlines]

    def stage_def(self, stage: str) -> Optional[dict]:
        for s in self.stage_deadlines:
            if s["stage"] == stage:
                return s
        return None


@dataclass
class ReferenceView:
    """基础资料 + 事件叠加后的时间化视图（对某一事件序列求值）。"""

    base: ReferenceData
    extra_calibrations: dict = field(default_factory=dict)   # instrument_id -> [Calibration]
    qualification_exits: list = field(default_factory=list)  # [{qualification_id, effective_at, known_at, ...}]

    @classmethod
    def build(cls, base: ReferenceData, events: list[dict],
              knowledge_at: Optional[str] = None) -> "ReferenceView":
        """按事件流构建视图；knowledge_at 之后的系统认知不可见。"""
        view = cls(base=base)
        cutoff = parse_ts(knowledge_at) if knowledge_at else None
        for ev in events:
            if cutoff is not None and parse_ts(ev["recorded_at"]) > cutoff:
                continue
            if ev["type"] == "calibration_recorded":
                p = ev["payload"]
                cal = Calibration(
                    calibration_id=p["calibration_id"],
                    calibrated_at=p["calibrated_at"],
                    valid_until=p["valid_until"],
                    certificate_no=p["certificate_no"],
                    known_at=ev["recorded_at"],
                    source=ev["event_id"],
                )
                view.extra_calibrations.setdefault(p["instrument_id"], []).append(cal)
            elif ev["type"] == "qualification_exited":
                p = ev["payload"]
                view.qualification_exits.append({
                    "institution_id": p["institution_id"],
                    "qualification_id": p["qualification_id"],
                    "effective_at": p["effective_at"],
                    "legal_basis": p.get("legal_basis", ""),
                    "known_at": ev["recorded_at"],
                })
        return view

    # ------------------------------------------------------------- 机构资质

    def qualification_status(self, institution_id: str, at: str,
                             qualification_id: Optional[str] = None) -> dict:
        """某时刻机构资质是否有效。资格退出按 effective_at 截断时间窗。"""
        inst = self.base.institutions.get(institution_id)
        if inst is None:
            return {"institution_id": institution_id, "at": at, "valid": False,
                    "reason": "机构不存在"}
        moment = parse_ts(at)
        results = []
        for q in inst["qualifications"]:
            if qualification_id and q.qualification_id != qualification_id:
                continue
            exit_ev = next(
                (e for e in self.qualification_exits
                 if e["qualification_id"] == q.qualification_id
                 and parse_ts(e["effective_at"]) <= moment),
                None,
            )
            in_window_ = in_window(moment, q.valid_from, q.valid_to)
            valid = in_window_ and q.status == "有效" and exit_ev is None
            results.append({
                "qualification_id": q.qualification_id,
                "type": q.type,
                "certificate_no": q.certificate_no,
                "valid": valid,
                "in_window": in_window_,
                "register_status": q.status,
                "exited": exit_ev is not None,
                "exit_effective_at": exit_ev["effective_at"] if exit_ev else None,
            })
        return {
            "institution_id": institution_id,
            "at": at,
            "valid": any(r["valid"] for r in results),
            "qualifications": results,
        }

    # ------------------------------------------------------------- 人员授权

    def operator_authorization(self, operator_id: str, at: str,
                               scope: Optional[str] = None) -> dict:
        op = self.base.operators.get(operator_id)
        if op is None:
            return {"operator_id": operator_id, "at": at, "authorized": False,
                    "reason": "操作人员不存在"}
        moment = parse_ts(at)
        grants = []
        for a in op["authorizations"]:
            if scope is not None and a.scope != scope:
                continue
            grants.append({
                "authorization_id": a.authorization_id,
                "scope": a.scope,
                "valid": in_window(moment, a.valid_from, a.valid_to),
            })
        return {
            "operator_id": operator_id,
            "at": at,
            "authorized": any(g["valid"] for g in grants),
            "authorizations": grants,
        }

    # ------------------------------------------------------------- 仪器校准

    def calibrations_of(self, instrument_id: str) -> list[Calibration]:
        ins = self.base.instruments.get(instrument_id)
        base_cals = list(ins["calibrations"]) if ins else []
        return base_cals + list(self.extra_calibrations.get(instrument_id, []))

    def instrument_status(self, instrument_id: str, at: str) -> dict:
        """某时刻仪器校准状态，并列出判定所依据的证书。"""
        ins = self.base.instruments.get(instrument_id)
        if ins is None:
            return {"instrument_id": instrument_id, "at": at, "calibrated": False,
                    "reason": "仪器不存在"}
        moment = parse_ts(at)
        covering = [
            c for c in self.calibrations_of(instrument_id)
            if parse_ts(c.calibrated_at) <= moment <= parse_ts(c.valid_until)
        ]
        covering.sort(key=lambda c: c.calibrated_at)
        active = covering[-1] if covering else None
        return {
            "instrument_id": instrument_id,
            "at": at,
            "calibrated": active is not None,
            "active_calibration": _calibration_dict(active) if active else None,
            "known_calibrations": [
                _calibration_dict(c) for c in self.calibrations_of(instrument_id)
            ],
        }


def _calibration_dict(c: Calibration) -> dict:
    return {
        "calibration_id": c.calibration_id,
        "calibrated_at": c.calibrated_at,
        "valid_until": c.valid_until,
        "certificate_no": c.certificate_no,
        "known_at": c.known_at,
        "source": c.source,
    }
