"""时间化基准资料投影。

机构资质、人员授权、仪器校准、阶段期限与处置规则全部以事件入链，
投影时按"业务发生时间"切出任意时点的有效状态：

* 登记一条采样记录时，校验的是 **采样时刻** 机构是否在资质有效期内、
  操作人员是否已授权、仪器校准是否覆盖该时刻；
* 事后补录的校准证书不会"改写历史"，只会作为新事实追加，
  并由 :meth:`InstrumentTimeline.anomalies` 标出回溯/重叠矛盾。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .clock import fmt_ts, parse_ts
from .hashing import digest

INF = datetime(9999, 12, 31, tzinfo=parse_ts("2000-01-01T00:00:00Z").tzinfo)


def _or_inf(value: Optional[str]) -> datetime:
    return parse_ts(value) if value else INF


# ---------------------------------------------------------------- 事实条目


@dataclass(frozen=True)
class OrgSegment:
    valid_from: datetime
    valid_to: datetime
    status: str                       # active / suspended / withdrawn
    qualification_no: str
    scope: tuple[str, ...]
    reason: str
    event_hash: str
    recorded_at: datetime


@dataclass(frozen=True)
class AuthSegment:
    valid_from: datetime
    valid_to: datetime
    org_id: str
    role_title: str
    revoked: bool
    reason: str
    event_hash: str
    recorded_at: datetime


@dataclass(frozen=True)
class Calibration:
    calibrated_at: datetime
    valid_from: datetime
    valid_until: datetime
    certificate_no: str
    calibrating_org: str
    recorded_at: datetime
    event_hash: str
    note: str = ""
    supersedes: tuple[str, ...] = ()

    def covers(self, at: datetime) -> bool:
        return self.valid_from <= at < self.valid_until


@dataclass(frozen=True)
class PhaseDeadline:
    phase: str
    days: int
    description: str
    event_hash: str


@dataclass(frozen=True)
class EnforcementRule:
    fault_type: str
    action: str
    authority_role: str
    description: str
    event_hash: str


# ---------------------------------------------------------------- 异常


@dataclass(frozen=True)
class CalibrationAnomaly:
    instrument_id: str
    kind: str            # backdated / overlap / expired_at_sample
    at: str
    detail: str
    certificates: tuple[str, ...]


# ---------------------------------------------------------------- 单实体时间线


@dataclass
class OrgTimeline:
    org_id: str
    name: str = ""
    segments: list[OrgSegment] = field(default_factory=list)

    def at(self, when: datetime, known_at: Optional[datetime] = None) -> Optional[OrgSegment]:
        """返回采样/处置时刻有效的一段。

        ``known_at`` 限定"认知时点"：只有收录时间不晚于该时点的材料可见，
        事后补录的资质段不会穿越回历史时刻被当作"当时采用"的材料。
        同一时刻多段时取最迟收录的一段。
        """
        hits = [
            s for s in self.segments
            if s.valid_from <= when < s.valid_to
            and (known_at is None or s.recorded_at <= known_at)
        ]
        if not hits:
            return None
        return sorted(hits, key=lambda s: s.recorded_at)[-1]


@dataclass
class PersonTimeline:
    person_id: str
    name: str = ""
    segments: list[AuthSegment] = field(default_factory=list)

    def at(self, when: datetime, org_id: Optional[str] = None,
           known_at: Optional[datetime] = None) -> Optional[AuthSegment]:
        hits = [
            s for s in self.segments
            if s.valid_from <= when < s.valid_to and not s.revoked
            and (org_id is None or s.org_id == org_id)
            and (known_at is None or s.recorded_at <= known_at)
        ]
        return sorted(hits, key=lambda s: s.valid_from)[-1] if hits else None


@dataclass
class InstrumentTimeline:
    instrument_id: str
    name: str = ""
    org_id: str = ""
    serial_no: str = ""
    retired_at: Optional[datetime] = None
    calibrations: list[Calibration] = field(default_factory=list)

    def at(self, when: datetime, known_at: Optional[datetime] = None) -> Optional[Calibration]:
        """时刻有效的校准。

        多个证书同时覆盖时，以校准时间较晚者为准；``known_at`` 限定认知时点，
        使事后补录、追溯生效的证书无法冒充采样当时采用的证书。
        """
        hits = [c for c in self.calibrations
                if c.covers(when) and (known_at is None or c.recorded_at <= known_at)]
        if not hits:
            return None
        return sorted(hits, key=lambda c: (c.calibrated_at, c.recorded_at))[-1]

    def anomalies(self, sample_times: Optional[list[datetime]] = None,
                  recording_grace_hours: int = 7 * 24) -> list[CalibrationAnomaly]:
        """检测校准时间矛盾。

        * ``overlap``：两张证书声称的有效期重叠（资质状态歧义）；
        * ``backdated``：收录时间显著晚于校准起始时间（超过宽限期，默认 7 日），
          证书追溯覆盖收录之前的时间段 —— 典型的"事后补一张证书盖住采样时刻"；
          宽限期内的正常归档延迟不算异常；
        * ``expired_at_sample``：给定采样时刻没有任何有效校准覆盖（或仪器已停用）。
        """
        from datetime import timedelta

        grace = timedelta(hours=recording_grace_hours)
        out: list[CalibrationAnomaly] = []
        cal = sorted(self.calibrations, key=lambda c: c.recorded_at)
        for i, later in enumerate(cal):
            for earlier in cal[:i]:
                # 新证书明确声明替代旧证书的，重叠属正常换证（旧证自新证生效日起停用）
                if earlier.certificate_no in later.supersedes:
                    continue
                overlap_start = max(later.valid_from, earlier.valid_from)
                overlap_end = min(later.valid_until, earlier.valid_until)
                if overlap_start < overlap_end:
                    out.append(CalibrationAnomaly(
                        self.instrument_id, "overlap", fmt_ts(overlap_start),
                        f"证书 {later.certificate_no} 与 {earlier.certificate_no} "
                        f"有效期重叠 {fmt_ts(overlap_start)}~{fmt_ts(overlap_end)}",
                        (earlier.certificate_no, later.certificate_no),
                    ))
            # 超过宽限期才收录、却声称早已生效：追溯录入
            if later.recorded_at - later.valid_from > grace:
                out.append(CalibrationAnomaly(
                    self.instrument_id, "backdated", fmt_ts(later.recorded_at),
                    f"证书 {later.certificate_no} 声称 {fmt_ts(later.valid_from)} 起生效，"
                    f"但 {fmt_ts(later.recorded_at)} 才收录（迟 "
                    f"{(later.recorded_at - later.valid_from).days} 日，超过 {recording_grace_hours // 24} 日宽限），"
                    f"属事后追溯录入",
                    (later.certificate_no,),
                ))
        for at in sample_times or []:
            if self.retired_at is not None and at >= self.retired_at:
                out.append(CalibrationAnomaly(
                    self.instrument_id, "expired_at_sample", fmt_ts(at),
                    f"仪器已于 {fmt_ts(self.retired_at)} 停用，采样发生在停用之后", (),
                ))
            elif self.at(at) is None:
                certs = tuple(sorted({c.certificate_no for c in cal}))
                out.append(CalibrationAnomaly(
                    self.instrument_id, "expired_at_sample", fmt_ts(at),
                    f"采样时刻 {fmt_ts(at)} 无有效校准证书覆盖", certs,
                ))
        return out


# ---------------------------------------------------------------- 投影


class Registry:
    """从事件流重建的基准资料投影。"""

    def __init__(self) -> None:
        self.orgs: dict[str, OrgTimeline] = {}
        self.persons: dict[str, PersonTimeline] = {}
        self.instruments: dict[str, InstrumentTimeline] = {}
        self.deadlines: dict[str, PhaseDeadline] = {}
        self.rules: dict[str, EnforcementRule] = {}

    def apply(self, event_type: str, payload: dict, event_hash: str) -> None:
        handler = getattr(self, f"_on_{event_type.replace('.', '_')}", None)
        if handler:
            handler(payload, event_hash, parse_ts(payload["recorded_at"]))

    # ----- 机构 -----

    def _on_registry_org_registered(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.orgs.setdefault(p["org_id"], OrgTimeline(p["org_id"]))
        tl.name = p["name"]
        tl.segments.append(OrgSegment(
            parse_ts(p["valid_from"]), _or_inf(p.get("valid_to")),
            "active", p["qualification_no"], tuple(p.get("scope", [])),
            p.get("reason", "资质登记"), h, rec,
        ))

    def _on_registry_org_renewed(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.orgs[p["org_id"]]
        tl.segments.append(OrgSegment(
            parse_ts(p["valid_from"]), _or_inf(p.get("valid_to")),
            "active", p["qualification_no"], tuple(p.get("scope", tl.segments[-1].scope)),
            p.get("reason", "资质延续"), h, rec,
        ))

    def _on_registry_org_suspended(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.orgs[p["org_id"]]
        last = tl.segments[-1]
        tl.segments.append(OrgSegment(
            parse_ts(p["at"]), _or_inf(p.get("resume_at")),
            "suspended", last.qualification_no, last.scope, p["reason"], h, rec,
        ))

    def _on_registry_org_withdrawn(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.orgs[p["org_id"]]
        last = tl.segments[-1]
        tl.segments.append(OrgSegment(
            parse_ts(p["at"]), INF, "withdrawn",
            last.qualification_no, last.scope, p["reason"], h, rec,
        ))

    # ----- 人员 -----

    def _on_registry_person_authorized(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.persons.setdefault(p["person_id"], PersonTimeline(p["person_id"]))
        tl.name = p["name"]
        tl.segments.append(AuthSegment(
            parse_ts(p["valid_from"]), _or_inf(p.get("valid_to")),
            p["org_id"], p.get("role_title", "操作人员"), False,
            p.get("reason", "岗位授权"), h, rec,
        ))

    def _on_registry_person_auth_revoked(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.persons[p["person_id"]]
        last = tl.segments[-1]
        tl.segments.append(AuthSegment(
            last.valid_from, parse_ts(p["at"]), last.org_id, last.role_title,
            True, p["reason"], h, rec,
        ))

    # ----- 仪器 -----

    def _on_registry_instrument_registered(self, p: dict, h: str, rec: datetime) -> None:
        self.instruments[p["instrument_id"]] = InstrumentTimeline(
            p["instrument_id"], p["name"], p["org_id"], p.get("serial_no", ""),
        )

    def _on_registry_instrument_calibrated(self, p: dict, h: str, rec: datetime) -> None:
        tl = self.instruments[p["instrument_id"]]
        tl.calibrations.append(Calibration(
            parse_ts(p["calibrated_at"]),
            parse_ts(p.get("valid_from", p["calibrated_at"])),
            parse_ts(p["valid_until"]),
            p["certificate_no"], p.get("calibrating_org", ""),
            parse_ts(p["recorded_at"]), h, p.get("note", ""),
            tuple(p.get("supersedes", [])),
        ))

    def _on_registry_instrument_retired(self, p: dict, h: str, rec: datetime) -> None:
        self.instruments[p["instrument_id"]].retired_at = parse_ts(p["at"])

    # ----- 期限与规则 -----

    def _on_deadline_phase_set(self, p: dict, h: str, rec: datetime) -> None:
        self.deadlines[p["phase"]] = PhaseDeadline(
            p["phase"], int(p["days"]), p.get("description", ""), h,
        )

    def _on_rule_enforcement_set(self, p: dict, h: str, rec: datetime) -> None:
        self.rules[p["fault_type"]] = EnforcementRule(
            p["fault_type"], p["action"], p["authority_role"],
            p.get("description", ""), h,
        )

    # ----- 查询 -----

    def org_at(self, org_id: str, at: datetime,
               known_at: Optional[datetime] = None) -> Optional[OrgSegment]:
        tl = self.orgs.get(org_id)
        return tl.at(at, known_at) if tl else None

    def person_at(self, person_id: str, at: datetime, org_id: Optional[str] = None,
                  known_at: Optional[datetime] = None):
        tl = self.persons.get(person_id)
        return tl.at(at, org_id, known_at) if tl else None

    def instrument_at(self, instrument_id: str, at: datetime,
                      known_at: Optional[datetime] = None) -> Optional[Calibration]:
        tl = self.instruments.get(instrument_id)
        return tl.at(at, known_at) if tl else None

    def deadline_days(self, phase: str) -> Optional[int]:
        d = self.deadlines.get(phase)
        return d.days if d else None

    def all_calibration_anomalies(self, sample_times: dict[str, list[datetime]]) -> list[CalibrationAnomaly]:
        out: list[CalibrationAnomaly] = []
        for inst_id, tl in self.instruments.items():
            out.extend(tl.anomalies(sample_times.get(inst_id, [])))
        return sorted(out, key=lambda a: (a.instrument_id, a.at, a.kind))

    def summary(self) -> dict:
        """基准资料稳定摘要（按标识排序，不含收录时间，便于跨副本比对）。"""
        return {
            "orgs": sorted(
                [
                    {
                        "org_id": oid,
                        "qualification_no": tl.segments[-1].qualification_no,
                        "latest_status": tl.segments[-1].status,
                    }
                    for oid, tl in self.orgs.items()
                ],
                key=lambda x: x["org_id"],
            ),
            "instruments": sorted(
                [
                    {
                        "instrument_id": iid,
                        "latest_certificate": (
                            sorted(tl.calibrations, key=lambda c: c.calibrated_at)[-1].certificate_no
                            if tl.calibrations else None
                        ),
                    }
                    for iid, tl in self.instruments.items()
                ],
                key=lambda x: x["instrument_id"],
            ),
            "deadlines": {k: v.days for k, v in sorted(self.deadlines.items())},
            "rules": sorted(self.rules),
        }

    def digest(self) -> str:
        return digest({"registry": self.summary()})
