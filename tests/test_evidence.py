"""整改取证协同后台的端到端测试。

覆盖：时间化参考查询、采样校验留痕、补录规则、更正留痕、
重复上报幂等、多人同时复核、逾期升级、资格退出、
四类处置权限、涉嫌犯罪移送双审批、校准时间矛盾完整重放、
篡改检测与 HTTP 冒烟。
"""

import json
import threading
import urllib.request
from pathlib import Path

import pytest

from src.context import load_full_context
from src.evidence.api import run_server
from src.evidence.domain import Conflict, NotFound, PermissionDenied, ValidationFailed
from src.evidence.reference import ReferenceData
from src.evidence.service import EvidenceService
from src.evidence.store import EventStore
from src.evidence.timeutil import parse_ts

CONTEXT_PATH = Path(__file__).resolve().parent.parent / "fixtures" / "context.json"


class FakeClock:
    """可控时钟：recorded_at 由它给出，测试因此确定。"""

    def __init__(self, start="2026-03-01T00:00:00Z"):
        self._t = parse_ts(start)

    def __call__(self):
        return self._t

    def set(self, ts):
        self._t = parse_ts(ts)

    def advance(self, days=0, hours=0):
        from datetime import timedelta

        self._t = self._t + timedelta(days=days, hours=hours)


@pytest.fixture()
def clock():
    return FakeClock()


@pytest.fixture()
def svc(clock):
    ctx = load_full_context(CONTEXT_PATH)
    store = EventStore(":memory:", clock=clock)
    return EvidenceService(store, ReferenceData.from_context(ctx), clock=clock)


def make_sampling(svc, record_id="smp-1", **overrides):
    params = dict(
        institution_id="inst-huajian-001", operator_id="op-zhang-001",
        instrument_id="ins-aa-002", sampled_at="2026-02-10T08:00:00Z",
        scope="水和废水采样", items=["pH"], raw_data_hash="sha256:raw-1",
        actor="张某(虚构)", role="机构人员", record_id=record_id,
    )
    params.update(overrides)
    return svc.record_sampling(**params)


def make_case(svc, case_id="case-1", record_id="smp-1", **sampling_overrides):
    make_sampling(svc, record_id=record_id, **sampling_overrides)
    return svc.open_case(sampling_record_id=record_id, reason="例行检查发现问题",
                         actor="王执法", role="执法员", case_id=case_id)


def advance_to_review(svc, case_id="case-1"):
    svc.submit_self_check(case_id=case_id, stage="self_check",
                          occurred_at="2026-03-02T09:00:00Z",
                          content={"report": "自查报告"}, actor="张某", role="机构人员")
    svc.submit_self_check(case_id=case_id, stage="rectification",
                          occurred_at="2026-03-05T09:00:00Z",
                          content={"plan": "整改完成"}, actor="张某", role="机构人员")


# ---------------------------------------------------------------- 时间化参考查询

class TestTemporalReference:
    def test_qualification_as_of(self, svc):
        ok = svc.qualification_at("inst-huajian-001", "2026-02-01T00:00:00Z")
        assert ok["valid"] is True
        early = svc.qualification_at("inst-huajian-001", "2020-01-01T00:00:00Z")
        assert early["valid"] is False  # 资质尚未生效

    def test_operator_authorization_window(self, svc):
        valid = svc.authorization_at("op-li-002", "2025-06-01T00:00:00Z",
                                     scope="水和废水采样")
        assert valid["authorized"] is True
        expired = svc.authorization_at("op-li-002", "2026-06-01T00:00:00Z",
                                       scope="水和废水采样")
        assert expired["authorized"] is False  # 授权已过期

    def test_instrument_calibration_window(self, svc):
        before = svc.instrument_at("ins-gc-001", "2026-02-01T00:00:00Z")
        assert before["calibrated"] is True
        after = svc.instrument_at("ins-gc-001", "2026-03-15T00:00:00Z")
        assert after["calibrated"] is False  # 2026-03-01 后已过有效期

    def test_unknown_entities(self, svc):
        assert svc.qualification_at("inst-none", "2026-01-01T00:00:00Z")["valid"] is False
        assert svc.instrument_at("ins-none", "2026-01-01T00:00:00Z")["calibrated"] is False


# ---------------------------------------------------------------- 采样登记校验

class TestSamplingValidation:
    def test_valid_sampling_has_no_flags(self, svc):
        result = make_sampling(svc)
        assert result["flags"] == []

    def test_expired_instrument_flagged(self, svc):
        result = make_sampling(svc, instrument_id="ins-gc-001",
                               sampled_at="2026-03-15T08:00:00Z")
        flags = [f["flag"] for f in result["flags"]]
        assert "instrument_calibration_expired" in flags

    def test_expired_authorization_flagged(self, svc):
        result = make_sampling(svc, operator_id="op-li-002",
                               institution_id="inst-lanzhou-002")
        flags = [f["flag"] for f in result["flags"]]
        assert "operator_unauthorized" in flags

    def test_unknown_instrument_rejected(self, svc):
        with pytest.raises(NotFound):
            make_sampling(svc, instrument_id="ins-ghost")


# ---------------------------------------------------------------- 补录与更正

class TestBackfillAndCorrection:
    def test_backfill_requires_reason(self, svc):
        make_case(svc)
        with pytest.raises(ValidationFailed, match="原因"):
            svc.submit_self_check(case_id="case-1", stage="self_check",
                                  occurred_at="2026-02-20T09:00:00Z",
                                  content={"x": 1}, actor="张某", role="机构人员",
                                  is_backfill=True, references=["smp-1"])

    def test_backfill_requires_existing_references(self, svc):
        make_case(svc)
        with pytest.raises(ValidationFailed, match="引用"):
            svc.submit_self_check(case_id="case-1", stage="self_check",
                                  occurred_at="2026-02-20T09:00:00Z",
                                  content={"x": 1}, actor="张某", role="机构人员",
                                  is_backfill=True, backfill_reason="系统故障")
        with pytest.raises(ValidationFailed, match="不存在"):
            svc.submit_self_check(case_id="case-1", stage="self_check",
                                  occurred_at="2026-02-20T09:00:00Z",
                                  content={"x": 1}, actor="张某", role="机构人员",
                                  is_backfill=True, backfill_reason="系统故障",
                                  references=["evt-ghost"])

    def test_backfill_accepted_with_reason_and_reference(self, svc):
        make_case(svc)
        result = svc.submit_self_check(
            case_id="case-1", stage="self_check",
            occurred_at="2026-02-20T09:00:00Z", content={"x": 1},
            actor="张某", role="机构人员", is_backfill=True,
            backfill_reason="原始系统故障，依据纸质记录补录",
            references=["smp-1"])
        assert result["deduplicated"] is False
        state = svc.case_state("case-1")
        backfill = state["submissions"][0]["backfill"]
        assert backfill["reason"].startswith("原始系统故障")
        assert backfill["references"][0]["kind"] == "sampling_record"

    def test_correction_preserves_original(self, svc, clock):
        make_case(svc)
        svc.submit_self_check(case_id="case-1", stage="self_check",
                              occurred_at="2026-03-02T09:00:00Z",
                              content={"value": "原始值"}, actor="张某", role="机构人员")
        target = svc.timeline("case-1")["events"][-2]  # self_check_submitted
        clock.set("2026-03-10T09:00:00Z")
        svc.correct_record(case_id="case-1", target_event_id=target["event_id"],
                           reason="笔误", correction={"value": "更正值"},
                           actor="张某", role="机构人员")
        events = svc.timeline("case-1")["events"]
        originals = [e for e in events if e["type"] == "self_check_submitted"]
        corrections = [e for e in events if e["type"] == "record_corrected"]
        assert len(originals) == 1 and originals[0]["payload"]["content"]["value"] == "原始值"
        assert len(corrections) == 1
        assert corrections[0]["payload"]["target_event_id"] == target["event_id"]
        # 重放到更正之前：只能看到原事实
        before = svc.replay("case-1", at="2026-03-05T00:00:00Z")
        assert before["state"]["corrections"] == []
        assert before["state"]["submissions"][0]["event_id"] == target["event_id"]
        # 重放到更正之后：原事实仍在，更正并列呈现
        after = svc.replay("case-1", at="2026-03-11T00:00:00Z")
        assert len(after["state"]["corrections"]) == 1

    def test_correction_rejects_foreign_event(self, svc):
        make_case(svc)
        make_sampling(svc, record_id="smp-2")
        other = svc.store.events(stream="sampling:smp-2")[0]
        with pytest.raises(ValidationFailed):
            svc.correct_record(case_id="case-1", target_event_id=other["event_id"],
                               reason="x", correction={}, actor="张某", role="机构人员")


# ---------------------------------------------------------------- 重复上报

class TestIdempotency:
    def test_same_idempotency_key_returns_same_receipt(self, svc):
        make_case(svc)
        kwargs = dict(case_id="case-1", stage="self_check",
                      occurred_at="2026-03-02T09:00:00Z",
                      content={"a": 1}, actor="张某", role="机构人员",
                      idempotency_key="submit-001")
        first = svc.submit_self_check(**kwargs)
        second = svc.submit_self_check(**kwargs)
        assert first["receipts"][0]["receipt_id"] == second["receipts"][0]["receipt_id"]
        events = [e for e in svc.timeline("case-1")["events"]
                  if e["type"] == "self_check_submitted"]
        assert len(events) == 1  # 重复上报不产生重复事件

    def test_same_content_different_key_deduplicated(self, svc):
        make_case(svc)
        kwargs = dict(case_id="case-1", stage="self_check",
                      occurred_at="2026-03-02T09:00:00Z",
                      content={"a": 1}, actor="张某", role="机构人员")
        first = svc.submit_self_check(**kwargs)
        second = svc.submit_self_check(**kwargs)
        assert second["deduplicated"] is True
        assert first["receipts"][0]["receipt_id"] == second["receipts"][0]["receipt_id"]

    def test_different_content_must_use_correction(self, svc):
        make_case(svc)
        svc.submit_self_check(case_id="case-1", stage="self_check",
                              occurred_at="2026-03-02T09:00:00Z",
                              content={"a": 1}, actor="张某", role="机构人员")
        with pytest.raises(Conflict):
            svc.submit_self_check(case_id="case-1", stage="self_check",
                                  occurred_at="2026-03-02T10:00:00Z",
                                  content={"a": 2}, actor="张某", role="机构人员")


# ---------------------------------------------------------------- 多人同时复核

class TestConcurrentReview:
    def test_conflicting_round_rejected(self, svc):
        make_case(svc)
        advance_to_review(svc)
        svc.submit_review(case_id="case-1", decision="fail", expected_round=1,
                          actor="赵复核", role="复核员", comments="材料不全")
        # 复核不通过后退回到整改阶段、轮次变为 2
        state = svc.case_state("case-1")
        assert state["round"] == 2
        assert state["stages"][-1]["stage"] == "rectification"
        with pytest.raises(Conflict):
            svc.submit_review(case_id="case-1", decision="pass", expected_round=1,
                              actor="钱复核", role="复核员")

    def test_simultaneous_reviews_exactly_one_wins(self, svc):
        make_case(svc)
        advance_to_review(svc)
        barrier = threading.Barrier(2)
        outcomes = []

        def attempt(name):
            barrier.wait()
            try:
                svc.submit_review(case_id="case-1", decision="pass",
                                  expected_round=1, actor=name, role="复核员")
                outcomes.append("ok")
            except (Conflict, ValidationFailed):
                outcomes.append("rejected")

        threads = [threading.Thread(target=attempt, args=(f"复核员{i}",))
                   for i in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert outcomes.count("ok") == 1
        assert outcomes.count("rejected") == 1
        reviews = [e for e in svc.timeline("case-1")["events"]
                   if e["type"] == "review_submitted"]
        assert len(reviews) == 1  # 链上只有一份复核结论

    def test_review_pass_closes_case(self, svc):
        make_case(svc)
        advance_to_review(svc)
        svc.submit_review(case_id="case-1", decision="pass", expected_round=1,
                          actor="赵复核", role="复核员")
        assert svc.case_state("case-1")["status"] == "closed"

    def test_review_requires_reviewer_role(self, svc):
        make_case(svc)
        advance_to_review(svc)
        with pytest.raises(PermissionDenied):
            svc.submit_review(case_id="case-1", decision="pass", expected_round=1,
                              actor="王执法", role="执法员")


# ---------------------------------------------------------------- 整改逾期

class TestOverdue:
    def test_overdue_emitted_once_with_escalation(self, svc, clock):
        make_case(svc)
        clock.set("2026-04-01T00:00:00Z")  # 自查期限 15 天，已逾期
        first = svc.check_overdue()
        assert len(first["overdue"]) == 1
        assert first["overdue"][0]["stage"] == "self_check"
        second = svc.check_overdue()
        assert second["overdue"] == []  # 幂等：不重复生成
        types = [e["type"] for e in svc.timeline("case-1")["events"]]
        assert "stage_overdue" in types
        assert "escalation_recommended" in types

    def test_not_overdue_before_deadline(self, svc, clock):
        make_case(svc)
        clock.set("2026-03-10T00:00:00Z")
        assert svc.check_overdue()["overdue"] == []

    def test_rectification_overdue_suggests_perfunctory(self, svc, clock):
        make_case(svc)
        svc.submit_self_check(case_id="case-1", stage="self_check",
                              occurred_at="2026-03-02T09:00:00Z",
                              content={"a": 1}, actor="张某", role="机构人员")
        clock.set("2026-05-01T00:00:00Z")  # 整改期限 30 天，已逾期
        result = svc.check_overdue()
        assert len(result["overdue"]) == 1
        escalations = [e for e in svc.timeline("case-1")["events"]
                       if e["type"] == "escalation_recommended"]
        assert escalations[-1]["payload"]["suggested_disposition"] == \
            "perfunctory_rectification"


# ---------------------------------------------------------------- 资格依法退出

class TestQualificationExit:
    def test_exit_truncates_validity(self, svc, clock):
        clock.set("2026-04-01T00:00:00Z")
        svc.qualification_exit(institution_id="inst-lanzhou-002",
                               qualification_id="qual-cma-002",
                               effective_at="2026-04-01T00:00:00Z",
                               legal_basis="检验检测机构资质认定管理办法(虚构条文)",
                               actor="孙负责人", role="执法负责人")
        before = svc.qualification_at("inst-lanzhou-002", "2026-03-01T00:00:00Z")
        after = svc.qualification_at("inst-lanzhou-002", "2026-05-01T00:00:00Z")
        assert before["valid"] is True   # 退出前的历史事实不变
        assert after["valid"] is False
        assert after["qualifications"][0]["exited"] is True

    def test_open_case_flagged_on_exit(self, svc, clock):
        make_case(svc, institution_id="inst-lanzhou-002", operator_id="op-li-002",
                  instrument_id="ins-aa-002", sampled_at="2025-06-01T08:00:00Z")
        clock.set("2026-04-01T00:00:00Z")
        svc.qualification_exit(institution_id="inst-lanzhou-002",
                               qualification_id="qual-cma-002",
                               effective_at="2026-04-01T00:00:00Z",
                               legal_basis="虚构条文", actor="孙负责人", role="执法负责人")
        flags = [f["flag"] for f in svc.case_state("case-1")["flags"]]
        assert "institution_qualification_exited" in flags

    def test_sampling_after_exit_flagged(self, svc, clock):
        clock.set("2026-04-01T00:00:00Z")
        svc.qualification_exit(institution_id="inst-lanzhou-002",
                               qualification_id="qual-cma-002",
                               effective_at="2026-04-01T00:00:00Z",
                               legal_basis="虚构条文", actor="孙负责人", role="执法负责人")
        result = make_sampling(svc, record_id="smp-exit",
                               institution_id="inst-lanzhou-002",
                               operator_id="op-li-002",
                               sampled_at="2025-06-01T08:00:00Z")
        # 采样时刻在退出前，资质仍有效（历史认定不变）
        assert "institution_qualification_invalid" not in \
            [f["flag"] for f in result["flags"]]
        result2 = svc.record_sampling(
            institution_id="inst-lanzhou-002", operator_id="op-li-002",
            instrument_id="ins-aa-002", sampled_at="2026-05-01T08:00:00Z",
            scope="水和废水采样", items=["pH"], raw_data_hash="sha256:x",
            actor="李某", role="机构人员", record_id="smp-exit-2")
        flags = [f["flag"] for f in result2["flags"]]
        assert "institution_qualification_invalid" in flags


# ---------------------------------------------------------------- 处置权限

class TestDispositions:
    def test_false_report_by_officer(self, svc):
        make_case(svc)
        result = svc.issue_disposition(case_id="case-1",
                                       disposition_type="false_report",
                                       actor="王执法", role="执法员",
                                       reason="漏报校准记录")
        assert result["receipts"]
        dispositions = svc.case_state("case-1")["dispositions"]
        assert dispositions[0]["action"] == "责令改正"

    def test_perfunctory_requires_leader(self, svc):
        make_case(svc)
        with pytest.raises(PermissionDenied):
            svc.issue_disposition(case_id="case-1",
                                  disposition_type="perfunctory_rectification",
                                  actor="王执法", role="执法员", reason="敷衍")
        svc.issue_disposition(case_id="case-1",
                              disposition_type="perfunctory_rectification",
                              actor="孙负责人", role="执法负责人", reason="敷衍")
        assert svc.case_state("case-1")["dispositions"][0]["action"] == "约谈并立案"

    def test_unknown_disposition_type(self, svc):
        make_case(svc)
        with pytest.raises(ValidationFailed):
            svc.issue_disposition(case_id="case-1", disposition_type="invented",
                                  actor="王执法", role="执法员", reason="x")


# ---------------------------------------------------------------- 涉嫌犯罪移送

class TestCrimeTransfer:
    def _propose(self, svc):
        return svc.issue_disposition(case_id="case-1",
                                     disposition_type="suspected_crime",
                                     action="propose", actor="王执法", role="执法员",
                                     reason="涉嫌篡改监测数据")

    def test_full_transfer_flow(self, svc):
        make_case(svc)
        proposal = self._propose(svc)
        transfer_id = proposal["transfer_id"]
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="approve", actor="周法制", role="法制审核员",
                              reason="证据链完整")
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="approve", actor="孙负责人", role="部门负责人",
                              reason="同意移送")
        executed = svc.issue_disposition(
            case_id="case-1", disposition_type="suspected_crime",
            action="execute", actor="孙负责人", role="执法负责人",
            reason="移送", receiving_organ="公安机关(虚构)",
            transfer_no="移字[2026]001号", receipt_no="公收[2026]018号",
            received_at="2026-03-20T10:00:00Z")
        assert len(executed["receipts"]) == 2  # 移送执行 + 案件状态变更
        state = svc.case_state("case-1")
        assert state["status"] == "transferred"
        assert state["transfer"]["executed"]["receiving_organ"] == "公安机关(虚构)"
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="outcome", actor="孙负责人", role="执法负责人",
                              reason="公安机关已受理", accepted=True)
        assert svc.case_state("case-1")["transfer"]["outcome"]["accepted"] is True
        assert transfer_id == state["transfer"]["executed"]["transfer_id"]

    def test_execute_requires_both_approvals(self, svc):
        make_case(svc)
        self._propose(svc)
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="approve", actor="周法制", role="法制审核员",
                              reason="ok")
        with pytest.raises(Conflict, match="审批未完成"):
            svc.issue_disposition(case_id="case-1",
                                  disposition_type="suspected_crime",
                                  action="execute", actor="孙负责人",
                                  role="执法负责人", reason="移送",
                                  receiving_organ="公安机关(虚构)",
                                  transfer_no="T1", receipt_no="R1",
                                  received_at="2026-03-20T10:00:00Z")

    def test_approvals_must_be_distinct_people_and_roles(self, svc):
        make_case(svc)
        self._propose(svc)
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="approve", actor="周法制", role="法制审核员",
                              reason="ok")
        with pytest.raises(Conflict):  # 同一角色重复审批
            svc.issue_disposition(case_id="case-1",
                                  disposition_type="suspected_crime",
                                  action="approve", actor="吴法制", role="法制审核员",
                                  reason="ok")
        with pytest.raises(PermissionDenied):  # 审批角色不在矩阵内
            svc.issue_disposition(case_id="case-1",
                                  disposition_type="suspected_crime",
                                  action="approve", actor="王执法", role="执法员",
                                  reason="ok")

    def test_proposer_cannot_approve(self, svc):
        make_case(svc)
        self._propose(svc)
        with pytest.raises(Conflict, match="提案人"):
            svc.issue_disposition(case_id="case-1",
                                  disposition_type="suspected_crime",
                                  action="approve", actor="王执法", role="部门负责人",
                                  reason="ok")

    def test_rejected_outcome_reopens_case(self, svc):
        make_case(svc)
        self._propose(svc)
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="approve", actor="周法制", role="法制审核员",
                              reason="ok")
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="approve", actor="孙负责人", role="部门负责人",
                              reason="ok")
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="execute", actor="孙负责人", role="执法负责人",
                              reason="移送", receiving_organ="公安机关(虚构)",
                              transfer_no="T1", receipt_no="R1",
                              received_at="2026-03-20T10:00:00Z")
        svc.issue_disposition(case_id="case-1", disposition_type="suspected_crime",
                              action="outcome", actor="孙负责人", role="执法负责人",
                              reason="证据不足退回", accepted=False)
        assert svc.case_state("case-1")["status"] == "open"


# ---------------------------------------------------- 校准时间矛盾：完整重放

class TestCalibrationContradictionReplay:
    def _scenario(self, svc, clock):
        """一次看似普通的校准时间矛盾：

        3-15 采样时仪器校准已过期（3-01 到期）→ 案件开立 →
        4-01 机构补登一份 3-10 校准的证书，声称覆盖采样时刻。
        """
        clock.set("2026-03-16T09:00:00Z")
        make_case(svc, instrument_id="ins-gc-001",
                  sampled_at="2026-03-15T08:00:00Z")
        clock.set("2026-04-01T10:00:00Z")
        svc.record_calibration(instrument_id="ins-gc-001",
                               calibrated_at="2026-03-10T00:00:00Z",
                               valid_until="2026-09-10T00:00:00Z",
                               certificate_no="JJ-FIC-2026-0099",
                               actor="张某", role="机构人员")

    def test_contradiction_flagged_on_case(self, svc, clock):
        self._scenario(svc, clock)
        flags = [f["flag"] for f in svc.case_state("case-1")["flags"]]
        assert "instrument_calibration_expired" in flags
        assert "calibration_time_contradiction" in flags

    def test_replay_shows_knowledge_evolution(self, svc, clock):
        self._scenario(svc, clock)
        # 3-20 重放：系统只知道仪器过期，矛盾尚未显现
        early = svc.replay("case-1", at="2026-03-20T00:00:00Z")
        early_flags = [f["flag"] for f in early["state"]["flags"]]
        assert "instrument_calibration_expired" in early_flags
        assert "calibration_time_contradiction" not in early_flags
        # 最新重放：矛盾完整呈现，两个时刻的事实都保留
        full = svc.replay("case-1")
        full_flags = [f["flag"] for f in full["state"]["flags"]]
        assert "calibration_time_contradiction" in full_flags
        assert full["events_considered"] > early["events_considered"]
        # 两个视角各自稳定：重放结果可复现
        assert svc.replay("case-1", at="2026-03-20T00:00:00Z")["digest"] == early["digest"]
        assert svc.replay("case-1")["digest"] == full["digest"]

    def test_instrument_status_depends_on_knowledge_time(self, svc, clock):
        self._scenario(svc, clock)
        # 以 3-16 的系统认知看 3-15：仪器过期
        then = svc.instrument_at("ins-gc-001", "2026-03-15T08:00:00Z",
                                 knowledge_at="2026-03-16T09:00:00Z")
        assert then["calibrated"] is False
        # 以当前认知看同一时刻：补登证书覆盖了它——两个答案都为真，
        # 差异本身就是证据
        now = svc.instrument_at("ins-gc-001", "2026-03-15T08:00:00Z")
        assert now["calibrated"] is True
        assert now["active_calibration"]["certificate_no"] == "JJ-FIC-2026-0099"

    def test_summary_stable_across_calls(self, svc, clock):
        self._scenario(svc, clock)
        first = svc.summary("case-1")
        second = svc.summary("case-1")
        assert first == second
        assert first["digest"] == second["digest"]
        json.dumps(first, ensure_ascii=False)  # 可序列化


# ---------------------------------------------------------------- 回执与链校验

class TestVerification:
    def test_receipt_verifies(self, svc):
        make_case(svc)
        receipt = svc.timeline("case-1")["events"][0]
        receipt_id = f"rcpt-{receipt['seq']:08d}-{receipt['hash'][:12]}"
        result = svc.verify_receipt(receipt_id)
        assert result["valid"] is True
        assert all(result["checks"].values())

    def test_unknown_receipt(self, svc):
        assert svc.verify_receipt("rcpt-ghost")["valid"] is False

    def test_chain_verifies_and_detects_tamper(self, svc):
        make_case(svc)
        advance_to_review(svc)
        assert svc.verify_chain()["valid"] is True
        assert svc.verify_chain("case-1")["valid"] is True
        # 篡改任一历史事件的载荷，全链校验立即失败
        svc.store._conn.execute(
            "UPDATE events SET payload=? WHERE seq=1", ('{"tampered": true}',))
        svc.store._conn.commit()
        result = svc.verify_chain()
        assert result["valid"] is False
        assert result["problems"][0]["problem"] == "hash_mismatch"


# ---------------------------------------------------------------- HTTP 冒烟

class TestHttpApi:
    @pytest.fixture()
    def server(self):
        server = run_server(str(CONTEXT_PATH), ":memory:", 0)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield f"http://127.0.0.1:{server.server_address[1]}"
        server.shutdown()

    @staticmethod
    def _post(base, path, body, headers=None):
        req = urllib.request.Request(
            base + path, data=json.dumps(body).encode("utf-8"),
            headers={"Content-Type": "application/json", **(headers or {})},
            method="POST")
        try:
            with urllib.request.urlopen(req) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    @staticmethod
    def _get(base, path):
        try:
            with urllib.request.urlopen(base + path) as resp:
                return resp.status, json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_end_to_end_over_http(self, server):
        status, sampling = self._post(server, "/sampling-records", {
            "institution_id": "inst-huajian-001", "operator_id": "op-zhang-001",
            "instrument_id": "ins-gc-001", "sampled_at": "2026-03-15T08:00:00Z",
            "scope": "水和废水采样", "items": ["pH"],
            "raw_data_hash": "sha256:http-1", "actor": "张某", "role": "机构人员",
        }, headers={"Idempotency-Key": "http-smp-1"})
        assert status == 201
        assert sampling["flags"][0]["flag"] == "instrument_calibration_expired"
        record_id = sampling["record_id"]

        # 重复上报：同一幂等键，返回同一回执
        status2, again = self._post(server, "/sampling-records", {
            "institution_id": "inst-huajian-001", "operator_id": "op-zhang-001",
            "instrument_id": "ins-gc-001", "sampled_at": "2026-03-15T08:00:00Z",
            "scope": "水和废水采样", "items": ["pH"],
            "raw_data_hash": "sha256:http-1", "actor": "张某", "role": "机构人员",
        }, headers={"Idempotency-Key": "http-smp-1"})
        assert status2 == 201
        assert again["deduplicated"] is True
        assert again["receipts"][0]["receipt_id"] == sampling["receipts"][0]["receipt_id"]

        status, case = self._post(server, "/cases", {
            "sampling_record_id": record_id, "reason": "校准过期",
            "actor": "王执法", "role": "执法员"})
        assert status == 201
        case_id = case["case_id"]

        status, summary = self._get(server, f"/cases/{case_id}/summary")
        assert status == 200
        assert summary["status"] == "open"
        assert summary["digest"]

        status, verify = self._get(
            server, f"/receipts/{sampling['receipts'][0]['receipt_id']}/verify")
        assert status == 200 and verify["valid"] is True

        status, ref = self._get(
            server, "/reference/instruments/ins-gc-001/status"
                    "?at=2026-03-15T08:00:00Z")
        assert status == 200 and ref["calibrated"] is False

    def test_error_mapping(self, server):
        status, body = self._get(server, "/cases/case-ghost/summary")
        assert status == 404 and body["error"] == "not_found"
        status, body = self._post(server, "/cases", {
            "sampling_record_id": "smp-ghost", "reason": "x"})
        assert status == 404
