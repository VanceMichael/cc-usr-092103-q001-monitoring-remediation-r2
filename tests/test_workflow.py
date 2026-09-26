"""业务处置流转测试：补录/更正、错报漏报、敷衍整改、逾期、复核回避、移送结果。"""

import unittest

from src.evidence import access
from src.evidence.access import PermissionDenied
from src.evidence.errors import (
    CorrectionInvalid,
    DuplicateReport,
    LateEntryInvalid,
    OrderClosed,
    RectificationPerfunctory,
    ReviewerConflict,
    TransferStateError,
)
from tests._helpers import make_service

STAFF = access.Role.MONITOR_STAFF
LEAD = access.Role.MONITOR_LEAD
INSPECTOR = access.Role.ECO_INSPECTOR
REVIEWER = access.Role.ECO_REVIEWER
TRANSFER = access.Role.TRANSFER_OFFICER
DESK = access.Role.JUDICIAL_DESK


def open_case(svc, case_id="C-BUS", cmd="b0", when="2026-03-20T10:00:00Z"):
    svc.clock.freeze(when)
    return svc.register_sample(
        case_id=case_id, sample_ref=f"S-{case_id}", org_id="org-lvyuan",
        person_id="p-chenming", instrument_id="inst-wq-09",
        sampled_at="2026-03-20T09:00:00Z", raw_data_ref=f"raw://{case_id}",
        actor="chenming", role=STAFF, command_id=cmd)


class LateEntryAndCorrectionTest(unittest.TestCase):
    def test_late_entry_must_reference_old_record_and_explain(self):
        svc = make_service(None)
        r = open_case(svc, "C-L")
        sample_hash = next(e.event_hash for e in r.events
                           if e.event_type == "sample.registered")
        with self.assertRaises(LateEntryInvalid):
            svc.report_late_entry(
                "C-L", ref_event_hash=sample_hash, reason="   ",
                facts={}, actor="chenming", role=STAFF, command_id="l-bad")
        r2 = svc.report_late_entry(
            "C-L", ref_event_hash=sample_hash,
            reason="运维批注当日未随电子档归档", facts={"note": "..."},
            actor="chenming", role=STAFF, command_id="l-ok")
        p = r2.event.payload
        self.assertEqual(p["ref_event_hash"], sample_hash)
        self.assertEqual(p["ref_payload_hash"],
                         next(e for e in r.events if e.event_hash == sample_hash).payload_hash)

    def test_correction_keeps_original_fact(self):
        svc = make_service(None)
        open_case(svc, "C-C")
        svc.report_self_check("C-C", phase="p1", content="原值",
                              actor="liuna", role=LEAD, command_id="c-r")
        st = svc._state("C-C")
        target = st.phases_reported["p1"]
        with self.assertRaises(CorrectionInvalid):
            svc.correct_fact("C-C", target_event_hash=target, field_name="content",
                             new_value="原值", reason="无变化更正应拒绝",
                             actor="liuna", role=LEAD, command_id="c-same")
        r = svc.correct_fact(
            "C-C", target_event_hash=target, field_name="content",
            new_value="更正后", reason="笔误补正",
            actor="liuna", role=LEAD, command_id="c-ok")
        # 原事件原样保留
        original = svc.store.event_by_hash(target)
        self.assertEqual(original.payload["content"], "原值")
        self.assertEqual(r.event.payload["before_value"], "原值")
        self.assertEqual(r.event.payload["after_value"], "更正后")

    def test_staff_cannot_correct_without_lead_role(self):
        svc = make_service(None)
        open_case(svc, "C-P")
        with self.assertRaises(PermissionDenied):
            svc.correct_fact(
                "C-P", target_event_hash="0" * 64, field_name="x",
                new_value=1, reason="r", actor="chenming", role=STAFF,
                command_id="p1")


class DuplicateReportTest(unittest.TestCase):
    def test_duplicate_phase_returns_stable_rejection_receipt(self):
        svc = make_service(None)
        open_case(svc, "C-D")
        svc.report_self_check("C-D", phase="p1", content="首次",
                              actor="liuna", role=LEAD, command_id="d1")
        receipts = []
        event_count = len(svc.store.case_events("C-D"))
        for cid in ("d2", "d3", "d4"):
            try:
                svc.report_self_check("C-D", phase="p1", content="重复",
                                      actor="liuna", role=LEAD, command_id=cid)
                self.fail("应拒绝重复上报")
            except DuplicateReport as e:
                self.assertTrue(svc.receipts.verify(e.receipt))
                receipts.append(e.receipt)
        self.assertEqual({r["receipt_id"] for r in receipts}, {receipts[0]["receipt_id"]})
        self.assertEqual(len(svc.store.case_events("C-D")), event_count)  # 未新增事件

    def test_same_command_id_replays_accepted_report(self):
        svc = make_service(None)
        open_case(svc, "C-D2")
        r1 = svc.report_self_check("C-D2", phase="p1", content="x",
                                   actor="liuna", role=LEAD, command_id="same")
        r2 = svc.report_self_check("C-D2", phase="p1", content="x",
                                   actor="liuna", role=LEAD, command_id="same")
        self.assertEqual(r1.event.event_hash, r2.event.event_hash)
        self.assertTrue(r2.receipt["extra"]["idempotent_replay"])


class RectificationFlowTest(unittest.TestCase):
    def _order(self, svc, case_id, items=None):
        open_case(svc, case_id)
        svc.clock.freeze("2026-04-05T10:00:00Z")
        return svc.issue_rectification_order(
            case_id, items=items or [{"code": "I1", "requirement": "重新校准"}],
            actor="wangjing", role=INSPECTOR, command_id=f"{case_id}-ord")

    def test_perfunctory_rectification_flagged_and_returned(self):
        svc = make_service(None)
        self._order(svc, "C-F1", [
            {"code": "I1", "requirement": "重新校准"},
            {"code": "I2", "requirement": "人员培训"},
        ])
        svc.clock.freeze("2026-04-06T10:00:00Z")
        with self.assertRaises(RectificationPerfunctory) as ctx:
            svc.submit_rectification(
                "C-F1", order_id="ORD-001",
                measures=[{"item_code": "I1", "action": "已校准",
                           "evidence_refs": ["ev://1"]}],
                actor="zhaolei", role=STAFF, command_id="f1-sub")
        missing_codes = {m["item_code"] for m in ctx.exception.missing}
        self.assertEqual(missing_codes, {"I2"})
        # 敷衍提交与标记事件已入链，回执可核验
        self.assertTrue(svc.receipts.verify(ctx.exception.receipt))
        st = svc._state("C-F1")
        self.assertEqual(st.orders["ORD-001"].status, "perfunctory")

    def test_overdue_marked_on_late_submission_and_sweep(self):
        svc = make_service(None)
        self._order(svc, "C-O")
        # 巡检在截止前：无事件
        svc.clock.freeze("2026-05-05T09:00:00Z")
        self.assertEqual(svc.sweep_overdue(), [])
        # 巡检在截止后：登记逾期（幂等）
        svc.clock.freeze("2026-05-06T09:00:00Z")
        marked = svc.sweep_overdue()
        self.assertEqual(len(marked), 1)
        self.assertEqual(svc.sweep_overdue(), [])  # 再次巡检不重复
        # 逾期后整改仍可提交，受理且带 late
        r = svc.submit_rectification(
            "C-O", order_id="ORD-001",
            measures=[{"item_code": "I1", "action": "完成",
                       "evidence_refs": ["ev://x"]}],
            actor="zhaolei", role=STAFF, command_id="o-sub")
        self.assertTrue(r.event.payload["late"])
        self.assertTrue(r.event.payload["accepted"])

    def test_reviewer_must_not_be_submitter(self):
        svc = make_service(None)
        self._order(svc, "C-R")
        svc.clock.freeze("2026-04-06T10:00:00Z")
        svc.submit_rectification(
            "C-R", order_id="ORD-001",
            measures=[{"item_code": "I1", "action": "完成",
                       "evidence_refs": ["ev://x"]}],
            actor="zhaolei", role=STAFF, command_id="r-sub")
        with self.assertRaises(ReviewerConflict):
            svc.verify_rectification(
                "C-R", order_id="ORD-001", passed=True, comment="self",
                reviewer="zhaolei", role=REVIEWER, command_id="r-v1")

    def test_review_rejection_reopens_order(self):
        svc = make_service(None)
        self._order(svc, "C-RJ")
        svc.clock.freeze("2026-04-06T10:00:00Z")
        svc.submit_rectification(
            "C-RJ", order_id="ORD-001",
            measures=[{"item_code": "I1", "action": "完成",
                       "evidence_refs": ["ev://x"]}],
            actor="zhaolei", role=STAFF, command_id="rj-sub")
        r = svc.verify_rectification(
            "C-RJ", order_id="ORD-001", passed=False, comment="佐证不足",
            reviewer="sunli", role=REVIEWER, command_id="rj-v")
        kinds = [e.event_type for e in r.events]
        self.assertIn("rectification.verified", kinds)
        self.assertIn("rectification.rejected", kinds)
        self.assertEqual(svc._state("C-RJ").orders["ORD-001"].status, "rejected")
        with self.assertRaises(OrderClosed):
            svc.verify_rectification(
                "C-RJ", order_id="ORD-001", passed=True, comment="再次复核已结案通知",
                reviewer="sunli", role=REVIEWER, command_id="rj-v2")

    def test_duplicate_order_while_open_rejected(self):
        svc = make_service(None)
        self._order(svc, "C-DO")
        with self.assertRaises(OrderClosed):
            svc.issue_rectification_order(
                "C-DO", items=[{"code": "I9", "requirement": "x"}],
                actor="wangjing", role=INSPECTOR, command_id="do2")


class TransferFlowTest(unittest.TestCase):
    def _case_with_fault(self, case_id):
        svc = make_service(None)
        open_case(svc, case_id)
        svc.classify_reporting_fault(
            case_id, kind="misreport", detail="编造数据",
            actor="wangjing", role=INSPECTOR, command_id=f"{case_id}-f")
        return svc

    def test_transfer_requires_basis_and_single_in_flight(self):
        svc = make_service(None)
        open_case(svc, "C-T0")
        with self.assertRaises(TransferStateError):
            svc.transfer_suspected_crime(
                "C-T0", to_agency="公安", basis="无事实",
                actor="wangjing", role=TRANSFER, command_id="t0")
        svc = self._case_with_fault("C-T1")
        svc.transfer_suspected_crime(
            "C-T1", to_agency="公安", basis="涉嫌虚假证明",
            actor="wangjing", role=TRANSFER, command_id="t1")
        with self.assertRaises(TransferStateError):
            svc.transfer_suspected_crime(
                "C-T1", to_agency="公安", basis="重复移送",
                actor="wangjing", role=TRANSFER, command_id="t2")

    def test_transfer_result_records_real_outcome_and_allows_next(self):
        svc = self._case_with_fault("C-T2")
        r = svc.transfer_suspected_crime(
            "C-T2", to_agency="公安", basis="涉嫌虚假证明",
            actor="wangjing", role=TRANSFER, command_id="t3")
        trf = r.event.payload["transfer_id"]
        # 移送岗自己不能登记回流结果（公检法回流岗专属权限）
        with self.assertRaises(PermissionDenied):
            svc.record_transfer_result(
                "C-T2", transfer_id=trf, result="filed",
                actor="wangjing", role=TRANSFER, command_id="t4")
        r2 = svc.record_transfer_result(
            "C-T2", transfer_id=trf, result="returned",
            docket_no="", note="证据不足退查补证",
            actor="desk-1", role=DESK, command_id="t5")
        self.assertEqual(r2.event.payload["result"], "returned")
        # 已登记结果不可重复登记
        with self.assertRaises(TransferStateError):
            svc.record_transfer_result(
                "C-T2", transfer_id=trf, result="filed",
                actor="desk-1", role=DESK, command_id="t6")

    def test_close_blocked_until_everything_settled(self):
        from src.evidence.errors import DomainError
        svc = self._case_with_fault("C-T3")
        # 存在在途移送时不能关闭
        svc.transfer_suspected_crime(
            "C-T3", to_agency="公安", basis="涉嫌虚假证明",
            actor="wangjing", role=TRANSFER, command_id="t9")
        with self.assertRaises(DomainError):
            svc.close_case("C-T3", actor="sunli", role=REVIEWER, command_id="x1")
        # 回流立案后仍需无未闭环整改 —— 本案无整改单，可关闭
        svc.record_transfer_result(
            "C-T3", transfer_id="TRF-001", result="filed", docket_no="077",
            actor="desk-1", role=DESK, command_id="x2")
        svc.close_case("C-T3", actor="sunli", role=REVIEWER, command_id="x3")
        self.assertEqual(svc._state("C-T3").status, "closed")


if __name__ == "__main__":
    unittest.main()
