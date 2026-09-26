"""端到端剧本集成测试：校准时间矛盾被完整、稳定地重放。"""

import unittest

from src.evidence.scenario import run
from src.evidence.store import EventStore


class ScenarioReplayTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.res = run()

    def test_chain_and_bundle_valid(self):
        self.res.svc.store.verify()
        EventStore.verify_bundle(self.res.bundle)
        self.assertTrue(self.res.replay["chain_valid"])

    def test_sample_anchored_to_material_then_in_use(self):
        snap = self.res.replay["calibration_context"]
        self.assertEqual(snap["at"], "2026-03-20T09:00:00Z")
        self.assertEqual(snap["organization"]["qualification_no"], "Q-2024-LY-018")
        self.assertEqual(snap["organization"]["status_at_sample"], "active")
        self.assertEqual(snap["instrument"]["certificate_no"], "CAL-2025-1102")

    def test_backdated_certificate_flagged_as_contradiction(self):
        inst = self.res.replay["calibration_context"]["instrument"]
        self.assertTrue(inst["contradiction"])
        # 今天的 as-of 投影指向事后补交的证书
        self.assertEqual(inst["certificate_resolves_today"], "CAL-2026-0315X")
        kinds = {a["kind"] for a in self.res.replay["anomalies"]}
        self.assertEqual(kinds, {"overlap", "backdated"})
        backdated = [a for a in self.res.replay["anomalies"] if a["kind"] == "backdated"]
        self.assertEqual(len(backdated), 1)
        self.assertIn("CAL-2026-0315X", backdated[0]["detail"])

    def test_duplicate_report_receipt_is_stable(self):
        a, b = self.res.duplicate_receipt_a, self.res.duplicate_receipt_b
        self.assertEqual(a["receipt_id"], b["receipt_id"])
        self.assertTrue(self.res.svc.receipts.verify(a))
        self.assertEqual(a["extra"]["status"], "rejected")
        self.assertTrue(a["extra"]["stable"])

    def test_original_facts_preserved_with_corrections(self):
        corrected = [f for f in self.res.replay["original_facts"] if f["superseded"]]
        self.assertTrue(corrected)
        for f in corrected:
            self.assertIn("content", [c["field"] for c in f["corrected_fields"]])

    def test_late_entry_references_existing_record_with_reason(self):
        entries = [t for t in self.res.replay["timeline"]
                   if t["kind"] == "self_check.late_entry"]
        self.assertEqual(len(entries), 1)
        self.assertTrue(entries[0]["references"]["reason"])
        self.assertEqual(entries[0]["references"]["ref_event_hash"],
                         self.res.sample_event_hash)

    def test_rectification_arc_perfunctory_overdue_verified(self):
        orders = self.res.replay["rectifications"]
        self.assertEqual(len(orders), 1)
        o = orders[0]
        self.assertEqual(o["status"], "verified")
        self.assertTrue(o["overdue"])
        self.assertEqual(o["submission_count"], 2)
        self.assertTrue(o["review"]["passed"])
        self.assertNotEqual(o["review"]["reviewer"], "zhaolei")  # 回避

    def test_transfer_roundtrip_real_result(self):
        transfers = self.res.replay["transfers"]
        self.assertEqual(len(transfers), 1)
        t = transfers[0]
        self.assertEqual(t["status"], "filed")
        self.assertEqual(t["result"]["result"], "filed")
        self.assertIn("077", t["result"]["docket_no"])
        # 回流登记由公检法回流岗完成
        self.assertEqual(t["result"]["recorded_at"], "2026-05-20T15:00:00.000Z")

    def test_replay_digest_stable(self):
        again = run()
        self.assertEqual(again.replay["digest"], self.res.replay["digest"])
        self.assertEqual(again.replay["case_head"], self.res.replay["case_head"])

    def test_all_command_receipts_verify(self):
        for t in self.res.replay["timeline"]:
            # 每条时间线事件都在链上可核验
            self.assertEqual(len(t["event_hash"]), 64)
        for r in (self.res.accepted_receipt, self.res.verify_receipt,
                  self.res.transfer_result_receipt, self.res.perfunctory_receipt):
            self.assertTrue(self.res.svc.receipts.verify(r))


if __name__ == "__main__":
    unittest.main()
