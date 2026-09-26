"""哈希链、幂等与回执测试。"""

import json
import tempfile
import unittest
from pathlib import Path

from src.evidence import access
from src.evidence.clock import Clock
from src.evidence.store import (
    GENESIS,
    ChainTampered,
    ConcurrentModification,
    EventStore,
)
from tests._helpers import make_service

STAFF = access.Role.MONITOR_STAFF
LEAD = access.Role.MONITOR_LEAD


class ChainCoreTest(unittest.TestCase):
    def test_double_track_hashes_chain_in_order(self):
        svc = make_service(None)
        svc.register_sample(
            case_id="C1", sample_ref="S1", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://1", actor="chenming", role=STAFF, command_id="c1")
        svc.clock.freeze("2026-03-21T00:00:00Z")
        svc.report_self_check("C1", phase="p1", content="x",
                              actor="liuna", role=LEAD, command_id="c2")

        svc.store.verify()
        svc.store.verify_case("C1")
        events = svc.store.case_events("C1")
        self.assertEqual(events[0].prev_case_hash, GENESIS)
        for prev, cur in zip(events, events[1:]):
            self.assertEqual(cur.prev_case_hash, prev.event_hash)
        # 全局链与案件链同库时头一致
        self.assertEqual(svc.store.global_head, events[-1].event_hash)

    def test_payload_tampering_breaks_case_chain(self):
        svc = make_service(None)
        svc.register_sample(
            case_id="C2", sample_ref="S2", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://2", actor="chenming", role=STAFF, command_id="c1")
        svc.report_self_check("C2", phase="p1", content="x",
                              actor="liuna", role=LEAD, command_id="c2")
        head = svc.store.global_head

        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "events.jsonl"
            svc.store.path = path
            for ev in svc.store.all_events():
                with open(path, "a", encoding="utf-8") as f:
                    f.write(ev.to_line() + "\n")
            # 直接篡改磁盘上第二条案件事件的载荷
            lines = path.read_text(encoding="utf-8").splitlines()
            record = json.loads(lines[2])
            record["payload"]["content"] = "篡改后的内容"
            lines[2] = json.dumps(record, ensure_ascii=False, sort_keys=True)
            path.write_text("\n".join(lines) + "\n", encoding="utf-8")

            with self.assertRaises(ChainTampered):
                EventStore(clock=Clock(), path=path)

        self.assertEqual(svc.store.global_head, head)

    def test_event_deletion_breaks_global_chain(self):
        svc = make_service(None)
        svc.register_sample(
            case_id="C3", sample_ref="S3", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://3", actor="chenming", role=STAFF, command_id="c1")
        svc.report_self_check("C3", phase="p1", content="x",
                              actor="liuna", role=LEAD, command_id="c2")
        svc.report_self_check("C3", phase="p2", content="y",
                              actor="liuna", role=LEAD, command_id="c3")
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "events.jsonl"
            # 删除中间一条事件（保留首尾）：全局链与案件链都应断裂
            lines = [ev.to_line() for ev in svc.store.all_events()]
            path.write_text("\n".join(lines[:1] + lines[2:]) + "\n", encoding="utf-8")
            with self.assertRaises(ChainTampered):
                EventStore(clock=Clock(), path=path)

    def test_case_bundle_independently_verifiable(self):
        svc = make_service(None)
        svc.register_sample(
            case_id="C4", sample_ref="S4", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://4", actor="chenming", role=STAFF, command_id="c1")
        svc.report_self_check("C4", phase="p1", content="x",
                              actor="liuna", role=LEAD, command_id="c2")
        bundle = svc.store.export_case_bundle("C4")
        EventStore.verify_bundle(bundle)  # 不抛异常即通过
        # 篡改 bundle 中的原始数据引用
        bundle["events"][1]["payload"]["raw_data_ref"] = "raw://changed"
        with self.assertRaises(ChainTampered):
            EventStore.verify_bundle(bundle)

    def test_repeated_command_id_replays_first_result(self):
        svc = make_service(None)
        r1 = svc.register_sample(
            case_id="C5", sample_ref="S5", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://5", actor="chenming", role=STAFF, command_id="same")
        # 相同 command_id 重试注册：幂等重放，不产生新事件
        r2 = svc.register_sample(
            case_id="C5", sample_ref="S5", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://5", actor="chenming", role=STAFF, command_id="same")
        self.assertEqual(r1.events[0].event_hash, r2.events[0].event_hash)
        self.assertTrue(r2.receipt["extra"]["idempotent_replay"])
        self.assertEqual(len(svc.store.case_events("C5")), 2)

    def test_concurrent_verifiers_only_one_succeeds(self):
        svc = make_service(None, when="2026-04-05T10:00:00Z")
        svc.register_sample(
            case_id="C6", sample_ref="S6", org_id="org-lvyuan", person_id="p-chenming",
            instrument_id="inst-wq-09", sampled_at="2026-03-20T09:00:00Z",
            raw_data_ref="raw://6", actor="chenming", role=STAFF, command_id="c1")
        svc.issue_rectification_order(
            "C6", items=[{"code": "I1", "requirement": "重新校准"}],
            actor="wangjing", role=access.Role.ECO_INSPECTOR, command_id="c2")
        svc.clock.freeze("2026-04-06T10:00:00Z")
        svc.submit_rectification(
            "C6", order_id="ORD-001",
            measures=[{"item_code": "I1", "action": "已校准",
                       "evidence_refs": ["ev://1"]}],
            actor="zhaolei", role=STAFF, command_id="c3")
        head = svc.store.case_head("C6")
        r_a = svc.verify_rectification(
            "C6", order_id="ORD-001", passed=True, comment="ok",
            reviewer="sunli", role=access.Role.ECO_REVIEWER,
            expected_case_hash=head, command_id="c4")
        with self.assertRaises(ConcurrentModification):
            svc.verify_rectification(
                "C6", order_id="ORD-001", passed=False, comment="conflict",
                reviewer="zhoufang", role=access.Role.ECO_REVIEWER,
                expected_case_hash=head, command_id="c5")
        # 先入链的结论生效
        st = svc._state("C6")
        self.assertEqual(st.orders["ORD-001"].status, "verified")
        self.assertTrue(svc.receipts.verify(r_a.receipt))

    def test_receipt_signature_verifies_and_token_roundtrip(self):
        svc = make_service(None)
        rs = svc.receipts
        r = rs.issue(receipt_id="R-x", case_id="C", event_hash="e" * 64,
                     case_head="h" * 64, global_head="g" * 64,
                     recorded_at="2026-01-01T00:00:00Z", action="test")
        self.assertTrue(rs.verify(r))
        token = rs.encode(r)
        decoded = rs.decode(token)
        self.assertTrue(rs.verify(decoded))
        decoded["case_id"] = "FORGED"
        self.assertFalse(rs.verify(decoded))


if __name__ == "__main__":
    unittest.main()
