"""JSONL 持久化重启、跨进程重放与 HTTP API 端到端测试。"""

import json
import tempfile
import threading
import unittest
import urllib.request
from pathlib import Path

from src.evidence.api import serve
from src.evidence.app import build_service
from src.evidence.clock import Clock
from src.evidence.replay import build_replay
from src.evidence.scenario import FIXTURE, SECRET, run
from src.evidence.store import EventStore


def _req(method: str, url: str, body: dict | None = None,
         headers: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode("utf-8"))


class PersistenceTest(unittest.TestCase):
    def test_scenario_restarts_from_jsonl_and_replay_is_identical(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "events.jsonl"
            first = run(store_path=path)
            head1 = first.replay["case_head"]
            digest1 = first.replay["digest"]
            self.assertGreater(path.stat().st_size, 0)

            # 全新进程式重建：空存储加载 JSONL 即自动全链校验
            store = EventStore(clock=Clock(), path=path)
            svc = build_service(store_path=path, clock=Clock(), secret=SECRET,
                                registry_fixture=None)
            # 基准资料来自同一条全局链文件，无需重新导入夹具
            view = build_replay(svc.store, svc.registry, first.replay["case_id"])
            self.assertEqual(view.case_head, head1)
            self.assertTrue(view.chain_valid)
            self.assertEqual(len(view.timeline), len(first.replay["timeline"]))
            # 重放摘要稳定（同一份历史，任何时候重放得到同一摘要）
            view2 = build_replay(store, svc.registry, first.replay["case_id"])
            self.assertEqual(view2.digest, digest1)

    def test_registry_rebuilds_from_chain_without_fixture(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / "e.jsonl"
            run(store_path=path)
            svc = build_service(store_path=path, clock=Clock(), secret=SECRET)
            summary = svc.registry.summary()
            org_ids = {o["org_id"] for o in summary["orgs"]}
            self.assertEqual(org_ids, {"org-lvyuan", "org-xinze", "org-hengxin"})
            self.assertEqual(summary["deadlines"]["rectification"], 30)


class HttpApiTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "api.jsonl"
        self.server = serve("127.0.0.1", 0, store_path=self.path,
                            registry_fixture=FIXTURE,
                            clock=Clock.fixed("2026-03-20T10:00:00Z"))
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.tmp.cleanup()

    def test_health_and_registry(self):
        code, body = _req("GET", f"{self.base}/api/health")
        self.assertEqual(code, 200)
        self.assertTrue(body["ok"])
        code, body = _req("GET", f"{self.base}/api/registry/summary")
        self.assertEqual(code, 200)
        self.assertEqual(body["summary"]["deadlines"]["rectification"], 30)

    def test_full_case_flow_over_http(self):
        H_STAFF = {"X-Actor": "chenming", "X-Role": "monitor_staff"}
        H_LEAD = {"X-Actor": "liuna", "X-Role": "monitor_lead"}
        H_INSP = {"X-Actor": "wangjing", "X-Role": "eco_inspector"}
        H_REV = {"X-Actor": "sunli", "X-Role": "eco_reviewer"}
        H_TRF = {"X-Actor": "wangjing", "X-Role": "transfer_officer"}
        H_DESK = {"X-Actor": "desk-1", "X-Role": "judicial_desk"}

        # 采样登记
        code, body = _req("POST", f"{self.base}/api/samples", {
            "case_id": "CASE-HTTP-1", "sample_ref": "S-HTTP-1",
            "org_id": "org-lvyuan", "person_id": "p-chenming",
            "instrument_id": "inst-wq-09", "sampled_at": "2026-03-20T09:00:00Z",
            "raw_data_ref": "raw://http1", "command_id": "h1",
        }, H_STAFF)
        self.assertEqual(code, 201, body)
        self.assertEqual(len(body["events"]), 2)
        self.assertIn("sig", body["receipt"])

        # 越权：操作人员不能下达整改
        code, body = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/orders", {
            "items": [{"code": "I1", "requirement": "x"}], "command_id": "hx",
        }, H_STAFF)
        self.assertEqual(code, 403)

        # 上报 → 重复上报（稳定拒绝回执）
        _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/self-check", {
            "phase": "p1", "content": "ok", "command_id": "h2"}, H_LEAD)
        code, dup1 = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/self-check", {
            "phase": "p1", "content": "dup", "command_id": "h3"}, H_LEAD)
        self.assertEqual(code, 409)
        code, dup2 = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/self-check", {
            "phase": "p1", "content": "dup", "command_id": "h4"}, H_LEAD)
        self.assertEqual(dup1["receipt"]["receipt_id"], dup2["receipt"]["receipt_id"])

        # 无有效校准的采样被 422 拒绝
        code, body = _req("POST", f"{self.base}/api/samples", {
            "case_id": "CASE-HTTP-2", "sample_ref": "S-HTTP-2",
            "org_id": "org-lvyuan", "person_id": "p-chenming",
            "instrument_id": "inst-kq-12", "sampled_at": "2026-03-20T09:00:00Z",
            "raw_data_ref": "raw://http2", "command_id": "hb",
        }, H_STAFF)
        self.assertEqual(code, 422)

        # 错报 → 整改 → 敷衍 → 正式整改 → 复核通过 → 移送 → 回流 → 关闭
        _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/faults", {
            "kind": "misreport", "detail": "d", "command_id": "h5"}, H_INSP)
        _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/orders", {
            "items": [{"code": "I1", "requirement": "重新校准"}],
            "command_id": "h6"}, H_INSP)
        code, perf = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/orders/ORD-001/submissions", {
            "measures": [{"item_code": "I1", "action": "口头说改了"}],
            "command_id": "h7"}, H_STAFF)
        self.assertEqual(code, 409)
        self.assertIn("receipt", perf)

        code, sub = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/orders/ORD-001/submissions", {
            "measures": [{"item_code": "I1", "action": "已重新校准",
                          "evidence_refs": ["ev://1"]}],
            "command_id": "h8"}, H_STAFF)
        self.assertEqual(code, 201, sub)

        code, ver = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/orders/ORD-001/verify", {
            "passed": True, "comment": "通过", "command_id": "h9"}, H_REV)
        self.assertEqual(code, 201, ver)

        code, trf = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/transfers", {
            "to_agency": "公安", "basis": "涉嫌虚假证明", "command_id": "h10"}, H_TRF)
        self.assertEqual(code, 201, trf)
        code, res = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/transfers/TRF-001/result", {
            "result": "filed", "docket_no": "077", "command_id": "h11"}, H_DESK)
        self.assertEqual(code, 201, res)

        code, close = _req("POST", f"{self.base}/api/cases/CASE-HTTP-1/close", {
            "command_id": "h12"}, H_REV)
        self.assertEqual(code, 201, close)

        # 重放视图与材料包
        code, replay = _req("GET", f"{self.base}/api/cases/CASE-HTTP-1/replay")
        self.assertEqual(code, 200)
        self.assertTrue(replay["chain_valid"])
        kinds = {t["kind"] for t in replay["timeline"]}
        self.assertIn("transfer.result_recorded", kinds)
        code, bundle = _req("GET", f"{self.base}/api/cases/CASE-HTTP-1/bundle")
        self.assertEqual(code, 200)
        EventStore.verify_bundle(bundle)

        # 回执核验
        token = self.server.svc.receipts.encode(sub["receipt"])
        code, body = _req("POST", f"{self.base}/api/receipts/verify",
                          {"receipt_token": token})
        self.assertEqual(code, 200)
        self.assertTrue(body["valid"])

    def test_concurrent_review_over_http_one_wins(self):
        H_STAFF = {"X-Actor": "zhaolei", "X-Role": "monitor_staff"}
        H_INSP = {"X-Actor": "wangjing", "X-Role": "eco_inspector"}
        H_REVA = {"X-Actor": "sunli", "X-Role": "eco_reviewer"}
        H_REVB = {"X-Actor": "zhoufang", "X-Role": "eco_reviewer"}
        _req("POST", f"{self.base}/api/samples", {
            "case_id": "CASE-HTTP-C", "sample_ref": "S-C",
            "org_id": "org-lvyuan", "person_id": "p-chenming",
            "instrument_id": "inst-wq-09", "sampled_at": "2026-03-20T09:00:00Z",
            "raw_data_ref": "raw://c", "command_id": "c0"}, H_STAFF)
        _req("POST", f"{self.base}/api/cases/CASE-HTTP-C/orders", {
            "items": [{"code": "I1", "requirement": "x"}], "command_id": "c1"}, H_INSP)
        _req("POST", f"{self.base}/api/cases/CASE-HTTP-C/orders/ORD-001/submissions", {
            "measures": [{"item_code": "I1", "action": "a", "evidence_refs": ["e"]}],
            "command_id": "c2"}, H_STAFF)
        code, case = _req("GET", f"{self.base}/api/cases/CASE-HTTP-C")
        head = case["case_head"]
        ca, cb = [], []
        t1 = threading.Thread(target=lambda: ca.append(_req(
            "POST", f"{self.base}/api/cases/CASE-HTTP-C/orders/ORD-001/verify",
            {"passed": True, "comment": "A", "command_id": "c3",
             "expected_case_hash": head}, H_REVA)))
        t2 = threading.Thread(target=lambda: cb.append(_req(
            "POST", f"{self.base}/api/cases/CASE-HTTP-C/orders/ORD-001/verify",
            {"passed": False, "comment": "B", "command_id": "c4",
             "expected_case_hash": head}, H_REVB)))
        t1.start(); t2.start(); t1.join(); t2.join()
        codes = {ca[0][0], cb[0][0]}
        self.assertEqual(codes, {201, 412})


if __name__ == "__main__":
    unittest.main()
