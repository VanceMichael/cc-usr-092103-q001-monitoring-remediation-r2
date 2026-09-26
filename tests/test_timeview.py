"""时间化基准资料投影测试：当时有效、事后补录不穿越、资质退出、校准矛盾。"""

import unittest

from src.evidence.clock import parse_ts
from tests._helpers import make_service


class TemporalRegistryTest(unittest.TestCase):
    def test_as_of_queries_return_material_valid_at_sample_time(self):
        svc = make_service(None)
        reg = svc.registry
        at = parse_ts("2026-03-20T09:00:00Z")

        org = reg.org_at("org-lvyuan", at)
        self.assertIsNotNone(org)
        self.assertEqual(org.status, "active")
        self.assertEqual(org.qualification_no, "Q-2024-LY-018")

        person = reg.person_at("p-chenming", at, "org-lvyuan")
        self.assertIsNotNone(person)

        cal = reg.instrument_at("inst-wq-09", at, known_at=parse_ts("2026-03-20T10:00:00Z"))
        self.assertEqual(cal.certificate_no, "CAL-2025-1102")

    def test_backdated_certificate_not_visible_before_recording(self):
        """4 月 2 日才收录、声称 3 月 1 日生效的证书，在 3 月 20 日不可见。"""
        svc = make_service(None)
        at = parse_ts("2026-03-20T09:00:00Z")

        # 不带认知时点（"今天"的投影）：后补证书会胜出
        today = svc.registry.instrument_at("inst-wq-09", at)
        self.assertEqual(today.certificate_no, "CAL-2026-0315X")

        # 以 3 月 20 日登记时刻为认知时点：只能看到 CAL-2025-1102
        known = svc.registry.instrument_at("inst-wq-09", at, known_at=at)
        self.assertEqual(known.certificate_no, "CAL-2025-1102")

    def test_org_renewal_as_of_boundary(self):
        svc = make_service(None)
        # 新泽：旧证 2025-05-31 到期，新证 2025-06-01 延续
        old = svc.registry.org_at("org-xinze", parse_ts("2025-05-30T00:00:00Z"))
        new = svc.registry.org_at("org-xinze", parse_ts("2025-06-02T00:00:00Z"))
        edge = svc.registry.org_at("org-xinze", parse_ts("2025-05-31T23:59:58Z"))
        expired = svc.registry.org_at("org-xinze", parse_ts("2025-05-31T23:59:59Z"))
        self.assertEqual(old.qualification_no, "Q-2023-XZ-107")
        self.assertEqual(new.qualification_no, "Q-2025-XZ-107R")
        self.assertIn("soil", new.scope)
        self.assertEqual(edge.qualification_no, "Q-2023-XZ-107")
        self.assertIsNone(expired)  # 半开区间：到期瞬间旧证失效、新证未生效

    def test_withdrawn_qualification_blocks_future_samples(self):
        from src.evidence.errors import QualificationExited
        from src.evidence import access
        svc = make_service(None, when="2026-03-01T10:00:00Z")
        # 恒信 2026-02-10 已撤销
        with self.assertRaises(QualificationExited):
            svc.register_sample(
                case_id="C-W", sample_ref="S-W", org_id="org-hengxin",
                person_id="p-chenming", instrument_id="inst-wq-09",
                sampled_at="2026-03-01T08:00:00Z", raw_data_ref="raw://w",
                actor="chenming", role=access.Role.MONITOR_STAFF, command_id="w1")

    def test_runtime_withdrawal_blocks_only_later_samples(self):
        from src.evidence import access
        from src.evidence.errors import QualificationExited
        svc = make_service(None, when="2026-03-20T10:00:00Z")
        svc.register_sample(
            case_id="C-R", sample_ref="S-R", org_id="org-lvyuan",
            person_id="p-chenming", instrument_id="inst-wq-09",
            sampled_at="2026-03-20T09:00:00Z", raw_data_ref="raw://r",
            actor="chenming", role=access.Role.MONITOR_STAFF, command_id="r1")
        svc.clock.freeze("2026-03-22T10:00:00Z")
        svc.withdraw_qualification(
            "org-lvyuan", reason="执法检查发现严重问题，依法撤销",
            actor="wangjing", role=access.Role.ECO_INSPECTOR,
            at="2026-03-22T00:00:00Z")
        svc.clock.freeze("2026-03-23T10:00:00Z")
        with self.assertRaises(QualificationExited):
            svc.register_sample(
                case_id="C-R2", sample_ref="S-R2", org_id="org-lvyuan",
                person_id="p-chenming", instrument_id="inst-wq-09",
                sampled_at="2026-03-23T09:00:00Z", raw_data_ref="raw://r2",
                actor="chenming", role=access.Role.MONITOR_STAFF, command_id="r2")
        # 3 月 20 日的历史采样记录仍在，且当时资质有效（投影可重放）
        org = svc.registry.org_at("org-lvyuan", parse_ts("2026-03-20T09:00:00Z"))
        self.assertEqual(org.status, "active")

    def test_unauthorized_operator_rejected(self):
        from src.evidence import access
        from src.evidence.errors import UnauthorizedAtTime
        svc = make_service(None)
        # 赵磊 2025-01-01 才授权，2024 年采样不合法
        with self.assertRaises(UnauthorizedAtTime):
            svc.register_sample(
                case_id="C-A", sample_ref="S-A", org_id="org-lvyuan",
                person_id="p-zhaolei", instrument_id="inst-wq-09",
                sampled_at="2024-12-31T09:00:00Z", raw_data_ref="raw://a",
                actor="zhaolei", role=access.Role.MONITOR_STAFF, command_id="a1")

    def test_expired_instrument_without_calibration_rejected(self):
        from src.evidence import access
        from src.evidence.errors import InstrumentInvalidAtTime
        svc = make_service(None)
        # inst-kq-12 校准 2026-02-19 到期，3 月采样无覆盖
        with self.assertRaises(InstrumentInvalidAtTime):
            svc.register_sample(
                case_id="C-I", sample_ref="S-I", org_id="org-lvyuan",
                person_id="p-chenming", instrument_id="inst-kq-12",
                sampled_at="2026-03-20T09:00:00Z", raw_data_ref="raw://i",
                actor="chenming", role=access.Role.MONITOR_STAFF, command_id="i1")

    def test_calibration_anomalies_detect_overlap_and_backdate(self):
        svc = make_service(None)
        anomalies = svc.registry.all_calibration_anomalies(
            {"inst-wq-09": [parse_ts("2026-03-20T09:00:00Z")]})
        kinds = {(a.instrument_id, a.kind) for a in anomalies}
        self.assertIn(("inst-wq-09", "overlap"), kinds)
        self.assertIn(("inst-wq-09", "backdated"), kinds)
        backdated = [a for a in anomalies if a.kind == "backdated"]
        self.assertTrue(any("CAL-2026-0315X" in a.detail for a in backdated))
        # 正常归档延迟（1102 仅迟 3 天）不应报 backdated
        self.assertFalse(any("CAL-2025-1102" in a.detail for a in backdated))


if __name__ == "__main__":
    unittest.main()
