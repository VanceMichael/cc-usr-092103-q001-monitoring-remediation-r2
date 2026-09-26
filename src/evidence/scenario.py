"""可复现的端到端剧本：一次看似普通的校准时间矛盾如何被完整重放。

全程使用固定时钟；返回剧本中产生的关键标识，供测试断言与人工追查。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from . import access
from .app import build_service
from .clock import Clock
from .errors import DuplicateReport
from .replay import build_replay
from .store import ConcurrentModification, EventStore
from .service import RemediationService

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "registry.json"
SECRET = "demo-secret-key-0123456789abcdef"

CASE_ID = "CASE-2026-W031"
SAMPLE_REF = "SAMPLE-W20260320-07"
ORG = "org-lvyuan"
PERSON = "p-chenming"
INSTRUMENT = "inst-wq-09"


@dataclass
class ScenarioResult:
    svc: RemediationService
    sample_event_hash: str
    report_event_hash: str
    duplicate_receipt_a: dict
    duplicate_receipt_b: dict
    order_id: str
    perfunctory_receipt: dict
    accepted_receipt: dict
    verify_receipt: dict
    transfer_id: str
    bundle: dict
    transfer_result_receipt: dict
    replay: dict
    notes: list[str] = field(default_factory=list)


def run(*, store_path: Optional[Path] = None) -> ScenarioResult:
    clock = Clock.fixed("2026-03-20T10:00:00Z")
    svc = build_service(store_path=store_path, clock=clock, secret=SECRET,
                        registry_fixture=FIXTURE)
    notes: list[str] = []

    # 1) 采样登记：系统此刻只掌握 CAL-2025-1102，锚定"当时采用"的三件套
    r = svc.register_sample(
        case_id=CASE_ID, sample_ref=SAMPLE_REF, org_id=ORG, person_id=PERSON,
        instrument_id=INSTRUMENT, sampled_at="2026-03-20T09:00:00Z",
        raw_data_ref="oss://monitoring-raw/2026/W031/0900.dat",
        finding="COD 42mg/L，采样流程正常",
        actor="chenming", role=access.Role.MONITOR_STAFF, command_id="cmd-sample-01")
    sample_hash = next(e.event_hash for e in r.events
                       if e.event_type == "sample.registered")
    notes.append(f"采样登记入链 {sample_hash[:16]}，锚定当时有效的资质/授权/校准")

    # 2) 自查阶段上报
    clock.freeze("2026-03-21T09:00:00Z")
    r = svc.report_self_check(
        CASE_ID, phase="self_check_stage_1", content="已完成第一阶段摸底，台账已归档",
        actor="liuna", role=access.Role.MONITOR_LEAD, command_id="cmd-report-01")
    report_hash = r.event.event_hash

    # 3) 重复上报：拒绝入链，但拿到稳定回执；再次重复拿到同一张回执
    def duplicate(cmd: str) -> dict:
        try:
            svc.report_self_check(
                CASE_ID, phase="self_check_stage_1", content="重复提交",
                actor="liuna", role=access.Role.MONITOR_LEAD, command_id=cmd)
            raise AssertionError("重复上报必须被拒绝")
        except DuplicateReport as e:
            return e.receipt

    dup_a = duplicate("cmd-report-dup-1")
    dup_b = duplicate("cmd-report-dup-2")
    assert dup_a["receipt_id"] == dup_b["receipt_id"], "重复上报回执必须稳定"
    notes.append("重复上报两次返回同一张稳定回执（X-duplicate_report-…）")

    # 4) 自查补录：只能引用旧记录并解释原因
    svc.report_late_entry(
        CASE_ID, ref_event_hash=sample_hash,
        reason="采样当日纸质运维批注未随电子记录归档，事后从运维台账补入；"
               "不改变任何原始监测数值",
        facts={"maintenance_note": "采样前已完成探头清洗，记录见运维台账 YW-0317"},
        actor="chenming", role=access.Role.MONITOR_STAFF, command_id="cmd-late-01")

    # 5) 更正笔误：原事实保留在链上
    svc.correct_fact(
        CASE_ID, target_event_hash=report_hash, field_name="content",
        new_value="已完成第一阶段摸底，台账已归档（含运维批注补录核对）",
        reason="原文未说明已与补录批注核对，属表述补正，不涉及监测数据",
        actor="liuna", role=access.Role.MONITOR_LEAD, command_id="cmd-correct-01")

    # 6) 2026-04-02 机构补交了声称 3 月 1 日生效的校准证书（夹具中收录时间 4 月 2 日）
    clock.freeze("2026-04-03T09:30:00Z")
    svc.classify_reporting_fault(
        CASE_ID, kind="misreport",
        detail="仪器 inst-wq-09 于 4 月 2 日补交证书 CAL-2026-0315X，声称 3 月 1 日生效，"
               "与采样当时采用的 CAL-2025-1102 并存且有效期重叠，存在以追溯证书掩盖"
               "校准状态矛盾、误导监测结论的嫌疑",
        basis_event_hash=sample_hash,
        actor="wangjing", role=access.Role.ECO_INSPECTOR, command_id="cmd-fault-01")
    notes.append("执法机关据矛盾认定错报风险，处置规则随 fault 事件入链")

    # 7) 下达整改决定（30 日期限来自基准资料）
    clock.freeze("2026-04-05T10:00:00Z")
    r = svc.issue_rectification_order(
        CASE_ID,
        items=[
            {"code": "I1", "requirement": "inst-wq-09 立即停用并重新检定校准"},
            {"code": "I2", "requirement": "对采样操作人员开展校准状态核查培训"},
            {"code": "I3", "requirement": "重新提交 3 月 20 日采样的校准状态说明与原始数据"},
        ],
        actor="wangjing", role=access.Role.ECO_INSPECTOR, command_id="cmd-order-01")
    order_id = r.event.payload["order_id"]

    # 8) 第一次整改：敷衍（I2 缺佐证、I3 缺失）→ 记录退回，回执可核验
    clock.freeze("2026-04-08T11:00:00Z")
    from .errors import RectificationPerfunctory
    try:
        svc.submit_rectification(
            CASE_ID, order_id=order_id,
            measures=[
                {"item_code": "I1", "action": "已停用并送计量院",
                 "evidence_refs": ["oss://evidence/W031/stop-notice.pdf"]},
                {"item_code": "I2", "action": "口头提醒操作人员", "evidence_refs": []},
            ],
            actor="zhaolei", role=access.Role.MONITOR_STAFF, command_id="cmd-submit-01")
        raise AssertionError("敷衍整改必须被识别")
    except RectificationPerfunctory as e:
        perfunctory_receipt = e.receipt

    # 9) 逾期后完整整改：先重新校准（新证书即时收录，非追溯），逾期标记 + 受理
    clock.freeze("2026-05-05T09:30:00Z")
    svc.import_registry_bundle([
        {"type": "registry.instrument_calibrated",
         "payload": {
             "instrument_id": INSTRUMENT,
             "certificate_no": "CAL-2026-0505",
             "calibrating_org": "市计量测试研究院（虚构）",
             "calibrated_at": "2026-05-05T00:00:00Z",
             "valid_from": "2026-05-05T00:00:00Z",
             "valid_until": "2026-11-04T00:00:00Z",
             "recorded_at": "2026-05-05T09:30:00Z",
             "supersedes": ["CAL-2026-0315X", "CAL-2025-1102"],
             "note": "整改后停用送计量院重新检定校准，即时收录，替代此前两张争议证书"}},
    ], actor="registry.admin")
    clock.freeze("2026-05-08T09:00:00Z")
    r = svc.submit_rectification(
        CASE_ID, order_id=order_id,
        measures=[
            {"item_code": "I1", "action": "重新校准合格，取得 CAL-2026-0505",
             "evidence_refs": ["oss://evidence/W031/CAL-2026-0505.pdf"]},
            {"item_code": "I2", "action": "完成全员培训并考核",
             "evidence_refs": ["oss://evidence/W031/training-sign.pdf",
                               "oss://evidence/W031/exam-scores.xlsx"]},
            {"item_code": "I3", "action": "提交校准状态说明与原始数据比对报告",
             "evidence_refs": ["oss://evidence/W031/recheck-report.pdf"]},
        ],
        actor="zhaolei", role=access.Role.MONITOR_STAFF, command_id="cmd-submit-02")
    accepted_receipt = r.receipt

    # 10) 多人同时复核：A 成功；B 持旧链头被 OCC 拒绝；刷新后案件已闭环
    clock.freeze("2026-05-09T10:00:00Z")
    head_before = svc.store.case_head(CASE_ID)
    r = svc.verify_rectification(
        CASE_ID, order_id=order_id, passed=True,
        comment="三项措施均有佐证，逾期已记录，复核通过",
        reviewer="sunli", role=access.Role.ECO_REVIEWER,
        expected_case_hash=head_before, command_id="cmd-verify-01")
    verify_receipt = r.receipt
    try:
        svc.verify_rectification(
            CASE_ID, order_id=order_id, passed=False,
            comment="另一复核人持旧页面同时提交",
            reviewer="zhoufang", role=access.Role.ECO_REVIEWER,
            expected_case_hash=head_before, command_id="cmd-verify-02")
        raise AssertionError("并发复核必须有一方失败")
    except ConcurrentModification:
        notes.append("两名复核人同时提交：持旧链头者被乐观并发拒绝，先入链者生效")

    # 11) 涉嫌犯罪移送：随案导出可独立核验的材料包
    r = svc.transfer_suspected_crime(
        CASE_ID, to_agency="市公安局食品药品犯罪侦查支队（虚构）",
        basis="补交追溯式校准证书，涉嫌提供虚假证明文件、误导环境监测结论",
        actor="wangjing", role=access.Role.TRANSFER_OFFICER, command_id="cmd-transfer-01")
    transfer_id = r.event.payload["transfer_id"]
    bundle = svc.store.export_case_bundle(CASE_ID)
    EventStore.verify_bundle(bundle)

    # 12) 公检法回流真实结果：立案
    clock.freeze("2026-05-20T15:00:00Z")
    r = svc.record_transfer_result(
        CASE_ID, transfer_id=transfer_id, result="filed",
        docket_no="A（公）立字〔2026〕077号（虚构）",
        note="经审查认为涉嫌提供虚假证明文件，决定立案",
        actor="police-desk-02", role=access.Role.JUDICIAL_DESK,
        command_id="cmd-transfer-result-01")
    transfer_result_receipt = r.receipt

    # 13) 闭环
    svc.close_case(CASE_ID, actor="sunli", role=access.Role.ECO_REVIEWER,
                   command_id="cmd-close-01")

    replay = build_replay(svc.store, svc.registry, CASE_ID).to_dict()
    return ScenarioResult(
        svc=svc, sample_event_hash=sample_hash, report_event_hash=report_hash,
        duplicate_receipt_a=dup_a, duplicate_receipt_b=dup_b,
        order_id=order_id, perfunctory_receipt=perfunctory_receipt,
        accepted_receipt=accepted_receipt, verify_receipt=verify_receipt,
        transfer_id=transfer_id, bundle=bundle,
        transfer_result_receipt=transfer_result_receipt,
        replay=replay, notes=notes,
    )
