#!/usr/bin/env python3
"""演示：一次看似普通的校准时间矛盾，如何被完整重放。

故事线（全部为虚构数据）：
1. 2026-03-16 机构登记一条 3-15 的采样记录；系统按当时已知资料
   判定仪器校准已于 3-01 过期，自动留痕并立案；
2. 机构补录自查报告（引用原采样记录并说明原因）；
3. 2026-04-01 机构补登一份声称 3-10 校准的证书——与采样登记时
   的系统认知矛盾，系统产生 calibration_time_contradiction 事件；
4. 执法方、司法方、机构任何一方都可在任意时刻重放案件，
   得到一致的状态与摘要哈希。
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.context import load_full_context
from src.evidence.reference import ReferenceData
from src.evidence.service import EvidenceService
from src.evidence.store import EventStore
from src.evidence.timeutil import parse_ts


class Clock:
    def __init__(self, start):
        self._t = parse_ts(start)

    def __call__(self):
        return self._t

    def set(self, ts):
        self._t = parse_ts(ts)


def show(title, obj):
    print(f"\n=== {title} ===")
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def main():
    clock = Clock("2026-03-16T09:00:00Z")
    ctx = load_full_context(Path(__file__).resolve().parent.parent
                            / "fixtures" / "context.json")
    svc = EvidenceService(EventStore(":memory:", clock=clock),
                          ReferenceData.from_context(ctx), clock=clock)

    print("1) 采样登记：仪器校准已过期，系统自动留痕")
    sampling = svc.record_sampling(
        institution_id="inst-huajian-001", operator_id="op-zhang-001",
        instrument_id="ins-gc-001", sampled_at="2026-03-15T08:00:00Z",
        scope="水和废水采样", items=["pH", "COD"],
        raw_data_hash="sha256:demo-raw", actor="张某(虚构)", role="机构人员",
        record_id="smp-demo")
    show("采样校验标记", sampling["flags"])

    print("2) 立案")
    svc.open_case(sampling_record_id="smp-demo", reason="仪器校准过期仍出具数据",
                  actor="王执法", role="执法员", case_id="case-demo")

    print("3) 机构补录自查报告（引用旧记录并说明原因）")
    clock.set("2026-03-20T10:00:00Z")
    svc.submit_self_check(case_id="case-demo", stage="self_check",
                          occurred_at="2026-03-18T15:00:00Z",
                          content={"说明": "仪器送检期间仍采样，愿接受处理"},
                          actor="张某(虚构)", role="机构人员", is_backfill=True,
                          backfill_reason="自查系统故障，依据纸质记录补录",
                          references=["smp-demo"])

    print("4) 迟到补登校准证书，矛盾显现")
    clock.set("2026-04-01T10:00:00Z")
    svc.record_calibration(instrument_id="ins-gc-001",
                           calibrated_at="2026-03-10T00:00:00Z",
                           valid_until="2026-09-10T00:00:00Z",
                           certificate_no="JJ-FIC-2026-0099",
                           actor="张某(虚构)", role="机构人员")

    show("3-16（采样次日）视角重放",
         svc.replay("case-demo", at="2026-03-16T23:59:59Z")["state"]["flags"])
    show("当前视角重放",
         svc.replay("case-demo")["state"]["flags"])

    then = svc.instrument_at("ins-gc-001", "2026-03-15T08:00:00Z",
                             knowledge_at="2026-03-16T09:00:00Z")
    now = svc.instrument_at("ins-gc-001", "2026-03-15T08:00:00Z")
    print(f"\n同一采样时刻 2026-03-15：")
    print(f"  以 3-16 系统认知判定：校准有效 = {then['calibrated']}")
    print(f"  以当前系统认知判定：校准有效 = {now['calibrated']}"
          f"（依据补登证书 {now['active_calibration']['certificate_no']}）")
    print("  两个答案都为真——差异本身即证据，完整保留在链上。")

    summary = svc.summary("case-demo")
    show("稳定摘要（digest 可复现）", {"digest": summary["digest"],
                                     "flags": summary["flags"],
                                     "head_hash": summary["head_hash"]})
    chain = svc.verify_chain()
    print(f"\n证据链校验：valid={chain['valid']}，已校验事件 {chain['checked']} 条")
    receipt = svc.verify_receipt(sampling["receipts"][0]["receipt_id"])
    print(f"采样回执核验：valid={receipt['valid']}")


if __name__ == "__main__":
    main()
