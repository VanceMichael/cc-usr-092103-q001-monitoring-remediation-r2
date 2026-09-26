"""命令行入口。

用法：
    python -m src.evidence.cli demo [--store PATH]      运行完整演示剧本
    python -m src.evidence.cli serve [--port 8080]      启动 HTTP API
    python -m src.evidence.cli replay CASE_ID [--store PATH]
    python -m src.evidence.cli verify [--store PATH]    全库哈希链校验
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .app import build_service
from .clock import Clock
from .scenario import FIXTURE, SECRET, run
from .store import ChainTampered, EventStore
from .replay import build_replay


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def cmd_demo(args) -> int:
    res = run(store_path=Path(args.store) if args.store else None)
    print("=" * 72)
    print("整改取证协同后台 · 端到端演示：校准时间矛盾完整重放")
    print("=" * 72)
    for i, note in enumerate(res.notes, 1):
        print(f"{i}. {note}")
    print("-" * 72)
    print(f"案件：{res.replay['case_id']}（采样 {res.replay['sample_ref']}）")
    print(f"案件链头：{res.replay['case_head']}")
    print(f"全局链头：{res.replay['global_head']}")
    print(f"链完整：{res.replay['chain_valid']}")
    print(f"时间线条目：{len(res.replay['timeline'])}")
    print("校准矛盾检测：")
    for a in res.replay["anomalies"]:
        print(f"  - [{a['kind']}] {a['detail']}")
    snap = res.replay["calibration_context"]["instrument"]
    print(f"采样当时采用证书：{snap['certificate_no']}（事件 {snap['anchored_calibration_event'][:16]}）")
    print(f"今天投影解析证书：{snap['certificate_resolves_today']}；矛盾标记：{snap['contradiction']}")
    print(f"涉嫌犯罪移送：{res.transfer_id} → 回流结果已登记并立案")
    print(f"重放视图摘要：{res.replay['digest']}")
    print("-" * 72)
    print("时间线：")
    for item in res.replay["timeline"]:
        print(f"  #{item['case_seq']:02d} {item['occurred_at']} {item['label']}｜{item['summary']}")
        print(f"       hash={item['event_hash'][:16]} prev={item['prev_case_hash'][:12]}")
    if args.out:
        Path(args.out).write_text(
            json.dumps({"replay": res.replay, "bundle": res.bundle},
                       ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n完整重放与材料包已写出：{args.out}")
    return 0


def cmd_serve(args) -> int:
    from .api import serve
    server = serve(args.host, args.port,
                   store_path=Path(args.store) if args.store else None,
                   registry_fixture=FIXTURE)
    print(f"整改取证协同后台已启动：http://{args.host}:{args.port}", file=sys.stderr)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


def cmd_replay(args) -> int:
    svc = build_service(store_path=Path(args.store) if args.store else None,
                        clock=Clock(), secret=SECRET, registry_fixture=FIXTURE)
    view = build_replay(svc.store, svc.registry, args.case_id)
    _print(view.to_dict())
    return 0


def cmd_verify(args) -> int:
    store_path = Path(args.store) if args.store else None
    if store_path is None or not Path(store_path).exists():
        print("没有持久化事件文件，内存演示库无需校验", file=sys.stderr)
        return 0
    clock = Clock()
    try:
        EventStore(clock=clock, path=Path(store_path)).verify()
    except ChainTampered as exc:
        print(f"哈希链校验失败：{exc}", file=sys.stderr)
        return 2
    print("哈希链完整：全部事件哈希、双轨链接续一致")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="evidence", description="整改取证协同后台")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_demo = sub.add_parser("demo", help="运行端到端演示剧本")
    p_demo.add_argument("--store", help="事件 JSONL 持久化路径（缺省为纯内存）")
    p_demo.add_argument("--out", help="把完整重放视图与移送材料包写出到 JSON 文件")
    p_demo.set_defaults(func=cmd_demo)

    p_serve = sub.add_parser("serve", help="启动 HTTP JSON API")
    p_serve.add_argument("--host", default="127.0.0.1")
    p_serve.add_argument("--port", type=int, default=8080)
    p_serve.add_argument("--store", help="事件 JSONL 持久化路径")
    p_serve.set_defaults(func=cmd_serve)

    p_replay = sub.add_parser("replay", help="按案件输出执法重放视图")
    p_replay.add_argument("case_id")
    p_replay.add_argument("--store")
    p_replay.set_defaults(func=cmd_replay)

    p_verify = sub.add_parser("verify", help="校验持久化哈希链")
    p_verify.add_argument("--store", required=True)
    p_verify.set_defaults(func=cmd_verify)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
