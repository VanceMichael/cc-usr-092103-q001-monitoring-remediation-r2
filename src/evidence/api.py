"""证据链服务命令行入口。

用法：
    python -m src.evidence.api --context fixtures/context.json \
        --db evidence.db --port 8080
"""

from __future__ import annotations

import argparse
from http.server import ThreadingHTTPServer
from pathlib import Path

from ..context import load_full_context
from .reference import ReferenceData
from .routes import make_handler
from .service import EvidenceService
from .store import EventStore


def build_service(context_path: str, db_path: str = ":memory:") -> EvidenceService:
    ctx = load_full_context(Path(context_path))
    store = EventStore(db_path)
    return EvidenceService(store, ReferenceData.from_context(ctx))


def run_server(context_path: str, db_path: str, port: int) -> ThreadingHTTPServer:
    service = build_service(context_path, db_path)
    return ThreadingHTTPServer(("127.0.0.1", port), make_handler(service))


def main(argv=None):
    parser = argparse.ArgumentParser(description="生态环境整改取证协同后台")
    parser.add_argument("--context", default="fixtures/context.json",
                        help="领域资料路径")
    parser.add_argument("--db", default="evidence.db", help="SQLite 事件库路径")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)
    server = run_server(args.context, args.db, args.port)
    print(f"证据链后台已启动: http://127.0.0.1:{args.port} "
          f"(context={args.context}, db={args.db})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()
