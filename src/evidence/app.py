"""应用装配：把时钟、事件存储、回执服务与业务服务装在一起，
并支持从基准资料夹具导入。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

from .clock import Clock
from .receipts import ReceiptService
from .service import RemediationService
from .store import EventStore

DEFAULT_SECRET = "eco-evidence-demo-secret-key-0123456789"


def build_service(*, store_path: Optional[Path] = None, clock: Optional[Clock] = None,
                  secret: Optional[str] = None, registry_fixture: Optional[Path] = None,
                  registry_actor: str = "registry.admin") -> RemediationService:
    clock = clock or Clock()
    store = EventStore(clock=clock, path=store_path)
    receipts = ReceiptService((secret or os.environ.get("EVIDENCE_SECRET") or DEFAULT_SECRET))
    svc = RemediationService(store, clock, receipts)
    if registry_fixture is not None and not store.case_events("@registry"):
        entries = json.loads(Path(registry_fixture).read_text(encoding="utf-8"))
        svc.import_registry_bundle(entries, actor=registry_actor)
    return svc
