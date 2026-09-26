"""测试共享工具。"""

from pathlib import Path

from src.evidence.app import build_service
from src.evidence.clock import Clock
from src.evidence.scenario import FIXTURE, SECRET


def make_service(tmp_path: Path | None = None, when: str = "2026-03-20T10:00:00Z",
                 with_fixture: bool = True):
    clock = Clock.fixed(when)
    return build_service(
        store_path=tmp_path, clock=clock, secret=SECRET,
        registry_fixture=FIXTURE if with_fixture else None)
