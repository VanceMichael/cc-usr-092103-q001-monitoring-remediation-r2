"""读取并检查项目领域资料。"""

import json
from pathlib import Path

# 基础字段：保持与初版契约兼容
_BASE_REQUIRED = {"domain", "version", "facts", "sample_id"}
# 时间化证据链所需的参考资料字段
_REFERENCE_REQUIRED = {
    "generated_at",
    "institutions",
    "operators",
    "instruments",
    "stage_deadlines",
    "permission_matrix",
}


def load_context(path: Path) -> dict:
    """返回字段完整的领域资料。"""
    data = json.loads(path.read_text(encoding="utf-8"))
    if not _BASE_REQUIRED.issubset(data):
        raise ValueError("领域资料缺少必要字段")
    return data


def load_full_context(path: Path) -> dict:
    """返回包含参考资料（机构/人员/仪器/期限/权限）的完整领域资料。"""
    data = load_context(path)
    missing = _REFERENCE_REQUIRED - data.keys()
    if missing:
        raise ValueError(f"领域资料缺少参考资料字段: {sorted(missing)}")
    return data
