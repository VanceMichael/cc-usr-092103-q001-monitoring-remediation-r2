"""角色与处置权限矩阵。

权限按"处置动作"授予，角色之间相互制衡：复核不能由整改提交人本人完成，
涉嫌犯罪移送只能由生态环境部门移送岗发起，接收结果由跨部门回流登记。
"""

from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    REGISTRY_ADMIN = "registry_admin"   # 基准资料管理（机构/仪器/期限/权限台账维护）
    MONITOR_STAFF = "monitor_staff"     # 监测机构操作人员（采样、上报、补录、整改）
    MONITOR_LEAD = "monitor_lead"       # 机构质量负责人（机构内更正、申请复核）
    ECO_INSPECTOR = "eco_inspector"     # 生态环境执法人员（处置、移送、退查）
    ECO_REVIEWER = "eco_reviewer"       # 生态环境复核人员（独立复核）
    TRANSFER_OFFICER = "transfer_officer"  # 移送岗（涉嫌犯罪跨部门移送）
    JUDICIAL_DESK = "judicial_desk"     # 公安/检察回流登记（登记真实移送结果）
    AUDITOR = "auditor"                 # 只读审计（重放、核验、导包）


# action -> 允许的角色
PERMISSIONS: dict[str, set[Role]] = {
    "register.sample": {Role.MONITOR_STAFF, Role.MONITOR_LEAD},
    "report.self_check": {Role.MONITOR_STAFF, Role.MONITOR_LEAD},
    "report.late_entry": {Role.MONITOR_STAFF, Role.MONITOR_LEAD},
    "correct.fact": {Role.MONITOR_LEAD, Role.ECO_INSPECTOR},
    "issue.rectification_order": {Role.ECO_INSPECTOR},
    "submit.rectification": {Role.MONITOR_STAFF, Role.MONITOR_LEAD},
    "verify.rectification": {Role.ECO_REVIEWER},
    "classify.reporting_fault": {Role.ECO_INSPECTOR, Role.ECO_REVIEWER},
    "transfer.suspected_crime": {Role.TRANSFER_OFFICER, Role.ECO_INSPECTOR},
    "record.transfer_result": {Role.JUDICIAL_DESK},
    "withdraw.qualification": {Role.REGISTRY_ADMIN, Role.ECO_INSPECTOR},
    "read": {r for r in Role},
}


class PermissionDenied(Exception):
    pass


def require(action: str, role: Role) -> None:
    allowed = PERMISSIONS.get(action, set())
    if role not in allowed:
        raise PermissionDenied(f"角色 {role} 无权执行 {action}（允许：{sorted(r.value for r in allowed)}）")


def can(action: str, role: Role) -> bool:
    return role in PERMISSIONS.get(action, set())
