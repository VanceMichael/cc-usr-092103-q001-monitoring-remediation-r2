"""领域错误。"""


class DomainError(Exception):
    """所有可预期的业务拒绝都继承它，便于 API 层统一映射。"""


class NotFound(DomainError):
    pass


class DuplicateReport(DomainError):
    """同阶段重复上报：返回首次受理的事件哈希与回执线索。"""

    def __init__(self, message: str, first_event_hash: str, receipt: dict | None = None):
        super().__init__(message)
        self.first_event_hash = first_event_hash
        self.receipt = receipt


class QualificationExited(DomainError):
    """机构资质已依法退出，不得新增采样/上报。"""


class UnauthorizedAtTime(DomainError):
    """操作人员在业务时刻未获授权。"""


class InstrumentInvalidAtTime(DomainError):
    """仪器在采样时刻无有效校准或已停用。"""


class LateEntryInvalid(DomainError):
    """补录必须引用旧记录并说明原因。"""


class CorrectionInvalid(DomainError):
    pass


class ReviewerConflict(DomainError):
    """复核人不能是整改提交人本人。"""


class RectificationPerfunctory(DomainError):
    def __init__(self, message: str, missing: list[str], receipt: dict | None = None):
        super().__init__(message)
        self.missing = missing
        self.receipt = receipt


class OrderClosed(DomainError):
    pass


class TransferStateError(DomainError):
    pass
