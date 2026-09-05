from enum import StrEnum


class SignalState(StrEnum):
    NO_ACTION = "NO_ACTION"
    WATCH = "WATCH"
    ENTRY_CANDIDATE = "ENTRY_CANDIDATE"
    HOLD = "HOLD"
    REDUCE_CANDIDATE = "REDUCE_CANDIDATE"
    EXIT_CANDIDATE = "EXIT_CANDIDATE"
    BLOCKED_BY_RISK = "BLOCKED_BY_RISK"
    DATA_STALE = "DATA_STALE"


class DataMode(StrEnum):
    LIVE_LICENSED = "LIVE_LICENSED"
    DELAYED = "DELAYED"
    HISTORICAL = "HISTORICAL"
    DEMO_FIXTURE = "DEMO_FIXTURE"


class LicenseStatus(StrEnum):
    PENDING = "PENDING"
    APPROVED = "APPROVED"
    BLOCKED = "BLOCKED"
    EXPIRED = "EXPIRED"


class ModelStatus(StrEnum):
    CHAMPION = "CHAMPION"
    CHALLENGER = "CHALLENGER"
    EXPERIMENTAL = "EXPERIMENTAL"
    REJECTED = "REJECTED"
    ROLLED_BACK = "ROLLED_BACK"


class AlertSeverity(StrEnum):
    INFO = "INFO"
    WARN = "WARN"
    CRITICAL = "CRITICAL"


class AlertStatus(StrEnum):
    PENDING = "PENDING"
    SENT = "SENT"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    FAILED = "FAILED"
    SUPPRESSED = "SUPPRESSED"


class TaskStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    SKIPPED_DUPLICATE = "SKIPPED_DUPLICATE"


DISCLAIMER = (
    "本系统仅供本公司授权人员开展内部研究和风险监测。内容由统计模型和人工智能生成，"
    "可能错误、遗漏或滞后，不构成证券投资建议、收益承诺或交易指令；历史或回测表现不代表未来。"
    "任何投资决定须由授权人员结合独立资料、风险承受能力和持牌机构意见审慎作出，"
    "交易仅可在合法持牌券商端人工确认。"
)
