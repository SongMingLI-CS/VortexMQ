"""
任务状态枚举。

放在 core 而不是 models，是为了让 schemas / models / services
都能引用同一套状态机，避免 Pydantic 层反向依赖 ORM。
"""

from enum import Enum


class TaskStatus(str, Enum):
    """任务生命周期。WAITING / CANCELED 服务于 DAG 工作流。"""

    PENDING = "PENDING"
    WAITING = "WAITING"  # 仍有上游未成功，不能进入 Stream
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    DLQ = "DLQ"
    CANCELED = "CANCELED"  # 上游进入 DLQ 后的级联取消
