"""
Admin 管理面的请求 / 响应模型。

跨租户运维展示需要的字段比普通任务结果更宽（含 tenant_name、error_msg、
重试次数等），但仍不返回 payload——避免在任务大厅里批量泄漏敏感载荷。
"""

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.core.enums import TaskStatus


class AdminTaskItem(BaseModel):
    """任务大厅里的一行任务。"""

    model_config = ConfigDict(from_attributes=True)

    task_id: UUID
    tenant_id: UUID
    tenant_name: str
    status: TaskStatus
    task_type: str
    priority: int
    retry_count: int
    workflow_id: UUID | None = None
    error_msg: str | None = None
    execute_at: datetime
    created_at: datetime
    updated_at: datetime


class AdminTaskListResponse(BaseModel):
    """分页任务列表。"""

    items: list[AdminTaskItem]
    total: int
    page: int
    page_size: int


class AdminWorkerInfo(BaseModel):
    """存活 Worker 节点及其瞬时负载。"""

    name: str
    hostname: str | None = None
    pid: int | None = None
    started_at: datetime | None = None
    last_seen: datetime
    in_flight: int = 0
