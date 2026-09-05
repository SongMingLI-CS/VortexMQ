"""
任务相关的请求 / 响应校验模型。

API 层只与 Schema 交互，不直接暴露 ORM，避免把数据库字段泄漏给调用方。
"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.core.enums import TaskStatus
from app.core.payload import PayloadGuardMixin


class TaskCreateRequest(PayloadGuardMixin, BaseModel):
    """POST /api/v1/tasks 请求体。"""

    task_type: str = Field(
        ...,
        min_length=1,
        max_length=128,
        description="任务类型，例如 email.send / order.sync",
        examples=["email.send"],
    )
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="任务载荷。禁止顶层键 _vortex_sys；JSON 体积上限 256KiB",
        examples=[{"to": "ops@example.com", "subject": "hello"}],
    )
    priority: int = Field(
        default=0,
        ge=0,
        le=100,
        description="优先级，0-100，数值越大越优先",
    )
    execute_at: datetime | None = Field(
        default=None,
        description="计划执行时间（UTC）。为空或已到期则立即入 Stream，否则进入延迟 ZSet",
    )


class TaskCreateResponse(BaseModel):
    """任务受理成功后的回执。"""

    model_config = ConfigDict(from_attributes=True)

    task_id: UUID
    tenant_id: UUID
    status: TaskStatus
    task_type: str
    priority: int
    execute_at: datetime
    created_at: datetime


class TaskResultResponse(BaseModel):
    """GET /api/v1/tasks/{task_id}/result 成功时的回执。"""

    model_config = ConfigDict(from_attributes=True)

    task_id: UUID
    status: TaskStatus
    result_data: dict[str, Any] | None = None
