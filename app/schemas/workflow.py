"""DAG 工作流的请求 / 响应模型。"""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.core.enums import TaskStatus
from app.core.payload import PayloadGuardMixin


class WorkflowNode(PayloadGuardMixin, BaseModel):
    """图中的一个节点。node_id 仅用于本请求内描述边，入库后换成真实 task_id。"""

    node_id: str = Field(..., min_length=1, max_length=64, description="图内节点名，例如 A / extract")
    task_type: str = Field(..., min_length=1, max_length=128)
    payload: dict[str, Any] = Field(
        default_factory=dict,
        description="节点载荷。禁止顶层键 _vortex_sys；JSON 体积上限 256KiB",
    )
    priority: int = Field(default=0, ge=0, le=100)
    execute_at: datetime | None = None
    depends_on: list[str] = Field(
        default_factory=list,
        description="父节点 node_id 列表；空表示起始任务",
    )


class WorkflowCreateRequest(BaseModel):
    """一次性提交一张有向图。"""

    nodes: list[WorkflowNode] = Field(..., min_length=1, max_length=256)


class WorkflowTaskResult(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    node_id: str
    task_id: UUID
    status: TaskStatus
    task_type: str
    upstream_ids: list[UUID]
    downstream_ids: list[UUID]


class WorkflowCreateResponse(BaseModel):
    workflow_id: UUID
    tasks: list[WorkflowTaskResult]
