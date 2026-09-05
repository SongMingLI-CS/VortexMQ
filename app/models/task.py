"""
任务记录表。

任务状态以 PostgreSQL 为唯一事实来源。Redis Stream 负责即时唤醒，
ZSet 负责高精度延迟唤醒；Outbox Sweeper 补偿 Redis 投递失败。
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any

from sqlalchemy import DateTime, Enum, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base
from app.core.enums import TaskStatus

if TYPE_CHECKING:
    from app.models.tenant import Tenant


class TaskRecord(Base):
    """异步任务的持久化记录，是调度与追踪的唯一事实来源。"""

    __tablename__ = "task_records"
    __table_args__ = (
        # 租户内按状态过滤（控制台、对账）
        Index("ix_task_records_tenant_status", "tenant_id", "status"),
        # 后续 Worker 按状态 + 优先级抢占任务时使用
        Index(
            "ix_task_records_status_priority_created",
            "status",
            "priority",
            "created_at",
        ),
        # Outbox Sweeper：按状态 + 更新时间捞取过期 PENDING
        Index("ix_task_records_status_updated_at", "status", "updated_at"),
        Index("ix_task_records_workflow_id", "workflow_id"),
    )

    task_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="任务 ID，对外返回给调用方用于查询与追踪",
    )
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        comment="所属租户，级联删除保证租户下线时任务一并清理",
    )
    status: Mapped[TaskStatus] = mapped_column(
        Enum(TaskStatus, name="task_status", native_enum=True),
        nullable=False,
        default=TaskStatus.PENDING,
        comment="任务状态，新建时固定为 PENDING",
    )
    task_type: Mapped[str] = mapped_column(
        String(128),
        nullable=False,
        comment="任务类型，例如 email.send / order.sync，供后续路由到对应处理器",
    )
    payload: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        nullable=False,
        default=dict,
        comment="任务载荷，任意 JSON 对象",
    )
    priority: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="优先级，数值越大越先被 Worker 拾取",
    )
    retry_count: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
        comment="已失败次数，默认 0；达到 WORKER_MAX_RETRIES 后进入 DLQ",
    )
    error_msg: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
        comment="最近一次失败的异常堆栈，进入 DLQ 后便于人工排查",
    )
    execute_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="计划执行时间；未到期时只存在于延迟 ZSet，到期后由 Dispatcher 转入 Stream",
    )
    workflow_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        nullable=True,
        comment="所属 DAG 工作流；独立任务为空",
    )
    upstream_ids: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        comment="父任务 UUID 列表；全部 SUCCESS 后本任务才可从 WAITING 转为 PENDING",
    )
    downstream_ids: Mapped[list] = mapped_column(
        JSONB,
        nullable=False,
        default=list,
        comment="子任务 UUID 列表；本任务 SUCCESS 后尝试唤醒，DLQ 后级联 CANCELED",
    )
    result_data: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
        comment="成功执行后的返回值，供下游 XCom 注入与结果查询",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="创建时间（UTC）",
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
        comment="最后更新时间（UTC）",
    )

    tenant: Mapped[Tenant] = relationship(back_populates="tasks")
