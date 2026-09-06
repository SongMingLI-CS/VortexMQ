"""
跨租户任务查询（仅 Admin 面使用）。

普通数据访问一律带 tenant_id 谓词；这里的管理查询天然跨租户，因此只允许
携带 X-Admin-Key 的 Admin 端点进入，且不暴露任何 payload 字段。
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import as_utc
from app.core.enums import TaskStatus
from app.models.task import TaskRecord
from app.models.tenant import Tenant


def _build_conditions(
    *,
    status: TaskStatus | None,
    tenant_name: str | None,
    tenant_id: UUID | None,
    created_from: datetime | None,
    created_to: datetime | None,
) -> list:
    """把大厅筛选条件翻译成 SQL 谓词。租户用 name / id 两种方式均可。"""
    conditions = []
    if status is not None:
        conditions.append(TaskRecord.status == status)
    if tenant_name is not None:
        conditions.append(Tenant.name == tenant_name)
    if tenant_id is not None:
        conditions.append(TaskRecord.tenant_id == tenant_id)
    if created_from is not None:
        # 前端可能传无时区的 ISO；统一按 UTC 比较，避免和 DB 的 timestamptz 打架
        conditions.append(TaskRecord.created_at >= as_utc(created_from))
    if created_to is not None:
        conditions.append(TaskRecord.created_at <= as_utc(created_to))
    return conditions


async def list_admin_tasks(
    session: AsyncSession,
    *,
    limit: int,
    offset: int,
    status: TaskStatus | None = None,
    tenant_name: str | None = None,
    tenant_id: UUID | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
) -> list[tuple[TaskRecord, str]]:
    """任务大厅分页列表：最新创建在前，返回 (record, tenant_name) 二元组。"""
    conditions = _build_conditions(
        status=status,
        tenant_name=tenant_name,
        tenant_id=tenant_id,
        created_from=created_from,
        created_to=created_to,
    )
    stmt = (
        select(TaskRecord, Tenant.name)
        .join(Tenant, Tenant.id == TaskRecord.tenant_id)
        .where(*conditions)
        .order_by(TaskRecord.created_at.desc(), TaskRecord.task_id)
        .offset(offset)
        .limit(limit)
    )
    rows = (await session.execute(stmt)).all()
    return [(record, name) for record, name in rows]


async def count_admin_tasks(
    session: AsyncSession,
    *,
    status: TaskStatus | None = None,
    tenant_name: str | None = None,
    tenant_id: UUID | None = None,
    created_from: datetime | None = None,
    created_to: datetime | None = None,
) -> int:
    """与列表同条件的总数，用于分页 total。"""
    conditions = _build_conditions(
        status=status,
        tenant_name=tenant_name,
        tenant_id=tenant_id,
        created_from=created_from,
        created_to=created_to,
    )
    stmt = (
        select(func.count())
        .select_from(TaskRecord)
        .join(Tenant, Tenant.id == TaskRecord.tenant_id)
        .where(*conditions)
    )
    result = await session.execute(stmt)
    return int(result.scalar_one())


async def get_tenant_name(session: AsyncSession, tenant_id: UUID) -> str:
    """按租户 ID 查租户名；不存在返回空串（任务行不可能引用不存在的租户）。"""
    result = await session.execute(select(Tenant.name).where(Tenant.id == tenant_id))
    return result.scalar_one_or_none() or ""


async def list_workflow_tasks(
    session: AsyncSession, workflow_id: UUID
) -> list[TaskRecord]:
    """按 workflow_id 捞取整张 DAG 的任务（跨租户，仅 Admin 面使用）。"""
    stmt = (
        select(TaskRecord)
        .where(TaskRecord.workflow_id == workflow_id)
        .order_by(TaskRecord.created_at, TaskRecord.task_id)
    )
    return list((await session.execute(stmt)).scalars())
