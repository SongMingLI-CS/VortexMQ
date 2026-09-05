"""
Admin 管理面 API：任务大厅 / DLQ 重放 / 强制取消 / Worker 节点监控。

整组路由通过 router 级依赖强制 X-Admin-Key 鉴权（app.api.deps.get_admin），
不经过租户 X-API-Key 解析——这是有意为之的跨租户运维通道。
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_admin
from app.core.database import get_db
from app.core.enums import TaskStatus
from app.core.redis import list_worker_heartbeats
from app.crud.admin import count_admin_tasks, get_tenant_name, list_admin_tasks
from app.models.task import TaskRecord
from app.schemas.admin import (
    AdminTaskItem,
    AdminTaskListResponse,
    AdminWorkerInfo,
)
from app.services.admin import (
    AdminTaskNotFoundError,
    AdminTaskStateError,
    cancel_task,
    replay_task,
)

router = APIRouter(dependencies=[Depends(get_admin)])


def _to_item(record: TaskRecord, tenant_name: str) -> AdminTaskItem:
    """把 (ORM 任务, 租户名) 组装成大厅展示模型。"""
    return AdminTaskItem(
        task_id=record.task_id,
        tenant_id=record.tenant_id,
        tenant_name=tenant_name,
        status=record.status,
        task_type=record.task_type,
        priority=record.priority,
        retry_count=record.retry_count,
        workflow_id=record.workflow_id,
        error_msg=record.error_msg,
        execute_at=record.execute_at,
        created_at=record.created_at,
        updated_at=record.updated_at,
    )


@router.get(
    "/tasks",
    response_model=AdminTaskListResponse,
    summary="任务大厅：跨租户分页列表",
    description="按状态 / 租户名 / 租户 ID / 创建时间范围筛选，最新创建在前。",
)
async def list_tasks(
    status_filter: TaskStatus | None = Query(
        default=None, alias="status", description="按任务状态过滤"
    ),
    tenant_name: str | None = Query(
        default=None, max_length=128, description="按租户名精确过滤"
    ),
    tenant_id: UUID | None = Query(default=None, description="按租户 ID 过滤"),
    created_from: datetime | None = Query(
        default=None, description="创建时间下限（含），ISO 8601"
    ),
    created_to: datetime | None = Query(
        default=None, description="创建时间上限（含），ISO 8601"
    ),
    page: int = Query(default=1, ge=1, description="页码，从 1 开始"),
    page_size: int = Query(
        default=20, ge=1, le=100, description="每页条数，上限 100"
    ),
    db: AsyncSession = Depends(get_db),
) -> AdminTaskListResponse:
    total = await count_admin_tasks(
        db,
        status=status_filter,
        tenant_name=tenant_name,
        tenant_id=tenant_id,
        created_from=created_from,
        created_to=created_to,
    )
    offset = (page - 1) * page_size
    rows = await list_admin_tasks(
        db,
        limit=page_size,
        offset=offset,
        status=status_filter,
        tenant_name=tenant_name,
        tenant_id=tenant_id,
        created_from=created_from,
        created_to=created_to,
    )
    return AdminTaskListResponse(
        items=[_to_item(record, name) for record, name in rows],
        total=total,
        page=page,
        page_size=page_size,
    )


@router.post(
    "/tasks/{task_id}/replay",
    response_model=AdminTaskItem,
    summary="重放 DLQ 任务",
    description="仅 DLQ 任务可重放：重置重试次数并置回 PENDING，立即重新叫醒 Worker。",
    responses={
        404: {"description": "任务不存在"},
        409: {"description": "仅 DLQ 状态可重放"},
    },
)
async def replay_dlq_task(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> AdminTaskItem:
    try:
        record = await replay_task(db, task_id)
    except AdminTaskNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在"
        ) from exc
    except AdminTaskStateError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from exc
    # 重放是跨租户运维，回执里需要租户名方便大厅展示
    tenant_name = await get_tenant_name(db, record.tenant_id)
    return _to_item(record, tenant_name)


@router.post(
    "/tasks/{task_id}/cancel",
    response_model=AdminTaskItem,
    summary="强制取消任务",
    description="PENDING / RUNNING / WAITING 可取消；WAITING 下游会级联 CANCELED。",
    responses={
        404: {"description": "任务不存在"},
        409: {"description": "当前状态不可取消"},
    },
)
async def cancel_task_endpoint(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> AdminTaskItem:
    try:
        record = await cancel_task(db, task_id)
    except AdminTaskNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在"
        ) from exc
    except AdminTaskStateError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT, detail=str(exc)
        ) from exc
    tenant_name = await get_tenant_name(db, record.tenant_id)
    return _to_item(record, tenant_name)


@router.get(
    "/workers",
    response_model=list[AdminWorkerInfo],
    summary="Worker 节点监控",
    description="读取 Redis 心跳 ZSet 与负载 Hash，返回心跳仍有效的 Worker 及其瞬时负载。",
)
async def list_workers() -> list[AdminWorkerInfo]:
    heartbeats = await list_worker_heartbeats()
    return [AdminWorkerInfo(**item) for item in heartbeats]
