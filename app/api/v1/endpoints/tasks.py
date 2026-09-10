"""任务接收与结果查询接口。"""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_tenant
from app.core.database import get_db
from app.core.enums import TaskStatus
from app.crud.task import get_task_for_tenant
from app.models.tenant import Tenant
from app.schemas.task import TaskCreateRequest, TaskCreateResponse, TaskResultResponse
from app.services.task_service import submit_task

router = APIRouter()

_IN_FLIGHT = {TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.WAITING}
_FAILED = {TaskStatus.FAILED, TaskStatus.DLQ, TaskStatus.CANCELED}

# 终态但没有 error_msg 时的兜底说明：不能把「已取消」说成「执行失败」。
_FAILURE_MESSAGES = {
    TaskStatus.FAILED: "任务执行失败",
    TaskStatus.DLQ: "任务重试次数耗尽，已进入死信队列（可经管理面重放）",
    TaskStatus.CANCELED: "任务已被取消，未产出结果",
}


@router.post(
    "",
    response_model=TaskCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="提交异步任务",
    description="将任务持久化为 PENDING。已到期写入 Stream，未到期写入延迟 ZSet。",
    responses={
        413: {"description": "payload 超过 256KiB"},
    },
)
async def create_task(
    payload: TaskCreateRequest,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant),
) -> TaskCreateResponse:
    """接收任务参数，写入 PostgreSQL，初始状态为 PENDING。"""
    record = await submit_task(db, tenant, payload)
    return TaskCreateResponse.model_validate(record)


@router.get(
    "/{task_id}/result",
    response_model=TaskResultResponse,
    summary="查询任务执行结果",
    description="SUCCESS 返回 result_data；未完成返回 202；失败/取消返回 400 与 error_msg。",
    responses={
        200: {"description": "执行成功"},
        202: {"description": "任务仍在处理"},
        400: {"description": "任务失败或已取消"},
        404: {"description": "任务不存在"},
    },
)
async def get_task_result(
    task_id: UUID,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant),
) -> TaskResultResponse | JSONResponse:
    """按租户隔离查询 Result Backend；跨租户与不存在统一 404。"""
    record = await get_task_for_tenant(db, task_id, tenant.id)
    if record is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="任务不存在")

    if record.status == TaskStatus.SUCCESS:
        return TaskResultResponse(
            task_id=record.task_id,
            status=record.status,
            result_data=record.result_data,
        )

    if record.status in _IN_FLIGHT:
        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "task_id": str(record.task_id),
                "status": record.status.value,
                "message": "任务仍在处理中",
            },
        )

    if record.status in _FAILED:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={
                "task_id": str(record.task_id),
                "status": record.status.value,
                "error_msg": record.error_msg
                or _FAILURE_MESSAGES.get(record.status, "任务未产出结果"),
            },
        )

    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail=f"无法查询结果，当前状态为 {record.status.value}",
    )
