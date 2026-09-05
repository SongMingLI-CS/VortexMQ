"""
任务提交服务（Producer）。

顺序：PostgreSQL 提交 PENDING → 按 execute_at 选择 Stream 或延迟 ZSet。
Redis 投递失败不让接口 500：行已在库中，由 Outbox Sweeper 补偿。
"""

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.redis import schedule_wakeup
from app.crud.task import create_task
from app.models.task import TaskRecord
from app.models.tenant import Tenant
from app.schemas.task import TaskCreateRequest

logger = logging.getLogger(__name__)


async def submit_task(
    session: AsyncSession,
    tenant: Tenant,
    data: TaskCreateRequest,
) -> TaskRecord:
    """受理任务：先落库，再按是否到期唤醒 Worker。"""
    record = await create_task(session, tenant_id=tenant.id, data=data)
    try:
        channel = await schedule_wakeup(
            record.task_id,
            record.execute_at,
            tenant_id=record.tenant_id,
            priority=record.priority,
        )
        logger.info(
            "任务已投递: task_id=%s channel=%s execute_at=%s",
            record.task_id,
            channel,
            record.execute_at,
        )
        # 投递成功后刷新 updated_at，Outbox 只补偿真正陈旧的 PENDING。
        record.updated_at = utcnow()
        await session.commit()
    except Exception:
        logger.exception(
            "即时投递 Redis 失败，任务已落库，交由 Outbox Sweeper 补偿: task_id=%s",
            record.task_id,
        )
    return record
