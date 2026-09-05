"""
管理面运维操作：DLQ 重放与强制取消。

两条路径都遵循「PostgreSQL 先行」：状态机变更在行锁事务里 COMMIT 之后，
才去碰 Redis（重放叫醒 / 取消摘延迟项）。Redis 瞬时失败不回滚已提交的状态，
投递缺失交由 Outbox Sweeper 补偿，避免管理操作把任务状态撕成两半。
"""

from __future__ import annotations

import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.enums import TaskStatus
from app.core.redis import get_redis, schedule_wakeup, tenant_delayed_key
from app.crud.task import get_task_for_update
from app.models.task import TaskRecord
from app.services.workflow import cancel_descendants

logger = logging.getLogger("vortexmq.admin")

_CANCELABLE = {TaskStatus.PENDING, TaskStatus.RUNNING, TaskStatus.WAITING}


class AdminTaskNotFoundError(ValueError):
    """task_id 在 PostgreSQL 中不存在，映射为 HTTP 404。"""

    def __init__(self, task_id: UUID) -> None:
        self.task_id = task_id
        super().__init__(f"任务不存在: {task_id}")


class AdminTaskStateError(ValueError):
    """当前状态不允许该操作，映射为 HTTP 409 Conflict。"""


async def replay_task(session: AsyncSession, task_id: UUID) -> TaskRecord:
    """DLQ -> PENDING：重置重试计数与错误信息，执行时间为现在并重新叫醒。"""
    task = await get_task_for_update(session, task_id)
    if task is None:
        raise AdminTaskNotFoundError(task_id)
    if task.status != TaskStatus.DLQ:
        raise AdminTaskStateError(
            f"仅 DLQ 任务可重放，当前状态: {task.status.value}"
        )

    task.status = TaskStatus.PENDING
    task.retry_count = 0
    task.error_msg = None
    task.execute_at = utcnow()
    task.updated_at = utcnow()
    await session.commit()

    try:
        await schedule_wakeup(
            task.task_id,
            task.execute_at,
            tenant_id=task.tenant_id,
            priority=task.priority,
        )
        logger.info("DLQ 任务已重放为 PENDING 并叫醒: task_id=%s", task.task_id)
    except Exception:
        # 已落库为 PENDING；Redis 投递失败由 Outbox Sweeper 在 stale 后补偿
        logger.exception(
            "重放后 Redis 叫醒失败，交由 Outbox 补偿: task_id=%s", task.task_id
        )
    return task


async def cancel_task(session: AsyncSession, task_id: UUID) -> TaskRecord:
    """把积压 / 执行中 / 等待上游的任务置为 CANCELED，并级联取消 WAITING 下游。"""
    task = await get_task_for_update(session, task_id)
    if task is None:
        raise AdminTaskNotFoundError(task_id)
    # 已取消视为幂等成功；终态（SUCCESS / DLQ / FAILED）不可逆转
    if task.status == TaskStatus.CANCELED:
        return task
    if task.status not in _CANCELABLE:
        raise AdminTaskStateError(
            f"仅 PENDING / RUNNING / WAITING 可取消，当前状态: {task.status.value}"
        )

    task.status = TaskStatus.CANCELED
    task.updated_at = utcnow()
    await cancel_descendants(session, task)
    await session.commit()
    logger.warning("任务已被管理面取消: task_id=%s", task.task_id)

    # 若仍躺在延迟 ZSet（execute_at 未到），直接摘除，Dispatcher 不再搬运；
    # Stream 里已存在的消息由 Worker 在读到 CANCELED 时 XACK 丢弃。
    try:
        redis = get_redis()
        member = f"{task.task_id}|{int(task.priority)}"
        await redis.zrem(tenant_delayed_key(task.tenant_id), member)
    except Exception:
        logger.exception("取消后清理延迟 ZSet 失败，交由 Worker 兜底: task_id=%s", task.task_id)
    return task
