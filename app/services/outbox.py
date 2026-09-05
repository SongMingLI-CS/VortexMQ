"""
轻量级发件箱补偿（Outbox Sweeper）。

补偿对象：
- PG 已是 PENDING，但 Redis 投递（XADD 或 ZADD）失败的任务
- 租约过期仍停在 RUNNING、且 PEL 可能已丢失的僵尸任务

投递租约是 updated_at：短事务里刷新后立即 COMMIT，再在事务外碰 Redis。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from uuid import UUID

from app.core.clock import utcnow
from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.enums import TaskStatus
from app.core.redis import schedule_wakeup
from app.crud.task import claim_stale_pending_tasks, claim_stale_running_tasks

logger = logging.getLogger("vortexmq.outbox")


def _running_stale_seconds() -> int:
    """与 Worker CAS / XAUTOCLAIM 使用同一租约窗口，至少 1 秒。"""
    return max(1, settings.WORKER_CLAIM_IDLE_MS // 1000)


async def _wakeup_outside_txn(
    claimed: list[tuple[UUID, datetime, UUID, int]],
) -> int:
    published = 0
    for task_id, execute_at, tenant_id, priority in claimed:
        try:
            await schedule_wakeup(
                task_id, execute_at, tenant_id=tenant_id, priority=priority
            )
            published += 1
        except Exception:
            logger.exception(
                "Outbox 补偿投递失败，updated_at 已刷新，下轮 stale 后再试: task_id=%s",
                task_id,
            )
    return published


async def sweep_pending_tasks() -> int:
    """扫描并补偿投递一轮过期 PENDING，返回成功写入 Redis 的条数。"""
    claimed: list[tuple[UUID, datetime, UUID, int]] = []

    async with AsyncSessionLocal() as session:
        async with session.begin():
            tasks = await claim_stale_pending_tasks(
                session,
                stale_seconds=settings.OUTBOX_STALE_SECONDS,
                limit=settings.OUTBOX_BATCH_SIZE,
            )
            if not tasks:
                return 0

            now = utcnow()
            for task in tasks:
                task.updated_at = now
                claimed.append((task.task_id, task.execute_at, task.tenant_id, task.priority))

    return await _wakeup_outside_txn(claimed)


async def reclaim_stale_running_tasks() -> int:
    """把租约过期的 RUNNING 改回 PENDING，再按 execute_at 重新叫醒。"""
    claimed: list[tuple[UUID, datetime, UUID, int]] = []

    async with AsyncSessionLocal() as session:
        async with session.begin():
            tasks = await claim_stale_running_tasks(
                session,
                stale_seconds=_running_stale_seconds(),
                limit=settings.OUTBOX_BATCH_SIZE,
            )
            if not tasks:
                return 0

            now = utcnow()
            for task in tasks:
                # PEL 丢失时 RUNNING 永远不会被 XAUTOCLAIM；改回 PENDING 交给唤醒管道。
                task.status = TaskStatus.PENDING
                task.updated_at = now
                claimed.append((task.task_id, task.execute_at, task.tenant_id, task.priority))
                logger.warning(
                    "回收僵死 RUNNING -> PENDING: task_id=%s stale=%ss",
                    task.task_id,
                    _running_stale_seconds(),
                )

    return await _wakeup_outside_txn(claimed)


async def sweep_outbox_once() -> int:
    published = await sweep_pending_tasks()
    published += await reclaim_stale_running_tasks()
    if published:
        logger.info("Outbox 本轮补偿投递 %s 条任务", published)
    return published


async def run_outbox_sweeper() -> None:
    """API 进程内的后台循环；随 FastAPI Lifespan 启动 / 取消。"""
    logger.info(
        "Outbox Sweeper 已启动 interval=%ss stale=%ss running_stale=%ss",
        settings.OUTBOX_SWEEP_INTERVAL_SECONDS,
        settings.OUTBOX_STALE_SECONDS,
        _running_stale_seconds(),
    )
    try:
        while True:
            try:
                await sweep_outbox_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Outbox 扫描轮次异常，将在下一周期重试")
            await asyncio.sleep(settings.OUTBOX_SWEEP_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("Outbox Sweeper 正在停止")
        raise
