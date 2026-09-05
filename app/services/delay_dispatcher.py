"""
Delay Dispatcher：把 ZSet 中到期的任务转入 Stream。

高精度调度靠 Redis ZSet 的 score，而不是扫 PostgreSQL。
PG 仍保留 execute_at，作为 Redis 丢失后的补偿依据。
"""

from __future__ import annotations

import asyncio
import logging

from app.core.config import settings
from app.core.redis import dispatch_due_delayed_tasks

logger = logging.getLogger("vortexmq.dispatcher")


async def run_delay_dispatcher() -> None:
    """每秒按租户 EVAL 一次；仅控制面 Leader 运行。"""
    logger.info(
        "Delay Dispatcher 已启动 interval=%ss batch=%s delayed_key=%s",
        settings.DELAY_DISPATCH_INTERVAL_SECONDS,
        settings.DELAY_DISPATCH_BATCH_SIZE,
        "{tenant}:" + settings.REDIS_DELAYED_KEY,
    )
    try:
        while True:
            try:
                moved = await dispatch_due_delayed_tasks()
                if moved:
                    logger.info("到期任务已转入 Stream: count=%s", len(moved))
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Delay Dispatcher 本轮失败，将在下一秒重试")
            await asyncio.sleep(settings.DELAY_DISPATCH_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        logger.info("Delay Dispatcher 正在停止")
        raise
