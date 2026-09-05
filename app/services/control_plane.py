"""
控制面：只有 Leader 跑 Outbox Sweeper 与 Delay Dispatcher。

多个 API 副本都执行本循环；未当选的节点 Standby，不扫表、不跑 Lua。
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from app.core.config import settings
from app.core.leader import release_leader, tick_leader
from app.services.delay_dispatcher import run_delay_dispatcher
from app.services.outbox import run_outbox_sweeper

logger = logging.getLogger("vortexmq.control")


async def _stop_task(task: asyncio.Task | None) -> None:
    if task is None or task.done():
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def run_control_plane() -> None:
    """随 FastAPI Lifespan 启动。Leader 拉起后台任务，失锁则停掉。"""
    sweeper: asyncio.Task | None = None
    dispatcher: asyncio.Task | None = None
    logger.info(
        "控制面选主已启动 ttl=%sms renew=%ss",
        settings.CONTROL_LEADER_TTL_MS,
        settings.CONTROL_LEADER_RENEW_SECONDS,
    )
    try:
        while True:
            try:
                is_leader = await tick_leader()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("控制面选主本轮失败，本节点按 Standby 处理")
                is_leader = False
            if is_leader:
                started = False
                if sweeper is None or sweeper.done():
                    sweeper = asyncio.create_task(run_outbox_sweeper(), name="outbox-sweeper")
                    started = True
                if dispatcher is None or dispatcher.done():
                    dispatcher = asyncio.create_task(
                        run_delay_dispatcher(), name="delay-dispatcher"
                    )
                    started = True
                if started:
                    logger.info("Leader 已启动 Outbox Sweeper 与 Delay Dispatcher")
            else:
                if sweeper is not None:
                    logger.info("失去 Leader，停止 Sweeper / Dispatcher，进入 Standby")
                    await _stop_task(sweeper)
                    await _stop_task(dispatcher)
                    sweeper = None
                    dispatcher = None
            await asyncio.sleep(settings.CONTROL_LEADER_RENEW_SECONDS)
    except asyncio.CancelledError:
        logger.info("控制面正在停止")
        raise
    finally:
        await _stop_task(sweeper)
        await _stop_task(dispatcher)
        await release_leader()
