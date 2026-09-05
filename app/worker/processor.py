"""
单条 Stream 消息的处理：RUNNING → Registry 分发执行 → SUCCESS / PENDING+延迟 / DLQ，最后 XACK。

重试不再依赖 Sweeper 的 30 秒扫描，而是写入 ZSet 做指数退避；
到期后由 Delay Dispatcher 的 Lua 脚本原子转入 Stream。

任务类型路由：Worker 只认识 app/worker/registry 里注册过的 Handler。
查不到执行器会抛 UnregisteredTaskError，进入与业务失败相同的
重试 / DLQ 管道，保证「投了没处理器」也不会被静默吞掉。
"""

from __future__ import annotations

import asyncio
import logging
import traceback
from collections.abc import Awaitable
from contextlib import suppress
from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import update

from app.core.clock import as_utc, utcnow
from app.core.config import settings
from app.core.database import AsyncSessionLocal
from app.core.enums import TaskStatus
from app.core.metrics import observe_task_outcome, track_task_duration
from app.core.redis import get_redis, schedule_wakeup, zadd_delayed
from app.crud.task import (
    MAX_ERROR_MSG_LEN,
    claim_task_for_execution,
    get_task_for_update,
    get_task_with_tenant,
)
from app.models.task import TaskRecord
from app.services.workflow import awaken_downstream, cancel_descendants
# 导入内置 demo.* Handler（副作用：模块 import 期完成注册）
from app.worker import handlers as _demo_handlers  # noqa: F401
# 导入 AI Handler（副作用：模块 import 期完成注册 ai.deepseek.chat）
from app.handlers import ai_handlers as _ai_handlers  # noqa: F401
from app.worker.registry import UnregisteredTaskError, vortex_registry

logger = logging.getLogger("vortexmq.worker")

TERMINAL_STATUSES = {TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.DLQ, TaskStatus.CANCELED}
SKIP_STATUSES = TERMINAL_STATUSES | {TaskStatus.WAITING}


async def ack_message(message_id: str, stream_key: str) -> None:
    """从该租户车道的 PEL 中移除消息。必须 ACK 读到的那条 Stream，不能写全局键。"""
    redis = get_redis()
    await redis.xack(
        stream_key,
        settings.REDIS_CONSUMER_GROUP,
        message_id,
    )


def resolve_handler(task_type: str):
    """
    按 task_type 查注册表。

    未注册时抛 UnregisteredTaskError：调用方把它当作普通业务异常处理，
    重试计数 +1 / 指数退避 / 超过上限进 DLQ 的管道自动接管该消息。
    """
    handler = vortex_registry.get(task_type)
    if handler is None:
        raise UnregisteredTaskError(task_type)
    return handler


def compute_next_execute_at(retry_count: int) -> datetime:
    """指数退避：next_execute_at = now + base_delay * 2^retry_count。"""
    delay_seconds = settings.WORKER_RETRY_BASE_DELAY_SECONDS * (2 ** retry_count)
    return utcnow() + timedelta(seconds=delay_seconds)


async def _refresh_lease(task_id: UUID) -> None:
    """刷新 RUNNING 任务的 updated_at 租约，防止长任务被误判为僵尸。"""
    async with AsyncSessionLocal() as session:
        await session.execute(
            update(TaskRecord)
            .where(
                TaskRecord.task_id == task_id,
                TaskRecord.status == TaskStatus.RUNNING,
            )
            .values(updated_at=utcnow())
            .execution_options(synchronize_session=False)
        )
        await session.commit()


async def _run_with_lease_heartbeat(task_id: UUID, job: Awaitable[dict]) -> dict:
    """
    执行期间按 WORKER_LEASE_HEARTBEAT_SECONDS 周期性刷新 PG 租约。

    业务耗时可能远超 WORKER_CLAIM_IDLE_MS：若不续约，Outbox / 二次 CAS 会
    把仍在执行的任务回收重跑（B2）。崩溃时心跳协程随进程终止，updated_at 停止
    刷新，30 秒后自然回到可回收状态，恢复语义不变。
    """

    async def _heartbeat_loop() -> None:
        interval = max(0.05, settings.WORKER_LEASE_HEARTBEAT_SECONDS)
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    await _refresh_lease(task_id)
                except Exception:
                    logger.warning(
                        "租约心跳刷新失败，任务可能被误回收: task_id=%s",
                        task_id,
                        exc_info=True,
                    )
        except asyncio.CancelledError:
            raise

    heartbeat = asyncio.create_task(_heartbeat_loop(), name=f"lease-hb-{task_id}")
    try:
        return await job
    finally:
        heartbeat.cancel()
        with suppress(asyncio.CancelledError):
            await heartbeat


async def handle_message(message_id: str, fields: dict[str, str], *, stream_key: str) -> None:
    """处理一条已进入 PEL 的消息，返回前尽量保证与 PG 状态对齐。"""
    raw_task_id = fields.get("task_id")
    if not raw_task_id:
        logger.error("消息缺少 task_id，直接 XACK 丢弃: id=%s fields=%s", message_id, fields)
        await ack_message(message_id, stream_key)
        return

    try:
        task_id = UUID(raw_task_id)
    except (ValueError, TypeError, AttributeError):
        # 毒丸消息必须 XACK，否则会永久堵塞 PEL。
        logger.error("非法 task_id，XACK 丢弃: id=%s raw=%r", message_id, raw_task_id)
        await ack_message(message_id, stream_key)
        return

    async with AsyncSessionLocal() as session:
        task = await get_task_with_tenant(session, task_id)
        if task is None:
            logger.error("PostgreSQL 中不存在任务，XACK 丢弃: task_id=%s", task_id)
            await ack_message(message_id, stream_key)
            return

        # 防御越权执行：Stream 是共享水管，tenant_id 必须与 PG 行一致。
        msg_tenant = fields.get("tenant_id")
        if not msg_tenant or str(task.tenant_id) != msg_tenant:
            logger.warning(
                "越权警告: Stream tenant_id 与任务行不匹配，丢弃并 XACK: "
                "task_id=%s msg_tenant=%s row_tenant=%s",
                task_id,
                msg_tenant,
                task.tenant_id,
            )
            await ack_message(message_id, stream_key)
            return

        if task.status in SKIP_STATUSES:
            logger.info(
                "任务状态为 %s，不执行并 XACK: task_id=%s tenant=%s",
                task.status.value,
                task.task_id,
                task.tenant.name,
            )
            await ack_message(message_id, stream_key)
            return

        execute_at = as_utc(task.execute_at)
        if execute_at > utcnow():
            # PG 是事实来源：尚未到期的任务不应执行，更不能抢成 RUNNING。
            await zadd_delayed(task.task_id, execute_at, task.tenant_id, task.priority)
            await ack_message(message_id, stream_key)
            logger.info(
                "尚未到 execute_at，已放回延迟队列: task_id=%s execute_at=%s",
                task_id,
                execute_at,
            )
            return

        tenant_name = task.tenant.name
        task_type = task.task_type
        payload = dict(task.payload or {})
        tenant_id = task.tenant_id

        # CAS 乐观锁防重发：UPDATE … WHERE PENDING 或租约过期的 RUNNING。
        claimed_id = await claim_task_for_execution(
            session,
            task_id,
            stale_after=timedelta(milliseconds=settings.WORKER_CLAIM_IDLE_MS),
        )
        await session.commit()
        if claimed_id is None:
            logger.info(
                "CAS 未抢到执行权，XACK 重复消息: task_id=%s status=%s",
                task_id,
                task.status.value,
            )
            await ack_message(message_id, stream_key)
            return

    logger.info(
        "开始执行任务: tenant=%s tenant_id=%s task_id=%s task_type=%s",
        tenant_name,
        tenant_id,
        task_id,
        task_type,
    )

    try:
        # Registry 路由：未注册的 task_type 在此抛 UnregisteredTaskError，
        # 与业务异常走同一条 _persist_failure（重试退避 / DLQ）管道。
        handler = resolve_handler(task_type)
        async with track_task_duration():
            # 长任务执行期间持续刷新租约，避免被 Outbox / 二次 CAS 重复执行（B2）
            result_data = await _run_with_lease_heartbeat(task_id, handler(payload))
    except Exception:
        stack = traceback.format_exc()
        logger.exception(
            "任务执行失败: tenant=%s task_id=%s task_type=%s",
            tenant_name,
            task_id,
            task_type,
        )
        should_ack, metric_status = await _persist_failure(task_id, stack)
        if should_ack:
            if metric_status:
                observe_task_outcome(metric_status, tenant_id, task_type)
            await ack_message(message_id, stream_key)
            logger.info("失败路径已写入 PostgreSQL 并 XACK: task_id=%s", task_id)
        else:
            logger.error("失败状态写入 PostgreSQL 失败，保留 PEL 待认领: task_id=%s", task_id)
        return

    logger.info(
        "任务执行完成: tenant=%s task_id=%s task_type=%s",
        tenant_name,
        task_id,
        task_type,
    )

    persisted = await _persist_success(task_id, result_data)
    if persisted:
        observe_task_outcome("success", tenant_id, task_type)
        await ack_message(message_id, stream_key)
        logger.info("已 XACK，消费确认完毕: task_id=%s message_id=%s", task_id, message_id)
    else:
        logger.error("SUCCESS 写入失败，保留 PEL 待认领: task_id=%s", task_id)


async def _persist_success(task_id: UUID, result_data: dict) -> bool:
    """SUCCESS、result_data、下游 XCom 注入与 PENDING 放在同一事务；提交后再 XADD。"""
    try:
        ready = []
        async with AsyncSessionLocal() as session:
            # FOR UPDATE 防止并发丢失更新：只允许仍为 RUNNING 的抢占者写 SUCCESS。
            task = await get_task_for_update(session, task_id)
            if task is None:
                return True
            if task.status in TERMINAL_STATUSES:
                return True
            if task.status != TaskStatus.RUNNING:
                return True
            task.status = TaskStatus.SUCCESS
            task.result_data = result_data
            task.updated_at = utcnow()
            ready = await awaken_downstream(session, task)
            await session.commit()

        for child in ready:
            try:
                await schedule_wakeup(
                    child.task_id,
                    child.execute_at,
                    tenant_id=child.tenant_id,
                    priority=child.priority,
                )
            except Exception:
                logger.exception(
                    "下游已是 PENDING，XADD 失败交由 Outbox: task_id=%s",
                    child.task_id,
                )
        return True
    except Exception:
        logger.exception("写入 SUCCESS 失败: task_id=%s", task_id)
        return False


async def _persist_failure(task_id: UUID, stack: str) -> tuple[bool, str | None]:
    """
    业务异常落库：retry_count+1。

    返回 (是否 XACK, 指标 status)。
    status 为 failed（将重试）或 dlq；写库失败则 (False, None)。
    """
    try:
        metric_status: str | None = None
        delay_until = None
        delay_tenant_id = None
        delay_priority = 0
        async with AsyncSessionLocal() as session:
            # FOR UPDATE 防止两个 Worker 同时 +1 把 retry_count 写丢。
            task = await get_task_for_update(session, task_id)
            if task is None:
                return True, None
            if task.status in TERMINAL_STATUSES:
                return True, None
            if task.status != TaskStatus.RUNNING:
                return True, None

            next_retry = task.retry_count + 1
            if next_retry < settings.WORKER_MAX_RETRIES:
                delay_until = compute_next_execute_at(next_retry)
                delay_tenant_id = task.tenant_id
                delay_priority = task.priority
                task.status = TaskStatus.PENDING
                task.retry_count = next_retry
                task.error_msg = stack[:MAX_ERROR_MSG_LEN]
                task.execute_at = as_utc(delay_until)
                task.updated_at = utcnow()
                await session.commit()
                metric_status = "failed"
                logger.warning(
                    "任务指数退避后进入延迟队列: task_id=%s retry_count=%s/%s next_execute_at=%s",
                    task_id,
                    next_retry,
                    settings.WORKER_MAX_RETRIES,
                    delay_until,
                )
            else:
                task.status = TaskStatus.DLQ
                task.retry_count = next_retry
                task.error_msg = stack[:MAX_ERROR_MSG_LEN]
                task.updated_at = utcnow()
                await cancel_descendants(session, task)
                await session.commit()
                metric_status = "dlq"
                logger.error(
                    "任务进入 DLQ 并级联取消下游: task_id=%s retry_count=%s",
                    task_id,
                    next_retry,
                )

        if delay_until is not None and delay_tenant_id is not None:
            await zadd_delayed(task_id, delay_until, delay_tenant_id, delay_priority)
        return True, metric_status
    except Exception:
        logger.exception("写入失败状态失败: task_id=%s", task_id)
        return False, None
