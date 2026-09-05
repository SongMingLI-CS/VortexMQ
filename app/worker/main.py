"""
VortexMQ Worker 独立入口。

启动方式（与 API 进程互不依赖）：
    python -m app.worker

本进程不加载 FastAPI，只连接 PostgreSQL + Redis。
停机时停止拉取新消息，但会等当前 RUNNING 任务写完 PG 并 XACK。
"""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import socket
import sys

from redis.exceptions import ResponseError

from app.core.config import settings
from app.core.database import engine, init_db
from app.core.metrics import (
    beat_worker_heartbeat,
    clear_worker_heartbeat,
    decrement_worker_in_flight,
    increment_worker_in_flight,
    start_metrics_http_server,
)
from app.core.redis import (
    close_redis,
    ensure_consumer_group,
    get_redis,
    lane_keys_for_tenant,
    list_active_tenant_ids,
)
from app.worker.processor import handle_message

logger = logging.getLogger("vortexmq.worker")


def _consumer_name() -> str:
    """每个 Worker 进程必须有唯一消费者名，否则 PEL 归属会互相覆盖。"""
    if settings.WORKER_CONSUMER_NAME:
        return settings.WORKER_CONSUMER_NAME
    return f"{socket.gethostname()}-{os.getpid()}"


def install_shutdown_signals(stop_event: asyncio.Event) -> None:
    """
    捕获 SIGINT / SIGTERM，只置位停机事件，不直接取消正在跑的协程。

    默认 SIGINT 会变成 KeyboardInterrupt，asyncio.run 会取消全部任务，
    正在 RUNNING 的任务可能既没写成 SUCCESS/PENDING/DLQ，也没 XACK。
    这里改成协作式退出：主循环看到 stop_event 后不再 XREADGROUP，
    但当前 handle_message 会继续跑完再释放连接。
    """
    loop = asyncio.get_running_loop()

    def request_stop() -> None:
        if stop_event.is_set():
            return
        logger.info("收到停机信号：停止拉取新任务，等待当前 RUNNING 任务结束")
        stop_event.set()

    if sys.platform == "win32":
        # Windows 的事件循环不支持 loop.add_signal_handler，退回 signal.signal。
        # 回调可能发生在非事件循环线程，必须用 call_soon_threadsafe 置位。
        def _win_handler(signum: int, _frame: object) -> None:
            logger.info("收到信号 %s", signum)
            loop.call_soon_threadsafe(request_stop)

        signal.signal(signal.SIGINT, _win_handler)
        signal.signal(signal.SIGTERM, _win_handler)
        return

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, request_stop)


async def _ensure_tenant_groups(tenant_id: str) -> tuple[str, str]:
    lanes = lane_keys_for_tenant(tenant_id)
    for stream_key in lanes:
        await ensure_consumer_group(stream_key)
    return lanes


def _parse_stream_messages(result: object) -> list[tuple[str, dict[str, str], str]]:
    """把 XREADGROUP / 结构转成 (message_id, fields, stream_key)。"""
    if not result:
        return []
    entries: list[tuple[str, dict[str, str], str]] = []
    for stream_name, messages in result:
        for message_id, fields in messages:
            entries.append((message_id, dict(fields), str(stream_name)))
    return entries


async def _process_message(
    message_id: str, fields: dict[str, str], stream_key: str
) -> None:
    """包一层 in-flight 计数：心跳负载 = 本进程正在执行的消息数。"""
    increment_worker_in_flight()
    try:
        await handle_message(message_id, fields, stream_key=stream_key)
    finally:
        decrement_worker_in_flight()


async def _claim_idle_pending(
    consumer: str,
    tenants: list[str],
    cursor: int,
) -> tuple[list[tuple[str, dict[str, str], str]], int]:
    """按租户轮询 XAUTOCLAIM，一次最多认领 1 条。"""
    if not tenants:
        return [], cursor
    redis = get_redis()
    n = len(tenants)
    for offset in range(n):
        idx = (cursor + offset) % n
        tenant_id = tenants[idx]
        lanes = await _ensure_tenant_groups(tenant_id)
        for stream_key in lanes:
            try:
                result = await redis.xautoclaim(
                    name=stream_key,
                    groupname=settings.REDIS_CONSUMER_GROUP,
                    consumername=consumer,
                    min_idle_time=settings.WORKER_CLAIM_IDLE_MS,
                    start_id="0-0",
                    count=1,
                )
            except ResponseError as exc:
                logger.warning("XAUTOCLAIM 失败 stream=%s: %s", stream_key, exc)
                continue
            messages = result[1] if result else []
            claimed: list[tuple[str, dict[str, str], str]] = []
            for item in messages or []:
                message_id, fields = item[0], item[1]
                claimed.append((message_id, dict(fields), stream_key))
            if claimed:
                logger.info(
                    "XAUTOCLAIM 认领到 %s 条空闲消息 tenant=%s stream=%s",
                    len(claimed),
                    tenant_id,
                    stream_key,
                )
                return claimed, (idx + 1) % n
    return [], (cursor + 1) % n if n else cursor


async def _read_own_pending(
    consumer: str,
    tenant_id: str,
) -> list[tuple[str, dict[str, str], str]]:
    """读取某租户两条车道上属于本消费者的 PEL。"""
    redis = get_redis()
    lanes = await _ensure_tenant_groups(tenant_id)
    entries: list[tuple[str, dict[str, str], str]] = []
    for stream_key in lanes:
        result = await redis.xreadgroup(
            groupname=settings.REDIS_CONSUMER_GROUP,
            consumername=consumer,
            streams={stream_key: "0"},
            count=1,
        )
        entries.extend(_parse_stream_messages(result))
        if entries:
            break
    return entries


async def _read_new_messages(
    consumer: str,
    tenants: list[str],
    cursor: int,
) -> tuple[list[tuple[str, dict[str, str], str]], int]:
    """
    租户公平轮询：每个租户先读高优先级车道，再读普通车道。
    非阻塞探测一整圈；都空则短暂休眠，把时间片让给其他租户的新消息。
    """
    if not tenants:
        await asyncio.sleep(settings.WORKER_BLOCK_MS / 1000)
        return [], cursor

    redis = get_redis()
    n = len(tenants)
    for offset in range(n):
        idx = (cursor + offset) % n
        tenant_id = tenants[idx]
        lanes = await _ensure_tenant_groups(tenant_id)
        for stream_key in lanes:
            result = await redis.xreadgroup(
                groupname=settings.REDIS_CONSUMER_GROUP,
                consumername=consumer,
                streams={stream_key: ">"},
                count=1,
            )
            entries = _parse_stream_messages(result)
            if entries:
                return entries, (idx + 1) % n
    await asyncio.sleep(settings.WORKER_BLOCK_MS / 1000)
    return [], (cursor + 1) % n


async def run_worker() -> None:
    """Worker 主循环：建组 → 阻塞读取 → 认领空闲 PEL；停机时排空当前任务。"""
    consumer = _consumer_name()
    stop_event = asyncio.Event()
    install_shutdown_signals(stop_event)

    await init_db()
    await get_redis().ping()
    logger.info(
        "Worker 已启动 consumer=%s stream={tenant}:%s group=%s",
        consumer,
        settings.REDIS_STREAM_KEY,
        settings.REDIS_CONSUMER_GROUP,
    )

    # 固定消费者名重启时，先排空各租户车道上的自身 PEL。
    drained = 0
    for tenant_id in await list_active_tenant_ids():
        if stop_event.is_set():
            break
        while not stop_event.is_set():
            own_pending = await _read_own_pending(consumer, tenant_id)
            if not own_pending:
                break
            message_id, fields, stream_key = own_pending[0]
            try:
                await _process_message(message_id, fields, stream_key)
                drained += 1
            except Exception:
                logger.exception("启动排空 PEL 失败，保留待重试: id=%s", message_id)
                break
    if drained:
        logger.info("启动时已排空本消费者 PEL: count=%s", drained)

    new_cursor = 0
    claim_cursor = 0
    try:
        while True:
            await beat_worker_heartbeat(consumer)
            if stop_event.is_set():
                logger.info("停机标志已置位，不再拉取新任务")
                break

            tenants = await list_active_tenant_ids()
            new_messages, new_cursor = await _read_new_messages(consumer, tenants, new_cursor)
            for message_id, fields, stream_key in new_messages:
                try:
                    await _process_message(message_id, fields, stream_key)
                except Exception:
                    logger.exception("处理新消息失败，保留 PEL 待重试: id=%s", message_id)

            if stop_event.is_set():
                logger.info("当前消息已处理完毕，开始优雅退出")
                break

            if new_messages:
                continue

            pending, claim_cursor = await _claim_idle_pending(consumer, tenants, claim_cursor)
            if stop_event.is_set() and not pending:
                break
            for message_id, fields, stream_key in pending:
                try:
                    await _process_message(message_id, fields, stream_key)
                except Exception:
                    logger.exception("处理认领消息失败，保留 PEL 待重试: id=%s", message_id)

            if stop_event.is_set():
                logger.info("当前消息已处理完毕，开始优雅退出")
                break
    finally:
        try:
            await clear_worker_heartbeat(consumer)
        except Exception:
            logger.exception("清理 Worker 心跳失败: consumer=%s", consumer)
        await close_redis()
        await engine.dispose()
        logger.info("Worker 已释放 Redis / PostgreSQL 连接")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    )
    start_metrics_http_server()
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("收到 KeyboardInterrupt，Worker 退出")


if __name__ == "__main__":
    main()
