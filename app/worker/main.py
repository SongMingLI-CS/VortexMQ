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
    *,
    count: int = 1,
) -> tuple[list[tuple[str, dict[str, str], str]], int]:
    """按租户公平轮询 XAUTOCLAIM，最多认领 count 条空闲 PEL 消息。"""
    if count <= 0 or not tenants:
        return [], cursor
    redis = get_redis()
    claimed: list[tuple[str, dict[str, str], str]] = []
    n = len(tenants)
    scanned = 0
    while scanned < n and len(claimed) < count:
        idx = (cursor + scanned) % n
        tenant_id = tenants[idx]
        lanes = await _ensure_tenant_groups(tenant_id)
        remaining = count - len(claimed)
        for stream_key in lanes:
            if remaining <= 0:
                break
            try:
                result = await redis.xautoclaim(
                    name=stream_key,
                    groupname=settings.REDIS_CONSUMER_GROUP,
                    consumername=consumer,
                    min_idle_time=settings.WORKER_CLAIM_IDLE_MS,
                    start_id="0-0",
                    count=remaining,
                )
            except ResponseError as exc:
                logger.warning("XAUTOCLAIM 失败 stream=%s: %s", stream_key, exc)
                continue
            messages = result[1] if result else []
            for item in messages or []:
                message_id, fields = item[0], item[1]
                claimed.append((message_id, dict(fields), stream_key))
            remaining = count - len(claimed)
        scanned += 1
    if claimed:
        logger.info(
            "XAUTOCLAIM 认领到 %s 条空闲消息", len(claimed)
        )
    return claimed, (cursor + scanned) % n if n else cursor


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
    *,
    max_count: int = 1,
) -> tuple[list[tuple[str, dict[str, str], str]], int]:
    """
    非阻塞预取新消息：租户公平轮询，最多返回 max_count 条。

    每个租户先读高优先级车道再读普通车道（同租户两条键同 Hash Tag，
    单命令 Cluster 合法）；一圈扫完仍未集满则返回已取到的部分，
    剩余容量留给下一轮，避免空转扫全表。空闲时不 sleep——由调度器
    决定是 BLOCK 等待还是短暂轮询。
    """
    if max_count <= 0 or not tenants:
        return [], cursor
    redis = get_redis()
    entries: list[tuple[str, dict[str, str], str]] = []
    n = len(tenants)
    scanned = 0
    while scanned < n and len(entries) < max_count:
        idx = (cursor + scanned) % n
        tenant_id = tenants[idx]
        lanes = await _ensure_tenant_groups(tenant_id)
        remaining = max_count - len(entries)
        for stream_key in lanes:
            if remaining <= 0:
                break
            result = await redis.xreadgroup(
                groupname=settings.REDIS_CONSUMER_GROUP,
                consumername=consumer,
                streams={stream_key: ">"},
                count=remaining,
            )
            found = _parse_stream_messages(result)
            entries.extend(found)
            remaining = max_count - len(entries)
        scanned += 1
    return entries, (cursor + scanned) % n


def _max_in_flight() -> int:
    """单进程并发上限，至少 1。"""
    return max(1, settings.WORKER_MAX_IN_FLIGHT)


async def _safe_process_message(
    message_id: str, fields: dict[str, str], stream_key: str
) -> None:
    """协程安全的包装：异常只记日志不逃逸，避免 create_task 丢失异常。"""
    try:
        await _process_message(message_id, fields, stream_key)
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception(
            "处理消息协程异常，PEL 保留待认领: id=%s stream=%s",
            message_id,
            stream_key,
        )


def _dispatch_entries(
    pending: set[asyncio.Task],
    buffered: list[tuple[str, dict[str, str], str]],
    entries: list[tuple[str, dict[str, str], str]],
    capacity: int,
) -> None:
    """按容量派发协程；超出的条目留在缓冲，等容量释放后补派。"""
    for message_id, fields, stream_key in entries:
        if len(pending) < capacity:
            pending.add(
                asyncio.create_task(
                    _safe_process_message(message_id, fields, stream_key),
                    name=f"handle-{message_id}",
                )
            )
        else:
            buffered.append((message_id, fields, stream_key))


def _reap_done(pending: set[asyncio.Task]) -> None:
    """收走已完成任务并检索异常，防止 “Task exception was never retrieved”。"""
    for task in list(pending):
        if not task.done():
            continue
        pending.discard(task)
        if task.cancelled():
            continue
        exc = task.exception()
        if exc is not None:
            logger.error("消息处理协程异常退出: %s", exc, exc_info=exc)


async def _wait_first(pending: set[asyncio.Task], timeout: float) -> None:
    """等待至少一个在途任务结束；没有任务则等 timeout，避免空转。"""
    if not pending:
        await asyncio.sleep(timeout)
        return
    await asyncio.wait(
        set(pending), timeout=timeout, return_when=asyncio.FIRST_COMPLETED
    )


async def run_worker() -> None:
    """Worker 独立入口：信号 → 基础设施 → 消费循环；停机时排空在途任务。"""
    consumer = _consumer_name()
    stop_event = asyncio.Event()
    install_shutdown_signals(stop_event)

    await init_db()
    await get_redis().ping()
    logger.info(
        "Worker 已启动 consumer=%s stream={tenant}:%s group=%s max_in_flight=%s",
        consumer,
        settings.REDIS_STREAM_KEY,
        settings.REDIS_CONSUMER_GROUP,
        _max_in_flight(),
    )

    try:
        await run_worker_loop(consumer, stop_event)
    finally:
        await close_redis()
        await engine.dispose()
        logger.info("Worker 已释放 Redis / PostgreSQL 连接")


async def _block_for_new_messages(
    consumer: str,
    tenants: list[str],
    stop_event: asyncio.Event,
) -> list[tuple[str, dict[str, str], str]]:
    """
    全空空闲期的唤醒等待：为每个租户的两条车道各挂一个 BLOCK XREADGROUP。

    单命令只含同一租户键（Hash Tag 相同），Cluster 合法；任一租户车道来消息
    即唤醒，不存在“堵住某一家”的问题。stop_event 置位时立刻返回空，
    保证停机不会被最长阻塞拖住。
    ponytail: 阻塞读取会暂占 Redis 连接（每租户一条），租户数接近连接池上限时
    建议调大 app/core/redis.py 的 max_connections。
    """
    timeout_ms = settings.WORKER_BLOCK_MS
    if not tenants:
        await asyncio.sleep(timeout_ms / 1000)
        return []
    redis = get_redis()

    async def _read(
        lanes: tuple[str, str],
    ) -> list[tuple[str, dict[str, str], str]]:
        result = await redis.xreadgroup(
            groupname=settings.REDIS_CONSUMER_GROUP,
            consumername=consumer,
            streams={key: ">" for key in lanes},
            count=1,
            block=timeout_ms,
        )
        return _parse_stream_messages(result)

    readers: list[asyncio.Task] = []
    stop_watcher: asyncio.Task | None = None
    try:
        for tenant_id in tenants:
            lanes = await _ensure_tenant_groups(tenant_id)
            readers.append(
                asyncio.create_task(_read(lanes), name=f"block-{tenant_id}")
            )
        stop_watcher = asyncio.create_task(stop_event.wait(), name="stop-watcher")
        done, running = await asyncio.wait(
            [stop_watcher, *readers],
            timeout=timeout_ms / 1000 + 1.0,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in running:
            task.cancel()
        if running:
            await asyncio.gather(*running, return_exceptions=True)

        if stop_watcher in done:
            return []
        entries: list[tuple[str, dict[str, str], str]] = []
        for task in done:
            if task is stop_watcher or task.cancelled():
                continue
            exc = task.exception()
            if exc is not None:
                logger.warning("空闲 BLOCK 读取异常: %s", exc)
                continue
            entries.extend(task.result())
        return entries
    finally:
        if stop_watcher is not None and not stop_watcher.done():
            stop_watcher.cancel()
        for task in readers:
            if not task.done():
                task.cancel()


async def _drain_own_pending(consumer: str, stop_event: asyncio.Event) -> int:
    """固定消费者名重启时，先排空各租户车道上的自身 PEL（串行，语义不变）。"""
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
    return drained


async def run_worker_loop(consumer: str, stop_event: asyncio.Event) -> None:
    """
    Worker 消费循环：预取并发执行 + 空闲 PEL 认领 + 全空时 BLOCK 等待。

    并发边界：在途协程数不超过 WORKER_MAX_IN_FLIGHT；每个 Handler 仍是
    独立短事务（RUNNING 租约心跳 + CAS + FOR UPDATE），并发只受容量约束。
    停机：不再从 Stream 拉新消息，但会把已读入 PEL 的消息跑完再退出。
    """
    logger.info(
        "Worker 消费循环已启动 consumer=%s max_in_flight=%s",
        consumer,
        _max_in_flight(),
    )
    await _drain_own_pending(consumer, stop_event)

    pending: set[asyncio.Task] = set()
    buffered: list[tuple[str, dict[str, str], str]] = []
    cursor = 0
    claim_cursor = 0
    try:
        while True:
            await beat_worker_heartbeat(consumer)
            _reap_done(pending)

            # 已读入 PEL 但尚未派发的消息照常补派（停机期间也跑完）
            while buffered and len(pending) < _max_in_flight():
                message_id, fields, stream_key = buffered.pop(0)
                pending.add(
                    asyncio.create_task(
                        _safe_process_message(message_id, fields, stream_key),
                        name=f"handle-{message_id}",
                    )
                )

            if stop_event.is_set():
                if not pending and not buffered:
                    logger.info("停机标志已置位，在途消息已全部处理完毕")
                    break
                if pending:
                    await _wait_first(pending, 0.25)
                continue

            capacity = _max_in_flight()
            if len(pending) >= capacity:
                # 满载：等至少一个任务结束再补位，避免空转
                await _wait_first(pending, 0.25)
                continue

            tenants = await list_active_tenant_ids()
            want = capacity - len(pending)
            new_messages, cursor = await _read_new_messages(
                consumer, tenants, cursor, max_count=want
            )
            _dispatch_entries(pending, buffered, new_messages, capacity)
            if len(pending) >= capacity:
                continue

            claimed, claim_cursor = await _claim_idle_pending(
                consumer, tenants, claim_cursor, count=capacity - len(pending)
            )
            _dispatch_entries(pending, buffered, claimed, capacity)
            if len(pending) >= capacity:
                continue

            if len(pending) == 0:
                # 完全空闲：挂 BLOCK 等新消息，任一车道到达立即唤醒
                found = await _block_for_new_messages(
                    consumer, tenants, stop_event
                )
                _dispatch_entries(pending, buffered, found, capacity)
                continue

            # 部分满载且没有可补消息：等任务完成或短暂轮询，避免忙等
            await _wait_first(pending, 0.25)
    finally:
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        try:
            await clear_worker_heartbeat(consumer)
        except Exception:
            logger.exception("清理 Worker 心跳失败: consumer=%s", consumer)


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
