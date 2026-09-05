"""
Prometheus 指标定义与打点工具。

业务代码只调用本模块的函数 / 上下文管理器，不直接依赖 prometheus_client 的细节。
Gauge 类指标在 /metrics 抓取时从 Redis 实时读取，避免 API / Worker 进程内存各自为政。
"""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import UUID

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    start_http_server,
)

from app.core.config import settings

logger = logging.getLogger("vortexmq.metrics")

# 任务终态：success / failed / dlq（失败指将重试，dlq 指进入死信）
TASKS_TOTAL = Counter(
    "vortexmq_tasks_total",
    "任务处理计数（按终态、租户、任务类型）",
    ["status", "tenant_id", "task_type"],
)

TASK_DURATION = Histogram(
    "vortexmq_task_duration_seconds",
    "Worker 执行任务的耗时分布（秒）",
    buckets=(0.25, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 8.0, 13.0, 30.0),
)

QUEUE_SIZE = Gauge(
    "vortexmq_queue_size",
    "当前队列堆积量",
    ["queue"],
)

ACTIVE_WORKERS = Gauge(
    "vortexmq_active_workers",
    "心跳仍有效的 Worker 数量",
)

METRICS_CONTENT_TYPE = CONTENT_TYPE_LATEST


def observe_task_outcome(
    status: str,
    tenant_id: UUID | str,
    task_type: str,
) -> None:
    """记录一条任务结果。status 取 success / failed / dlq。"""
    TASKS_TOTAL.labels(
        status=status,
        tenant_id=str(tenant_id),
        task_type=task_type,
    ).inc()


@asynccontextmanager
async def track_task_duration() -> AsyncIterator[None]:
    """包住 Worker 业务执行，等价于 Histogram.time()，异常路径也会记录耗时。"""
    with TASK_DURATION.time():
        yield


def render_latest_metrics() -> bytes:
    """导出 Prometheus 文本格式。"""
    return generate_latest()


# 本进程正在执行的 Handler 并发数。事件循环单线程，同步自增/自减无需加锁。
_worker_in_flight = 0
_started_at = time.time()


def increment_worker_in_flight() -> None:
    """进入一条消息处理时调用（必须在首个 await 之前执行）。"""
    global _worker_in_flight
    _worker_in_flight += 1


def decrement_worker_in_flight() -> None:
    """消息处理退出时调用；防御性下限 0。"""
    global _worker_in_flight
    if _worker_in_flight > 0:
        _worker_in_flight -= 1


def worker_in_flight_count() -> int:
    """当前正在执行 Handler 的消息数，作为该 Worker 的瞬时负载。"""
    return _worker_in_flight


def _worker_process_started_at() -> str:
    """Worker 进程启动时间（ISO 8601）。本模块在 Worker 进程内是进程启动时导入。"""
    return datetime.fromtimestamp(_started_at, tz=timezone.utc).isoformat()


def start_metrics_http_server(port: int | None = None) -> None:
    """Worker 进程内另开一个 HTTP 端口供 Prometheus 抓取（不经过 FastAPI）。"""
    bind_port = port if port is not None else settings.WORKER_METRICS_PORT
    start_http_server(bind_port, addr="0.0.0.0")
    logger.info("Worker metrics 已监听 0.0.0.0:%s/metrics", bind_port)


async def beat_worker_heartbeat(consumer_name: str) -> None:
    """Worker 存活心跳：ZSet score 为当前 Unix 时间戳，负载写入同 slot 的 Hash。

    心跳与负载分开存储：ZSet 负责「谁还活着」与过期清理，Hash 负责展示
    hostname / pid / 进程启动时间 / in_flight。两条命令放同一条 pipeline，
    避免 Admin /metrics 抓取时读到旧 score 配新元数据。
    """
    from app.core.redis import get_redis, worker_heartbeat_key, worker_load_key

    redis = get_redis()
    meta = json.dumps(
        {
            "hostname": socket.gethostname(),
            "pid": os.getpid(),
            "started_at": _worker_process_started_at(),
            "in_flight": worker_in_flight_count(),
        },
        ensure_ascii=False,
    )
    pipe = redis.pipeline(transaction=False)
    pipe.zadd(worker_heartbeat_key(), {consumer_name: time.time()})
    pipe.hset(worker_load_key(), consumer_name, meta)
    await pipe.execute()


async def clear_worker_heartbeat(consumer_name: str) -> None:
    """进程退出时立刻摘掉心跳与负载，避免 Admin /workers 多报一个僵尸 Worker。"""
    from app.core.redis import get_redis, worker_heartbeat_key, worker_load_key

    redis = get_redis()
    await redis.zrem(worker_heartbeat_key(), consumer_name)
    await redis.hdel(worker_load_key(), consumer_name)


async def refresh_runtime_gauges() -> None:
    """
    从 Redis 刷新 Gauge，供 API /metrics 在抓取时调用。

    队列深度、存活 Worker 数是集群全局状态，不能用进程内 Gauge.inc/dec，
    否则多副本会各记一份、互相看不到。
    """
    from app.core.redis import (
        get_redis,
        sum_tenant_delayed_lengths,
        sum_tenant_stream_lengths,
        worker_heartbeat_key,
    )

    redis = get_redis()
    cutoff = time.time() - settings.WORKER_HEARTBEAT_TTL_SECONDS
    heartbeat = worker_heartbeat_key()
    stream_len = await sum_tenant_stream_lengths()
    delayed_len = await sum_tenant_delayed_lengths()
    pipe = redis.pipeline(transaction=False)
    pipe.zremrangebyscore(heartbeat, "-inf", cutoff)
    pipe.zcard(heartbeat)
    _removed, workers = await pipe.execute()

    QUEUE_SIZE.labels(queue="stream").set(float(stream_len or 0))
    QUEUE_SIZE.labels(queue="delayed").set(float(delayed_len or 0))
    ACTIVE_WORKERS.set(float(workers or 0))
