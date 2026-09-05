"""
Integration-test 共享工具。

约定：
- 所有 async 函数都运行在 TestClient 的 portal 事件循环上，测试侧通过
  ``client.portal.call(func, *args)`` 调用。
- 涉及数据库的 helper 结束后会 commit 当前 savepoint，便于同连接上的
  其他会话（API 依赖 / Worker / Outbox）看到变更。
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.redis import ensure_consumer_group, tenant_stream_key
from app.core.security import api_key_prefix, hash_api_key
from app.models.task import TaskRecord
from app.models.tenant import Tenant

DEFAULT_CONSUMER = "itest-consumer"


async def create_tenant(
    session: AsyncSession,
    api_key: str,
    *,
    name: str | None = None,
) -> UUID:
    """创建租户并返回其 id。"""
    tenant = Tenant(
        name=name or f"tenant-{uuid.uuid4().hex}",
        api_key_hash=hash_api_key(api_key),
        api_key_prefix=api_key_prefix(api_key),
    )
    session.add(tenant)
    await session.commit()
    await session.refresh(tenant)
    return tenant.id


def post_task(
    client: TestClient,
    api_key: str,
    *,
    task_type: str = "email.send",
    payload: dict[str, Any] | None = None,
    priority: int = 0,
    execute_at: datetime | None = None,
) -> tuple[int, dict]:
    """通过 API 提交任务，返回 (status_code, body)。"""
    body: dict[str, Any] = {
        "task_type": task_type,
        "payload": payload or {},
        "priority": priority,
    }
    if execute_at is not None:
        body["execute_at"] = execute_at.isoformat()
    response = client.post(
        "/api/v1/tasks",
        headers={"X-API-Key": api_key},
        json=body,
    )
    return response.status_code, response.json()


def fetch_result(client: TestClient, api_key: str, task_id: UUID) -> tuple[int, dict]:
    """查询任务结果，返回 (status_code, body)。"""
    response = client.get(
        f"/api/v1/tasks/{task_id}/result",
        headers={"X-API-Key": api_key},
    )
    return response.status_code, response.json()


async def fetch_task(session: AsyncSession, task_id: UUID) -> TaskRecord:
    """读取任务记录并结束本会话 savepoint，返回 ORM 对象。

    populate_existing：同一 fixture 会话的 identity map 可能持有过期对象
    （expire_on_commit=False），必须用 DB 当前行覆盖后再返回。
    """
    record = (
        await session.execute(
            select(TaskRecord)
            .where(TaskRecord.task_id == task_id)
            .execution_options(populate_existing=True)
        )
    ).scalar_one()
    await session.commit()
    return record


async def force_status(
    session: AsyncSession,
    task_id: UUID,
    status: TaskStatus,
    updated_at: datetime | None = None,
) -> None:
    """直接改写任务状态与 updated_at（模拟 Worker 崩溃 / 中途状态）。

    只使用位置参数：BlockingPortal.call() 不支持关键字参数。
    """
    values: dict[str, Any] = {"status": status}
    if updated_at is not None:
        values["updated_at"] = updated_at
    await session.execute(
        update(TaskRecord).where(TaskRecord.task_id == task_id).values(**values)
    )
    await session.commit()


async def stream_entries(
    redis: Redis,
    stream_key: str,
) -> list[tuple[str, dict[str, str]]]:
    """返回 Stream 内全部 (message_id, fields)，按时间正序。"""
    return await redis.xrange(stream_key, min="-", max="+")


async def entries_for_task(
    redis: Redis,
    stream_key: str,
    task_id: UUID,
) -> list[tuple[str, dict[str, str]]]:
    all_entries = await stream_entries(redis, stream_key)
    return [(mid, fields) for mid, fields in all_entries if fields.get("task_id") == str(task_id)]


async def delete_stream_entries(
    redis: Redis,
    stream_key: str,
    message_ids: list[str],
) -> None:
    if message_ids:
        await redis.xdel(stream_key, *message_ids)


async def read_new_message(
    redis: Redis,
    stream_key: str,
    consumer: str = DEFAULT_CONSUMER,
) -> tuple[str, dict[str, str]] | None:
    """把一条新消息读进消费者组 PEL，返回 (message_id, fields)。"""
    await ensure_consumer_group(stream_key)
    result = await redis.xreadgroup(
        groupname=settings.REDIS_CONSUMER_GROUP,
        consumername=consumer,
        streams={stream_key: ">"},
        count=1,
    )
    if not result or not result[0][1]:
        return None
    message_id, fields = result[0][1][0]
    return message_id, dict(fields)


async def group_pending_count(redis: Redis, stream_key: str) -> int:
    """返回该租户车道消费者组 PEL 中未 ACK 的消息数。"""
    summary = await redis.xpending(stream_key, settings.REDIS_CONSUMER_GROUP)
    return int(summary["pending"])


async def process_message(
    message_id: str,
    fields: dict[str, str],
    stream_key: str,
) -> None:
    """在 portal 事件循环上执行 Worker 的单条消息处理。"""
    from app.worker.processor import handle_message

    await handle_message(message_id, fields, stream_key=stream_key)


async def locate_immediate_message(
    redis: Redis,
    task_id: UUID,
    tenant_id: UUID,
) -> tuple[str, dict[str, str]]:
    """定位该任务在租户普通车道上的即时消息（应为恰好一条）。"""
    stream_key = tenant_stream_key(tenant_id, high=False)
    entries = await stream_entries(redis, stream_key)
    matching = [
        (mid, fields) for mid, fields in entries if fields.get("task_id") == str(task_id)
    ]
    assert len(matching) == 1, f"预期恰好 1 条消息，实际 {len(matching)}"
    return matching[0]
