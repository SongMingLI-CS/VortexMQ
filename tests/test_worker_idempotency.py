"""P0-3: Worker 幂等与 ACK 语义回归测试。

锁定行为：
- 同一任务的消息被重复投递（至少一次语义）时，只执行一次。
- 仍在租约内的 RUNNING 任务不会被第二个 Worker 重复执行。
- 租约过期后允许回收重跑（崩溃恢复前提）。
- PostgreSQL 落库失败时绝不 XACK，消息必须留在 PEL 供重试。
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.redis import tenant_stream_key
from app.crud.task import claim_task_for_execution
from app.worker import processor
from tests.helpers import (
    create_tenant,
    entries_for_task,
    fetch_task,
    force_status,
    group_pending_count,
    locate_immediate_message,
    post_task,
    process_message,
    read_new_message,
)

KEY = "vxk_idem_key_0123456789abcdefghij"
CONSUMER = "idem-consumer"
_LEASE = timedelta(milliseconds=30_000)


def _claim_with_lease(session: AsyncSession, task_id: UUID):
    """BlockingPortal.call() 不支持关键字参数，故把 stale_after 包进闭包。"""
    return claim_task_for_execution(session, task_id, stale_after=_LEASE)


async def _counting_execute(calls: list[str]) -> None:
    async def execute(task_type: str, payload: dict) -> dict:
        calls.append(task_type)
        return {"output": "ok"}

    return execute


def test_duplicate_message_after_success_executes_once(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """SUCCESS 之后重复投递同一条消息：跳过执行并 ACK。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_id
    )

    calls: list[str] = []
    monkeypatch.setattr(
        processor, "execute_simulated_job", client.portal.call(_counting_execute, calls)
    )

    client.portal.call(process_message, message_id, fields, stream_key)
    assert calls == ["email.send"]

    # 模拟 At-least-once 红投递：再写入一条内容完全相同的消息
    dup_id = client.portal.call(redis_client.xadd, stream_key, fields)
    dup_entries = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert any(mid == dup_id for mid, _ in dup_entries)

    client.portal.call(process_message, dup_id, fields, stream_key)
    assert calls == ["email.send"], "重复消息不得再次执行业务"

    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.SUCCESS
    assert record.result_data == {"output": "ok"}


def test_message_during_fresh_running_lease_is_not_executed(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """任务仍处于活租约（RUNNING，updated_at 新）时重复投递，不得二次执行。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    _, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_id
    )

    # 模拟 Worker A 已抢到并正在执行：状态 RUNNING，updated_at 是当前时间。
    client.portal.call(force_status, db_session, task_id, TaskStatus.RUNNING)

    calls: list[str] = []
    monkeypatch.setattr(
        processor, "execute_simulated_job", client.portal.call(_counting_execute, calls)
    )

    dup_id = client.portal.call(redis_client.xadd, stream_key, fields)
    client.portal.call(process_message, dup_id, fields, stream_key)

    assert calls == [], "活租约内的任务不允许第二个 Worker 重复执行"
    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.RUNNING


def test_success_persist_failure_keeps_message_in_pel(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """业务成功但 SUCCESS 落库失败：不得 XACK，消息保留在 PEL。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    message_id, fields = client.portal.call(
        read_new_message, redis_client, stream_key, CONSUMER
    )
    assert message_id is not None

    async def _ok_execute(task_type: str, payload: dict) -> dict:
        return {"output": "ok"}

    async def _fail_persist(_task_id: UUID, _result_data: dict) -> bool:
        return False

    monkeypatch.setattr(processor, "execute_simulated_job", _ok_execute)
    monkeypatch.setattr(processor, "_persist_success", _fail_persist)

    client.portal.call(process_message, message_id, fields, stream_key)

    pending = client.portal.call(group_pending_count, redis_client, stream_key)
    assert pending == 1, "落库失败后消息必须留在 PEL 等待重试"
    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.RUNNING


def test_failure_persist_failure_keeps_message_in_pel(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """业务异常且失败状态落库也失败：不得 XACK，消息保留在 PEL。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    message_id, fields = client.portal.call(
        read_new_message, redis_client, stream_key, CONSUMER
    )
    assert message_id is not None

    async def _boom_execute(task_type: str, payload: dict) -> dict:
        raise RuntimeError("boom")

    async def _fail_failure_persist(
        _task_id: UUID, _stack: str
    ) -> tuple[bool, str | None]:
        return False, None

    monkeypatch.setattr(processor, "execute_simulated_job", _boom_execute)
    monkeypatch.setattr(processor, "_persist_failure", _fail_failure_persist)

    client.portal.call(process_message, message_id, fields, stream_key)

    pending = client.portal.call(group_pending_count, redis_client, stream_key)
    assert pending == 1, "失败落库失败时消息必须留在 PEL 等待重试"


def test_claim_cas_blocks_during_lease_and_allows_after_expiry(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    """CAS 认领：租约内拒绝二次认领，租约过期后允许回收。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY)
    assert status == 201
    task_id = UUID(body["task_id"])

    lease = _LEASE
    first = client.portal.call(_claim_with_lease, db_session, task_id)
    assert first == task_id

    second = client.portal.call(_claim_with_lease, db_session, task_id)
    assert second is None, "活租约内第二次认领必须失败"

    # 租约过期：允许回收同一任务
    client.portal.call(
        force_status,
        db_session,
        task_id,
        TaskStatus.RUNNING,
        utcnow() - timedelta(seconds=120),
    )
    reclaimed = client.portal.call(_claim_with_lease, db_session, task_id)
    assert reclaimed == task_id


def test_long_task_with_lease_heartbeat_is_not_reexecuted(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """B2 回归：执行时间超过租约窗口的长任务靠心跳续约，不被二次认领重跑。"""
    monkeypatch.setattr(settings, "WORKER_CLAIM_IDLE_MS", 200)
    monkeypatch.setattr(settings, "WORKER_LEASE_HEARTBEAT_SECONDS", 0.05)

    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY, task_type="long.running")
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_id
    )

    async def _run_second_claim_and_finish() -> tuple[UUID | None, list[str]]:
        """在工作任务执行中模拟第二个 Worker 尝试 CAS 认领。"""
        start = asyncio.Event()
        release = asyncio.Event()
        executions: list[str] = []

        async def slow_job(task_type: str, payload: dict) -> dict:
            executions.append(task_type)
            start.set()
            await release.wait()  # 让任务“跑”得比 200ms 租约窗口更久
            return {"output": "slow-done"}

        monkeypatch.setattr(processor, "execute_simulated_job", slow_job)

        worker_task = asyncio.create_task(
            process_message(message_id, fields, stream_key)
        )
        await asyncio.wait_for(start.wait(), timeout=5)

        # 已远超 200ms 租约窗口；若心跳没续约，这里会认领成功 → 重复执行
        await asyncio.sleep(0.6)
        second_claim = await claim_task_for_execution(
            db_session, task_id, stale_after=timedelta(milliseconds=200)
        )

        release.set()
        await asyncio.wait_for(worker_task, timeout=5)
        return second_claim, executions

    second_claim, executions = client.portal.call(_run_second_claim_and_finish)
    assert second_claim is None, "长任务租约心跳存活期间不得被第二个 Worker 认领"
    assert executions == ["long.running"], "长任务只能被执行业务一次"

    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.SUCCESS
    assert record.result_data == {"output": "slow-done"}

