"""P0-4: Outbox / 补偿语义回归测试。

锁定行为（对应架构约束“PostgreSQL 先落库，Redis 投递失败靠 Outbox 补偿”）：
- 过期 PENDING（投递丢失）会被 Sweeper 重新唤醒，且投递后刷新 updated_at 租约。
- 仍排队的过期 PENDING 被重复补偿会产生重复消息，但 Worker 幂等保证只执行一次。
- 过期 RUNNING（Worker 崩溃 + PEL 丢失）会被回收为 PENDING 并重新投递。
- 新鲜 PENDING / 新鲜 RUNNING 不会被误扫。
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.enums import TaskStatus
from app.core.redis import tenant_stream_key
from app.services import outbox
from tests.helpers import (
    create_tenant,
    delete_stream_entries,
    entries_for_task,
    fetch_task,
    force_status,
    post_task,
    process_message,
    register_handler,
)

KEY_A = "vxk_outbox_a_key_0123456789abcdefgh"
KEY_B = "vxk_outbox_b_key_0123456789abcdefgh"
KEY_C = "vxk_outbox_c_key_0123456789abcdefgh"
KEY_D = "vxk_outbox_d_key_0123456789abcdefgh"
KEY_E = "vxk_outbox_e_key_0123456789abcdefgh"


async def _instant_email(payload: dict) -> dict:
    return {"output": "done:email.send"}


async def _instant_report(payload: dict) -> dict:
    return {"output": "done:report.build"}


def _stale(seconds: int = 120) -> object:
    return utcnow() - timedelta(seconds=seconds)


def test_sweep_republishes_lost_pending_and_refreshes_lease(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """Redis 投递丢失的过期 PENDING：Sweeper 重新投递一次，并刷新租约。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY_A)
    status, body = post_task(client, KEY_A)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    original = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(original) == 1
    client.portal.call(
        delete_stream_entries, redis_client, stream_key, [mid for mid, _ in original]
    )

    # 模拟行已陈旧（Redis 侧投递记录丢失，30 秒窗口之外）
    client.portal.call(
        force_status, db_session, task_id, TaskStatus.PENDING, _stale()
    )

    published = client.portal.call(outbox.sweep_pending_tasks)
    assert published == 1

    entries = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(entries) == 1, "Sweeper 应重新投递一条消息"

    # 投递成功后 updated_at 已刷新：立即再扫一轮不会重复认领
    assert client.portal.call(outbox.sweep_pending_tasks) == 0

    register_handler(monkeypatch, "email.send", _instant_email)
    message_id, fields = entries[0]
    client.portal.call(process_message, message_id, fields, stream_key)
    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.SUCCESS
    assert record.result_data == {"output": "done:email.send"}


def test_duplicate_compensation_runs_business_once(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """仍排队的过期 PENDING 被补偿后形成两条消息，业务仍只执行一次。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY_B)
    status, body = post_task(client, KEY_B, task_type="order.sync")
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    original = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(original) == 1

    client.portal.call(
        force_status, db_session, task_id, TaskStatus.PENDING, _stale()
    )

    assert client.portal.call(outbox.sweep_pending_tasks) == 1
    all_entries = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(all_entries) == 2, "补偿会再投递一条（At-least-once 允许重复）"

    calls: list[str] = []

    async def counting_execute(payload: dict) -> dict:
        calls.append("order.sync")
        return {"output": "ok"}

    register_handler(monkeypatch, "order.sync", counting_execute)
    for message_id, fields in all_entries:
        client.portal.call(process_message, message_id, fields, stream_key)

    assert calls == ["order.sync"], "重复消息不得重复执行业务"
    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.SUCCESS


def test_stale_running_reclaimed_and_rerun_to_success(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """崩溃遗留的过期 RUNNING（PEL 丢失）被回收重跑直至 SUCCESS。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY_C)
    status, body = post_task(client, KEY_C, task_type="report.build")
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    original = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    client.portal.call(
        delete_stream_entries, redis_client, stream_key, [mid for mid, _ in original]
    )

    # 模拟 Worker 认领后崩溃：RUNNING 且 updated_at 已过期、PEL 中无消息
    client.portal.call(
        force_status, db_session, task_id, TaskStatus.RUNNING, _stale()
    )

    reclaimed = client.portal.call(outbox.reclaim_stale_running_tasks)
    assert reclaimed == 1

    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.PENDING, "过期 RUNNING 应被回收为 PENDING"

    entries = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(entries) == 1, "回收后应重新唤醒任务"

    register_handler(monkeypatch, "report.build", _instant_report)
    message_id, fields = entries[0]
    client.portal.call(process_message, message_id, fields, stream_key)
    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.SUCCESS


def test_fresh_running_is_not_reclaimed(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    """仍在租约内的 RUNNING（长任务正在执行）不得被误回收。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY_D)
    status, body = post_task(client, KEY_D)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    original = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(original) == 1

    # 任务刚被认领，updated_at 是当前时间（租约未过期）
    client.portal.call(force_status, db_session, task_id, TaskStatus.RUNNING)

    assert client.portal.call(outbox.reclaim_stale_running_tasks) == 0

    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.RUNNING
    remaining = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert remaining == original, "新鲜 RUNNING 不应被重新投递"


def test_fresh_pending_is_not_swept(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    """刚提交的 PENDING（投递成功，updated_at 新鲜）不得被 Sweeper 补偿。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY_E)
    status, body = post_task(client, KEY_E)
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_id, high=False)
    original = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert len(original) == 1

    assert client.portal.call(outbox.sweep_pending_tasks) == 0

    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.PENDING
    remaining = client.portal.call(entries_for_task, redis_client, stream_key, task_id)
    assert remaining == original, "新鲜 PENDING 不应被补偿重复投递"

