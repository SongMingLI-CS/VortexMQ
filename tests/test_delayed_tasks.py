"""P1-4: 延迟任务 —— ZSet 入队与 Delay Dispatcher 原子搬运的端到端验证。

锁定行为：
- execute_at 在未来的任务只进入租户延迟 ZSet，不进入任何 Stream 车道。
- 到期后 Dispatcher（Lua）把任务从 ZSet 原子搬入同租户 Stream，并清理 ZSet。
- PG 行只记录状态，搬运过程不改动 execute_at（Redis 只负责叫醒）。
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.enums import TaskStatus
from app.core.redis import (
    dispatch_due_delayed_tasks,
    lane_keys_for_tenant,
    tenant_delayed_key,
    tenant_stream_key,
)
from tests.helpers import create_tenant, entries_for_task, fetch_task, post_task

KEY = "vxk_delay_key_0123456789abcdefghij"

_DUE_IN = timedelta(hours=1)
_SLACK = timedelta(minutes=5)


async def _delayed_members(redis: Redis, tenant_id: UUID) -> dict[str, float]:
    """返回租户 ZSet 中全部 (member, score)，member 形如 task_id|priority。"""
    raw = await redis.zrange(tenant_delayed_key(tenant_id), 0, -1, withscores=True)
    return {str(member): float(score) for member, score in raw}


def test_delayed_task_waits_in_zset_then_dispatcher_moves_to_stream(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)

    execute_at = utcnow() + _DUE_IN
    status, body = post_task(
        client,
        KEY,
        task_type="demo.sleep",
        payload={"kind": "delayed"},
        execute_at=execute_at,
    )
    assert status == 201
    task_id = UUID(body["task_id"])

    # 1) 只进 ZSet：普通车道与高优车道都查不到该任务
    member = f"{task_id}|0"
    delayed = client.portal.call(_delayed_members, redis_client, tenant_id)
    assert member in delayed, "未来任务必须进入租户延迟 ZSet"
    assert delayed[member] > execute_at.timestamp() - 1

    for lane in lane_keys_for_tenant(tenant_id):
        stream_entries = client.portal.call(
            entries_for_task, redis_client, lane, task_id
        )
        assert stream_entries == [], f"到期前不得进入 Stream: {lane}"

    # PG 状态与 execute_at 保持不变
    pending = client.portal.call(fetch_task, db_session, task_id)
    assert pending.status == TaskStatus.PENDING

    # 2) 模拟时间走到 execute_at 之后，Dispatcher 执行原子搬运
    moved = client.portal.call(
        dispatch_due_delayed_tasks, execute_at + _SLACK
    )
    assert any(item.split("|")[0] == str(task_id) for item in moved), "到期任务应被搬移"

    # 3) ZSet 已清理；普通车道（priority=0 < 高优阈值）恰好一条消息
    remaining = client.portal.call(_delayed_members, redis_client, tenant_id)
    assert member not in remaining, "搬移后 ZSet 必须移除该 member"

    normal_lane = tenant_stream_key(tenant_id, high=False)
    stream_entries = client.portal.call(
        entries_for_task, redis_client, normal_lane, task_id
    )
    assert len(stream_entries) == 1, "到期后普通车道应恰好一条消息"
    _message_id, fields = stream_entries[0]
    assert fields["task_id"] == str(task_id)
    assert fields["tenant_id"] == str(tenant_id)

    high_lane = tenant_stream_key(tenant_id, high=True)
    assert (
        client.portal.call(entries_for_task, redis_client, high_lane, task_id) == []
    )

    # 搬运不改变 PG 里的计划执行时间（Redis 只是叫醒层）
    after = client.portal.call(fetch_task, db_session, task_id)
    assert after.status == TaskStatus.PENDING
    assert abs((after.execute_at - pending.execute_at).total_seconds()) < 1


def test_high_priority_delayed_task_moves_to_high_lane(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    """高优延迟任务到期后进入高优车道（priority >= 50）。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)

    execute_at = utcnow() + _DUE_IN
    status, body = post_task(
        client,
        KEY,
        task_type="demo.sleep",
        payload={"kind": "delayed"},
        priority=80,
        execute_at=execute_at,
    )
    assert status == 201
    task_id = UUID(body["task_id"])

    member = f"{task_id}|80"
    delayed = client.portal.call(_delayed_members, redis_client, tenant_id)
    assert member in delayed

    client.portal.call(dispatch_due_delayed_tasks, execute_at + _SLACK)

    high_lane = tenant_stream_key(tenant_id, high=True)
    normal_lane = tenant_stream_key(tenant_id, high=False)
    high_entries = client.portal.call(
        entries_for_task, redis_client, high_lane, task_id
    )
    assert len(high_entries) == 1, "高优任务应落入高优车道"
    assert (
        client.portal.call(entries_for_task, redis_client, normal_lane, task_id) == []
    )
    assert high_entries[0][1]["priority"] == "80"
