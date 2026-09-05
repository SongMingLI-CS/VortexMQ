"""阶段四：Python Client SDK 的端到端验证。

复用 conftest.py 的隔离 FastAPI 实例：用 ``httpx.ASGITransport(app=app)``
让 SDK 在进程内直接命中应用，所有异步调用经 ``client.portal.call`` 跑在
TestClient 的 portal 事件循环上，确保 asyncpg 连接与 API 依赖同一 loop。
"""

from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import httpx
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.enums import TaskStatus
from app.core.redis import tenant_delayed_key, tenant_stream_key
from app.main import app
from tests.helpers import (
    create_tenant,
    fetch_task,
    locate_immediate_message,
    process_message,
)
from vortexmq_client import AsyncVortexMQClient, Workflow

IMMEDIATE_KEY = "vxk_sdk_immediate_0123456789abcdef"
DELAYED_KEY = "vxk_sdk_delayed_0123456789abcdef"
WORKFLOW_KEY = "vxk_sdk_workflow_0123456789abcdef"


def _sdk(api_key: str) -> AsyncVortexMQClient:
    """命中进程内 FastAPI 实例的异步 SDK。"""
    return AsyncVortexMQClient(
        "http://testserver", api_key, transport=httpx.ASGITransport(app=app)
    )


async def _zset_members(redis: Redis, tenant_id: UUID) -> set[str]:
    raw = await redis.zrange(tenant_delayed_key(tenant_id), 0, -1, withscores=True)
    return {str(member) for member, _ in raw}


def test_sdk_submit_immediate_task_and_fetch_result(
    client: TestClient, db_session: AsyncSession, redis_client: Redis
) -> None:
    """SDK 提交即时任务 → Worker 执行 → 查询结果。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, IMMEDIATE_KEY)

    sdk = _sdk(IMMEDIATE_KEY)
    try:
        task_id = client.portal.call(
            sdk.submit_task, "demo.echo", {"hello": "world"}
        )
        assert UUID(task_id)

        # 走真实 Worker 路径执行 demo.echo
        message_id, fields = client.portal.call(
            locate_immediate_message, redis_client, UUID(task_id), tenant_id
        )
        client.portal.call(
            process_message, message_id, fields, tenant_stream_key(tenant_id)
        )

        result = client.portal.call(sdk.get_task_result, task_id)
        assert result["status"] == "SUCCESS"
        assert result["result_data"] == {"echo": {"hello": "world"}}
    finally:
        client.portal.call(sdk.aclose)


def test_sdk_submit_delayed_task(
    client: TestClient, db_session: AsyncSession, redis_client: Redis
) -> None:
    """SDK 提交延迟任务：未到期只进 ZSet，查询结果仍是 PENDING。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, DELAYED_KEY)

    sdk = _sdk(DELAYED_KEY)
    try:
        execute_at = utcnow() + timedelta(hours=1)
        task_id = client.portal.call(
            sdk.submit_task, "demo.echo", {"kind": "delayed"}, 0, execute_at
        )
        assert UUID(task_id)

        # 未到期：不进入任何 Stream，只落在租户延迟 ZSet
        members = client.portal.call(_zset_members, redis_client, tenant_id)
        assert f"{task_id}|0" in members

        result = client.portal.call(sdk.get_task_result, task_id)
        assert result["status"] == "PENDING"
    finally:
        client.portal.call(sdk.aclose)


def test_sdk_workflow_builder_submits_dag(
    client: TestClient, db_session: AsyncSession
) -> None:
    """Workflow 构建器编排 A -> B，经 SDK 成功提交且初始状态正确。"""
    assert client.portal is not None
    client.portal.call(create_tenant, db_session, WORKFLOW_KEY)

    sdk = _sdk(WORKFLOW_KEY)
    try:
        wf = Workflow()
        node_a = wf.add_node("node_a", "etl.extract", {"source": "db"})
        node_b = wf.add_node("node_b", "etl.transform", {}, depends_on=[node_a])

        task_ids = client.portal.call(sdk.submit_workflow, wf)
        assert len(task_ids) == 2
        assert len(set(task_ids)) == 2

        # 返回顺序与节点插入顺序一致：A 为起始 PENDING，B 依赖 A 保持 WAITING
        a_record = client.portal.call(fetch_task, db_session, UUID(task_ids[0]))
        b_record = client.portal.call(fetch_task, db_session, UUID(task_ids[1]))
        assert a_record.status == TaskStatus.PENDING
        assert b_record.status == TaskStatus.WAITING
    finally:
        client.portal.call(sdk.aclose)
