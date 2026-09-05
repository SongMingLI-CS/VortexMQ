"""P0-2: 多租户隔离 —— 跨租户读取资源必须统一 404，与“任务不存在”不可区分。

安全规则：tenant_id 只来自服务端解析 X-API-Key 的结果，
任何跨租户访问都应“看起来不存在”，不得泄漏任务存在性。
"""

from __future__ import annotations

import uuid
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis import tenant_stream_key
from tests.helpers import (
    create_tenant,
    entries_for_task,
    fetch_result,
    locate_immediate_message,
    post_task,
    process_message,
    register_handler,
)

A_KEY = "vxk_iso_a_key_0123456789abcdefgh"
B_KEY = "vxk_iso_b_key_0123456789abcdefgh"


async def _email_done(payload: dict) -> dict:
    return {"output": "done:email.send"}


async def _order_done(payload: dict) -> dict:
    return {"output": "done:order.sync"}


def test_cross_tenant_result_lookup_is_not_found(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    assert client.portal is not None
    tenant_a = client.portal.call(create_tenant, db_session, A_KEY)
    client.portal.call(create_tenant, db_session, B_KEY)

    status, body = post_task(client, A_KEY, task_type="email.send")
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_a, high=False)
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_a
    )
    assert fields["tenant_id"] == str(tenant_a)

    register_handler(monkeypatch, "email.send", _email_done)
    client.portal.call(process_message, message_id, fields, stream_key)

    # 属主租户能看到 SUCCESS 结果
    owner_status, owner_body = fetch_result(client, A_KEY, task_id)
    assert owner_status == 200
    assert owner_body == {
        "task_id": str(task_id),
        "status": "SUCCESS",
        "result_data": {"output": "done:email.send"},
    }

    # 其他租户查询同一个 task_id：与“任务不存在”完全一致（404 + 同一 detail）
    other_status, other_body = fetch_result(client, B_KEY, task_id)
    assert other_status == 404
    assert other_body == {"detail": "任务不存在"}

    unknown_status, unknown_body = fetch_result(
        client, A_KEY, uuid.uuid4()
    )
    assert unknown_status == 404
    assert other_body == unknown_body


def test_tenant_cannot_see_other_tenants_pending_task(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """未完成任务：属主看到 202，跨租户一律 404。"""
    assert client.portal is not None
    tenant_a = client.portal.call(create_tenant, db_session, A_KEY)
    client.portal.call(create_tenant, db_session, B_KEY)

    status, body = post_task(client, A_KEY, task_type="order.sync")
    assert status == 201
    task_id = UUID(body["task_id"])

    stream_key = tenant_stream_key(tenant_a, high=False)
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_a
    )
    register_handler(monkeypatch, "order.sync", _order_done)
    client.portal.call(process_message, message_id, fields, stream_key)

    owner_status, owner_body = fetch_result(client, A_KEY, task_id)
    assert owner_status == 200
    assert owner_body["status"] == "SUCCESS"

    # B 自己的任务生命周期不受 A 影响
    b_status, b_body = post_task(client, B_KEY, task_type="report.build")
    assert b_status == 201
    b_task_id = UUID(b_body["task_id"])

    b_entries = client.portal.call(
        entries_for_task, redis_client, tenant_stream_key(tenant_a, high=False), b_task_id
    )
    assert b_entries == []

    # B 用其密钥看不到 A 的已成功任务
    cross_status, _ = fetch_result(client, B_KEY, task_id)
    assert cross_status == 404
