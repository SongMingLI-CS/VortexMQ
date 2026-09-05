"""Admin 管理面 API：鉴权、任务大厅、DLQ 重放、强制取消与 Worker 监控。

锁定行为：
- ADMIN_API_KEY 未配置时管理接口 503；配置后缺失 / 错误 Key 一律 401。
- 任务大厅支持按 status / tenant_name / tenant_id / 时间范围筛选与分页。
- 重放只允许 DLQ：置回 PENDING、重置 retry_count 与 error_msg，并重新叫醒 Worker。
- 取消允许 PENDING / RUNNING / WAITING，WAITING 下游级联 CANCELED；终态返回 409。
- /workers 只返回心跳有效期内、负载 Hash 同步更新的 Worker，过期节点被清理。
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.metrics import beat_worker_heartbeat, clear_worker_heartbeat
from app.core.redis import tenant_stream_key, worker_heartbeat_key, worker_load_key
from app.core.security import api_key_prefix, hash_api_key
from app.models.task import TaskRecord
from app.models.tenant import Tenant
from app.worker import handlers  # noqa: F401,E402  # 注册内置 demo.*（重放后执行）
from tests.helpers import (
    create_tenant,
    fetch_task,
    locate_immediate_message,
    process_message,
)

ADMIN_KEY = "vxk_admin_secret_0123456789abcdefg"
_HALL_BASE = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)


def _admin_headers() -> dict[str, str]:
    return {"X-Admin-Key": ADMIN_KEY}


async def _insert_task(
    session: AsyncSession,
    tenant_id: UUID,
    status: TaskStatus = TaskStatus.PENDING,
    task_type: str = "demo.noop",
    retry_count: int = 0,
    error_msg: str | None = None,
    created_at: datetime | None = None,
) -> TaskRecord:
    """直接插入任务行（绕过投递管道），便于构造大厅 / DLQ / 终态样本。"""
    row = TaskRecord(
        tenant_id=tenant_id,
        status=status,
        task_type=task_type,
        payload={},
        priority=0,
        retry_count=retry_count,
        error_msg=error_msg,
        execute_at=datetime.now(timezone.utc),
        workflow_id=None,
        upstream_ids=[],
        downstream_ids=[],
    )
    if created_at is not None:
        row.created_at = created_at
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return row


def _enable_admin(monkeypatch) -> None:
    """显式启用管理面 Key；同时清掉 .env 可能带来的残留值。"""
    monkeypatch.setattr(settings, "ADMIN_API_KEY", ADMIN_KEY)


async def _create_named_tenant(session: AsyncSession, api_key: str, name: str) -> UUID:
    """创建指定名称的租户（BlockingPortal.call 不支持关键字参数，故本地自建）。"""
    tenant = Tenant(
        name=name,
        api_key_hash=hash_api_key(api_key),
        api_key_prefix=api_key_prefix(api_key),
    )
    session.add(tenant)
    await session.commit()
    await session.refresh(tenant)
    return tenant.id


def test_admin_requires_configured_key_and_rejects_invalid(
    client: TestClient, monkeypatch
) -> None:
    # 未配置 ADMIN_API_KEY：无论带不带 Header 都应 503，不能匿名放行
    monkeypatch.setattr(settings, "ADMIN_API_KEY", "")
    response = client.get("/api/v1/admin/tasks")
    assert response.status_code == 503
    assert response.json() == {"detail": "Admin API 未启用：请设置 ADMIN_API_KEY"}

    _enable_admin(monkeypatch)

    missing = client.get("/api/v1/admin/tasks")
    assert missing.status_code == 401
    assert missing.json() == {"detail": "缺少 Admin API Key"}

    wrong = client.get(
        "/api/v1/admin/tasks", headers={"X-Admin-Key": ADMIN_KEY + "-wrong"}
    )
    assert wrong.status_code == 401
    assert wrong.json() == {"detail": "无效的 Admin API Key"}

    ok = client.get("/api/v1/admin/tasks", headers=_admin_headers())
    assert ok.status_code == 200
    assert ok.json() == {"items": [], "total": 0, "page": 1, "page_size": 20}


def test_task_hall_filters_and_pagination(
    client: TestClient, db_session: AsyncSession, monkeypatch
) -> None:
    assert client.portal is not None
    _enable_admin(monkeypatch)

    alpha_id = client.portal.call(
        _create_named_tenant, db_session, "vxk_admin_hall_a_0123456789abcdefg", "hall-alpha"
    )
    beta_id = client.portal.call(
        _create_named_tenant, db_session, "vxk_admin_hall_b_0123456789abcdefg", "hall-beta"
    )

    day = timedelta(days=1)
    client.portal.call(
        _insert_task, db_session, alpha_id, TaskStatus.DLQ, "demo.fail", 3, "boom", _HALL_BASE
    )
    client.portal.call(
        _insert_task, db_session, alpha_id, TaskStatus.PENDING, "demo.noop", 0, None, _HALL_BASE + day
    )
    client.portal.call(
        _insert_task, db_session, alpha_id, TaskStatus.SUCCESS, "demo.echo", 0, None, _HALL_BASE + 2 * day
    )
    client.portal.call(
        _insert_task, db_session, beta_id, TaskStatus.PENDING, "demo.noop", 0, None, _HALL_BASE + 3 * day
    )

    # 默认：全量 4 条，最新创建在前
    all_body = client.get("/api/v1/admin/tasks", headers=_admin_headers()).json()
    assert all_body["total"] == 4
    assert all_body["page"] == 1
    assert [item["tenant_name"] for item in all_body["items"]] == [
        "hall-beta",
        "hall-alpha",
        "hall-alpha",
        "hall-alpha",
    ]

    # status 筛选
    pending_body = client.get(
        "/api/v1/admin/tasks", headers=_admin_headers(), params={"status": "PENDING"}
    ).json()
    assert pending_body["total"] == 2
    assert {item["status"] for item in pending_body["items"]} == {"PENDING"}

    # 租户名 + 状态组合筛选
    combo = client.get(
        "/api/v1/admin/tasks",
        headers=_admin_headers(),
        params={"status": "PENDING", "tenant_name": "hall-alpha"},
    ).json()
    assert combo["total"] == 1
    assert combo["items"][0]["tenant_id"] == str(alpha_id)

    # 租户 ID 筛选
    by_id = client.get(
        "/api/v1/admin/tasks", headers=_admin_headers(), params={"tenant_id": str(alpha_id)}
    ).json()
    assert by_id["total"] == 3

    # 时间范围筛选：落在 alpha 的 SUCCESS（base+2d）
    from_ts = (_HALL_BASE + 1.5 * day).isoformat()
    to_ts = (_HALL_BASE + 2.5 * day).isoformat()
    ranged = client.get(
        "/api/v1/admin/tasks",
        headers=_admin_headers(),
        params={"created_from": from_ts, "created_to": to_ts},
    ).json()
    assert ranged["total"] == 1
    assert ranged["items"][0]["status"] == "SUCCESS"
    assert ranged["items"][0]["retry_count"] == 0

    # 分页：page_size=2 时第一页返回 t4/t3，第二页返回 t2/t1
    page1 = client.get(
        "/api/v1/admin/tasks", headers=_admin_headers(), params={"page": 1, "page_size": 2}
    ).json()
    page2 = client.get(
        "/api/v1/admin/tasks", headers=_admin_headers(), params={"page": 2, "page_size": 2}
    ).json()
    assert page1["total"] == 4
    assert len(page1["items"]) == 2
    assert [item["task_type"] for item in page1["items"]] == ["demo.noop", "demo.echo"]
    assert [item["task_type"] for item in page2["items"]] == ["demo.noop", "demo.fail"]
    assert page2["items"][1]["tenant_name"] == "hall-alpha"
    assert page2["items"][1]["status"] == "DLQ"
    assert page2["items"][1]["error_msg"] == "boom"


def test_replay_dlq_back_to_pending_and_rerun_to_success(
    client: TestClient, db_session: AsyncSession, redis_client: Redis, monkeypatch
) -> None:
    assert client.portal is not None
    _enable_admin(monkeypatch)

    tenant_id = client.portal.call(
        create_tenant, db_session, "vxk_admin_replay_0123456789abcdefgh"
    )
    task = client.portal.call(
        _insert_task, db_session, tenant_id, TaskStatus.DLQ, "demo.echo", 3, "boom"
    )
    task_id = task.task_id

    body = client.post(
        f"/api/v1/admin/tasks/{task_id}/replay", headers=_admin_headers()
    )
    assert body.status_code == 200
    payload = body.json()
    assert payload["status"] == "PENDING"
    assert payload["retry_count"] == 0
    assert payload["error_msg"] is None
    assert payload["tenant_name"].startswith("tenant-")
    assert payload["task_type"] == "demo.echo"

    # 端到端：重放后任务被重新叫醒，Worker 消费后跑完 SUCCESS
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_id
    )
    stream_key = client.portal.call(tenant_stream_key, tenant_id)
    client.portal.call(process_message, message_id, fields, stream_key)
    record = client.portal.call(fetch_task, db_session, task_id)
    assert record.status == TaskStatus.SUCCESS

    # 非 DLQ 状态不可重放 → 409
    success_task = client.portal.call(
        _insert_task, db_session, tenant_id, TaskStatus.SUCCESS, "demo.echo"
    )
    conflict = client.post(
        f"/api/v1/admin/tasks/{success_task.task_id}/replay", headers=_admin_headers()
    )
    assert conflict.status_code == 409
    assert "DLQ" in conflict.json()["detail"]

    # 不存在的任务 → 404
    missing = client.post(
        f"/api/v1/admin/tasks/{uuid.uuid4()}/replay", headers=_admin_headers()
    )
    assert missing.status_code == 404
    assert missing.json() == {"detail": "任务不存在"}


def test_cancel_cascades_to_workflow_descendants_and_rejects_terminal(
    client: TestClient, db_session: AsyncSession, monkeypatch
) -> None:
    assert client.portal is not None
    _enable_admin(monkeypatch)

    api_key = "vxk_admin_cancel_0123456789abcdefgh"
    tenant_id = client.portal.call(create_tenant, db_session, api_key)

    # A -> B 的 DAG：A PENDING、B WAITING
    wf = client.post(
        "/api/v1/workflows",
        headers={"X-API-Key": api_key},
        json={
            "nodes": [
                {
                    "node_id": "A",
                    "task_type": "demo.echo",
                    "payload": {},
                    "priority": 0,
                },
                {
                    "node_id": "B",
                    "task_type": "demo.echo",
                    "payload": {},
                    "priority": 0,
                    "depends_on": ["A"],
                },
            ]
        },
    )
    assert wf.status_code == 201
    nodes = {item["node_id"]: item for item in wf.json()["tasks"]}
    a_id = UUID(nodes["A"]["task_id"])
    b_id = UUID(nodes["B"]["task_id"])

    # 取消 PENDING 的 A → A CANCELED，WAITING 的 B 级联 CANCELED
    cancel_a = client.post(
        f"/api/v1/admin/tasks/{a_id}/cancel", headers=_admin_headers()
    )
    assert cancel_a.status_code == 200
    assert cancel_a.json()["status"] == "CANCELED"
    a_record = client.portal.call(fetch_task, db_session, a_id)
    b_record = client.portal.call(fetch_task, db_session, b_id)
    assert a_record.status == TaskStatus.CANCELED
    assert b_record.status == TaskStatus.CANCELED, "取消上游必须级联取消 WAITING 下游"

    # 幂等：重复取消仍 200
    again = client.post(
        f"/api/v1/admin/tasks/{a_id}/cancel", headers=_admin_headers()
    )
    assert again.status_code == 200
    assert again.json()["status"] == "CANCELED"

    # RUNNING 允许取消
    running = client.portal.call(
        _insert_task, db_session, tenant_id, TaskStatus.RUNNING, "demo.sleep"
    )
    cancel_running = client.post(
        f"/api/v1/admin/tasks/{running.task_id}/cancel", headers=_admin_headers()
    )
    assert cancel_running.status_code == 200
    assert cancel_running.json()["status"] == "CANCELED"

    # 终态（SUCCESS）不可取消 → 409
    success_task = client.portal.call(
        _insert_task, db_session, tenant_id, TaskStatus.SUCCESS, "demo.echo"
    )
    conflict = client.post(
        f"/api/v1/admin/tasks/{success_task.task_id}/cancel", headers=_admin_headers()
    )
    assert conflict.status_code == 409

    missing = client.post(
        f"/api/v1/admin/tasks/{uuid.uuid4()}/cancel", headers=_admin_headers()
    )
    assert missing.status_code == 404
    assert missing.json() == {"detail": "任务不存在"}

def test_workers_endpoint_lists_alive_and_prunes_stale(
    client: TestClient, redis_client: Redis, monkeypatch
) -> None:
    assert client.portal is not None
    _enable_admin(monkeypatch)
    monkeypatch.setattr(settings, "WORKER_HEARTBEAT_TTL_SECONDS", 60)

    # 一个存活 Worker + 一个心跳已过期的僵尸
    client.portal.call(beat_worker_heartbeat, "worker-host-a")
    stale_score = datetime.now(timezone.utc).timestamp() - 3600
    client.portal.call(
        redis_client.zadd, worker_heartbeat_key(), {"worker-stale": stale_score}
    )
    client.portal.call(
        redis_client.hset,
        worker_load_key(),
        "worker-stale",
        '{"hostname": "ghost", "pid": 1, "in_flight": 3}',
    )

    body = client.get("/api/v1/admin/workers", headers=_admin_headers())
    assert body.status_code == 200
    workers = body.json()
    assert [item["name"] for item in workers] == ["worker-host-a"]
    live = workers[0]
    assert live["hostname"]
    assert live["pid"] > 0
    assert live["in_flight"] == 0
    assert live["last_seen"]

    # 过期心跳已被清出 ZSet，僵尸 Hash field 一并被清理
    names = client.portal.call(redis_client.zrange, worker_heartbeat_key(), 0, -1)
    assert names == ["worker-host-a"]
    remaining_loads = client.portal.call(redis_client.hgetall, worker_load_key())
    assert list(remaining_loads) == ["worker-host-a"]

    # 正常退出的 Worker 会清除自己的心跳与负载
    client.portal.call(clear_worker_heartbeat, "worker-host-a")
    empty = client.get("/api/v1/admin/workers", headers=_admin_headers()).json()
    assert empty == []

