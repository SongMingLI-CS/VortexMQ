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
    read_new_message,
    register_handler,
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
    assert ok.json() == {
        "items": [],
        "total": 0,
        "page_size": 20,
        "next_cursor": None,
    }


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
    assert all_body["page_size"] == 20
    assert all_body["next_cursor"] is None, "一页已取完所有数据，不应再有下一页"
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
    # keyset 翻页：page_size=2 首页返回最新两条（beta noop、alpha echo）
    page1 = client.get(
        "/api/v1/admin/tasks",
        headers=_admin_headers(),
        params={"page_size": 2},
    ).json()
    assert page1["total"] == 4
    assert page1["page_size"] == 2
    assert len(page1["items"]) == 2
    assert [item["task_type"] for item in page1["items"]] == ["demo.noop", "demo.echo"]
    next_cursor = page1["next_cursor"]
    assert isinstance(next_cursor, str) and next_cursor

    # 翻页之间插入一条更新的任务：新行只会出现在更靠前的页；
    # 已签发的游标仍按 (created_at, task_id) 锚点续页——不重、不漏。
    client.portal.call(
        _insert_task,
        db_session,
        alpha_id,
        TaskStatus.PENDING,
        "demo.new",
        0,
        None,
        _HALL_BASE + 4 * day,
    )

    page2 = client.get(
        "/api/v1/admin/tasks",
        headers=_admin_headers(),
        params={"page_size": 2, "cursor": next_cursor},
    ).json()
    assert [item["task_type"] for item in page2["items"]] == ["demo.noop", "demo.fail"]
    assert page2["items"][1]["tenant_name"] == "hall-alpha"
    assert page2["items"][1]["status"] == "DLQ"
    assert page2["items"][1]["error_msg"] == "boom"
    assert page2["next_cursor"] is None, "已是末页：next_cursor 应为 null"

    # 篡改 / 无效游标统一 422，不落到 SQL 层
    bad = client.get(
        "/api/v1/admin/tasks",
        headers=_admin_headers(),
        params={"page_size": 2, "cursor": "AAAA"},
    )
    assert bad.status_code == 422
    assert bad.json() == {"detail": "无效的分页游标"}


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


def test_replay_dlq_revives_canceled_workflow_descendants(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """DLQ 重放必须复活此前被级联取消的下游，否则整张 DAG 卡死。

    A -> B -> C：A 首次执行即 DLQ（MAX_RETRIES=1）→ B/C 被级联取消；
    重放 A 后 B/C 回到 WAITING；A 成功逐级唤醒，最终整条链 SUCCESS。
    """
    assert client.portal is not None
    _enable_admin(monkeypatch)
    monkeypatch.setattr(settings, "WORKER_MAX_RETRIES", 1)

    api_key = "vxk_admin_replay_wf_0123456789ab"
    client.portal.call(create_tenant, db_session, api_key)

    wf = client.post(
        "/api/v1/workflows",
        headers={"X-API-Key": api_key},
        json={
            "nodes": [
                {"node_id": "A", "task_type": "demo.fail", "payload": {}},
                {
                    "node_id": "B",
                    "task_type": "demo.echo",
                    "payload": {},
                    "depends_on": ["A"],
                },
                {
                    "node_id": "C",
                    "task_type": "demo.echo",
                    "payload": {},
                    "depends_on": ["B"],
                },
            ]
        },
    )
    assert wf.status_code == 201
    nodes = {item["node_id"]: item for item in wf.json()["tasks"]}
    a_id = UUID(nodes["A"]["task_id"])
    b_id = UUID(nodes["B"]["task_id"])
    c_id = UUID(nodes["C"]["task_id"])

    # A 执行一次即 DLQ → B、C 被级联取消
    tenant_row = client.portal.call(fetch_task, db_session, a_id)
    stream_key = client.portal.call(tenant_stream_key, tenant_row.tenant_id)
    a_message_id, a_fields = client.portal.call(
        read_new_message, redis_client, stream_key
    )
    assert a_fields["task_id"] == str(a_id)
    client.portal.call(process_message, a_message_id, a_fields, stream_key)

    a_record = client.portal.call(fetch_task, db_session, a_id)
    b_record = client.portal.call(fetch_task, db_session, b_id)
    c_record = client.portal.call(fetch_task, db_session, c_id)
    assert a_record.status == TaskStatus.DLQ
    assert b_record.status == TaskStatus.CANCELED
    assert c_record.status == TaskStatus.CANCELED

    # 重放 A：自身回 PENDING，B/C 复活为 WAITING
    replay = client.post(
        f"/api/v1/admin/tasks/{a_id}/replay", headers=_admin_headers()
    )
    assert replay.status_code == 200
    assert replay.json()["status"] == "PENDING"
    b_record = client.portal.call(fetch_task, db_session, b_id)
    c_record = client.portal.call(fetch_task, db_session, c_id)
    assert b_record.status == TaskStatus.WAITING, "重放必须复活被级联取消的下游"
    assert c_record.status == TaskStatus.WAITING

    # 端到端：重放后 A 成功 → 唤醒 B → 唤醒 C，全链最终 SUCCESS
    async def _succeed(payload: dict) -> dict:
        return {"output": "replayed-ok"}

    register_handler(monkeypatch, "demo.fail", _succeed)

    # XREADGROUP '>' 只返回新消息：已 XACK 的旧 A 消息不会被重复消费
    replay_message_id, replay_fields = client.portal.call(
        read_new_message, redis_client, stream_key
    )
    assert replay_fields["task_id"] == str(a_id)
    client.portal.call(process_message, replay_message_id, replay_fields, stream_key)

    # A SUCCESS 后 B 被唤醒为 PENDING 并投递
    b_record = client.portal.call(fetch_task, db_session, b_id)
    assert b_record.status == TaskStatus.PENDING
    b_message_id, b_fields = client.portal.call(
        read_new_message, redis_client, stream_key
    )
    assert b_fields["task_id"] == str(b_id)
    client.portal.call(process_message, b_message_id, b_fields, stream_key)

    # B SUCCESS 后 C 被唤醒并跑完
    c_record = client.portal.call(fetch_task, db_session, c_id)
    assert c_record.status == TaskStatus.PENDING
    c_message_id, c_fields = client.portal.call(
        read_new_message, redis_client, stream_key
    )
    assert c_fields["task_id"] == str(c_id)
    client.portal.call(process_message, c_message_id, c_fields, stream_key)

    for task_id in (a_id, b_id, c_id):
        record = client.portal.call(fetch_task, db_session, task_id)
        assert record.status == TaskStatus.SUCCESS, f"{task_id} 应为 SUCCESS"


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


def test_workflow_detail_endpoint_returns_dag_edges(
    client: TestClient, db_session: AsyncSession, monkeypatch
) -> None:
    """GET /admin/workflows/{id} 返回整张 DAG 的节点与上下游边（供控制台可视化）。"""
    assert client.portal is not None
    _enable_admin(monkeypatch)
    api_key = "vxk_wf_detail_0123456789abcdefgh"
    client.portal.call(create_tenant, db_session, api_key)

    wf = client.post(
        "/api/v1/workflows",
        headers={"X-API-Key": api_key},
        json={
            "nodes": [
                {"node_id": "A", "task_type": "demo.noop", "payload": {}},
                {
                    "node_id": "B",
                    "task_type": "demo.echo",
                    "payload": {},
                    "depends_on": ["A"],
                },
            ]
        },
    )
    assert wf.status_code == 201
    workflow_id = wf.json()["workflow_id"]
    a_id = next(t["task_id"] for t in wf.json()["tasks"] if t["node_id"] == "A")
    b_id = next(t["task_id"] for t in wf.json()["tasks"] if t["node_id"] == "B")

    detail = client.get(
        f"/api/v1/admin/workflows/{workflow_id}", headers=_admin_headers()
    )
    assert detail.status_code == 200
    body = detail.json()
    assert body["workflow_id"] == workflow_id
    nodes = {n["task_id"]: n for n in body["nodes"]}
    assert set(nodes) == {a_id, b_id}
    assert nodes[a_id]["upstream_ids"] == []
    assert nodes[a_id]["downstream_ids"] == [b_id]
    assert nodes[b_id]["upstream_ids"] == [a_id]
    assert nodes[b_id]["downstream_ids"] == []

    missing = client.get(
        f"/api/v1/admin/workflows/{uuid.uuid4()}", headers=_admin_headers()
    )
    assert missing.status_code == 404

