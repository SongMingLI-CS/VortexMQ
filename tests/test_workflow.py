"""P1-6: DAG 工作流 —— 环检测、状态流转 + XCom、级联取消的端到端验证。

锁定行为：
- 自环或成环依赖在提交时返回 400 Bad Request。
- A->B：A SUCCESS 后 B 从 WAITING 变 PENDING，且 B 的 payload 被注入
  _vortex_sys.upstream_results（A 的 result_data）。
- 上游进入 DLQ 时，仍为 WAITING 的下游自动 CANCELED。
"""

from __future__ import annotations

from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.payload import (
    MAX_XCOM_INJECT_BYTES,
    VORTEX_SYS_KEY,
    VORTEX_UPSTREAM_RESULTS_KEY,
    payload_size_bytes,
)
from app.core.redis import tenant_stream_key
from tests.helpers import (
    create_tenant,
    entries_for_task,
    fetch_task,
    locate_immediate_message,
    process_message,
    read_new_message,
    rearm_into_past,
)

KEY = "vxk_flow_key_0123456789abcdefghijk"


def _node(
    node_id: str,
    task_type: str,
    *,
    payload: dict | None = None,
    depends_on: list[str] | None = None,
) -> dict:
    body: dict = {
        "node_id": node_id,
        "task_type": task_type,
        "payload": payload or {},
        "priority": 0,
    }
    if depends_on:
        body["depends_on"] = depends_on
    return body


def _post_workflow(
    client: TestClient, api_key: str, nodes: list[dict]
) -> tuple[int, dict]:
    response = client.post(
        "/api/v1/workflows",
        headers={"X-API-Key": api_key},
        json={"nodes": nodes},
    )
    return response.status_code, response.json()


def _task_map(body: dict) -> dict[str, dict]:
    return {item["node_id"]: item for item in body["tasks"]}


def test_cyclic_and_self_dependent_workflows_return_400(
    client: TestClient, db_session: AsyncSession
) -> None:
    assert client.portal is not None
    client.portal.call(create_tenant, db_session, KEY)

    # 自环：A 依赖 A
    status, body = _post_workflow(
        client, KEY, [_node("A", "demo.echo", depends_on=["A"])]
    )
    assert status == 400
    assert "A" in body["detail"]

    # 循环依赖：A -> B -> A
    status, body = _post_workflow(
        client,
        KEY,
        [
            _node("A", "demo.echo", depends_on=["B"]),
            _node("B", "demo.noop", depends_on=["A"]),
        ],
    )
    assert status == 400
    assert "环" in body["detail"]

    # 依赖不存在的节点同样 400
    status, body = _post_workflow(
        client,
        KEY,
        [_node("A", "demo.echo", depends_on=["missing"])],
    )
    assert status == 400
    assert "missing" in body["detail"]


def test_parent_success_wakes_child_and_injects_xcom(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    """A 成功后 B: WAITING -> PENDING，payload 注入上游 result_data。"""
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)

    status, body = _post_workflow(
        client,
        KEY,
        [
            _node("A", "demo.echo", payload={"seed": "parent-value"}),
            _node("B", "demo.echo", payload={"name": "child"}, depends_on=["A"]),
        ],
    )
    assert status == 201
    nodes = _task_map(body)
    a_task_id = UUID(nodes["A"]["task_id"])
    b_task_id = UUID(nodes["B"]["task_id"])

    assert nodes["A"]["status"] == "PENDING"
    assert nodes["B"]["status"] == "WAITING"

    # 执行 A：demo.echo 把 payload 回显为 result_data
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, a_task_id, tenant_id
    )
    client.portal.call(
        process_message, message_id, fields, tenant_stream_key(tenant_id)
    )

    a_record = client.portal.call(fetch_task, db_session, a_task_id)
    assert a_record.status == TaskStatus.SUCCESS
    assert a_record.result_data == {"echo": {"seed": "parent-value"}}

    # B 被唤醒：WAITING -> PENDING，且拿到 _vortex_sys.upstream_results
    b_record = client.portal.call(fetch_task, db_session, b_task_id)
    assert b_record.status == TaskStatus.PENDING
    injected = b_record.payload[VORTEX_SYS_KEY][VORTEX_UPSTREAM_RESULTS_KEY]
    assert injected == {
        str(a_task_id): {"echo": {"seed": "parent-value"}},
    }, "下游 payload 必须注入上游 result_data"

    # B 的消息也应已进入 Stream（父 SUCCESS 提交后再唤醒）
    b_entries = client.portal.call(
        entries_for_task,
        redis_client,
        tenant_stream_key(tenant_id, high=False),
        b_task_id,
    )
    assert len(b_entries) == 1, "唤醒后的 B 应被投递到 Stream"


def test_upstream_dlq_cascades_cancel_to_waiting_child(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """A 进入 DLQ 后，等待中的 B 自动 CANCELED。"""
    assert client.portal is not None
    # 压缩重试次数：第 2 次失败即 DLQ，减少测试等待。
    monkeypatch.setattr(settings, "WORKER_MAX_RETRIES", 2)

    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = _post_workflow(
        client,
        KEY,
        [
            _node("A", "demo.fail"),
            _node("B", "demo.echo", depends_on=["A"]),
        ],
    )
    assert status == 201
    nodes = _task_map(body)
    a_task_id = UUID(nodes["A"]["task_id"])
    b_task_id = UUID(nodes["B"]["task_id"])
    assert nodes["A"]["status"] == "PENDING"
    assert nodes["B"]["status"] == "WAITING"

    stream_key = tenant_stream_key(tenant_id, high=False)
    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, a_task_id, tenant_id
    )

    # 第一次失败：A 回到 PENDING，B 仍是 WAITING
    client.portal.call(process_message, message_id, fields, stream_key)
    a_record = client.portal.call(fetch_task, db_session, a_task_id)
    b_record = client.portal.call(fetch_task, db_session, b_task_id)
    assert a_record.status == TaskStatus.PENDING
    assert a_record.retry_count == 1
    assert b_record.status == TaskStatus.WAITING

    # 模拟退避到期后第二次执行：A 进入 DLQ，触发级联取消 B
    message_id = client.portal.call(
        rearm_into_past,
        db_session,
        a_task_id,
        redis_client,
        stream_key,
        fields,
    )
    client.portal.call(process_message, message_id, fields, stream_key)

    a_record = client.portal.call(fetch_task, db_session, a_task_id)
    b_record = client.portal.call(fetch_task, db_session, b_task_id)
    assert a_record.status == TaskStatus.DLQ
    assert "demo.fail" in (a_record.error_msg or "")
    assert b_record.status == TaskStatus.CANCELED, "上游 DLQ 后下游必须级联取消"


def test_xcom_injection_is_bounded_per_child(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
) -> None:
    """扇入注入超过 XCom 预算时，超出的上游结果被丢弃，下游仍能正常唤醒。

    两个父节点各自返回约 200KB result_data，合并超过 256KiB 预算；
    子节点照常 WAITING -> PENDING，但 _vortex_sys.upstream_results 只保留
    预算内的上游，JSONB 行不会无界膨胀。
    """
    assert client.portal is not None
    tenant_id = client.portal.call(create_tenant, db_session, KEY)

    big = "x" * 200_000
    status, body = _post_workflow(
        client,
        KEY,
        [
            _node("P1", "demo.echo", payload={"big": big}),
            _node("P2", "demo.echo", payload={"big": big}),
            _node("C", "demo.echo", depends_on=["P1", "P2"]),
        ],
    )
    assert status == 201
    nodes = _task_map(body)
    p1_id = UUID(nodes["P1"]["task_id"])
    p2_id = UUID(nodes["P2"]["task_id"])
    c_id = UUID(nodes["C"]["task_id"])
    assert nodes["C"]["status"] == "WAITING"

    # 普通车道（high=False 即默认）；任务大厅 XADD 顺序与节点提交顺序一致
    stream_key = client.portal.call(tenant_stream_key, tenant_id)

    # 依次消费两个父节点（XADD 顺序与节点提交顺序一致）
    for parent_id in (p1_id, p2_id):
        message_id, fields = client.portal.call(
            read_new_message, redis_client, stream_key
        )
        assert fields["task_id"] == str(parent_id)
        client.portal.call(process_message, message_id, fields, stream_key)
        record = client.portal.call(fetch_task, db_session, parent_id)
        assert record.status == TaskStatus.SUCCESS

    # 两个父节点都成功，C 应被唤醒为 PENDING
    c_record = client.portal.call(fetch_task, db_session, c_id)
    assert c_record.status == TaskStatus.PENDING
    injected = c_record.payload[VORTEX_SYS_KEY][VORTEX_UPSTREAM_RESULTS_KEY]
    assert isinstance(injected, dict)
    assert (
        len(injected) == 1
    ), "两路约 200KB 上游超出 256KiB 预算，应丢弃超出的那一路"
    assert payload_size_bytes(injected) <= MAX_XCOM_INJECT_BYTES
