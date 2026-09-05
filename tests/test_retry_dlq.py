"""P1-5: 重试与 DLQ —— 业务异常 / 未注册类型的退避与死信端到端验证。

锁定行为：
- 执行抛异常后：任务回到 PENDING，retry_count +1，execute_at 按
  now + WORKER_RETRY_BASE_DELAY_SECONDS * 2^retry_count 推迟，并写入延迟 ZSet。
- 连续失败直到 retry_count 达到 WORKER_MAX_RETRIES：任务进入 DLQ，
  error_msg 完整记录异常堆栈。
- 未注册的 task_type 抛 UnregisteredTaskError，同样被重试/DLQ 管道接管。
"""

from __future__ import annotations

from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.clock import utcnow
from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.redis import tenant_delayed_key, tenant_stream_key
from app.worker import handlers  # noqa: F401,E402  # 注册内置 demo.*
from tests.helpers import (
    create_tenant,
    fetch_task,
    locate_immediate_message,
    post_task,
    process_message,
    rearm_into_past,
)

KEY = "vxk_retry_key_0123456789abcdefghij"
MAX_RETRIES = settings.WORKER_MAX_RETRIES  # 默认 3：第 3 次失败进入 DLQ
BACKOFF_BASE = 1.0


def _drive_until_dlq(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    task_id: UUID,
    tenant_id: UUID,
    message_id: str,
    fields: dict[str, str],
    *,
    task_type: str,
    marker: str,
) -> None:
    """循环执行任务直到 DLQ；非终局失败之间模拟退避到期并重新投递。"""
    stream_key = tenant_stream_key(tenant_id, high=False)
    current_message_id = message_id

    for attempt in range(1, MAX_RETRIES + 1):
        before = utcnow()
        client.portal.call(process_message, current_message_id, fields, stream_key)
        record = client.portal.call(fetch_task, db_session, task_id)

        if attempt < MAX_RETRIES:
            # 状态回到 PENDING、retry_count 累加、错误堆栈已记录
            assert record.status == TaskStatus.PENDING, (
                f"第 {attempt} 次失败后应回到 PENDING"
            )
            assert record.retry_count == attempt
            assert record.error_msg and marker in record.error_msg
            assert "Traceback" in record.error_msg, "error_msg 应保存完整堆栈"

            # 指数退避：delay = base * 2^attempt（_persist_failure 传入 next_retry）
            delay_seconds = BACKOFF_BASE * (2**attempt)
            delta = (record.execute_at - before).total_seconds()
            assert (
                abs(delta - delay_seconds) < 1.5
            ), f"execute_at 应按指数退避推迟，预期 {delay_seconds:.1f}s 实际 {delta:.2f}s"

            # 失败后必须写进延迟 ZSet，等 Dispatcher 到期搬运
            member = f"{task_id}|0"
            score = client.portal.call(
                redis_client.zscore, tenant_delayed_key(tenant_id), member
            )
            assert score is not None and float(score) > before.timestamp()

            # 模拟退避到期：execute_at 拨回过去并重新投递一条消息
            current_message_id = client.portal.call(
                rearm_into_past,
                db_session,
                task_id,
                redis_client,
                stream_key,
                fields,
            )
        else:
            assert record.status == TaskStatus.DLQ, "超过上限必须进入 DLQ"
            assert record.retry_count == MAX_RETRIES
            assert record.error_msg and marker in record.error_msg
            assert "Traceback" in record.error_msg


def test_registered_handler_failure_backs_off_then_dlq(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """demo.fail 恒定失败：指数退避 3 次后进入 DLQ 并记录堆栈。"""
    assert client.portal is not None
    monkeypatch.setattr(settings, "WORKER_RETRY_BASE_DELAY_SECONDS", BACKOFF_BASE)

    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY, task_type="demo.fail")
    assert status == 201
    task_id = UUID(body["task_id"])

    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_id
    )

    _drive_until_dlq(
        client,
        db_session,
        redis_client,
        task_id,
        tenant_id,
        message_id,
        fields,
        task_type="demo.fail",
        marker="demo.fail",
    )

    # 结果查询接口对 DLQ 返回 400 + error_msg
    result = client.get(
        f"/api/v1/tasks/{task_id}/result", headers={"X-API-Key": KEY}
    )
    assert result.status_code == 400
    assert result.json()["status"] == "DLQ"
    assert "RuntimeError" in result.json()["error_msg"]


def test_unregistered_task_type_is_taken_over_by_retry_dlq(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """未注册 task_type：抛 UnregisteredTaskError，由重试/DLQ 管道接管。"""
    assert client.portal is not None
    monkeypatch.setattr(settings, "WORKER_RETRY_BASE_DELAY_SECONDS", BACKOFF_BASE)

    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    status, body = post_task(client, KEY, task_type="ghost.task")
    assert status == 201
    task_id = UUID(body["task_id"])

    message_id, fields = client.portal.call(
        locate_immediate_message, redis_client, task_id, tenant_id
    )

    _drive_until_dlq(
        client,
        db_session,
        redis_client,
        task_id,
        tenant_id,
        message_id,
        fields,
        task_type="ghost.task",
        marker="UnregisteredTaskError",
    )

    record = client.portal.call(fetch_task, db_session, task_id)
    assert "ghost.task" in (record.error_msg or ""), "堆栈应指明缺失的任务类型"
