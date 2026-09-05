"""The smallest end-to-end proof that VortexMQ's core task flow works."""

from __future__ import annotations

import uuid
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.enums import TaskStatus
from app.core.redis import tenant_stream_key
from app.core.security import api_key_prefix, hash_api_key
from app.models.task import TaskRecord
from app.models.tenant import Tenant
from app.worker import processor
from tests.helpers import register_handler


async def _create_test_tenant(session: AsyncSession, api_key: str) -> UUID:
    tenant = Tenant(
        name=f"e2e-{uuid.uuid4().hex}",
        api_key_hash=hash_api_key(api_key),
        api_key_prefix=api_key_prefix(api_key),
    )
    session.add(tenant)
    await session.commit()
    await session.refresh(tenant)
    return tenant.id


async def _task_snapshot(session: AsyncSession, task_id: UUID) -> dict:
    record = (
        await session.execute(select(TaskRecord).where(TaskRecord.task_id == task_id))
    ).scalar_one()
    snapshot = {
        "task_id": record.task_id,
        "tenant_id": record.tenant_id,
        "task_type": record.task_type,
        "status": record.status,
    }
    # End this fixture session's read-only savepoint before another session uses
    # the shared test connection. The outer fixture transaction still rolls back.
    await session.commit()
    return snapshot


async def _stream_messages(redis: Redis, stream_key: str):
    return await redis.xrange(stream_key, min="-", max="+")


async def _run_worker(message_id: str, fields: dict[str, str], stream_key: str) -> None:
    await processor.handle_message(message_id, fields, stream_key=stream_key)


def test_immediate_task_full_lifecycle(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """POST -> PostgreSQL/Stream -> Worker -> SUCCESS result API."""
    assert client.portal is not None
    api_key = "vxk_test_core_flow_0123456789abcdef"
    tenant_id = client.portal.call(_create_test_tenant, db_session, api_key)

    response = client.post(
        "/api/v1/tasks",
        headers={"X-API-Key": api_key},
        json={
            "task_type": "email.send",
            "payload": {
                "to": "mvp@example.com",
                "subject": "VortexMQ E2E",
            },
        },
    )

    assert response.status_code == 201
    body = response.json()
    task_id = UUID(body["task_id"])
    assert task_id

    stored = client.portal.call(_task_snapshot, db_session, task_id)
    assert stored == {
        "task_id": task_id,
        "tenant_id": tenant_id,
        "task_type": "email.send",
        "status": TaskStatus.PENDING,
    }

    stream_key = tenant_stream_key(tenant_id)
    messages = client.portal.call(_stream_messages, redis_client, stream_key)
    matching = [item for item in messages if item[1].get("task_id") == str(task_id)]
    assert len(matching) == 1
    message_id, fields = matching[0]
    assert fields["tenant_id"] == str(tenant_id)

    async def execute_without_delay(payload: dict) -> dict:
        assert payload["to"] == "mvp@example.com"
        return {"output": "email accepted"}

    register_handler(monkeypatch, "email.send", execute_without_delay)
    client.portal.call(_run_worker, message_id, fields, stream_key)

    result_response = client.get(
        f"/api/v1/tasks/{task_id}/result",
        headers={"X-API-Key": api_key},
    )
    assert result_response.status_code == 200
    assert result_response.json() == {
        "task_id": str(task_id),
        "status": "SUCCESS",
        "result_data": {"output": "email accepted"},
    }
