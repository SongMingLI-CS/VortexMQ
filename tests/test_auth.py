"""P0-1: 认证边界 —— 缺失 / 空 / 未知 / 前缀命中但密钥错误的 X-API-Key 一律 401。

锁定行为：未认证（缺失）与认证失败（无效）都不再落回 FastAPI 422，
响应均为稳定 JSON {"detail": ...}，避免把“有没有带 Header”暴露成 422/401 差异。
"""

from __future__ import annotations

import uuid

from fastapi.testclient import TestClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import api_key_prefix
from tests.helpers import create_tenant, post_task

_TASK = {"task_type": "email.send", "payload": {"hello": "world"}}


def test_missing_api_key_returns_401(client: TestClient) -> None:
    response = client.post("/api/v1/tasks", json=_TASK)
    assert response.status_code == 401
    assert response.json() == {"detail": "缺少 API Key"}


def test_missing_api_key_on_result_endpoint_returns_401(client: TestClient) -> None:
    response = client.get(f"/api/v1/tasks/{uuid.uuid4()}/result")
    assert response.status_code == 401
    assert response.json() == {"detail": "缺少 API Key"}


def test_empty_api_key_header_returns_401(client: TestClient) -> None:
    response = client.post("/api/v1/tasks", headers={"X-API-Key": ""}, json=_TASK)
    assert response.status_code == 401
    assert response.json() == {"detail": "无效的 API Key"}


def test_unknown_short_key_returns_401(client: TestClient) -> None:
    response = client.post("/api/v1/tasks", headers={"X-API-Key": "vxk_short"}, json=_TASK)
    assert response.status_code == 401
    assert response.json() == {"detail": "无效的 API Key"}


def test_unknown_long_key_returns_401(client: TestClient) -> None:
    key = "vxk_" + "a" * 40
    response = client.post("/api/v1/tasks", headers={"X-API-Key": key}, json=_TASK)
    assert response.status_code == 401
    assert response.json() == {"detail": "无效的 API Key"}


def test_registered_prefix_with_wrong_secret_returns_401(
    client: TestClient,
    db_session: AsyncSession,
) -> None:
    """前缀命中候选行但 bcrypt Verify 不通过，必须 401，不能仅凭前缀放行。"""
    real_key = "vxk_auth_prefix_0123456789abcdefghijk"
    assert client.portal is not None
    client.portal.call(create_tenant, db_session, real_key)

    guessed = real_key[:24] + "zz"
    assert api_key_prefix(guessed) == api_key_prefix(real_key)
    response = client.post("/api/v1/tasks", headers={"X-API-Key": guessed}, json=_TASK)
    assert response.status_code == 401
    assert response.json() == {"detail": "无效的 API Key"}


def test_valid_api_key_returns_201(client: TestClient, db_session: AsyncSession) -> None:
    api_key = "vxk_auth_valid_0123456789abcdefghij"
    assert client.portal is not None
    client.portal.call(create_tenant, db_session, api_key)
    status_code, body = post_task(client, api_key)
    assert status_code == 201
    assert body["status"] == "PENDING"
