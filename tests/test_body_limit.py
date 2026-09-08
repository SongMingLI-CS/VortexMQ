"""全局 HTTP 请求体上限测试。

锁定行为：
- 超过 MAX_HTTP_BODY_BYTES 的请求体在解析前被中间件拦截，返回 413，
  不依赖 Content-Length（chunked 传输同样受控）。
- 正常体积请求不受影响（字段级 256KiB payload 校验仍按原逻辑走）。
"""

from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.core.payload import MAX_HTTP_BODY_BYTES


def test_body_over_global_limit_returns_413_before_parse(
    client: TestClient,
) -> None:
    """>2MiB 请求体直接 413，不进入 Pydantic 解析。"""
    huge_body = json.dumps(
        {"task_type": "demo.noop", "payload": {"blob": "x" * (MAX_HTTP_BODY_BYTES + 1024)}}
    )
    response = client.post(
        "/api/v1/tasks",
        content=huge_body,
        headers={"Content-Type": "application/json"},
    )
    assert response.status_code == 413
    assert response.json() == {"detail": "Payload Too Large"}


def test_normal_body_still_reaches_app_layer(client: TestClient) -> None:
    """正常体积 body 不被误伤：无 Key 仍返回鉴权 401，而非 413。"""
    response = client.post(
        "/api/v1/tasks",
        json={"task_type": "demo.noop", "payload": {}},
    )
    assert response.status_code == 401
    assert response.json()["detail"] == "缺少 API Key"
