"""可观测性契约：request id 贯穿请求与响应，就绪探针真实探活依赖。

复用 conftest.py 的隔离 TestClient（真实 PostgreSQL / Redis），不额外引入基础设施。
"""

from __future__ import annotations

from fastapi.testclient import TestClient


def test_generated_request_id_is_returned(client: TestClient) -> None:
    """未带 X-Request-ID 时服务端生成一个（uuid4 hex），并回写响应头。"""
    response = client.get("/health")
    assert response.status_code == 200
    generated = response.headers.get("x-request-id")
    assert generated is not None and len(generated) == 32


def test_inbound_request_id_is_reused(client: TestClient) -> None:
    """调用方 / 网关带来的合法 id 必须沿用，便于跨系统串日志。"""
    response = client.get("/health", headers={"X-Request-ID": "trace-abc-123"})
    assert response.headers["x-request-id"] == "trace-abc-123"


def test_oversized_request_id_is_replaced(client: TestClient) -> None:
    """非法 id（过长 / 非可见字符）一律丢弃重建，避免日志注入与头膨胀。"""
    injected = "x" * 300
    response = client.get("/health", headers={"X-Request-ID": injected})
    returned = response.headers["x-request-id"]
    assert returned != injected
    assert len(returned) == 32


def test_error_response_also_carries_request_id(client: TestClient) -> None:
    """401 这类错误响应也带 X-Request-ID，用户报错时可以直接对上日志。"""
    response = client.post("/api/v1/tasks", json={"task_type": "x", "payload": {}})
    assert response.status_code == 401
    assert response.headers.get("x-request-id")


def test_readiness_probe_checks_real_dependencies(client: TestClient) -> None:
    """就绪探针必须真的连 PG 与 Redis，而不是无脑返回 200。"""
    response = client.get("/health/ready")
    assert response.status_code == 200
    assert response.json() == {
        "status": "ok",
        "checks": {"postgres": "ok", "redis": "ok"},
    }


def test_readiness_probe_degrades_when_redis_is_down(
    client: TestClient, monkeypatch
) -> None:
    """Redis 不可用时必须返回 503 + degraded，让编排系统摘掉这个副本。"""

    class _BrokenRedis:
        async def ping(self) -> bool:
            raise ConnectionError("redis down")

    monkeypatch.setattr("app.core.redis.get_redis", lambda: _BrokenRedis())

    response = client.get("/health/ready")
    assert response.status_code == 503
    body = response.json()
    assert body["status"] == "degraded"
    assert body["checks"]["redis"] == "error"
    assert body["checks"]["postgres"] == "ok"
