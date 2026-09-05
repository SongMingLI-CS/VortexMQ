"""VortexMQ 同步 / 异步 HTTP 客户端。

底层通信走 ``httpx``（同步 + 异步），每次请求自动注入 ``X-API-Key``。
HTTP 层错误（4xx / 5xx）直接以 ``httpx.HTTPStatusError`` 抛出，调用方无需
自己拼 URL、拼 Header、拼 DAG。
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any

import httpx

if TYPE_CHECKING:
    from vortexmq_client.workflow import Workflow

_TASKS_PATH = "/api/v1/tasks"
_WORKFLOWS_PATH = "/api/v1/workflows"


def _task_body(
    task_type: str,
    payload: dict[str, Any],
    priority: int,
    execute_at: datetime | None,
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "task_type": task_type,
        "payload": payload,
        "priority": priority,
    }
    if execute_at is not None:
        body["execute_at"] = execute_at.isoformat()
    return body


class VortexMQClient:
    """同步客户端。推荐用 ``with`` 管理底层连接池。"""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 30.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self._client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key},
            timeout=timeout,
            transport=transport,
        )

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> VortexMQClient:
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def submit_task(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int = 0,
        execute_at: datetime | None = None,
    ) -> str:
        """提交单个任务，返回 ``task_id``。"""
        response = self._client.post(
            _TASKS_PATH, json=_task_body(task_type, payload, priority, execute_at)
        )
        response.raise_for_status()
        return response.json()["task_id"]

    def get_task_result(self, task_id: str) -> dict[str, Any]:
        """查询任务结果。

        SUCCESS(200) 返回含 ``result_data`` 的字典；未完成(202) 返回含
        ``status`` 的字典；失败/不存在（400/404）抛出 ``HTTPStatusError``。
        """
        response = self._client.get(f"{_TASKS_PATH}/{task_id}/result")
        response.raise_for_status()
        return response.json()

    def submit_workflow(self, workflow: Workflow) -> list[str]:
        """把编排好的 DAG 一次性提交，返回各节点 ``task_id``（按节点插入顺序）。"""
        response = self._client.post(_WORKFLOWS_PATH, json=workflow.to_payload())
        response.raise_for_status()
        return [item["task_id"] for item in response.json()["tasks"]]


class AsyncVortexMQClient:
    """异步客户端。推荐用 ``async with`` 管理底层连接池。"""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        *,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            headers={"X-API-Key": api_key},
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> AsyncVortexMQClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def submit_task(
        self,
        task_type: str,
        payload: dict[str, Any],
        priority: int = 0,
        execute_at: datetime | None = None,
    ) -> str:
        """提交单个任务，返回 ``task_id``。"""
        response = await self._client.post(
            _TASKS_PATH, json=_task_body(task_type, payload, priority, execute_at)
        )
        response.raise_for_status()
        return response.json()["task_id"]

    async def get_task_result(self, task_id: str) -> dict[str, Any]:
        """查询任务结果，语义同同步版。"""
        response = await self._client.get(f"{_TASKS_PATH}/{task_id}/result")
        response.raise_for_status()
        return response.json()

    async def submit_workflow(self, workflow: Workflow) -> list[str]:
        """把编排好的 DAG 一次性提交，返回各节点 ``task_id``（按节点插入顺序）。"""
        response = await self._client.post(_WORKFLOWS_PATH, json=workflow.to_payload())
        response.raise_for_status()
        return [item["task_id"] for item in response.json()["tasks"]]
