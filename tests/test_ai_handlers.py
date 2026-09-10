"""阶段三：ai.deepseek.chat Handler 的单元 + 端到端验证（Mock 路径）。

XCom 模板渲染与 DAG 上下文传递在未配置 DEEPSEEK_API_KEY 时也应完整工作。
"""

from __future__ import annotations

from typing import Any
from uuid import UUID

import httpx
import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.redis import tenant_stream_key
from app.handlers import ai_handlers
from app.handlers.ai_handlers import AIProviderError, _render_user_prompt, deepseek_chat
from app.main import app
from tests.helpers import (
    create_tenant,
    fetch_task,
    locate_immediate_message,
    process_message,
)
from vortexmq_client import AsyncVortexMQClient, Workflow

KEY = "vxk_ai_demo_0123456789abcdefghijkl"

OUTLINE_PROMPT = "生成《赛博修仙传》大纲"
CHAPTER1_PROMPT = "根据以下大纲撰写第一章：\n{output}"


def test_render_user_prompt_flattens_single_upstream() -> None:
    """单个上游时，其 result_data 字段被打平到顶层，可直接写 {output}。"""
    upstream = {"task-1": {"output": "大纲内容ABC"}}
    assert _render_user_prompt("第一章：{output}", upstream) == "第一章：大纲内容ABC"
    # 无上游时原样返回
    assert _render_user_prompt("无上游模板", {}) == "无上游模板"
    # 原始映射可通过 upstream 取到
    assert _render_user_prompt("{upstream}", upstream) == str(upstream)


def test_ai_chat_dag_xcom_mock_end_to_end(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """Outline -> Chapter1 在显式开启离线模拟时走通，且 XCom 注入下游。

    AI_MOCK_ENABLED 必须显式打开：默认配置下缺凭证是显式失败，而不是假数据。
    """
    assert client.portal is not None
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "")
    monkeypatch.setattr(settings, "AI_MOCK_ENABLED", True)
    tenant_id = client.portal.call(create_tenant, db_session, KEY)

    sdk = AsyncVortexMQClient(
        "http://testserver", KEY, transport=httpx.ASGITransport(app=app)
    )
    try:
        wf = Workflow()
        outline = wf.add_node(
            "Outline_Agent", "ai.deepseek.chat", {"user_prompt_template": OUTLINE_PROMPT}
        )
        wf.add_node(
            "Chapter1_Agent",
            "ai.deepseek.chat",
            {"user_prompt_template": CHAPTER1_PROMPT},
            depends_on=[outline],
        )
        task_ids = client.portal.call(sdk.submit_workflow, wf)
    finally:
        client.portal.call(sdk.aclose)

    outline_id, chapter_id = UUID(task_ids[0]), UUID(task_ids[1])

    # 执行大纲节点
    mid, fields = client.portal.call(
        locate_immediate_message, redis_client, outline_id, tenant_id
    )
    client.portal.call(
        process_message, mid, fields, tenant_stream_key(tenant_id)
    )

    outline_record = client.portal.call(fetch_task, db_session, outline_id)
    assert outline_record.status == TaskStatus.SUCCESS
    assert "[Mock AI Response]" in outline_record.result_data["output"]
    # 起始节点无上游，不应出现上游注入提示
    assert "上游上下文" not in outline_record.result_data["output"]

    # 大纲成功后，第一章被唤醒（注入 XCom）并进入 Stream
    chapter_record = client.portal.call(fetch_task, db_session, chapter_id)
    assert chapter_record.status == TaskStatus.PENDING

    mid2, fields2 = client.portal.call(
        locate_immediate_message, redis_client, chapter_id, tenant_id
    )
    client.portal.call(
        process_message, mid2, fields2, tenant_stream_key(tenant_id)
    )

    chapter_record = client.portal.call(fetch_task, db_session, chapter_id)
    assert chapter_record.status == TaskStatus.SUCCESS
    assert "[Mock AI Response]" in chapter_record.result_data["output"]
    # XCom 生效：下游注入了恰好 1 项上游上下文
    assert "注入了 1 项上游上下文" in chapter_record.result_data["output"]


class _FakeResponse:
    """最小 httpx.Response 替身：Handler 只用到 status_code / text / json()。"""

    def __init__(self, status_code: int, *, text: str = "", payload: object = None) -> None:
        self.status_code = status_code
        self.text = text
        self._payload = payload

    def json(self) -> object:
        if self._payload is None:
            raise ValueError("响应体不是 JSON")
        return self._payload


class _FakeAsyncClient:
    """替换 ai_handlers 内部的 httpx.AsyncClient，避免单元测试打真实网络。"""

    def __init__(self, response: _FakeResponse) -> None:
        self._response = response
        self.last_request: dict[str, Any] = {}

    def __call__(self, *args: Any, **kwargs: Any) -> "_FakeAsyncClient":
        return self

    async def __aenter__(self) -> "_FakeAsyncClient":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def post(self, url: str, *, headers: dict, json: dict) -> _FakeResponse:
        self.last_request = {"url": url, "headers": headers, "json": json}
        return self._response


def _install_fake_transport(monkeypatch, response: _FakeResponse) -> _FakeAsyncClient:
    fake = _FakeAsyncClient(response)
    monkeypatch.setattr(ai_handlers.httpx, "AsyncClient", fake)
    return fake


async def test_chat_without_key_fails_loudly_by_default(monkeypatch) -> None:
    """默认配置（AI_MOCK_ENABLED=false）下缺凭证必须显式失败，禁止 fallback 成假数据。"""
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "")
    monkeypatch.setattr(settings, "AI_MOCK_ENABLED", False)

    with pytest.raises(AIProviderError) as excinfo:
        await deepseek_chat({"user_prompt_template": "写一段"})

    message = str(excinfo.value)
    assert "DEEPSEEK_API_KEY" in message
    assert "AI_MOCK_ENABLED" in message


async def test_chat_rejects_empty_prompt(monkeypatch) -> None:
    """空 / 缺失提示词是调用方错误，不应消耗一次真实模型调用。"""
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "sk-test")
    with pytest.raises(AIProviderError):
        await deepseek_chat({"user_prompt_template": "   "})
    with pytest.raises(AIProviderError):
        await deepseek_chat({})


async def test_chat_rejects_out_of_range_generation_params(monkeypatch) -> None:
    """越界参数在网络请求之前失败，避免 payload 把生成预算写到天文数字。"""
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "sk-test")

    with pytest.raises(AIProviderError):
        await deepseek_chat({"user_prompt_template": "x", "max_tokens": 10**9})
    with pytest.raises(AIProviderError):
        await deepseek_chat({"user_prompt_template": "x", "max_tokens": 0})
    with pytest.raises(AIProviderError):
        await deepseek_chat({"user_prompt_template": "x", "temperature": "hot"})
    with pytest.raises(AIProviderError):
        await deepseek_chat({"user_prompt_template": "x", "temperature": 9.9})


async def test_chat_maps_provider_http_error_with_status_code(monkeypatch) -> None:
    """供应商 401 必须转成带状态码的显式错误（进 DLQ error_msg），而非裸 HTTP 异常。"""
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "sk-test")
    _install_fake_transport(
        monkeypatch, _FakeResponse(401, text='{"error":"invalid api key"}')
    )

    with pytest.raises(AIProviderError) as excinfo:
        await deepseek_chat({"user_prompt_template": "写一段"})

    message = str(excinfo.value)
    assert "401" in message
    assert "invalid api key" in message


async def test_chat_rejects_malformed_provider_response(monkeypatch) -> None:
    """响应结构异常（缺 choices）不允许静默成功。"""
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "sk-test")
    _install_fake_transport(
        monkeypatch, _FakeResponse(200, text="{}", payload={"unexpected": True})
    )

    with pytest.raises(AIProviderError):
        await deepseek_chat({"user_prompt_template": "写一段"})


async def test_chat_returns_real_provider_content_and_forwards_params(monkeypatch) -> None:
    """成功路径：模型返回的 content 原样作为 output，bounded 参数透传给供应商。"""
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "sk-test")
    fake = _install_fake_transport(
        monkeypatch,
        _FakeResponse(
            200, payload={"choices": [{"message": {"content": "真实模型输出"}}]}
        ),
    )

    result = await deepseek_chat(
        {
            "user_prompt_template": "写一段",
            "model": "deepseek-reasoner",
            "max_tokens": 128,
        }
    )

    assert result == {"output": "真实模型输出"}
    sent = fake.last_request["json"]
    assert sent["model"] == "deepseek-reasoner"
    assert sent["max_tokens"] == 128
    assert sent["messages"] == [{"role": "user", "content": "写一段"}]
    assert fake.last_request["headers"]["Authorization"] == "Bearer sk-test"

