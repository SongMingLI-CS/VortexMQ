"""阶段三：ai.deepseek.chat Handler 的单元 + 端到端验证（Mock 路径）。

XCom 模板渲染与 DAG 上下文传递在未配置 DEEPSEEK_API_KEY 时也应完整工作。
"""

from __future__ import annotations

from uuid import UUID

import httpx
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import TaskStatus
from app.core.redis import tenant_stream_key
from app.handlers.ai_handlers import _render_user_prompt
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
    """Outline -> Chapter1 经 ai.deepseek.chat Mock 路径走通，且 XCom 注入下游。"""
    assert client.portal is not None
    monkeypatch.setattr(settings, "DEEPSEEK_API_KEY", "")
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
