"""AI 相关 Handler：调用 DeepSeek 完成文本生成，支持 XCom 上游上下文注入。

- 注册为 ``ai.deepseek.chat``；
- 从 payload 提取 ``user_prompt_template``，若存在 ``_vortex_sys.upstream_results``
  则把上游 result_data 打平后渲染进模板（多节点上下文传递）；
- 未配置 ``DEEPSEEK_API_KEY`` 时走 Mock 路径：``await asyncio.sleep(1)`` 后返回
  模拟文本，保证流水线随时可测、可演示。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from app.core.config import settings
from app.core.payload import VORTEX_SYS_KEY, VORTEX_UPSTREAM_RESULTS_KEY
from app.worker.registry import vortex_registry

logger = logging.getLogger("vortexmq.handlers.ai")

DEEPSEEK_CHAT_URL = "https://api.deepseek.com/v1/chat/completions"
_DEFAULT_MODEL = "deepseek-chat"


def _extract_upstream_results(payload: dict[str, Any]) -> dict[str, Any]:
    """读取 DAG 注入的上游结果：payload["_vortex_sys"]["upstream_results"]。"""
    sys_ns = payload.get(VORTEX_SYS_KEY)
    if not isinstance(sys_ns, dict):
        return {}
    results = sys_ns.get(VORTEX_UPSTREAM_RESULTS_KEY)
    return results if isinstance(results, dict) else {}


def _render_user_prompt(template: str, upstream_results: dict[str, Any]) -> str:
    """把上游 XCom 结果注入提示词模板。

    upstream_results 形如 ``{parent_task_id: result_data}``。为让模板可读：
    - 把每个上游 result_data（dict）的字段打平到顶层——单个上游返回
      ``{"output": ...}`` 时可直接写 ``{output}``；
    - 保留 ``upstream`` 指向原始 dict，可用 ``{upstream[<task_id>]}`` 精确取值。

    渲染失败（占位符缺失 / 模板含非法花括号）时退回原始模板，不阻塞流水线。
    """
    if not upstream_results:
        return template
    context: dict[str, Any] = {"upstream": upstream_results}
    for result in upstream_results.values():
        if isinstance(result, dict):
            context.update(result)
    try:
        return template.format_map(context)
    except (KeyError, ValueError, IndexError) as exc:
        logger.warning("提示词模板渲染失败，退回原始模板: %s", exc)
        return template


@vortex_registry.register("ai.deepseek.chat")
async def deepseek_chat(payload: dict[str, Any]) -> dict[str, Any]:
    """调用 DeepSeek 生成文本，返回 ``{"output": 生成的文本}``。

    payload 约定：
    - user_prompt_template (str)：用户提示词模板，可含 ``{output}`` 等 XCom 占位符；
    - model / temperature / max_tokens（可选）：透传给 DeepSeek。
    """
    template = str(payload.get("user_prompt_template", ""))
    upstream_results = _extract_upstream_results(payload)
    prompt = _render_user_prompt(template, upstream_results)

    if not settings.DEEPSEEK_API_KEY:
        # 防 CI 阻塞：无凭证时走 Mock，保证 Showcase / 测试无需外部 API 也能跑通。
        await asyncio.sleep(1)
        output = (
            "[Mock AI Response] 这是一个测试网文段落：少年陆尘在赛博都市的废墟中醒来，"
            "经脉里流动的不是真气，而是加密后的数据流。"
        )
        if upstream_results:
            output += f"（本节点注入了 {len(upstream_results)} 项上游上下文）"
        return {"output": output}

    headers = {"Authorization": f"Bearer {settings.DEEPSEEK_API_KEY}"}
    body = {
        "model": payload.get("model", _DEFAULT_MODEL),
        "messages": [{"role": "user", "content": prompt}],
        "temperature": float(payload.get("temperature", 0.7)),
        "max_tokens": int(payload.get("max_tokens", 2048)),
    }
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(DEEPSEEK_CHAT_URL, headers=headers, json=body)
    response.raise_for_status()
    data = response.json()
    text = data["choices"][0]["message"]["content"]
    return {"output": text}
