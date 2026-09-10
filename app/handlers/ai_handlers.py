"""AI 相关 Handler：调用 DeepSeek 完成文本生成，支持 XCom 上游上下文注入。

设计约束（生产真实性优先）：

- 注册为 ``ai.deepseek.chat``；
- 从 payload 提取 ``user_prompt_template``，若存在 ``_vortex_sys.upstream_results``
  则把上游 result_data 打平后渲染进模板（多节点上下文传递）；
- **未配置 ``DEEPSEEK_API_KEY`` 时默认显式失败**（``AIProviderError``），由 Worker
  现有的重试 / DLQ 管道接管，绝不静默返回假数据冒充模型输出。
  只有显式打开 ``AI_MOCK_ENABLED=true`` 才走离线模拟（本地演示 / CI）；
- 超时、鉴权失败、配额不足、响应结构异常统一抛 ``AIProviderError``，异常信息保留
  HTTP 状态码与响应片段，DLQ 里的 ``error_msg`` 可直接定位问题；
- ``model`` / ``temperature`` / ``max_tokens`` 在 Handler 内做边界校验，避免调用方
  用 payload 把生成预算写到天文数字。
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

_DEFAULT_MODEL = "deepseek-chat"
_MIN_TEMPERATURE = 0.0
_MAX_TEMPERATURE = 2.0
_DEFAULT_TEMPERATURE = 0.7
_DEFAULT_MAX_TOKENS = 2048
# 供应商错误响应体截断长度：DLQ 的 error_msg 有 8000 字节上限，不要把整段 body 抄进去
_MAX_ERROR_BODY = 500
# 离线模拟耗时：让演示时序接近真实调用，不至于瞬间完成看不出异步效果
_MOCK_LATENCY_SECONDS = 1.0
_MOCK_OUTPUT = (
    "[Mock AI Response] 这是一个测试网文段落：少年陆尘在赛博都市的废墟中醒来，"
    "经脉里流动的不是真气，而是加密后的数据流。"
)


class AIProviderError(RuntimeError):
    """模型供应商侧失败（缺凭证 / 网络 / HTTP 错误 / 响应异常）。

    继承 RuntimeError：Worker 把它当普通业务异常，走 retry_count +1 → 指数退避 →
    超过 WORKER_MAX_RETRIES 进 DLQ 的同一条管道，异常栈会写进 error_msg。
    """


def _bounded_float(
    payload: dict[str, Any], key: str, *, default: float, low: float, high: float
) -> float:
    """读取浮点生成参数并校验范围；非法值抛 AIProviderError（不是静默取默认值）。"""
    raw = payload.get(key, default)
    try:
        value = float(raw)
    except (TypeError, ValueError) as exc:
        raise AIProviderError(f"payload.{key} 必须是数字，实际为 {raw!r}") from exc
    if not low <= value <= high:
        raise AIProviderError(f"payload.{key} 必须落在 [{low}, {high}] 内，实际为 {value}")
    return value


def _bounded_int(
    payload: dict[str, Any], key: str, *, default: int, low: int, high: int
) -> int:
    """读取整数生成参数并校验范围；非法值抛 AIProviderError。"""
    raw = payload.get(key, default)
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise AIProviderError(f"payload.{key} 必须是整数，实际为 {raw!r}")
    try:
        value = int(raw)
    except (TypeError, ValueError) as exc:
        raise AIProviderError(f"payload.{key} 必须是整数，实际为 {raw!r}") from exc
    if not low <= value <= high:
        raise AIProviderError(f"payload.{key} 必须落在 [{low}, {high}] 内，实际为 {value}")
    return value


def _mock_response(upstream_results: dict[str, Any]) -> dict[str, Any]:
    """离线模拟输出；仅在 AI_MOCK_ENABLED=true 且无凭证时使用。"""
    output = _MOCK_OUTPUT
    if upstream_results:
        output += f"（本节点注入了 {len(upstream_results)} 项上游上下文）"
    return {"output": output}


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


async def _call_deepseek(body: dict[str, Any], model: str) -> str:
    """真实调用 DeepSeek；任何失败都转成带上下文的 AIProviderError。"""
    headers = {"Authorization": f"Bearer {settings.DEEPSEEK_API_KEY}"}
    timeout = settings.DEEPSEEK_TIMEOUT_SECONDS
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.post(
                settings.DEEPSEEK_API_URL, headers=headers, json=body
            )
    except httpx.TimeoutException as exc:
        raise AIProviderError(
            f"DeepSeek 调用超时（timeout={timeout}s, model={model}）: {exc}"
        ) from exc
    except httpx.HTTPError as exc:
        raise AIProviderError(f"DeepSeek 网络错误（model={model}）: {exc}") from exc

    if response.status_code >= 400:
        # 401/403 鉴权、402 余额、429 限流、5xx 供应商故障都在这里落地：
        # 状态码与响应片段进 error_msg，运维不必翻 Worker 日志就能区分原因。
        raise AIProviderError(
            f"DeepSeek 返回 HTTP {response.status_code}（model={model}）: "
            f"{response.text[:_MAX_ERROR_BODY]}"
        )

    try:
        data = response.json()
        content = data["choices"][0]["message"]["content"]
    except (ValueError, KeyError, IndexError, TypeError) as exc:
        raise AIProviderError(
            f"DeepSeek 响应结构异常（model={model}）: {response.text[:_MAX_ERROR_BODY]}"
        ) from exc

    if not isinstance(content, str) or not content.strip():
        raise AIProviderError(f"DeepSeek 返回空内容（model={model}）")
    return content


@vortex_registry.register("ai.deepseek.chat")
async def deepseek_chat(payload: dict[str, Any]) -> dict[str, Any]:
    """调用 DeepSeek 生成文本，返回 ``{"output": 生成的文本}``。

    payload 约定：
    - user_prompt_template (str, 必填)：用户提示词模板，可含 ``{output}`` 等 XCom 占位符；
    - model / temperature / max_tokens（可选）：透传给 DeepSeek，越界即失败。
    """
    template = str(payload.get("user_prompt_template", ""))
    if not template.strip():
        raise AIProviderError("payload.user_prompt_template 缺失或为空，无法构造模型请求")

    upstream_results = _extract_upstream_results(payload)
    prompt = _render_user_prompt(template, upstream_results)

    if not settings.DEEPSEEK_API_KEY:
        if not settings.AI_MOCK_ENABLED:
            # 生产默认路径：没凭证就明确失败，不偷偷返回假文本冒充模型输出。
            raise AIProviderError(
                "未配置 DEEPSEEK_API_KEY，ai.deepseek.chat 无法调用真实模型。"
                "如需离线演示，请显式设置 AI_MOCK_ENABLED=true。"
            )
        await asyncio.sleep(_MOCK_LATENCY_SECONDS)
        logger.warning("AI_MOCK_ENABLED=true：ai.deepseek.chat 返回离线模拟文本")
        return _mock_response(upstream_results)

    model = str(payload.get("model") or _DEFAULT_MODEL)
    body: dict[str, Any] = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": _bounded_float(
            payload,
            "temperature",
            default=_DEFAULT_TEMPERATURE,
            low=_MIN_TEMPERATURE,
            high=_MAX_TEMPERATURE,
        ),
        "max_tokens": _bounded_int(
            payload,
            "max_tokens",
            default=_DEFAULT_MAX_TOKENS,
            low=1,
            high=settings.AI_MAX_TOKENS_LIMIT,
        ),
    }
    return {"output": await _call_deepseek(body, model)}
