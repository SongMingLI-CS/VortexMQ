"""
请求链路可观测性：结构化日志格式 + 贯穿请求的 request id。

设计取舍：

- **不重复 uvicorn 的访问日志**。成功请求（<400）只在 DEBUG 级别逐条记录，
  出错请求（>=400）记 INFO 并带 request id —— 生产日志保持安静但不丢异常。
- **只记录 method / path / status / duration / request_id**：不记录 query string
  （可能带敏感参数）、不记录 header、不记录 body，避免把凭证或载荷写进日志。
- 入站 ``X-Request-ID`` 合法则沿用（便于与网关 / 调用方日志对齐），否则生成；
  同一个 id 回写响应头，并注入本请求内所有 ``vortexmq.*`` 日志行（rid=…）。
"""

from __future__ import annotations

import logging
import re
import time
import uuid
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any

from app.core.config import settings

logger = logging.getLogger("vortexmq.access")

#: 当前请求的关联 ID；不在请求上下文时为空。
request_id_var: ContextVar[str] = ContextVar("vortexmq_request_id", default="-")

# 只接受较短的可见 ASCII：避免把任意 header 内容原样写进日志（日志注入 / 泄漏面）
_VALID_REQUEST_ID = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")

_LOG_FORMAT = "%(asctime)s [%(levelname)s] %(name)s rid=%(request_id)s %(message)s"


class _RequestIdFilter(logging.Filter):
    """把当前请求 id 注入每条日志记录，供格式串使用。"""

    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = request_id_var.get()
        return True


def configure_logging() -> None:
    """统一进程日志格式（DEBUG 控制级别），并让每条日志带 rid=。"""
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(_LOG_FORMAT))
    handler.addFilter(_RequestIdFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(logging.DEBUG if settings.DEBUG else logging.INFO)


Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
Scope = dict[str, Any]


def _inbound_request_id(scope: Scope) -> str | None:
    """取出合法的入站 X-Request-ID；非法值一律丢弃并重新生成。"""
    for key, value in scope.get("headers") or []:
        if key == b"x-request-id":
            candidate = value.decode("latin-1").strip()
            return candidate if _VALID_REQUEST_ID.match(candidate) else None
    return None


class RequestContextMiddleware:
    """为每个 HTTP 请求绑定 request id，回写响应头，并按状态记一条访问日志。"""

    def __init__(self, app: Callable) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        request_id = _inbound_request_id(scope) or uuid.uuid4().hex
        token = request_id_var.set(request_id)
        response_status: int | None = None
        started = time.perf_counter()

        async def send_with_request_id(message: dict[str, Any]) -> None:
            nonlocal response_status
            if message["type"] == "http.response.start":
                response_status = int(message["status"])
                headers = list(message.get("headers") or [])
                headers.append((b"x-request-id", request_id.encode("ascii")))
                message["headers"] = headers
            await send(message)

        try:
            await self.app(scope, receive, send_with_request_id)
        finally:
            duration_ms = (time.perf_counter() - started) * 1000.0
            # 成功请求走 DEBUG（uvicorn 已有访问日志，不重复刷 INFO）；
            # 4xx / 5xx 或未回响应走 INFO，保证异常永远可见。
            level = (
                logging.DEBUG
                if response_status is not None and response_status < 400
                else logging.INFO
            )
            logger.log(
                level,
                "%s %s -> %s %.1fms",
                scope.get("method", "-"),
                scope.get("path", "-"),
                response_status if response_status is not None else "no-response",
                duration_ms,
            )
            request_id_var.reset(token)
