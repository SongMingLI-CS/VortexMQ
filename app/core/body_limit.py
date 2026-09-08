"""
全局 HTTP 请求体体积闸门（纯 ASGI 中间件）。

Starlette / FastAPI 默认会在 Pydantic 解析前把整个 body 读进内存，单字段
256KiB 校验拦不住“解析前的超大 body”。本中间件在请求进入路由前预读并限流：
- 读满 max_bytes 立即停止累积，排空剩余 chunk 后直接回 413；
- 正常体积则把读到的 body 原样重放给应用，行为与无中间件一致。

优点：不依赖 Content-Length（chunked 也拦得住），不需要服务端特定实现。
代价：中间件持有完整 body 一次（上限已封顶，2MiB 量级可接受）。
"""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from app.core.payload import MAX_HTTP_BODY_BYTES

_413_BODY = json.dumps({"detail": "Payload Too Large"}, separators=(",", ":")).encode(
    "utf-8"
)
_413_HEADERS = [
    (b"content-type", b"application/json"),
    (b"content-length", str(len(_413_BODY)).encode("ascii")),
]

Receive = Callable[[], Awaitable[dict[str, Any]]]
Send = Callable[[dict[str, Any]], Awaitable[None]]
Scope = dict[str, Any]


class BodySizeLimitMiddleware:
    """拦截超过 max_bytes 的 HTTP 请求体，返回 413。"""

    def __init__(self, app: Callable, max_bytes: int = MAX_HTTP_BODY_BYTES) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        body = bytearray()
        draining = False
        disconnected = False
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected = True
                break
            if message["type"] != "http.request":
                continue
            if not draining:
                body.extend(message.get("body", b""))
                if len(body) > self.max_bytes:
                    # 已超限：不再累积，只排空剩余 chunk 让连接正常收尾
                    draining = True
            if message.get("more_body", False):
                continue
            break

        if disconnected:
            return
        if draining or len(body) > self.max_bytes:
            await send(
                {
                    "type": "http.response.start",
                    "status": 413,
                    "headers": _413_HEADERS,
                }
            )
            await send({"type": "http.response.body", "body": _413_BODY})
            return

        payload = bytes(body)
        delivered = False

        async def replayed_receive() -> dict[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": payload, "more_body": False}
            return {"type": "http.disconnect"}

        await self.app(scope, replayed_receive, send)
