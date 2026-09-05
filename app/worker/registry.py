"""
任务处理器注册表（P1-8）。

Worker 不再按 task_type 硬编码 if/else，而是通过注册表把字符串路由到
具体的异步 Handler。真实业务只需：

    @vortex_registry.register("email.send")
    async def send_email(payload: dict) -> dict:
        ...

Worker 拿不到处理器时抛出 UnregisteredTaskError，交由系统的
指数退避重试 / DLQ 机制接管，而不是静默吞掉。
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

# Handler 约定：async (payload: dict) -> dict。
# payload 可能携带系统保留命名空间 _vortex_sys（如 DAG XCom 注入的上游结果）。
Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class UnregisteredTaskError(KeyError):
    """任务类型没有对应的 Handler。

    继承 KeyError 而不是裸 Exception，方便调用方用 ``except KeyError``
    捕获所有「查不到执行器」的分支。
    """

    def __init__(self, task_type: str) -> None:
        self.task_type = task_type
        super().__init__(f"未注册的任务类型: {task_type}")


class HandlerRegistry:
    """task_type -> Handler 的进程内路由表。

    注册发生在模块 import 期（装饰器副作用），Worker 运行期只读。
    handlers 属性暴露底层 dict，供测试用 monkeypatch.setitem 临时注入。
    """

    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    @property
    def handlers(self) -> dict[str, Handler]:
        return self._handlers

    def register(self, task_type: str):
        """装饰器：``@vortex_registry.register("email.send")``。"""

        def decorator(handler: Handler) -> Handler:
            if not callable(handler):
                raise TypeError(f"注册表项必须是可调用对象: {task_type}")
            self._handlers[task_type] = handler
            return handler

        return decorator

    def get(self, task_type: str) -> Handler | None:
        """返回 Handler；未注册返回 None（不抛异常，便于探测）。"""
        return self._handlers.get(task_type)

    def require(self, task_type: str) -> Handler:
        """返回 Handler；未注册抛 UnregisteredTaskError。"""
        handler = self._handlers.get(task_type)
        if handler is None:
            raise UnregisteredTaskError(task_type)
        return handler

    def registered_names(self) -> list[str]:
        """当前已注册的任务类型（按字典序，便于日志与观测）。"""
        return sorted(self._handlers)


# 进程级单例。app/worker/handlers 与各业务模块在 import 期向它注册。
vortex_registry = HandlerRegistry()
