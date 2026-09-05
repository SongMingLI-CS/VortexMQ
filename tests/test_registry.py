"""P1-8: 任务处理器注册表的行为单测。

只测注册表自身的契约（装饰器注册 / 查找 / UnregisteredTaskError），
不依赖 PostgreSQL / Redis。
"""

from __future__ import annotations

from app.worker.registry import HandlerRegistry, UnregisteredTaskError, vortex_registry
from app.worker import handlers  # noqa: F401,E402  # 副作用：注册 demo.*


def test_decorator_registers_and_require_resolves() -> None:
    registry = HandlerRegistry()

    @registry.register("unit.echo")
    async def echo(payload: dict) -> dict:
        return {"echo": payload}

    # register 装饰器应原样返回被装饰函数，便于链式复用
    assert registry.require("unit.echo") is echo
    assert registry.get("unit.echo") is echo
    assert registry.get("ghost.type") is None


def test_require_unregistered_raises_with_task_type() -> None:
    registry = HandlerRegistry()
    try:
        registry.require("ghost.type")
    except UnregisteredTaskError as exc:
        assert exc.task_type == "ghost.type"
        assert "ghost.type" in str(exc)
    else:  # pragma: no cover - 防御误改
        raise AssertionError("require() 对未注册类型必须抛 UnregisteredTaskError")


def test_registered_names_are_sorted() -> None:
    registry = HandlerRegistry()

    @registry.register("zeta.handler")
    async def zeta(payload: dict) -> dict:
        return payload

    @registry.register("alpha.handler")
    async def alpha(payload: dict) -> dict:
        return payload

    assert registry.registered_names() == ["alpha.handler", "zeta.handler"]


def test_builtin_demo_handlers_are_registered() -> None:
    """导入 app.worker.handlers 后，内置 demo.* 应全部可路由。"""
    for task_type in ("demo.sleep", "demo.echo", "demo.noop", "demo.fail"):
        assert vortex_registry.get(task_type) is not None, f"缺少内置 Handler: {task_type}"
