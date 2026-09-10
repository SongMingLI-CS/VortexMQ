"""P1-8: 任务处理器注册表的行为单测。

只测注册表自身的契约（装饰器注册 / 查找 / UnregisteredTaskError），
不依赖 PostgreSQL / Redis。
"""

from __future__ import annotations

import pytest

from app.core.config import settings
from app.worker.registry import HandlerRegistry, UnregisteredTaskError, vortex_registry
from app.worker import handlers  # noqa: F401,E402  # 副作用：注册 demo.*
from app.worker import handlers as handlers_module
from app.worker.handlers import DEMO_MAX_SLEEP_SECONDS, demo_sleep


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


def test_demo_handler_registration_respects_toggle(monkeypatch) -> None:
    """ENABLE_DEMO_HANDLERS=false 时装饰器不写路由表，实现生产/演示隔离。"""
    fresh = HandlerRegistry()
    monkeypatch.setattr(handlers_module, "vortex_registry", fresh)

    monkeypatch.setattr(settings, "ENABLE_DEMO_HANDLERS", False)

    @handlers_module._register_demo_handler("demo.disabled-probe")
    async def disabled_probe(payload: dict) -> dict:
        return payload

    assert fresh.get("demo.disabled-probe") is None

    monkeypatch.setattr(settings, "ENABLE_DEMO_HANDLERS", True)

    @handlers_module._register_demo_handler("demo.enabled-probe")
    async def enabled_probe(payload: dict) -> dict:
        return payload

    assert fresh.get("demo.enabled-probe") is enabled_probe
    assert disabled_probe is not None


async def test_demo_sleep_rejects_out_of_range_duration() -> None:
    """sleep_seconds 越界必须显式失败：
    否则租户可以用一个 sleep_seconds=1e9 的任务长期占满 Worker 在途槽位。
    """
    with pytest.raises(ValueError):
        await demo_sleep({"sleep_seconds": 10**9})
    with pytest.raises(ValueError):
        await demo_sleep({"sleep_seconds": -1})
    with pytest.raises(ValueError):
        await demo_sleep({"sleep_seconds": "forever"})

    # 合法值（含边界）正常执行，返回结构稳定
    assert await demo_sleep({"sleep_seconds": 0}) == {"output": "data_from_demo.sleep"}
    assert DEMO_MAX_SLEEP_SECONDS > 0

