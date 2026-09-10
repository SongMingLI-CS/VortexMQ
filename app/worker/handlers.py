"""
内置演示 Handler。

按 task_type 注册的处理器，供压测 / 冒烟 / 集成测试复用：

- demo.sleep：可配 sleep_seconds 的模拟业务，force_fail=true 时抛错；
- demo.echo：原样回显 payload，适合验证链路数据与 DAG XCom；
- demo.noop：零耗时成功，适合只验证投递/结果回传；
- demo.fail：恒定抛错，用于退避重试与 DLQ 的确定性测试。

生产隔离：整组通过 ENABLE_DEMO_HANDLERS 开关注册（默认 true，方便本地开发与
CI）。生产环境应设为 false —— 否则任何租户都能把 demo.* 当免费算力占用
Worker 在途槽位。真实业务 Handler 应放在自己的业务模块里，同样用
``@vortex_registry.register`` 注册。
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from app.core.config import settings
from app.worker.registry import Handler, vortex_registry

_DEFAULT_SLEEP_SECONDS = 3.0
# 演示 Handler 的单次睡眠上限。不加限制时，租户可以用 sleep_seconds=1e9
# 长期占满一个在途槽位（单进程只有 WORKER_MAX_IN_FLIGHT 个）——这是真实的
# 资源耗尽面，所以越界直接失败（进重试 / DLQ），而不是悄悄截断时长。
DEMO_MAX_SLEEP_SECONDS = 60.0


def _register_demo_handler(task_type: str) -> Callable[[Handler], Handler]:
    """按 ENABLE_DEMO_HANDLERS 注册 demo.*；关闭时函数仍可被测试直接调用。"""

    def decorator(handler: Handler) -> Handler:
        if settings.ENABLE_DEMO_HANDLERS:
            vortex_registry.register(task_type)(handler)
        return handler

    return decorator


@_register_demo_handler("demo.sleep")
async def demo_sleep(payload: dict) -> dict:
    """模拟一段业务耗时后成功；payload.force_fail=true 时主动失败。"""
    raw = payload.get("sleep_seconds", _DEFAULT_SLEEP_SECONDS)
    try:
        duration = float(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"payload.sleep_seconds 必须是数字，实际为 {raw!r}") from exc
    if not 0.0 <= duration <= DEMO_MAX_SLEEP_SECONDS:
        raise ValueError(
            f"payload.sleep_seconds 必须落在 [0, {DEMO_MAX_SLEEP_SECONDS}] 内，"
            f"实际为 {duration}"
        )
    await asyncio.sleep(duration)
    if payload.get("force_fail"):
        raise RuntimeError("demo.sleep 收到 force_fail=true，模拟业务失败")
    return {"output": "data_from_demo.sleep"}


@_register_demo_handler("demo.echo")
async def demo_echo(payload: dict) -> dict:
    """把收到的 payload 原样放进 result_data.echo，便于观察 Worker 看到的数据。"""
    return {"echo": payload}


@_register_demo_handler("demo.noop")
async def demo_noop(payload: dict) -> dict:
    """零耗时成功；适合压测只验证投递吞吐，不关心处理器行为。"""
    return {"output": "noop"}


@_register_demo_handler("demo.fail")
async def demo_fail(payload: dict) -> dict:
    """恒定失败；用于确定性触发指数退避与 DLQ。"""
    raise RuntimeError("demo.fail 恒定为业务失败")
