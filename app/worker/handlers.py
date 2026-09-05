"""
内置演示 Handler（P1-8）。

把原先 processor.execute_simulated_job 的「无脑 sleep 3 秒」重构为
按类型注册的处理器，供压测 / 冒烟 / 集成测试复用：

- demo.sleep：可配 sleep_seconds 的模拟业务，force_fail=true 时抛错；
- demo.echo：原样回显 payload，适合验证链路数据与 DAG XCom；
- demo.noop：零耗时成功，适合只验证投递/结果回传；
- demo.fail：恒定抛错，用于退避重试与 DLQ 的确定性测试。

真实业务 Handler 应放在自己的业务模块里，同样用 @vortex_registry.register。
"""

from __future__ import annotations

import asyncio

from app.worker.registry import vortex_registry

_DEFAULT_SLEEP_SECONDS = 3.0


@vortex_registry.register("demo.sleep")
async def demo_sleep(payload: dict) -> dict:
    """模拟一段业务耗时后成功；payload.force_fail=true 时主动失败。"""
    duration = float(payload.get("sleep_seconds", _DEFAULT_SLEEP_SECONDS))
    await asyncio.sleep(max(0.0, duration))
    if payload.get("force_fail"):
        raise RuntimeError("demo.sleep 收到 force_fail=true，模拟业务失败")
    return {"output": "data_from_demo.sleep"}


@vortex_registry.register("demo.echo")
async def demo_echo(payload: dict) -> dict:
    """把收到的 payload 原样放进 result_data.echo，便于观察 Worker 看到的数据。"""
    return {"echo": payload}


@vortex_registry.register("demo.noop")
async def demo_noop(payload: dict) -> dict:
    """零耗时成功；适合压测只验证投递吞吐，不关心处理器行为。"""
    return {"output": "noop"}


@vortex_registry.register("demo.fail")
async def demo_fail(payload: dict) -> dict:
    """恒定失败；用于确定性触发指数退避与 DLQ。"""
    raise RuntimeError("demo.fail 恒定为业务失败")
