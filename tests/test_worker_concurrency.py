"""Worker 并发预取回归测试。

验证 run_worker_loop 的并发边界：
- WORKER_MAX_IN_FLIGHT=2 时，三个任务在同一时刻最多只有两个在途；
- 预取确实并行（同时观测到两个 Handler 在跑），而不是退化成串行；
- 消息全部处理且每个 Handler 恰好执行一次，任务最终 SUCCESS；
- 停机时在途任务排空后循环退出。
"""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.enums import TaskStatus
from app.worker import main as worker_main
from tests.helpers import (
    create_tenant,
    fetch_task,
    post_task,
    register_handler,
)

KEY = "vxk_conc_key_0123456789abcdefghij"
CONSUMER = "conc-consumer"


async def _set_event(event: asyncio.Event) -> None:
    """BlockingPortal.call 里必须传协程函数，不能直接传 Event.set 绑定方法。"""
    event.set()


def test_worker_prefetch_respects_max_in_flight(
    client: TestClient,
    db_session: AsyncSession,
    redis_client: Redis,
    monkeypatch,
) -> None:
    """三个任务 + 上限 2：先并行跑 2 个，第 3 个等空位；每个只执行一次。"""
    assert client.portal is not None
    monkeypatch.setattr(settings, "WORKER_MAX_IN_FLIGHT", 2)
    monkeypatch.setattr(settings, "WORKER_BLOCK_MS", 250)
    # 测试隔离的 Redis 实例：conftest 只打了 app.core.redis.get_redis，
    # worker_main 在 import 期已绑定同名函数对象，需单独替换。
    monkeypatch.setattr(worker_main, "get_redis", lambda: redis_client)

    tenant_id = client.portal.call(create_tenant, db_session, KEY)
    task_ids: list[UUID] = []
    for _ in range(3):
        status, body = post_task(client, KEY)
        assert status == 201
        task_ids.append(UUID(body["task_id"]))

    async def _prepare() -> dict:
        state: dict = {
            "active": 0,
            "max_active": 0,
            "calls": 0,
            "two_active": asyncio.Event(),
            "release": asyncio.Event(),
            "stop_event": asyncio.Event(),
            "worker_task": None,
        }

        async def handler(payload: dict) -> dict:
            # 每处都同步改 dict，事件循环单线程无锁安全
            state["active"] += 1
            state["max_active"] = max(state["max_active"], state["active"])
            state["calls"] += 1
            if state["active"] == settings.WORKER_MAX_IN_FLIGHT:
                state["two_active"].set()
            try:
                # 闸门：等测试侧放开再收尾，用来观测“同一时刻几个在途”
                await asyncio.wait_for(state["release"].wait(), timeout=5)
            finally:
                state["active"] -= 1
            return {"output": "ok"}

        register_handler(monkeypatch, "email.send", handler)
        state["worker_task"] = asyncio.create_task(
            worker_main.run_worker_loop(CONSUMER, state["stop_event"]),
            name="worker-loop-under-test",
        )
        return state

    state = client.portal.call(_prepare)

    # 1) 容量 = 2：应观测到两个 Handler 同时在途（证明预取并行）
    async def _wait_two() -> None:
        await asyncio.wait_for(state["two_active"].wait(), timeout=5)

    client.portal.call(_wait_two)

    # 短暂停留，给调度器“错误地越界派发第 3 条”留出时间
    time.sleep(0.3)
    assert state["max_active"] == 2, f"并发应被限制在 2，实际 {state['max_active']}"

    # 2) 放行，让第 3 个任务占空位执行
    client.portal.call(_set_event, state["release"])

    async def _wait_all_success() -> None:
        for _ in range(200):
            done = True
            for task_id in task_ids:
                record = await fetch_task(db_session, task_id)
                if record.status != TaskStatus.SUCCESS:
                    done = False
                    break
            if done:
                return
            await asyncio.sleep(0.05)
        raise AssertionError("任务未在超时内全部 SUCCESS")

    client.portal.call(_wait_all_success)

    # 3) 停机：等在途任务排空后循环应自行退出
    async def _stop() -> None:
        state["stop_event"].set()
        await asyncio.wait_for(state["worker_task"], timeout=5)

    client.portal.call(_stop)

    assert state["calls"] == 3, f"每个消息应恰好执行一次，实际 {state['calls']}"
    assert state["max_active"] == 2
