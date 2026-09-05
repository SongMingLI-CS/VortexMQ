"""
控制面 Leader 租约（基于 Redis 的互斥锁，不是 Redlock）。

单 Key SET NX PX + Lua 续期/释放。Cluster 上该 Key 带 {vortex} Hash Tag，本身合法。
短暂双主窗口由 Outbox SKIP LOCKED 与 Worker CAS 兜底，不会撕状态。
"""

from __future__ import annotations

import logging
import os
import socket
from uuid import uuid4

from app.core.config import settings
from app.core.redis import get_redis, leader_lock_key

logger = logging.getLogger("vortexmq.leader")

_RENEW_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_LUA = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""

_holder_token: str | None = None
_is_leader: bool = False


def is_control_leader() -> bool:
    """当前进程是否持有控制面 Leader。供 /health 展示。"""
    return _is_leader


def _token() -> str:
    global _holder_token
    if _holder_token is None:
        _holder_token = f"{socket.gethostname()}-{os.getpid()}-{uuid4().hex[:8]}"
    return _holder_token


async def try_acquire_leader() -> bool:
    """尝试抢锁。成功则本节点成为 Leader。"""
    global _is_leader
    redis = get_redis()
    acquired = await redis.set(
        leader_lock_key(),
        _token(),
        nx=True,
        px=settings.CONTROL_LEADER_TTL_MS,
    )
    _is_leader = bool(acquired)
    return _is_leader


async def renew_leader() -> bool:
    """仅持有者能续期。失败表示锁已丢，必须停掉 Sweeper / Dispatcher。"""
    global _is_leader
    redis = get_redis()
    raw = await redis.eval(
        _RENEW_LUA,
        1,
        leader_lock_key(),
        _token(),
        str(settings.CONTROL_LEADER_TTL_MS),
    )
    _is_leader = int(raw or 0) == 1
    return _is_leader


async def tick_leader() -> bool:
    """Leader 续期；Standby 尝试当选。"""
    if _is_leader:
        ok = await renew_leader()
        if not ok:
            logger.warning("控制面 Leader 租约续期失败，降为 Standby")
        return ok
    won = await try_acquire_leader()
    if won:
        logger.info("本节点当选控制面 Leader token=%s", _token())
    return won


async def release_leader() -> None:
    """进程退出时释放，加速 failover；TTL 到期也会自动丢锁。"""
    global _is_leader
    try:
        redis = get_redis()
        await redis.eval(_RELEASE_LUA, 1, leader_lock_key(), _token())
    except Exception:
        logger.exception("释放 Leader 锁失败")
    _is_leader = False
