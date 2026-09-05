"""
Redis 键布局（Hash Tag，兼容 Redis Cluster）。

数据面按租户分片：同一租户的 Stream / 延迟 ZSet 都带 `{tenant_id}`，
Delay Dispatcher 的 Lua 只碰这一组 KEY，保证落在同一 Hash Slot。

控制面用 `{vortex}` 固定到另一个 slot，不和租户数据面抢槽。

  {tenant_id}:vortex:tasks:stream      普通车道
  {tenant_id}:vortex:tasks:stream:h    高优先级车道
  {tenant_id}:vortex:tasks:delayed     延迟 ZSet
  {vortex}:tenants                     租户索引 SET（单 Key，Python 侧读取）
  {vortex}:leader                      控制面选主锁
  {vortex}:metrics:workers             Worker 心跳 ZSet（member=consumer，score=unix 时间戳）
  {vortex}:metrics:workers:load        Worker 负载 Hash（field=consumer，value=JSON 元数据）
"""

import json
import time
from datetime import datetime, timezone
from uuid import UUID

from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import ResponseError

from app.core.clock import as_utc, utcnow
from app.core.config import settings

_pool: ConnectionPool | None = None
_client: Redis | None = None

# 单租户 Lua：KEYS 全部带同一 {tenant_id} Hash Tag，Cluster 合法。
# KEYS[1]=delayed  KEYS[2]=stream  KEYS[3]=stream:h
# ARGV[1]=now  ARGV[2]=batch  ARGV[3]=MAXLEN  ARGV[4]=高优先级阈值  ARGV[5]=tenant_id
_DISPATCH_TENANT_LUA = """
local delayed_key = KEYS[1]
local stream_key = KEYS[2]
local high_stream_key = KEYS[3]
local now_score = ARGV[1]
local batch_limit = tonumber(ARGV[2]) or 100
local maxlen = tonumber(ARGV[3]) or 100000
local high_threshold = tonumber(ARGV[4]) or 50
local tenant_id = ARGV[5]

local members = redis.call(
    'ZRANGEBYSCORE', delayed_key, '-inf', now_score,
    'LIMIT', 0, batch_limit
)
if #members == 0 then
    return {}
end

for i = 1, #members do
    local member = members[i]
    -- member 为 task_id|priority；兼容旧格式 task_id|tenant_id|priority（末段为优先级）
    local task_id = member
    local priority = 0
    local first = true
    for part in string.gmatch(member, '[^|]+') do
        if first then
            task_id = part
            first = false
        else
            local n = tonumber(part)
            if n then
                priority = n
            end
        end
    end
    local dest = stream_key
    if priority >= high_threshold then
        dest = high_stream_key
    end
    redis.call(
        'XADD', dest, 'MAXLEN', '~', maxlen, '*',
        'task_id', task_id, 'tenant_id', tenant_id, 'priority', tostring(priority)
    )
    redis.call('ZREM', delayed_key, member)
end

return members
"""


def _tenant_tag(tenant_id: UUID | str) -> str:
    """Redis Cluster Hash Tag：花括号内的部分决定 slot。"""
    return f"{{{tenant_id}}}"


def control_key(suffix: str) -> str:
    """控制面全局键，固定在 {vortex} 这个 slot。"""
    return f"{{{settings.REDIS_KEY_PREFIX}}}:{suffix}"


def tenant_index_key() -> str:
    return control_key("tenants")


def leader_lock_key() -> str:
    return control_key("leader")


def worker_heartbeat_key() -> str:
    return control_key("metrics:workers")


def worker_load_key() -> str:
    """Worker 负载元数据 Hash，与心跳 ZSet 同一 {vortex} slot。"""
    return control_key("metrics:workers:load")


def tenant_stream_key(tenant_id: UUID | str, *, high: bool = False) -> str:
    base = f"{_tenant_tag(tenant_id)}:{settings.REDIS_STREAM_KEY}"
    return f"{base}:h" if high else base


def tenant_delayed_key(tenant_id: UUID | str) -> str:
    return f"{_tenant_tag(tenant_id)}:{settings.REDIS_DELAYED_KEY}"


def lane_keys_for_tenant(tenant_id: UUID | str) -> tuple[str, str]:
    """同一租户先读高优先级车道，再读普通车道。两条键 Hash Tag 相同。"""
    return (
        tenant_stream_key(tenant_id, high=True),
        tenant_stream_key(tenant_id, high=False),
    )


def is_high_priority(priority: int) -> bool:
    return priority >= settings.REDIS_PRIORITY_HIGH_THRESHOLD


def get_redis() -> Redis:
    """懒加载异步客户端；底层由 ConnectionPool 复用 TCP 连接。"""
    global _pool, _client
    if _client is None:
        _pool = ConnectionPool.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            max_connections=20,
            socket_timeout=None,
            socket_connect_timeout=5,
        )
        _client = Redis(connection_pool=_pool)
    return _client


async def close_redis() -> None:
    """进程退出时释放客户端与连接池，避免 Redis 端残留空闲连接。"""
    global _pool, _client
    if _client is not None:
        await _client.aclose()
        _client = None
    if _pool is not None:
        await _pool.disconnect()
        _pool = None


async def register_tenant(tenant_id: UUID | str) -> None:
    """登记租户到控制面索引。与数据面不同 slot，必须单独命令，不能塞进租户 Lua。"""
    redis = get_redis()
    await redis.sadd(tenant_index_key(), str(tenant_id))


async def list_active_tenant_ids() -> list[str]:
    """有过投递记录的租户，供 Worker 公平轮询 / Dispatcher 逐租户 EVAL。"""
    redis = get_redis()
    raw = await redis.smembers(tenant_index_key())
    return sorted(str(item) for item in raw or [])


async def sum_tenant_stream_lengths() -> int:
    """所有租户车道 Stream 长度之和。pipeline 关闭事务，避免 Cluster 跨 slot MULTI。"""
    tenants = await list_active_tenant_ids()
    if not tenants:
        return 0
    redis = get_redis()
    pipe = redis.pipeline(transaction=False)
    for tenant_id in tenants:
        for key in lane_keys_for_tenant(tenant_id):
            pipe.xlen(key)
    lengths = await pipe.execute()
    return sum(int(item or 0) for item in lengths)


async def sum_tenant_delayed_lengths() -> int:
    tenants = await list_active_tenant_ids()
    if not tenants:
        return 0
    redis = get_redis()
    pipe = redis.pipeline(transaction=False)
    for tenant_id in tenants:
        pipe.zcard(tenant_delayed_key(tenant_id))
    lengths = await pipe.execute()
    return sum(int(item or 0) for item in lengths)


async def publish_task(task_id: UUID, tenant_id: UUID, priority: int = 0) -> str:
    """投递到该租户自己的 Stream，并登记到租户索引。"""
    redis = get_redis()
    await register_tenant(tenant_id)
    stream_key = tenant_stream_key(tenant_id, high=is_high_priority(priority))
    return await redis.xadd(
        stream_key,
        {
            "task_id": str(task_id),
            "tenant_id": str(tenant_id),
            "priority": str(priority),
        },
        maxlen=settings.REDIS_STREAM_MAXLEN,
        approximate=True,
    )


async def zadd_delayed(
    task_id: UUID,
    execute_at: datetime,
    tenant_id: UUID,
    priority: int = 0,
) -> None:
    """写入该租户的延迟 ZSet。member 为 task_id|priority，租户由 Key 上的 Hash Tag 决定。"""
    redis = get_redis()
    await register_tenant(tenant_id)
    score = as_utc(execute_at).timestamp()
    member = f"{task_id}|{int(priority)}"
    await redis.zadd(tenant_delayed_key(tenant_id), {member: score})


async def schedule_wakeup(
    task_id: UUID,
    execute_at: datetime,
    tenant_id: UUID,
    priority: int = 0,
) -> str:
    """按 execute_at 选择租户 Stream 或该租户的延迟 ZSet。"""
    when = as_utc(execute_at)
    if when <= utcnow():
        return await publish_task(task_id, tenant_id, priority)
    await zadd_delayed(task_id, execute_at, tenant_id, priority)
    return "delayed"


async def dispatch_due_delayed_tasks(
    now: datetime | None = None,
    limit: int | None = None,
) -> list[str]:
    """
    逐租户 EVAL：每个脚本只访问带同一 {tenant_id} 的 delayed + 两条 Stream。

    不要再把全局 ZSet 和各租户 Stream 写进同一个 Lua，Cluster 会 CROSSSLOT。
    """
    now_ts = as_utc(now).timestamp() if now is not None else utcnow().timestamp()
    batch = limit if limit is not None else settings.DELAY_DISPATCH_BATCH_SIZE
    moved: list[str] = []
    for tenant_id in await list_active_tenant_ids():
        chunk = await _dispatch_due_for_tenant(tenant_id, now_ts, batch)
        moved.extend(chunk)
    return moved


async def _dispatch_due_for_tenant(tenant_id: str, now_ts: float, batch: int) -> list[str]:
    redis = get_redis()
    delayed = tenant_delayed_key(tenant_id)
    normal, high = (
        tenant_stream_key(tenant_id, high=False),
        tenant_stream_key(tenant_id, high=True),
    )
    # lane_keys 是 (high, normal)，Lua KEYS[2] 要普通、KEYS[3] 要高优
    raw = await redis.eval(
        _DISPATCH_TENANT_LUA,
        3,
        delayed,
        normal,
        high,
        str(now_ts),
        str(batch),
        str(settings.REDIS_STREAM_MAXLEN),
        str(settings.REDIS_PRIORITY_HIGH_THRESHOLD),
        tenant_id,
    )
    if not raw:
        return []
    return [str(item) for item in raw]


async def ensure_consumer_group(stream_key: str) -> None:
    """为指定租户车道创建消费者组；已存在则忽略。"""
    redis = get_redis()
    try:
        await redis.xgroup_create(
            name=stream_key,
            groupname=settings.REDIS_CONSUMER_GROUP,
            id="0",
            mkstream=True,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _parse_worker_meta(raw: str | None) -> dict[str, object]:
    """心跳 Hash 里的 JSON 元数据；旧版本没有该字段时容忍为空。"""
    if not raw:
        return {}
    try:
        meta = json.loads(raw)
        return meta if isinstance(meta, dict) else {}
    except (TypeError, ValueError):
        return {}


async def list_worker_heartbeats() -> list[dict[str, object]]:
    """读取心跳 ZSet + 负载 Hash，返回存活 Worker 详情（Admin /workers 数据源）。

    - 先按 WORKER_HEARTBEAT_TTL_SECONDS 清掉过期 ZSet 成员，剩余成员即存活节点；
    - 顺带清理负载 Hash 中已无心跳的僵尸 field，避免 Hash 无限增长；
    - 负载 Hash 与 ZSet 同带 {vortex} Hash Tag，天然同 slot，无 Cluster 跨键问题。
      ponytail: 负载 Hash 僵尸 field 只在调用本函数时清理。若某部署从不调 /workers，
      崩溃残留会一直占着字段；量级等于「崩溃过的 Worker 名数量」，页面 /metrics 正常
      巡检会周期性触发清理，故不引入单独的清扫协程。
    """
    redis = get_redis()
    heartbeat = worker_heartbeat_key()
    load = worker_load_key()
    cutoff = time.time() - settings.WORKER_HEARTBEAT_TTL_SECONDS

    await redis.zremrangebyscore(heartbeat, "-inf", cutoff)
    scored = await redis.zrange(heartbeat, 0, -1, withscores=True) or []
    if not scored:
        return []

    names = [str(member) for member, _score in scored]
    scores = {str(member): float(score) for member, score in scored}
    raw_loads = await redis.hmget(load, names)

    # 心跳已消失的 Hash field 顺手摘掉；先 hgetall 全量再差集，防止 Hash 缓慢膨胀
    all_loads = await redis.hgetall(load)
    stale_fields = [field for field in all_loads if field not in scores]
    if stale_fields:
        await redis.hdel(load, *stale_fields)

    workers: list[dict[str, object]] = []
    for name, raw in zip(names, raw_loads):
        meta = _parse_worker_meta(raw)
        in_flight = meta.get("in_flight", 0)
        started_at = meta.get("started_at")
        if isinstance(started_at, str):
            try:
                parsed_started_at: object = datetime.fromisoformat(started_at)
            except ValueError:
                parsed_started_at = None
        else:
            parsed_started_at = None
        workers.append(
            {
                "name": name,
                "hostname": meta.get("hostname"),
                "pid": meta.get("pid"),
                "started_at": parsed_started_at,
                "last_seen": datetime.fromtimestamp(scores[name], tz=timezone.utc),
                "in_flight": in_flight if isinstance(in_flight, int) else 0,
            }
        )
    return sorted(workers, key=lambda item: str(item["name"]))
