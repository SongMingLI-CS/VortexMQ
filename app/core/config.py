"""
应用配置。

设计要点：
- 使用 pydantic-settings，配置来源统一为环境变量 / .env，避免硬编码。
- DATABASE_URL 必须带 asyncpg 驱动前缀，才能走 SQLAlchemy 异步引擎。
"""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """全局配置单例的数据源。"""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    APP_NAME: str = "VortexMQ"
    APP_VERSION: str = "0.1.0"
    DEBUG: bool = True

    # 管理面 API 凭证（X-Admin-Key 明文）。为空时 /api/v1/admin/** 全部返回 503，
    # 避免误部署把跨租户管理接口暴露成匿名可调。必须用随机长串覆盖默认值。
    ADMIN_API_KEY: str = ""

    # 例：postgresql+asyncpg://postgres:postgres@localhost:5432/vortexmq
    DATABASE_URL: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/vortexmq"

    # Redis 连接。Streams 只作为投递管道，任务状态仍以 PostgreSQL 为准。
    REDIS_URL: str = "redis://localhost:6379/0"
    # 控制面 Hash Tag 前缀，不要带花括号。实际键为 {vortex}:tenants / {vortex}:leader
    REDIS_KEY_PREFIX: str = "vortex"
    # 数据面键后缀。实际键为 {tenant_id}:vortex:tasks:stream，保证同一租户落同一 Hash Slot
    REDIS_STREAM_KEY: str = "vortex:tasks:stream"
    REDIS_DELAYED_KEY: str = "vortex:tasks:delayed"
    REDIS_CONSUMER_GROUP: str = "vortex:workers"
    # 为空时 Worker 用 hostname-pid 生成，保证多实例消费者名不冲突
    WORKER_CONSUMER_NAME: str = ""
    # XREADGROUP 阻塞毫秒数；到期后循环继续，便于响应 Ctrl+C
    WORKER_BLOCK_MS: int = 5000
    # PEL 中空闲超过该毫秒数的消息可被其他 Worker XAUTOCLAIM
    WORKER_CLAIM_IDLE_MS: int = 30000
    # 长任务租约心跳间隔（秒）。执行期间周期性刷新 RUNNING 任务的 updated_at，
    # 使超时执行的任务不被 Outbox / 二次 CAS 误判为僵尸重复执行。
    # 必须明显小于 WORKER_CLAIM_IDLE_MS（默认 30s → 心跳 5s）。
    WORKER_LEASE_HEARTBEAT_SECONDS: float = 5.0
    # 业务失败后的最大执行次数：第 3 次失败进入 DLQ（retry_count 从 0 累加）
    WORKER_MAX_RETRIES: int = 3
    # 指数退避基数（秒）：next_execute_at = now + base * 2^retry_count
    WORKER_RETRY_BASE_DELAY_SECONDS: float = 5.0

    # Outbox Sweeper：补偿 PG 已提交但 Redis 投递失败的任务
    OUTBOX_SWEEP_INTERVAL_SECONDS: int = 10
    OUTBOX_STALE_SECONDS: int = 30
    OUTBOX_BATCH_SIZE: int = 20

    # Delay Dispatcher：每秒把各租户 ZSet 中到期的任务原子转入同 slot 的 Stream
    DELAY_DISPATCH_INTERVAL_SECONDS: float = 1.0
    DELAY_DISPATCH_BATCH_SIZE: int = 100

    # 控制面选主：仅 Leader 跑 Sweeper / Dispatcher。TTL 必须明显大于续约间隔
    CONTROL_LEADER_TTL_MS: int = 10_000
    CONTROL_LEADER_RENEW_SECONDS: float = 3.0

    # Worker 指标 HTTP 端口；Prometheus 在 Docker 网络内抓取 worker:8001/metrics
    WORKER_METRICS_PORT: int = 8001
    WORKER_HEARTBEAT_TTL_SECONDS: int = 15
    REDIS_WORKER_HEARTBEAT_KEY: str = "vortex:metrics:workers"  # 兼容旧 .env；实际键为 {vortex}:metrics:workers

    # Stream 近似裁剪上限，防止只 XACK 不删除把 Redis 磁盘写满
    REDIS_STREAM_MAXLEN: int = 100_000
    # 优先级 >= 该值走租户高优先级车道，避免租户内普通任务堵住紧急任务
    REDIS_PRIORITY_HIGH_THRESHOLD: int = 50


settings = Settings()
