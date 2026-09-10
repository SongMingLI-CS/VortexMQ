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
    # 默认关闭调试：直接 uvicorn 启动时不会把 SQL（含 payload INSERT）打进日志。
    # 需要框架级调试输出时再显式打开。
    DEBUG: bool = False
    # SQLAlchemy engine echo 独立开关。不要把敏感 payload 通过 SQL 日志泄漏出去。
    SQL_ECHO: bool = False

    # 启动期建表兜底（仅 create_all，不做 ALTER）。默认 true 让本地开发与 docker compose
    # 免手工迁移即可跑；生产多副本应设为 false，改由部署流水线执行
    # `python -m alembic upgrade head`，避免启动期 DDL 与版本化迁移漂移。
    AUTO_CREATE_SCHEMA: bool = True

    # 管理面 API 凭证（X-Admin-Key 明文）。为空时 /api/v1/admin/** 全部返回 503，
    # 避免误部署把跨租户管理接口暴露成匿名可调。必须用随机长串覆盖默认值。
    ADMIN_API_KEY: str = ""

    # 内置演示 Handler（demo.sleep / demo.echo / demo.noop / demo.fail）注册开关。
    # 本地开发与 CI 需要它们（压测脚本、冒烟用例）；生产建议设为 false，
    # 否则任何租户都能用 demo.* 占用 Worker 在途槽位。
    ENABLE_DEMO_HANDLERS: bool = True

    # DeepSeek LLM API 凭证。未配置且未显式打开 AI_MOCK_ENABLED 时，
    # ai.deepseek.chat 会抛 AIProviderError（交由重试 / DLQ 管道显式暴露），
    # 绝不会静默返回假数据冒充模型输出。
    DEEPSEEK_API_KEY: str = ""
    DEEPSEEK_API_URL: str = "https://api.deepseek.com/v1/chat/completions"
    # 单次模型调用超时（秒）。必须小于 Worker 租约回收阈值（WORKER_CLAIM_IDLE_MS/1000），
    # 否则调用还没返回就可能被 Outbox 判为僵尸任务回收重跑。
    DEEPSEEK_TIMEOUT_SECONDS: float = 120.0
    # max_tokens 上限：防止调用方用 payload 把生成预算写到天文数字。
    AI_MAX_TOKENS_LIMIT: int = 8192
    # 离线模拟开关（默认 false）。仅当显式为 true 时，缺少 DEEPSEEK_API_KEY 的
    # ai.deepseek.chat 才返回模拟文本，用于本地演示 / CI 无外部依赖跑通 DAG。
    # 生产必须保持 false：真实调用失败不允许 fallback 成假数据。
    AI_MOCK_ENABLED: bool = False

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
    # Worker 单进程最大并发在途消息数：>1 时预取多条并行执行（至少一次语义下
    # 副作用需幂等）。提高并发时请同步调大 PostgreSQL 连接池（engine pool_size）。
    WORKER_MAX_IN_FLIGHT: int = 4
    # 空闲等待毫秒数：无消息时用 XREADGROUP BLOCK 阻塞等待新消息 / 停机信号，
    # 到期后循环继续，便于响应 Ctrl+C
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
    # Worker 心跳有效期（秒）。心跳 ZSet 的实际键由 app/core/redis.py 拼装为
    # {vortex}:metrics:workers，不通过环境变量配置。
    WORKER_HEARTBEAT_TTL_SECONDS: int = 15

    # Stream 近似裁剪上限，防止只 XACK 不删除把 Redis 磁盘写满
    REDIS_STREAM_MAXLEN: int = 100_000
    # 优先级 >= 该值走租户高优先级车道，避免租户内普通任务堵住紧急任务
    REDIS_PRIORITY_HIGH_THRESHOLD: int = 50


settings = Settings()
