"""
异步数据库基础设施。

设计要点：
- create_async_engine + async_sessionmaker：全链路 async/await，不阻塞事件循环。
- expire_on_commit=False：提交后仍可读取 ORM 属性，避免额外 refresh 往返。
- 当前用 metadata.create_all 建表，适合脚手架阶段；生产环境应切换到 Alembic 迁移。
"""

from collections.abc import AsyncGenerator

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings


class Base(DeclarativeBase):
    """所有 ORM 模型的声明基类。"""


engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI 依赖：每个请求分配一个独立 Session。

    业务层负责 commit；发生异常时由调用方 rollback。
    async with 结束时会自动 close，避免连接泄漏。
    """
    async with AsyncSessionLocal() as session:
        yield session


async def init_db() -> None:
    """根据 ORM 元数据创建尚不存在的表，并为已有库补齐新增列。"""
    from app.models import TaskRecord, Tenant  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        # create_all 不会 ALTER 已存在的表；本地第一步库需要补 error_msg
        await conn.execute(
            text("ALTER TABLE task_records ADD COLUMN IF NOT EXISTS error_msg TEXT")
        )
        await conn.execute(
            text(
                "ALTER TABLE task_records ADD COLUMN IF NOT EXISTS execute_at "
                "TIMESTAMPTZ NOT NULL DEFAULT NOW()"
            )
        )
        await conn.execute(
            text("ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'WAITING'")
        )
        await conn.execute(
            text("ALTER TYPE task_status ADD VALUE IF NOT EXISTS 'CANCELED'")
        )
        await conn.execute(
            text("ALTER TABLE task_records ADD COLUMN IF NOT EXISTS workflow_id UUID")
        )
        await conn.execute(
            text(
                "ALTER TABLE task_records ADD COLUMN IF NOT EXISTS upstream_ids "
                "JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE task_records ADD COLUMN IF NOT EXISTS downstream_ids "
                "JSONB NOT NULL DEFAULT '[]'::jsonb"
            )
        )
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_task_records_workflow_id "
                "ON task_records (workflow_id)"
            )
        )
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_task_records_status_updated_at "
                "ON task_records (status, updated_at)"
            )
        )
        await conn.execute(
            text("ALTER TABLE task_records ADD COLUMN IF NOT EXISTS result_data JSONB")
        )
        await _migrate_tenant_api_key_hash(conn)


async def _migrate_tenant_api_key_hash(conn) -> None:
    """
    平滑升级：旧库 tenants.api_key 明文 → api_key_hash + api_key_prefix，然后删明文列。
    新库由 create_all 直接建哈希列，本函数只补索引。
    """
    cols = {
        row[0]
        for row in (
            await conn.execute(
                text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = 'public' AND table_name = 'tenants'"
                )
            )
        ).all()
    }
    if not cols:
        return

    if "api_key_hash" not in cols:
        await conn.execute(text("ALTER TABLE tenants ADD COLUMN api_key_hash VARCHAR(128)"))
    if "api_key_prefix" not in cols:
        await conn.execute(text("ALTER TABLE tenants ADD COLUMN api_key_prefix VARCHAR(32)"))

    if "api_key" in cols:
        from app.core.security import api_key_prefix, hash_api_key

        rows = (
            await conn.execute(
                text(
                    "SELECT id, api_key FROM tenants "
                    "WHERE api_key IS NOT NULL AND (api_key_hash IS NULL OR api_key_prefix IS NULL)"
                )
            )
        ).all()
        for tenant_id, plain in rows:
            await conn.execute(
                text(
                    "UPDATE tenants SET api_key_hash = :h, api_key_prefix = :p WHERE id = :id"
                ),
                {"h": hash_api_key(plain), "p": api_key_prefix(plain), "id": tenant_id},
            )
        await conn.execute(text("ALTER TABLE tenants DROP COLUMN IF EXISTS api_key"))

    await conn.execute(
        text("CREATE UNIQUE INDEX IF NOT EXISTS ix_tenants_api_key_hash ON tenants (api_key_hash)")
    )
    await conn.execute(
        text(
            "CREATE UNIQUE INDEX IF NOT EXISTS ix_tenants_api_key_prefix "
            "ON tenants (api_key_prefix)"
        )
    )
    empty_or_filled = (
        await conn.execute(
            text(
                "SELECT COUNT(*) FROM tenants "
                "WHERE api_key_hash IS NULL OR api_key_prefix IS NULL"
            )
        )
    ).scalar_one()
    if empty_or_filled == 0:
        await conn.execute(text("ALTER TABLE tenants ALTER COLUMN api_key_hash SET NOT NULL"))
        await conn.execute(text("ALTER TABLE tenants ALTER COLUMN api_key_prefix SET NOT NULL"))

