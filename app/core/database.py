"""
异步数据库基础设施。

设计要点：
- create_async_engine + async_sessionmaker：全链路 async/await，不阻塞事件循环。
- expire_on_commit=False：提交后仍可读取 ORM 属性，避免额外 refresh 往返。
- 建表收敛到 Alembic 迁移（migrations/）：多副本并发跑启动期 DDL 会产生
  数据竞争。init_db 保留 create_all 仅作脚手架兜底，生产环境请执行
  `python -m alembic upgrade head`。
"""

from collections.abc import AsyncGenerator
import logging

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings

logger = logging.getLogger("vortexmq.db")


class Base(DeclarativeBase):
    """所有 ORM 模型的声明基类。"""


engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.SQL_ECHO,
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
    """
    脚手架兜底建表：仅对尚不存在的库执行 create_all，不做任何 ALTER。

    生产环境 / 多副本部署必须改用 Alembic：
        全新库   -> python -m alembic upgrade head
        旧版建库 -> python -m alembic stamp head 后再升级
    这样 schema 演进走 migrations/versions/ 下的版本化迁移，避免多个副本
    同时执行启动期 DDL 互相竞争（以及 CREATE TYPE ADD VALUE 的事务限制）。

    ``AUTO_CREATE_SCHEMA=false`` 时完全跳过本函数，schema 只能由迁移创建——
    这是生产多副本部署的推荐配置。
    """
    if not settings.AUTO_CREATE_SCHEMA:
        logger.info("AUTO_CREATE_SCHEMA=false，跳过启动期建表：schema 由 Alembic 迁移管理")
        return

    from app.models import TaskRecord, Tenant  # noqa: F401

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

