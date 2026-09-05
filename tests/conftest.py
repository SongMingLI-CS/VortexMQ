"""Integration-test fixtures backed by isolated PostgreSQL and Redis state."""

from __future__ import annotations

import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import pytest
from fastapi.testclient import TestClient
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from app.core import redis as redis_module
from app.core.config import settings
from app.core.database import Base, get_db
from app.main import app
from app.services import outbox as outbox_module
from app.worker import processor


@dataclass
class _TestRuntime:
    client: TestClient
    db_session: AsyncSession
    redis: Redis
    db_connection: AsyncConnection
    db_transaction: Any
    db_engine: AsyncEngine
    admin_engine: AsyncEngine
    schema: str
    initial_redis_keys: set[str]


@asynccontextmanager
async def _empty_lifespan(_app):
    """Infrastructure is initialized by the fixtures, not the production lifespan."""
    yield


@pytest.fixture
def _test_runtime(monkeypatch: pytest.MonkeyPatch):
    """Keep all async resources on TestClient's portal event loop."""
    schema = f"vortexmq_test_{uuid.uuid4().hex}"
    database_url = os.getenv("TEST_DATABASE_URL", settings.DATABASE_URL)
    redis_url = os.getenv("TEST_REDIS_URL", "redis://localhost:6379/15")
    redis_prefix = f"vortexmq-test-{uuid.uuid4().hex}"

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = _empty_lifespan
    monkeypatch.setattr(settings, "REDIS_KEY_PREFIX", redis_prefix)

    runtime: _TestRuntime | None = None
    session_factory: async_sessionmaker[AsyncSession] | None = None

    async def setup(client: TestClient) -> _TestRuntime:
        nonlocal session_factory

        admin_engine = create_async_engine(database_url)
        async with admin_engine.begin() as connection:
            await connection.execute(text(f'CREATE SCHEMA "{schema}"'))

        db_engine = create_async_engine(
            database_url,
            connect_args={"server_settings": {"search_path": schema}},
        )
        async with db_engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)

        db_connection = await db_engine.connect()
        db_transaction = await db_connection.begin()
        session_factory = async_sessionmaker(
            bind=db_connection,
            class_=AsyncSession,
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        db_session = session_factory()

        redis = Redis.from_url(redis_url, decode_responses=True)
        await redis.ping()
        initial_redis_keys = {key async for key in redis.scan_iter(match="*")}

        return _TestRuntime(
            client=client,
            db_session=db_session,
            redis=redis,
            db_connection=db_connection,
            db_transaction=db_transaction,
            db_engine=db_engine,
            admin_engine=admin_engine,
            schema=schema,
            initial_redis_keys=initial_redis_keys,
        )

    async def override_get_db():
        assert session_factory is not None
        async with session_factory() as session:
            yield session

    async def teardown(current: _TestRuntime) -> None:
        await current.db_session.close()
        if current.db_transaction.is_active:
            await current.db_transaction.rollback()
        await current.db_connection.close()
        await current.db_engine.dispose()

        current_keys = {key async for key in current.redis.scan_iter(match="*")}
        new_keys = sorted(current_keys - current.initial_redis_keys)
        if new_keys:
            await current.redis.delete(*new_keys)
        await current.redis.aclose()

        async with current.admin_engine.begin() as connection:
            await connection.execute(text(f'DROP SCHEMA "{current.schema}" CASCADE'))
        await current.admin_engine.dispose()

    app.dependency_overrides[get_db] = override_get_db
    try:
        with TestClient(app, raise_server_exceptions=True) as test_client:
            assert test_client.portal is not None
            runtime = test_client.portal.call(setup, test_client)
            assert session_factory is not None

            monkeypatch.setattr(redis_module, "get_redis", lambda: runtime.redis)
            monkeypatch.setattr(processor, "get_redis", lambda: runtime.redis)
            monkeypatch.setattr(processor, "AsyncSessionLocal", session_factory)
            # Outbox 函数通过模块级 AsyncSessionLocal 开短事务，同样指向隔离的 session factory
            monkeypatch.setattr(outbox_module, "AsyncSessionLocal", session_factory)
            try:
                yield runtime
            finally:
                # The portal exists only while TestClient's context is open.
                test_client.portal.call(teardown, runtime)
                runtime = None
    finally:
        app.dependency_overrides.pop(get_db, None)
        app.router.lifespan_context = original_lifespan


@pytest.fixture
def client(_test_runtime: _TestRuntime) -> TestClient:
    """FastAPI client using the isolated test infrastructure."""
    return _test_runtime.client


@pytest.fixture
def db_session(_test_runtime: _TestRuntime) -> AsyncSession:
    """Session inside an outer transaction that is rolled back after each test."""
    return _test_runtime.db_session


@pytest.fixture
def redis_client(_test_runtime: _TestRuntime) -> Redis:
    """Async Redis client; keys created by a test are removed afterward."""
    return _test_runtime.redis
