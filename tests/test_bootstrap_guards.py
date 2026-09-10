"""启动期 bootstrap 的安全线：`AUTO_CREATE_SCHEMA=false` 时不得触碰数据库。

生产多副本部署要求 schema 只由 Alembic 迁移创建，因此这个开关必须在
「不执行任何 DDL」这一点上被锁死，而不是仅仅改变建表内容。
"""

from __future__ import annotations

from app.core import database
from app.core.config import settings


async def test_init_db_skips_ddl_when_auto_create_disabled(monkeypatch) -> None:
    class _NoTouchEngine:
        def begin(self):  # pragma: no cover - 被调用即失败
            raise AssertionError("AUTO_CREATE_SCHEMA=false 时不允许执行任何启动期 DDL")

    monkeypatch.setattr(settings, "AUTO_CREATE_SCHEMA", False)
    monkeypatch.setattr(database, "engine", _NoTouchEngine())

    # 不抛异常即通过：函数在没有触碰数据库的情况下直接返回
    await database.init_db()


async def test_init_db_runs_ddl_when_auto_create_enabled(monkeypatch) -> None:
    """默认（true）时仍会执行 create_all 兜底，保证本地脚手架可用。"""
    called: list[str] = []

    class _RecordingConnection:
        async def run_sync(self, fn) -> None:
            called.append("run_sync")

    class _RecordingBegin:
        async def __aenter__(self) -> _RecordingConnection:
            return _RecordingConnection()

        async def __aexit__(self, *exc: object) -> bool:
            return False

    class _RecordingEngine:
        def begin(self) -> _RecordingBegin:
            return _RecordingBegin()

    monkeypatch.setattr(settings, "AUTO_CREATE_SCHEMA", True)
    monkeypatch.setattr(database, "engine", _RecordingEngine())

    await database.init_db()
    assert called == ["run_sync"]
