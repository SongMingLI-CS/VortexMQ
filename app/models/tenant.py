"""
租户表。

多租户隔离的第一道边界：每个调用方持有独立 API Key。
明文只在签发时展示一次；表里只存 bcrypt 哈希和查找前缀。
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import TYPE_CHECKING

from sqlalchemy import DateTime, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base

if TYPE_CHECKING:
    from app.models.task import TaskRecord


class Tenant(Base):
    """租户：VortexMQ 的资源归属主体。"""

    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        comment="租户主键，使用 UUID 避免多环境/分片时的自增冲突",
    )
    name: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        nullable=False,
        comment="租户名称，全局唯一",
    )
    api_key_hash: Mapped[str] = mapped_column(
        String(128),
        unique=True,
        nullable=False,
        comment="API Key 的 bcrypt 哈希；鉴权时用明文 Verify，禁止把哈希当查找键",
    )
    api_key_prefix: Mapped[str] = mapped_column(
        String(32),
        unique=True,
        index=True,
        nullable=False,
        comment="明文 Key 前 24 位，仅用于定位候选行，不能单独通过鉴权",
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="创建时间（UTC）",
    )

    tasks: Mapped[list[TaskRecord]] = relationship(
        back_populates="tenant",
        cascade="all, delete-orphan",
    )
