"""租户数据访问。"""

from __future__ import annotations

import asyncio

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import API_KEY_PREFIX_LEN, api_key_prefix, verify_api_key
from app.models.tenant import Tenant


async def get_tenant_by_api_key(session: AsyncSession, api_key: str) -> Tenant | None:
    """
    Header 明文 Key → 前缀定位 → bcrypt Verify。

    bcrypt 带随机盐，不能 SELECT hash = bcrypt(key)。先用前缀缩小到候选行再校验。
    """
    if not api_key or len(api_key) < API_KEY_PREFIX_LEN:
        return None

    prefix = api_key_prefix(api_key)
    result = await session.execute(select(Tenant).where(Tenant.api_key_prefix == prefix))
    tenant = result.scalar_one_or_none()
    if tenant is None:
        return None

    matched = await asyncio.to_thread(verify_api_key, api_key, tenant.api_key_hash)
    if not matched:
        return None
    return tenant
