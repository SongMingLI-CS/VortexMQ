"""租户签发：动态生成密钥对，明文只回传一次，库里只存哈希。"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import api_key_prefix, generate_api_key, hash_api_key
from app.models.tenant import Tenant


class TenantAlreadyExistsError(ValueError):
    """同名租户已存在，且未指定轮换。"""


async def create_tenant(
    session: AsyncSession,
    name: str,
    *,
    rotate: bool = False,
) -> tuple[Tenant, str]:
    """
    创建租户或轮换 API Key。

    返回 (tenant, plaintext_key)。明文不会写入数据库，调用方必须立刻展示给操作者。
    """
    result = await session.execute(select(Tenant).where(Tenant.name == name))
    tenant = result.scalar_one_or_none()
    if tenant is not None and not rotate:
        raise TenantAlreadyExistsError(
            f"租户已存在: {name}。若要轮换密钥请加 --rotate"
        )

    plaintext = ""
    hashed = ""
    prefix = ""
    for _ in range(8):
        plaintext = generate_api_key()
        prefix = api_key_prefix(plaintext)
        clash = await session.execute(select(Tenant).where(Tenant.api_key_prefix == prefix))
        other = clash.scalar_one_or_none()
        if other is None or (tenant is not None and other.id == tenant.id):
            hashed = hash_api_key(plaintext)
            break
    else:
        raise RuntimeError("无法生成唯一的 API Key 前缀，请重试")

    if tenant is None:
        tenant = Tenant(name=name, api_key_hash=hashed, api_key_prefix=prefix)
        session.add(tenant)
    else:
        tenant.api_key_hash = hashed
        tenant.api_key_prefix = prefix

    await session.commit()
    await session.refresh(tenant)
    return tenant, plaintext
