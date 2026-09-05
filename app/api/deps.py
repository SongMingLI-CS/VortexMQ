"""
请求级依赖。

租户身份通过 X-API-Key 解析，而不是放在 JSON body 里：
调用方无法伪造 tenant_id，多租户隔离由服务端强制完成。
"""

from fastapi import Depends, Header, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.database import get_db
from app.crud.tenant import get_tenant_by_api_key
from app.models.tenant import Tenant


async def get_current_tenant(
    x_api_key: str | None = Header(
        default=None, alias="X-API-Key", description="租户 API Key"
    ),
    db: AsyncSession = Depends(get_db),
) -> Tenant:
    """校验 API Key 并返回对应租户；缺失或无效统一返回 401。"""
    if x_api_key is None:
        # 未携带凭据属于“未认证”，与“无效凭据”同属 401；不能落回 FastAPI 的 422。
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="缺少 API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    # Header 是明文；库里是 bcrypt 哈希，Verify 通过才放行。
    tenant = await get_tenant_by_api_key(db, x_api_key)
    if tenant is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="无效的 API Key",
            headers={"WWW-Authenticate": "ApiKey"},
        )
    return tenant
