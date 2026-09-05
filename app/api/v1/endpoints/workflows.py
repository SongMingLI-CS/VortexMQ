"""工作流提交接口。"""

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_tenant
from app.core.database import get_db
from app.models.tenant import Tenant
from app.schemas.workflow import WorkflowCreateRequest, WorkflowCreateResponse
from app.services.workflow import WorkflowValidationError, submit_workflow

router = APIRouter()


@router.post(
    "",
    response_model=WorkflowCreateResponse,
    status_code=status.HTTP_201_CREATED,
    summary="提交 DAG 工作流",
    description="一次性提交有向图。含环则 400；无环则起始节点 PENDING 入队，其余 WAITING。",
    responses={
        400: {"description": "图不合法"},
        413: {"description": "某个节点 payload 超过 256KiB"},
    },
)
async def create_workflow(
    payload: WorkflowCreateRequest,
    db: AsyncSession = Depends(get_db),
    tenant: Tenant = Depends(get_current_tenant),
) -> WorkflowCreateResponse:
    try:
        return await submit_workflow(db, tenant, payload)
    except WorkflowValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
