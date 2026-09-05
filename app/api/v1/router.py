"""v1 路由聚合：后续新增资源在此挂载，保持 main.py 精简。"""

from fastapi import APIRouter

from app.api.v1.endpoints import tasks, workflows

api_router = APIRouter()
api_router.include_router(tasks.router, prefix="/tasks", tags=["tasks"])
api_router.include_router(workflows.router, prefix="/workflows", tags=["workflows"])
