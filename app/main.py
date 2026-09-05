"""
VortexMQ 应用入口。

职责：组装 FastAPI、挂载路由、初始化连接，并拉起控制面选主循环。
仅 Leader 跑 Outbox Sweeper 与 Delay Dispatcher；Worker 仍是独立进程。
"""

import asyncio
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api.v1.router import api_router
from app.core.config import settings
from app.core.database import engine, init_db
from app.core.leader import is_control_leader
from app.core.metrics import METRICS_CONTENT_TYPE, refresh_runtime_gauges, render_latest_metrics
from app.core.payload import PAYLOAD_TOO_LARGE
from app.core.redis import close_redis, get_redis
from app.services.control_plane import run_control_plane


@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动时建表、连通 Redis，并拉起控制面选主。租户密钥需 python -m app.cli create-tenant 签发。"""
    await init_db()
    await get_redis().ping()

    control_task = asyncio.create_task(run_control_plane(), name="control-plane")
    try:
        yield
    finally:
        control_task.cancel()
        with suppress(asyncio.CancelledError):
            await control_task
        await close_redis()
        await engine.dispose()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="多租户异步任务调度与消息流管道中间件",
    lifespan=lifespan,
)

# 所有业务接口统一挂在 /api/v1 下，方便以后做版本演进
app.include_router(api_router, prefix="/api/v1")


@app.exception_handler(RequestValidationError)
async def request_validation_handler(_request, exc: RequestValidationError) -> JSONResponse:
    """体积超限返回 413；其余校验失败仍是 422。"""
    for err in exc.errors():
        msg = str(err.get("msg", ""))
        ctx_error = (err.get("ctx") or {}).get("error")
        if PAYLOAD_TOO_LARGE in msg or (
            ctx_error is not None and PAYLOAD_TOO_LARGE in str(ctx_error)
        ):
            return JSONResponse(
                status_code=413,
                content={"detail": "Payload Too Large"},
            )
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


@app.get("/health", tags=["ops"], summary="健康检查")
async def health() -> dict[str, str]:
    """供容器探针使用的轻量探活接口。role 标明本副本是否在跑 Sweeper / Dispatcher。"""
    return {
        "status": "ok",
        "service": settings.APP_NAME,
        "role": "leader" if is_control_leader() else "standby",
    }


@app.get("/metrics", tags=["ops"], summary="Prometheus 指标", include_in_schema=False)
async def metrics() -> Response:
    """抓取时从 Redis 刷新 Gauge，再导出 Counter / Histogram / Gauge。"""
    try:
        await refresh_runtime_gauges()
    except Exception:
        # Redis 短暂不可用时仍返回进程内指标，避免 Prometheus 整次 scrape 失败
        pass
    return Response(content=render_latest_metrics(), media_type=METRICS_CONTENT_TYPE)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
