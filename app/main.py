"""
VortexMQ 应用入口。

职责：组装 FastAPI、挂载路由、初始化连接，并拉起控制面选主循环。
仅 Leader 跑 Outbox Sweeper 与 Delay Dispatcher；Worker 仍是独立进程。
"""

import asyncio
import logging
from contextlib import asynccontextmanager, suppress

from fastapi import FastAPI, Response, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.api.v1.router import api_router
from app.core import redis as redis_module
from app.core.body_limit import BodySizeLimitMiddleware
from app.core.config import settings
from app.core.database import engine, init_db
from app.core.leader import is_control_leader
from app.core.metrics import METRICS_CONTENT_TYPE, refresh_runtime_gauges, render_latest_metrics
from app.core.observability import RequestContextMiddleware, configure_logging
from app.core.payload import PAYLOAD_TOO_LARGE
from app.services.control_plane import run_control_plane

logger = logging.getLogger("vortexmq.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """启动时建表、连通 Redis，并拉起控制面选主。租户密钥需 python -m app.cli create-tenant 签发。"""
    configure_logging()
    await init_db()
    await redis_module.get_redis().ping()

    control_task = asyncio.create_task(run_control_plane(), name="control-plane")
    try:
        yield
    finally:
        control_task.cancel()
        with suppress(asyncio.CancelledError):
            await control_task
        await redis_module.close_redis()
        await engine.dispose()


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="多租户异步任务调度与消息流管道中间件",
    lifespan=lifespan,
)

# 所有业务接口统一挂在 /api/v1 下，方便以后做版本演进
app.include_router(api_router, prefix="/api/v1")

# 全局请求体体积上限（不依赖 Content-Length，chunked 同样拦截）
app.add_middleware(BodySizeLimitMiddleware)
# 后添加的中间件在最外层：请求 id 覆盖限流中间件，保证 413 也带 X-Request-ID
app.add_middleware(RequestContextMiddleware)


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
    """进程存活探针（不做依赖 I/O）。role 标明本副本是否在跑 Sweeper / Dispatcher。

    依赖是否可用请看 ``/health/ready``。
    """
    return {
        "status": "ok",
        "service": settings.APP_NAME,
        "role": "leader" if is_control_leader() else "standby",
    }


@app.get("/health/ready", tags=["ops"], summary="就绪检查（真实探活依赖）")
async def health_ready(response: Response) -> dict[str, object]:
    """真实探活 PostgreSQL 与 Redis；任一不可用返回 503，供编排系统摘流量。

    PG 用一次性 NullPool 引擎现连现断：这样才能区分「连接池里有僵尸连接」与
    「数据库真的不可用」，也避免探针把连接绑死在某个事件循环上。一次
    ``SELECT 1`` + ``PING`` 的开销可以忽略，不要在这里跑重查询。
    """
    checks: dict[str, str] = {}

    try:
        probe_engine = create_async_engine(settings.DATABASE_URL, poolclass=NullPool)
        try:
            async with probe_engine.connect() as connection:
                await connection.execute(text("SELECT 1"))
        finally:
            await probe_engine.dispose()
        checks["postgres"] = "ok"
    except Exception as exc:  # noqa: BLE001 - 探针必须吞掉异常并降级，不能抛给探针
        logger.warning("就绪检查 PostgreSQL 失败: %s", exc)
        checks["postgres"] = "error"

    try:
        await redis_module.get_redis().ping()
        checks["redis"] = "ok"
    except Exception as exc:  # noqa: BLE001
        logger.warning("就绪检查 Redis 失败: %s", exc)
        checks["redis"] = "error"

    healthy = all(value == "ok" for value in checks.values())
    if not healthy:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    return {"status": "ok" if healthy else "degraded", "checks": checks}


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
