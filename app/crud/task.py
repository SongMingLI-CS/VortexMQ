"""任务记录数据访问。"""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.clock import as_utc, utcnow
from app.core.enums import TaskStatus
from app.models.task import TaskRecord
from app.schemas.task import TaskCreateRequest

MAX_ERROR_MSG_LEN = 8000


async def create_task(
    session: AsyncSession,
    *,
    tenant_id: UUID,
    data: TaskCreateRequest,
) -> TaskRecord:
    """将新任务以 PENDING 状态写入数据库。"""
    execute_at = as_utc(data.execute_at) if data.execute_at is not None else utcnow()
    record = TaskRecord(
        tenant_id=tenant_id,
        status=TaskStatus.PENDING,
        task_type=data.task_type,
        payload=data.payload,
        priority=data.priority,
        retry_count=0,
        execute_at=execute_at,
        workflow_id=None,
        upstream_ids=[],
        downstream_ids=[],
    )
    session.add(record)
    await session.commit()
    await session.refresh(record)
    return record


async def get_task_for_tenant(
    session: AsyncSession,
    task_id: UUID,
    tenant_id: UUID,
) -> TaskRecord | None:
    """按 task_id 读取本租户任务；跨租户视为不存在。"""
    result = await session.execute(
        select(TaskRecord).where(
            TaskRecord.task_id == task_id,
            TaskRecord.tenant_id == tenant_id,
        )
    )
    return result.scalar_one_or_none()


async def get_task_with_tenant(
    session: AsyncSession,
    task_id: UUID,
) -> TaskRecord | None:
    """按 task_id 读取任务并带上租户，供 Worker 打日志与状态流转。"""
    result = await session.execute(
        select(TaskRecord)
        .options(selectinload(TaskRecord.tenant))
        .where(TaskRecord.task_id == task_id)
    )
    return result.scalar_one_or_none()


async def update_task_status(
    session: AsyncSession,
    task: TaskRecord,
    status: TaskStatus,
    *,
    retry_count: int | None = None,
    error_msg: str | None = None,
    execute_at: datetime | None = None,
) -> TaskRecord:
    """更新任务状态并提交。Worker 在 RUNNING / 终态两步各开独立事务。"""
    task.status = status
    if retry_count is not None:
        task.retry_count = retry_count
    if error_msg is not None:
        task.error_msg = error_msg[:MAX_ERROR_MSG_LEN]
    if execute_at is not None:
        task.execute_at = as_utc(execute_at)
    task.updated_at = utcnow()
    await session.commit()
    await session.refresh(task)
    return task


async def claim_stale_pending_tasks(
    session: AsyncSession,
    *,
    stale_seconds: int,
    limit: int,
) -> list[TaskRecord]:
    """
    捞取需要补偿投递的 PENDING 任务。

    SELECT ... FOR UPDATE SKIP LOCKED 的含义：
    - FOR UPDATE：短事务内锁住这些行，只用来刷新 updated_at（投递租约）。
    - SKIP LOCKED：遇到已被其他事务锁住的行直接跳过，而不是等待。
      选主短暂双主时也不会互相堵住，也不会重复认领同一批。

    调用方必须在本事务内只改 updated_at 并立即 COMMIT，再在事务外 XADD/ZADD。
    禁止在持有行锁时 await Redis：网络抖动会把连接池和 Worker 更新一起卡死。
    """
    stmt = (
        select(TaskRecord)
        .where(
            TaskRecord.status == TaskStatus.PENDING,
            TaskRecord.updated_at <= func.now() - timedelta(seconds=stale_seconds),
        )
        .order_by(TaskRecord.priority.desc(), TaskRecord.created_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def claim_stale_running_tasks(
    session: AsyncSession,
    *,
    stale_seconds: int,
    limit: int,
) -> list[TaskRecord]:
    """
    回收租约过期的 RUNNING（Worker 被 kill -9 且 PEL 丢失时，XAUTOCLAIM 救不回来）。

    短事务内改回 PENDING 并刷新 updated_at，COMMIT 后再投递 Redis。
    阈值必须不小于 WORKER_CLAIM_IDLE_MS，避免把仍在执行的长任务当成僵尸。
    """
    stmt = (
        select(TaskRecord)
        .where(
            TaskRecord.status == TaskStatus.RUNNING,
            TaskRecord.updated_at <= func.now() - timedelta(seconds=stale_seconds),
        )
        .order_by(TaskRecord.updated_at.asc())
        .limit(limit)
        .with_for_update(skip_locked=True)
    )
    result = await session.execute(stmt)
    return list(result.scalars().all())


async def claim_task_for_execution(
    session: AsyncSession,
    task_id: UUID,
    *,
    stale_after: timedelta,
) -> UUID | None:
    """
    CAS 乐观锁防重发：把任务原子抢成 RUNNING。

    等价于 UPDATE … WHERE status IN ('PENDING', 'RUNNING') RETURNING，
    但对仍在租约内的 RUNNING 排除，避免第二个 Worker 把活任务再跑一遍。
    租约过期的 RUNNING（原消费者崩溃）允许回收。
    返回空：已被别人抢走、仍在活租约内，或已进入终态 / WAITING。
    """
    now = utcnow()
    stale_before = now - stale_after
    stmt = (
        update(TaskRecord)
        .where(
            TaskRecord.task_id == task_id,
            or_(
                TaskRecord.status == TaskStatus.PENDING,
                and_(
                    TaskRecord.status == TaskStatus.RUNNING,
                    TaskRecord.updated_at <= stale_before,
                ),
            ),
        )
        .values(status=TaskStatus.RUNNING, updated_at=now)
        .returning(TaskRecord.task_id)
        .execution_options(synchronize_session=False)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


def parse_uuid_list(raw: object) -> list[UUID]:
    """JSONB 里的 UUID 读出来是字符串。"""
    if not raw:
        return []
    return [item if isinstance(item, UUID) else UUID(str(item)) for item in raw]


async def get_task_for_update(
    session: AsyncSession,
    task_id: UUID,
) -> TaskRecord | None:
    """SELECT … FOR UPDATE，供 DAG 唤醒 / 取消 / 成功失败落库时锁住任务行。"""
    result = await session.execute(
        select(TaskRecord)
        .where(TaskRecord.task_id == task_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    return result.scalar_one_or_none()


async def get_upstream_snapshots(
    session: AsyncSession,
    task_ids: list[UUID],
    *,
    tenant_id: UUID,
) -> dict[UUID, tuple[TaskStatus, dict | None]]:
    """批量读取上游状态与 result_data。强制带租户谓词，防止跨租户注入 XCom。"""
    if not task_ids:
        return {}
    result = await session.execute(
        select(TaskRecord.task_id, TaskRecord.status, TaskRecord.result_data).where(
            TaskRecord.task_id.in_(task_ids),
            TaskRecord.tenant_id == tenant_id,
        )
    )
    return {row.task_id: (row.status, row.result_data) for row in result.all()}
