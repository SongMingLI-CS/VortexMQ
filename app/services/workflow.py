"""
DAG 工作流：环检测、批量入库、成功唤醒、失败蔓延。

图的边存在 PostgreSQL 行上（upstream_ids / downstream_ids），
Redis 仍然只叫醒已经变成 PENDING 的节点。
"""

from __future__ import annotations

import logging
import uuid
from collections import defaultdict, deque
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm.attributes import flag_modified

from app.core.clock import as_utc, utcnow
from app.core.enums import TaskStatus
from app.core.payload import VORTEX_SYS_KEY, VORTEX_UPSTREAM_RESULTS_KEY
from app.core.redis import schedule_wakeup
from app.crud.task import get_task_for_update, get_upstream_snapshots, parse_uuid_list
from app.models.task import TaskRecord
from app.models.tenant import Tenant
from app.schemas.workflow import (
    WorkflowCreateRequest,
    WorkflowCreateResponse,
    WorkflowTaskResult,
)

logger = logging.getLogger("vortexmq.workflow")


class WorkflowValidationError(ValueError):
    """图不合法（环、缺边、重复 node_id），由 API 层转成 HTTP 400。"""


def topological_sort(nodes: dict[str, list[str]]) -> list[str]:
    """
    Kahn 算法，O(V+E)。

    nodes: node_id -> 父节点列表（入边）。
    返回一个合法拓扑序；存在环或引用了不存在的节点时抛 WorkflowValidationError。
    """
    indegree: dict[str, int] = {node_id: 0 for node_id in nodes}
    children: dict[str, list[str]] = defaultdict(list)

    for node_id, parents in nodes.items():
        seen: set[str] = set()
        for parent in parents:
            if parent not in nodes:
                raise WorkflowValidationError(f"depends_on 引用了不存在的节点: {parent}")
            if parent == node_id:
                raise WorkflowValidationError(f"节点不能依赖自己: {node_id}")
            if parent in seen:
                continue
            seen.add(parent)
            children[parent].append(node_id)
            indegree[node_id] += 1

    queue = deque(node_id for node_id, deg in indegree.items() if deg == 0)
    order: list[str] = []
    while queue:
        current = queue.popleft()
        order.append(current)
        for child in children[current]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)

    if len(order) != len(nodes):
        cyclic = sorted(node_id for node_id, deg in indegree.items() if deg > 0)
        raise WorkflowValidationError(f"工作流包含环，涉及节点: {', '.join(cyclic)}")
    return order


async def submit_workflow(
    session: AsyncSession,
    tenant: Tenant,
    data: WorkflowCreateRequest,
) -> WorkflowCreateResponse:
    """无环则同一事务批量入库；仅起始 PENDING 节点在提交后进入 Redis。"""
    node_ids = [node.node_id for node in data.nodes]
    if len(node_ids) != len(set(node_ids)):
        raise WorkflowValidationError("node_id 在同一张图内必须唯一")

    graph = {node.node_id: list(node.depends_on) for node in data.nodes}
    topological_sort(graph)

    workflow_id = uuid.uuid4()
    task_id_by_node = {node_id: uuid.uuid4() for node_id in node_ids}

    records: list[TaskRecord] = []
    for node in data.nodes:
        upstream = [task_id_by_node[parent] for parent in node.depends_on]
        downstream = [
            task_id_by_node[other.node_id]
            for other in data.nodes
            if node.node_id in other.depends_on
        ]
        execute_at = as_utc(node.execute_at) if node.execute_at is not None else utcnow()
        status = TaskStatus.PENDING if not node.depends_on else TaskStatus.WAITING
        records.append(
            TaskRecord(
                task_id=task_id_by_node[node.node_id],
                tenant_id=tenant.id,
                status=status,
                task_type=node.task_type,
                payload=node.payload,
                priority=node.priority,
                retry_count=0,
                execute_at=execute_at,
                workflow_id=workflow_id,
                upstream_ids=[str(item) for item in upstream],
                downstream_ids=[str(item) for item in downstream],
            )
        )

    session.add_all(records)
    await session.commit()
    for record in records:
        await session.refresh(record)

    roots = [record for record in records if record.status == TaskStatus.PENDING]
    wakeup_ok = False
    for record in roots:
        try:
            await schedule_wakeup(
                record.task_id,
                record.execute_at,
                tenant_id=record.tenant_id,
                priority=record.priority,
            )
            # 投递成功后刷新 updated_at，避免 Outbox 把仍在排队的 PENDING 当成投递失败。
            record.updated_at = utcnow()
            wakeup_ok = True
        except Exception:
            logger.exception(
                "工作流起始任务投递 Redis 失败，交由 Outbox 补偿: task_id=%s",
                record.task_id,
            )
    if wakeup_ok:
        await session.commit()

    logger.info(
        "工作流已受理: workflow_id=%s nodes=%s roots=%s",
        workflow_id,
        len(records),
        len(roots),
    )
    return WorkflowCreateResponse(
        workflow_id=workflow_id,
        tasks=[
            WorkflowTaskResult(
                node_id=node.node_id,
                task_id=task_id_by_node[node.node_id],
                status=TaskStatus.PENDING if not node.depends_on else TaskStatus.WAITING,
                task_type=node.task_type,
                upstream_ids=[task_id_by_node[p] for p in node.depends_on],
                downstream_ids=[
                    task_id_by_node[other.node_id]
                    for other in data.nodes
                    if node.node_id in other.depends_on
                ],
            )
            for node in data.nodes
        ],
    )


async def awaken_downstream(session: AsyncSession, finished: TaskRecord) -> list[TaskRecord]:
    """
    父任务已在本事务中写成 SUCCESS 之后，尝试把子任务从 WAITING 推到 PENDING。

    并发场景：B 依赖 A 和 C，A、C 几乎同时 SUCCESS。
    两个 Worker 都会走进这里去唤醒 B。

    必须 SELECT B FOR UPDATE：
    - 先拿到 B 的行锁，再读 B 的全部上游状态。
    - 未拿到锁的另一方会等待；等锁释放后再看 B，此时要么已是 PENDING（跳过），
      要么仍是 WAITING 且上游已齐（由后完成的那一方负责改 PENDING）。
    - 若不加锁：双方都读到 WAITING + 上游已齐，各自改 PENDING 并各 XADD 一次。

    只锁子任务、不锁上游：上游已经是终态，再锁容易和对方更新自己那一行形成死锁。
    本事务里 finished 已是 SUCCESS，读己之写能看见自己；另一方若尚未提交，
    这边会看到对方仍非 SUCCESS，于是不唤醒，把机会留给后提交的那一方。
    """
    # 先 flush，让本事务刚写入的 result_data / SUCCESS 对后续 SELECT 可见。
    await session.flush()

    ready: list[TaskRecord] = []
    child_ids = parse_uuid_list(finished.downstream_ids)
    # 排序防止 AB-BA 死锁：与 cancel_descendants 使用同一把 UUID 字典序。
    for child_id in sorted(child_ids, key=str):
        child = await get_task_for_update(session, child_id)
        if child is None:
            logger.error("下游任务不存在: parent=%s child=%s", finished.task_id, child_id)
            continue
        if child.tenant_id != finished.tenant_id:
            logger.error("下游租户不匹配，拒绝唤醒: child=%s", child_id)
            continue
        if child.status != TaskStatus.WAITING:
            continue

        upstream_ids = parse_uuid_list(child.upstream_ids)
        # 防御跨租户 XCom：快照查询必须带本任务 tenant_id。
        snapshots = await get_upstream_snapshots(
            session, upstream_ids, tenant_id=finished.tenant_id
        )
        snapshots[finished.task_id] = (finished.status, finished.result_data)
        if not upstream_ids:
            all_ok = True
        else:
            all_ok = all(
                snapshots.get(uid, (None, None))[0] == TaskStatus.SUCCESS
                for uid in upstream_ids
            )

        if not all_ok:
            logger.info(
                "下游仍有未完成上游，保持 WAITING: child=%s parent=%s",
                child.task_id,
                finished.task_id,
            )
            continue

        # 写入系统保留命名空间，避免覆盖用户同名业务字段。
        # 下游读取 payload["_vortex_sys"]["upstream_results"][parent_task_id]。
        upstream_results = {
            str(uid): (snapshots[uid][1] if uid in snapshots else None)
            for uid in upstream_ids
        }
        merged_payload = dict(child.payload or {})
        sys_ns = merged_payload.get(VORTEX_SYS_KEY)
        if not isinstance(sys_ns, dict):
            sys_ns = {}
        else:
            sys_ns = dict(sys_ns)
        sys_ns[VORTEX_UPSTREAM_RESULTS_KEY] = upstream_results
        merged_payload[VORTEX_SYS_KEY] = sys_ns
        child.payload = merged_payload
        # JSONB 赋新 dict 后仍显式标记脏，避免 SQLAlchemy 漏检原地结构变化。
        flag_modified(child, "payload")

        child.status = TaskStatus.PENDING
        child.updated_at = utcnow()
        ready.append(child)
        logger.info(
            "下游上游已齐，已注入 XCom 并 WAITING -> PENDING: child=%s woken_by=%s parents=%s",
            child.task_id,
            finished.task_id,
            len(upstream_ids),
        )
    return ready


async def cancel_descendants(session: AsyncSession, failed: TaskRecord) -> int:
    """
    从 failed 出发 BFS 全部直接/间接下游，将仍为 WAITING 的节点标为 CANCELED。

    先无锁收集子孙（提交后边不再变），再按 UUID 字符串升序 FOR UPDATE，
    与 awaken_downstream 同一锁顺序，避免 AB-BA 死锁。
    """
    frontier = deque(parse_uuid_list(failed.downstream_ids))
    visited: set[UUID] = set()

    while frontier:
        current_id = frontier.popleft()
        if current_id in visited:
            continue
        visited.add(current_id)

        row = (
            await session.execute(
                select(TaskRecord.task_id, TaskRecord.tenant_id, TaskRecord.downstream_ids).where(
                    TaskRecord.task_id == current_id
                )
            )
        ).one_or_none()
        if row is None or row.tenant_id != failed.tenant_id:
            continue
        frontier.extend(
            item for item in parse_uuid_list(row.downstream_ids) if item not in visited
        )

    canceled = 0
    # 排序防止 AB-BA 死锁
    for current_id in sorted(visited, key=str):
        child = await get_task_for_update(session, current_id)
        if child is None or child.tenant_id != failed.tenant_id:
            continue

        if child.status == TaskStatus.WAITING:
            child.status = TaskStatus.CANCELED
            child.updated_at = utcnow()
            canceled += 1
            logger.warning(
                "上游进入 DLQ，级联取消: child=%s failed=%s",
                child.task_id,
                failed.task_id,
            )

    return canceled
