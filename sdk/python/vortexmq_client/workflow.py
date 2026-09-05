"""Fluent DAG 构建器：把「图」用可读的链式调用描述出来，再交给后端校验。

图本身的合法性（环、缺边、重复 node_id）仍由服务端把关，SDK 只负责把
开发者直觉上的依赖关系翻译成后端 ``POST /api/v1/workflows`` 所需的 JSON。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any


class WorkflowNode:
    """图中的一个节点。``node_id`` 仅在本张图内唯一，用于描述依赖边。"""

    def __init__(
        self,
        node_id: str,
        task_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 0,
        execute_at: datetime | None = None,
        depends_on: list[WorkflowNode] | None = None,
    ) -> None:
        self.node_id = node_id
        self.task_type = task_type
        self.payload = payload or {}
        self.priority = priority
        self.execute_at = execute_at
        self.depends_on = list(depends_on or [])

    def to_dict(self) -> dict[str, Any]:
        """序列化为后端 ``WorkflowNode`` 的请求字段。"""
        node: dict[str, Any] = {
            "node_id": self.node_id,
            "task_type": self.task_type,
            "payload": self.payload,
            "priority": self.priority,
        }
        if self.execute_at is not None:
            node["execute_at"] = self.execute_at.isoformat()
        if self.depends_on:
            node["depends_on"] = [parent.node_id for parent in self.depends_on]
        return node


class Workflow:
    """一张有向无环图。按插入顺序持有节点，提交时输出后端格式。"""

    def __init__(self) -> None:
        self._nodes: list[WorkflowNode] = []
        self._by_id: dict[str, WorkflowNode] = {}

    def add_node(
        self,
        node_id: str,
        task_type: str,
        payload: dict[str, Any] | None = None,
        *,
        priority: int = 0,
        execute_at: datetime | None = None,
        depends_on: list[WorkflowNode] | None = None,
    ) -> WorkflowNode:
        """新增一个节点并返回它，便于把它作为下游的 ``depends_on``。"""
        if node_id in self._by_id:
            raise ValueError(f"node_id 在本张工作流内必须唯一: {node_id}")
        node = WorkflowNode(
            node_id,
            task_type,
            payload,
            priority=priority,
            execute_at=execute_at,
            depends_on=depends_on,
        )
        self._nodes.append(node)
        self._by_id[node_id] = node
        return node

    @property
    def nodes(self) -> list[WorkflowNode]:
        """按插入顺序返回节点副本。"""
        return list(self._nodes)

    def to_payload(self) -> dict[str, Any]:
        """输出 ``POST /api/v1/workflows`` 的 JSON body。"""
        return {"nodes": [node.to_dict() for node in self._nodes]}
