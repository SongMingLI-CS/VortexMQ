import { useCallback, useEffect, useState } from 'react'
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
  type NodeProps,
  type NodeTypes,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { api, ApiError } from '../api/client'
import type { TaskStatus, WorkflowDetail, WorkflowNode } from '../types'
import { StatusBadge } from './StatusBadge'

interface TaskNodeData {
  taskType: string
  status: TaskStatus
  taskId: string
}

type TaskFlowNode = Node<TaskNodeData>

function TaskNode({ data }: NodeProps) {
  const d = data as unknown as TaskNodeData
  return (
    <div className={`dag-node dag-node--${d.status.toLowerCase()}`}>
      <div className="dag-node__type">{d.taskType}</div>
      <StatusBadge status={d.status} />
      <div className="dag-node__id" title={d.taskId}>
        {d.taskId.slice(0, 8)}…
      </div>
    </div>
  )
}

const nodeTypes: NodeTypes = { task: TaskNode }

function layout(nodes: WorkflowNode[]): { flowNodes: TaskFlowNode[]; edges: Edge[] } {
  // 1) 按上游数量做拓扑分层（Kahn）
  const indegree = new Map<string, number>()
  const children = new Map<string, string[]>()
  nodes.forEach((n) => {
    indegree.set(n.task_id, n.upstream_ids.length)
    n.upstream_ids.forEach((up) => {
      const arr = children.get(up) ?? []
      arr.push(n.task_id)
      children.set(up, arr)
    })
  })

  const level = new Map<string, number>()
  const remaining = new Map(indegree)
  const queue = nodes.filter((n) => n.upstream_ids.length === 0).map((n) => n.task_id)
  queue.forEach((id) => level.set(id, 0))
  while (queue.length) {
    const cur = queue.shift()!
    for (const child of children.get(cur) ?? []) {
      level.set(child, Math.max(level.get(child) ?? 0, (level.get(cur) ?? 0) + 1))
      const deg = (remaining.get(child) ?? 1) - 1
      remaining.set(child, deg)
      if (deg === 0) queue.push(child)
    }
  }

  // 2) 按层分组并布局
  const byLevel = new Map<number, string[]>()
  nodes.forEach((n) => {
    const lvl = level.get(n.task_id) ?? 0
    const arr = byLevel.get(lvl) ?? []
    arr.push(n.task_id)
    byLevel.set(lvl, arr)
  })
  const byId = new Map(nodes.map((n) => [n.task_id, n]))

  const flowNodes: TaskFlowNode[] = []
  const X_GAP = 280
  const Y_GAP = 150
  ;[...byLevel.keys()]
    .sort((a, b) => a - b)
    .forEach((lvl) => {
      const ids = byLevel.get(lvl)!
      const total = ids.length
      ids.forEach((id, i) => {
        const n = byId.get(id)!
        flowNodes.push({
          id,
          type: 'task',
          position: { x: lvl * X_GAP, y: (i - (total - 1) / 2) * Y_GAP },
          data: { taskType: n.task_type, status: n.status, taskId: n.task_id },
        })
      })
    })

  // 3) 每条下游边只建一次
  const edges: Edge[] = []
  nodes.forEach((n) => {
    n.downstream_ids.forEach((child) => {
      edges.push({
        id: `${n.task_id}->${child}`,
        source: n.task_id,
        target: child,
        animated: true,
      })
    })
  })

  return { flowNodes, edges }
}

export function DagView({
  workflowId,
  onClose,
}: {
  workflowId: string
  onClose: () => void
}) {
  const [detail, setDetail] = useState<WorkflowDetail | null>(null)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    setError(null)
    try {
      setDetail(await api.getWorkflow(workflowId))
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    }
  }, [workflowId])

  useEffect(() => {
    load()
  }, [load])

  const { flowNodes, edges } = detail
    ? layout(detail.nodes)
    : { flowNodes: [], edges: [] }

  return (
    <div className="modal">
      <div className="modal__box modal__box--wide">
        <div className="modal__head">
          <h2>
            工作流 DAG <span className="mono">{workflowId}</span>
          </h2>
          <button className="btn btn--ghost" onClick={onClose}>
            关闭
          </button>
        </div>
        {error && <div className="banner banner--error">{error}</div>}
        <div className="dag">
          <ReactFlow
            nodes={flowNodes}
            edges={edges}
            nodeTypes={nodeTypes}
            fitView
            nodesDraggable={false}
          >
            <Background />
            <Controls />
            <MiniMap />
          </ReactFlow>
        </div>
      </div>
    </div>
  )
}
