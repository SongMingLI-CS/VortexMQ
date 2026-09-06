import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { AdminTaskItem, TaskStatus } from '../types'
import { DagView } from './DagView'
import { StatusBadge } from './StatusBadge'

const ALL_STATUSES: TaskStatus[] = [
  'PENDING',
  'WAITING',
  'RUNNING',
  'SUCCESS',
  'FAILED',
  'DLQ',
  'CANCELED',
]

function fmt(iso: string): string {
  return new Date(iso).toLocaleString()
}

function shortId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 12)}…` : id
}

export function TaskHall() {
  const [status, setStatus] = useState<string>('')
  const [page, setPage] = useState(1)
  const pageSize = 20
  const [data, setData] = useState<{ items: AdminTaskItem[]; total: number } | null>(
    null,
  )
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [workflowId, setWorkflowId] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const res = await api.listTasks({
        status: status || undefined,
        page,
        page_size: pageSize,
      })
      setData(res)
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [status, page])

  useEffect(() => {
    load()
  }, [load])

  const totalPages = data ? Math.max(1, Math.ceil(data.total / pageSize)) : 1

  async function act(kind: 'replay' | 'cancel', id: string) {
    setBusy(id)
    setError(null)
    try {
      if (kind === 'replay') await api.replayTask(id)
      else await api.cancelTask(id)
      await load()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setBusy(null)
    }
  }

  return (
    <div className="panel">
      <div className="panel__toolbar">
        <label>
          状态{' '}
          <select
            value={status}
            onChange={(e) => {
              setStatus(e.target.value)
              setPage(1)
            }}
          >
            <option value="">全部</option>
            {ALL_STATUSES.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        </label>
        <button onClick={load} disabled={loading}>
          刷新
        </button>
        <span className="panel__total">共 {data?.total ?? 0} 条</span>
      </div>

      {error && <div className="banner banner--error">{error}</div>}

      <table className="table">
        <thead>
          <tr>
            <th>任务 ID</th>
            <th>租户</th>
            <th>类型</th>
            <th>状态</th>
            <th>优先级</th>
            <th>重试</th>
            <th>创建时间</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          {(data?.items ?? []).map((t) => (
            <tr key={t.task_id}>
              <td title={t.task_id}>{shortId(t.task_id)}</td>
              <td>{t.tenant_name}</td>
              <td>{t.task_type}</td>
              <td>
                <StatusBadge status={t.status} />
              </td>
              <td>{t.priority}</td>
              <td>{t.retry_count}</td>
              <td>{fmt(t.created_at)}</td>
              <td className="actions">
                {t.workflow_id && (
                  <button
                    className="btn btn--ghost"
                    onClick={() => setWorkflowId(t.workflow_id!)}
                  >
                    DAG
                  </button>
                )}
                {t.status === 'DLQ' && (
                  <button
                    className="btn"
                    disabled={busy === t.task_id}
                    onClick={() => act('replay', t.task_id)}
                  >
                    重放
                  </button>
                )}
                {(t.status === 'PENDING' ||
                  t.status === 'RUNNING' ||
                  t.status === 'WAITING') && (
                  <button
                    className="btn btn--danger"
                    disabled={busy === t.task_id}
                    onClick={() => act('cancel', t.task_id)}
                  >
                    取消
                  </button>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>

      {loading && <div className="panel__loading">加载中…</div>}
      {!loading && data && data.items.length === 0 && (
        <div className="empty">没有符合条件的任务。</div>
      )}

      <div className="pagination">
        <button disabled={page <= 1} onClick={() => setPage(page - 1)}>
          上一页
        </button>
        <span>
          {page} / {totalPages}
        </span>
        <button disabled={page >= totalPages} onClick={() => setPage(page + 1)}>
          下一页
        </button>
      </div>

      {workflowId && (
        <DagView workflowId={workflowId} onClose={() => setWorkflowId(null)} />
      )}
    </div>
  )
}
