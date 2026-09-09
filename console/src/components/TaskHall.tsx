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
  // history[i] = 第 i+2 页实际使用的翻页游标；当前页 = history.length + 1
  const [history, setHistory] = useState<string[]>([])
  const [nextCursor, setNextCursor] = useState<string | null>(null)
  const pageSize = 20
  const [data, setData] = useState<{ items: AdminTaskItem[]; total: number } | null>(
    null,
  )
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [workflowId, setWorkflowId] = useState<string | null>(null)
  const [busy, setBusy] = useState<string | null>(null)

  const loadPage = useCallback(
    async (cursor: string | undefined, stack: string[]) => {
      setLoading(true)
      setError(null)
      try {
        const res = await api.listTasks({
          status: status || undefined,
          cursor,
          page_size: pageSize,
        })
        setData(res)
        setHistory(stack)
        setNextCursor(res.next_cursor)
      } catch (err) {
        setError(err instanceof ApiError ? err.message : String(err))
      } finally {
        setLoading(false)
      }
    },
    [status],
  )

  const loadFirst = useCallback(() => loadPage(undefined, []), [loadPage])

  useEffect(() => {
    void loadFirst()
  }, [loadFirst])

  // 刷新当前页：用本页（stack 末尾）的游标原地重取，操作后状态即时可见
  const refresh = useCallback(() => {
    const cursor = history.length ? history[history.length - 1] : undefined
    return loadPage(cursor, history)
  }, [history, loadPage])

  const goNext = useCallback(() => {
    if (!nextCursor) return
    const stack = [...history, nextCursor]
    void loadPage(nextCursor, stack)
  }, [history, nextCursor, loadPage])

  const goPrev = useCallback(() => {
    if (history.length === 0) return
    const stack = history.slice(0, -1)
    const cursor = stack.length ? stack[stack.length - 1] : undefined
    void loadPage(cursor, stack)
  }, [history, loadPage])

  const page = history.length + 1
  const totalPages = data ? Math.max(1, Math.ceil(data.total / pageSize)) : 1

  async function act(kind: 'replay' | 'cancel', id: string) {
    setBusy(id)
    setError(null)
    try {
      if (kind === 'replay') await api.replayTask(id)
      else await api.cancelTask(id)
      await refresh()
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
              // status 变化会让 loadFirst 重建，effect 自动回到第一页
              setStatus(e.target.value)
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
        <button onClick={() => void refresh()} disabled={loading}>
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
        <button disabled={history.length === 0} onClick={goPrev}>
          上一页
        </button>
        <span>
          {page} / {totalPages}
        </span>
        <button disabled={!nextCursor} onClick={goNext}>
          下一页
        </button>
      </div>

      {workflowId && (
        <DagView workflowId={workflowId} onClose={() => setWorkflowId(null)} />
      )}
    </div>
  )
}
