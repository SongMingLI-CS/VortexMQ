import { useCallback, useEffect, useState } from 'react'
import { api, ApiError } from '../api/client'
import type { AdminWorkerInfo } from '../types'

export function WorkerPanel() {
  const [workers, setWorkers] = useState<AdminWorkerInfo[] | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  const load = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      setWorkers(await api.listWorkers())
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    load()
    const timer = setInterval(load, 5000)
    return () => clearInterval(timer)
  }, [load])

  return (
    <div className="panel">
      <div className="panel__toolbar">
        <h2>Worker 节点</h2>
        <button onClick={load} disabled={loading}>
          刷新
        </button>
      </div>
      {error && <div className="banner banner--error">{error}</div>}
      <table className="table">
        <thead>
          <tr>
            <th>名称</th>
            <th>主机</th>
            <th>PID</th>
            <th>启动时间</th>
            <th>最后心跳</th>
            <th>负载</th>
          </tr>
        </thead>
        <tbody>
          {(workers ?? []).map((w) => (
            <tr key={w.name}>
              <td>{w.name}</td>
              <td>{w.hostname ?? '-'}</td>
              <td>{w.pid ?? '-'}</td>
              <td>{w.started_at ? new Date(w.started_at).toLocaleString() : '-'}</td>
              <td>{new Date(w.last_seen).toLocaleString()}</td>
              <td>
                <span className={`in-flight ${w.in_flight > 0 ? 'in-flight--busy' : ''}`}>
                  {w.in_flight}
                </span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {workers && workers.length === 0 && (
        <div className="empty">暂无存活 Worker。</div>
      )}
      {loading && workers === null && (
        <div className="panel__loading">加载中…</div>
      )}
    </div>
  )
}
