import { useState, type FormEvent } from 'react'
import { api, ApiError, setAdminKey } from '../api/client'

export function AdminKeyGate({ onAuthed }: { onAuthed: () => void }) {
  const [key, setKey] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function submit(e: FormEvent) {
    e.preventDefault()
    if (!key.trim()) return
    setLoading(true)
    setError(null)
    setAdminKey(key.trim())
    try {
      // 用一次真实请求校验 Key（错误 Key → 401，未启用 → 503）
      await api.listWorkers()
      onAuthed()
    } catch (err) {
      setError(err instanceof ApiError ? err.message : String(err))
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="gate">
      <form className="gate__form" onSubmit={submit}>
        <h1>VortexMQ Console</h1>
        <p>输入管理面 API Key（<code>X-Admin-Key</code>）以继续。</p>
        <input
          type="password"
          value={key}
          onChange={(e) => setKey(e.target.value)}
          placeholder="X-Admin-Key"
          autoFocus
        />
        {error && <div className="banner banner--error">{error}</div>}
        <button type="submit" disabled={loading}>
          {loading ? '校验中…' : '进入控制台'}
        </button>
      </form>
    </div>
  )
}
