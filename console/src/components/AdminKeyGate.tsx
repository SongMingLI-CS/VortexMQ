import { useState, type FormEvent } from 'react'
import { api, ApiError, clearAdminKey, setAdminKey } from '../api/client'

export function AdminKeyGate({ onAuthed }: { onAuthed: () => void }) {
  const [key, setKey] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)

  async function submit(e: FormEvent) {
    e.preventDefault()
    const candidate = key.trim()
    if (!candidate) return
    setLoading(true)
    setError(null)
    // 先用候选 Key 打一次真实请求校验（错误 Key → 401，未启用 → 503）；
    // 校验失败立即清掉，绝不让浏览器里留下一个「看起来已登录」的错误凭证。
    setAdminKey(candidate)
    try {
      await api.listWorkers()
      onAuthed()
    } catch (err) {
      clearAdminKey()
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
