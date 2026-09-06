import { useState } from 'react'
import { clearAdminKey, hasAdminKey } from './api/client'
import { AdminKeyGate } from './components/AdminKeyGate'
import { TaskHall } from './components/TaskHall'
import { WorkerPanel } from './components/WorkerPanel'

type Tab = 'tasks' | 'workers'

export default function App() {
  const [authed, setAuthed] = useState(hasAdminKey())
  const [tab, setTab] = useState<Tab>('tasks')

  if (!authed) {
    return <AdminKeyGate onAuthed={() => setAuthed(true)} />
  }

  function logout() {
    clearAdminKey()
    setAuthed(false)
  }

  return (
    <div className="app">
      <header className="app__header">
        <h1>VortexMQ Console</h1>
        <nav className="tabs">
          <button
            className={tab === 'tasks' ? 'tab tab--active' : 'tab'}
            onClick={() => setTab('tasks')}
          >
            任务大厅
          </button>
          <button
            className={tab === 'workers' ? 'tab tab--active' : 'tab'}
            onClick={() => setTab('workers')}
          >
            Worker 监控
          </button>
        </nav>
        <button className="btn btn--ghost" onClick={logout}>
          退出
        </button>
      </header>
      <main className="app__main">
        {tab === 'tasks' ? <TaskHall /> : <WorkerPanel />}
      </main>
    </div>
  )
}
