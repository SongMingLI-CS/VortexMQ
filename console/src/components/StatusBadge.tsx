import type { TaskStatus } from '../types'

const LABEL: Record<TaskStatus, string> = {
  PENDING: '排队中',
  WAITING: '等待上游',
  RUNNING: '执行中',
  SUCCESS: '成功',
  FAILED: '失败',
  DLQ: '死信',
  CANCELED: '已取消',
}

export function StatusBadge({ status }: { status: TaskStatus }) {
  return (
    <span className={`badge badge--${status.toLowerCase()}`} title={status}>
      {LABEL[status]}
    </span>
  )
}
