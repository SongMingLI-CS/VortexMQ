export type TaskStatus =
  | 'PENDING'
  | 'WAITING'
  | 'RUNNING'
  | 'SUCCESS'
  | 'FAILED'
  | 'DLQ'
  | 'CANCELED'

export interface AdminTaskItem {
  task_id: string
  tenant_id: string
  tenant_name: string
  status: TaskStatus
  task_type: string
  priority: number
  retry_count: number
  workflow_id: string | null
  error_msg: string | null
  execute_at: string
  created_at: string
  updated_at: string
}

export interface AdminTaskListResponse {
  items: AdminTaskItem[]
  total: number
  page: number
  page_size: number
}

export interface AdminWorkerInfo {
  name: string
  hostname: string | null
  pid: number | null
  started_at: string | null
  last_seen: string
  in_flight: number
}

export interface WorkflowNode {
  task_id: string
  task_type: string
  status: TaskStatus
  priority: number
  upstream_ids: string[]
  downstream_ids: string[]
  error_msg: string | null
  created_at: string
}

export interface WorkflowDetail {
  workflow_id: string
  nodes: WorkflowNode[]
}
