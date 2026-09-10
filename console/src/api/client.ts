import type {
  AdminTaskItem,
  AdminTaskListResponse,
  AdminWorkerInfo,
  WorkflowDetail,
} from '../types'

const KEY_STORAGE = 'vortexmq.adminKey'

export class ApiError extends Error {
  status: number

  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

export function getAdminKey(): string {
  return localStorage.getItem(KEY_STORAGE) ?? ''
}

export function setAdminKey(key: string): void {
  localStorage.setItem(KEY_STORAGE, key)
}

export function clearAdminKey(): void {
  localStorage.removeItem(KEY_STORAGE)
}

export function hasAdminKey(): boolean {
  return getAdminKey().length > 0
}

type UnauthorizedHandler = () => void

let unauthorizedHandler: UnauthorizedHandler | null = null

/** 注册 401 回调：管理面 Key 失效（被轮换 / 后端换了 Key）时把用户送回登录门。 */
export function setUnauthorizedHandler(handler: UnauthorizedHandler | null): void {
  unauthorizedHandler = handler
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Key': getAdminKey(),
      ...(init?.headers ?? {}),
    },
  })
  if (!res.ok) {
    if (res.status === 401) {
      // 清掉失效凭证并通知 UI 退回登录门；否则控制台会一直显示 401
      // 却仍停在「已登录」的假状态。
      clearAdminKey()
      unauthorizedHandler?.()
    }
    let detail = `HTTP ${res.status}`
    try {
      const body = await res.json()
      if (typeof body?.detail === 'string') detail = body.detail
      else if (body?.detail) detail = JSON.stringify(body.detail)
    } catch {
      // 非 JSON 响应体，保留默认 detail
    }
    throw new ApiError(res.status, detail)
  }
  return (await res.json()) as T
}

export const api = {
  listTasks(params: { status?: string; cursor?: string; page_size?: number } = {}) {
    const q = new URLSearchParams()
    if (params.status) q.set('status', params.status)
    if (params.cursor) q.set('cursor', params.cursor)
    q.set('page_size', String(params.page_size ?? 20))
    return request<AdminTaskListResponse>(`/api/v1/admin/tasks?${q.toString()}`)
  },

  replayTask(taskId: string) {
    return request<AdminTaskItem>(`/api/v1/admin/tasks/${taskId}/replay`, {
      method: 'POST',
    })
  },

  cancelTask(taskId: string) {
    return request<AdminTaskItem>(`/api/v1/admin/tasks/${taskId}/cancel`, {
      method: 'POST',
    })
  },

  listWorkers() {
    return request<AdminWorkerInfo[]>(`/api/v1/admin/workers`)
  },

  getWorkflow(workflowId: string) {
    return request<WorkflowDetail>(`/api/v1/admin/workflows/${workflowId}`)
  },
}
