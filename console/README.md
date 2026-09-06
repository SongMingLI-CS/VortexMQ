# VortexMQ Console（前端控制台）

基于 **React + Vite + TypeScript + React Flow** 的运维控制台，调用管理面
`/api/v1/admin/**`（`X-Admin-Key` 鉴权）展示任务大厅、Worker 监控与 DAG 可视化。

## 功能

- **任务大厅**：跨租户任务列表，按状态过滤 + 分页；DLQ 任务「重放」，PENDING/RUNNING/WAITING 任务「取消」。
- **Worker 监控**：存活 Worker 节点、心跳时间、瞬时负载（每 5s 自动刷新）。
- **DAG 可视化**：对属于同一 `workflow_id` 的任务，用 React Flow 渲染节点与上下游依赖边（按拓扑层级自动布局，颜色反映状态）。

## 依赖的 API

| 端点 | 用途 |
|------|------|
| `GET /api/v1/admin/tasks` | 任务大厅（分页 + 状态过滤） |
| `POST /api/v1/admin/tasks/{id}/replay` | DLQ 重放 |
| `POST /api/v1/admin/tasks/{id}/cancel` | 强制取消 |
| `GET /api/v1/admin/workers` | Worker 监控 |
| `GET /api/v1/admin/workflows/{id}` | DAG 节点 + 边 |

## 开发运行

前置：Node.js ≥ 18，VortexMQ API 已在本机 `127.0.0.1:8000` 运行，且设置了 `ADMIN_API_KEY`。

```bash
cd console
npm install
npm run dev          # 打开 http://localhost:5173
```

开发期 Vite 会把 `/api` 代理到 `http://127.0.0.1:8000`（见 `vite.config.ts`），
无跨域问题。首次打开页面输入 `X-Admin-Key`（保存在 `localStorage`）。

## 生产构建

```bash
cd console
npm run build        # 产出 dist/
```

将 `dist/` 交给任意静态服务器（或 Nginx）托管，并把 `/api` 反向代理到 VortexMQ API 即可。
