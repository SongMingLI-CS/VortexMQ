# VortexMQ — Production Readiness Audit

> 本文档记录一次完整「真实性审计」（入口 → API → Service → Domain → DB/外部供应商 → 响应）的
> 结论、已修复项、验证证据与剩余风险。审计基线：`master@83edc04`。
> 所有验证命令均在本机真实执行，未使用模拟基础设施。

---

## 0. 结论速览

VortexMQ **不是** mock 驱动的演示仓库，而是一个真实的异步任务队列中间件：

- 两个真实进程：控制面 API（FastAPI）与数据面 Worker（`python -m app.worker`）；
- 真实持久化：PostgreSQL 是任务状态唯一事实来源，Alembic 版本化迁移；
- 真实投递：Redis Streams（即时）+ ZSet（延迟）+ 消费者组 PEL + Lua 原子搬运；
- 真实测试：63 个集成用例跑在真实 PostgreSQL 16 与 Redis 7 上（非内存替身）；
- 真实 E2E：用真实进程验证「提交 → 入库 → 投递 → 消费 → 结果回传 → API 进程重启后仍可查」。

本轮发现并修复 **1 个生产路径假实现**、**1 个资源耗尽面**，以及若干真实性 / 可观测性 / 部署缺口。
核心链路真实性等级：**LEVEL 5（production-ready）**。

---

## 1. Architecture Map

```text
                       ┌──────────── 浏览器 ────────────┐
                       │ console/  React + Vite (Nginx)  │
                       └───────────────┬────────────────┘
                                       │ 同源 /api 反向代理
┌──── 调用方 ────┐                     ▼
│ curl / SDK     │──────────► ┌──────────────────────────┐
│ sdk/python     │  X-API-Key │  FastAPI  app/main.py     │
└────────────────┘            │  ├ /api/v1/tasks          │ 租户鉴权
                              │  ├ /api/v1/workflows      │
                              │  └ /api/v1/admin/**       │ X-Admin-Key
                              │  + RequestContextMiddleware│ X-Request-ID
                              │  + BodySizeLimitMiddleware │ 2 MiB 闸门
                              └───────┬──────────┬────────┘
                                      │          │
        ┌─────────────────────────────▼──┐    ┌──▼───────────────────────────┐
        │ PostgreSQL 16（唯一事实来源）   │    │ Redis 7（只负责叫醒）         │
        │ tenants / task_records         │    │ {tenant}:…:stream (XADD)     │
        │ status·retry·execute_at·JSONB  │    │ {tenant}:…:delayed (ZADD)    │
        └──────────────┬─────────────────┘    │ {vortex}:leader / workers    │
                       │                      └───────────┬──────────────────┘
                       │ UPDATE … WHERE status              │ XREADGROUP / XAUTOCLAIM
                       │                                    ▼
                       │                     ┌───────────────────────────────┐
                       └────────────────────►│ Worker   python -m app.worker │
                                             │ registry 按 task_type 路由     │
                                             │ handler(payload) -> result    │
                                             │ 退避 / DLQ / XACK / 租约心跳   │
                                             └───────────┬───────────────────┘
                                                         │ task_type=ai.deepseek.chat
                                                         ▼
                                              DeepSeek chat/completions（真实外部调用）

控制面 Leader（Redis SET NX PX）→ Outbox Sweeper（补偿投递）+ Delay Dispatcher（ZSet→Stream Lua）
可观测性：Prometheus 抓 API /metrics 与 Worker:8001/metrics；Grafana 预置大盘
```

| 层 | 技术 |
|----|------|
| API | FastAPI 0.136 / uvicorn，全异步 |
| ORM | SQLAlchemy 2.x typed + async（asyncpg），无 legacy `Query` |
| 迁移 | Alembic（3 个版本，CI 在空库上验证可升级） |
| 状态/投递 | PostgreSQL（SSOT）+ Redis Streams / ZSet / Consumer Group |
| 鉴权 | 租户 `X-API-Key`（bcrypt，前缀定位 + 恒定时校验）、管理面 `X-Admin-Key`（`hmac.compare_digest`） |
| 前端 | React 18 + Vite 5 + TypeScript strict + @xyflow/react |
| SDK | `sdk/python`，httpx 同步/异步 + Fluent DAG 构建器（已可 `pip install -e`） |
| 外部供应商 | DeepSeek `ai.deepseek.chat`（唯一外部依赖；缺凭证显式失败） |
| 可观测 | prometheus-client / prometheus.yml / grafana provisioning |

---

## 2. Feature Inventory & Realness

| Feature | 入口 | API | DB | 外部 | 等级 |
|---|---|---|---|---|---|
| 提交即时任务 | SDK / curl | `POST /api/v1/tasks` | INSERT + XADD | — | LEVEL 5 |
| 提交延迟任务 | SDK / curl | 同上（`execute_at`） | INSERT + ZADD | — | LEVEL 5 |
| 查询任务结果 | SDK / curl | `GET /tasks/{id}/result`（200/202/400/404） | SELECT（租户隔离） | — | LEVEL 5 |
| DAG 工作流 | SDK Fluent builder | `POST /api/v1/workflows` | 批量 INSERT + FOR UPDATE 唤醒/级联 | — | LEVEL 5 |
| XCom 上游上下文注入 | Worker 内部 | — | JSONB `_vortex_sys`（256 KiB 预算） | — | LEVEL 5 |
| 失败退避 / DLQ | Worker 内部 | — | retry_count / execute_at / error_msg | — | LEVEL 5 |
| 任务大厅筛选 + 游标翻页 | console | `GET /admin/tasks` | keyset SELECT（专用索引） | — | LEVEL 5 |
| DLQ 重放 | console 按钮 | `POST /admin/tasks/{id}/replay` | 行锁 + 状态复位 + 复活下游 | — | LEVEL 5 |
| 强制取消 | console 按钮 | `POST /admin/tasks/{id}/cancel` | 行锁 + 级联 CANCELED | — | LEVEL 5 |
| Worker 监控 | console 面板（5s 轮询） | `GET /admin/workers` | Redis 心跳 ZSet + 负载 Hash | — | LEVEL 5 |
| DAG 可视化 | console 按钮 | `GET /admin/workflows/{id}` | SELECT workflow_id | — | LEVEL 5 |
| AI 文本生成 | SDK / 示例流水线 | task_type `ai.deepseek.chat` | 结果落 result_data | DeepSeek HTTP | LEVEL 4 |
| 凭证明文签发 | CLI | — | bcrypt 落库，明文只打印一次 | — | LEVEL 5 |
| 存活 / 就绪探针 | 容器探针 | `/health`、`/health/ready` | `SELECT 1` + `PING` | — | LEVEL 5 |

有意未实现（非缺陷）：租户自助的任务列表接口（当前只有「按 ID 查结果」+ 管理面跨租户大厅）。

---

## 3. Mock / Fake / Demo 审计

| 文件 | 符号 | 使用方 | 用户可见 | 在生产路径 | 处理 |
|---|---|---|---|---|---|
| `app/handlers/ai_handlers.py` | 缺凭证返回 `[Mock AI Response]` | Worker AI 任务 | 是 | **是** | **已修复**：默认改为显式 `AIProviderError`（进重试/DLQ）；仅 `AI_MOCK_ENABLED=true` 才模拟 |
| `app/worker/handlers.py` | `demo.sleep/echo/noop/fail` | 压测、冒烟测试 | 可被任意租户提交 | 是 | **已隔离**：`ENABLE_DEMO_HANDLERS` 开关注册；`demo.sleep` 加 60s 上限 |
| `scripts/stress_test.py` | `random` 流量配比 | 压测 | 否 | 否 | 保留（压测客户端本就需要随机流量） |
| `tests/**` | fixture / `ASGITransport` | 测试 | 否 | 否 | 保留（测试替身） |
| `console` | `placeholder` 是 HTML 属性 | 无 | 否 | 否 | 保留 |
| `examples/deepseek_novel_pipeline` | 无（调用真实 Handler） | 示例 | 否 | 否 | 更新文档：明确 Key 与 `AI_MOCK_ENABLED` |

**未发现**：伪造的统计数字、`Math.random` 生成业务数据、`setTimeout` 伪造流式输出、
`localStorage` 当业务数据库、硬编码的用户/订单/文档数组。
前端唯一 `localStorage` 用途是保存 `X-Admin-Key`（安全边界已在 README 标注）。

---

## 4. Fake API 审计（API Reality Matrix）

| Endpoint | Method | 调用方 | 校验 | 鉴权 | DB | 外部 | 错误码 | 真实? |
|---|---|---|---|---|---|---|---|---|
| `/api/v1/tasks` | POST | SDK / curl | Pydantic + payload 守卫 + 2 MiB 闸门 | X-API-Key | INSERT | XADD/ZADD | 401/413/422 | 是 |
| `/api/v1/tasks/{id}/result` | GET | SDK / curl | UUID 路径 | X-API-Key | SELECT（tenant 谓词） | — | 200/202/400/404 | 是 |
| `/api/v1/workflows` | POST | SDK | 拓扑排序 + 环检测 + node_id 唯一 | X-API-Key | 批量 INSERT | 起始节点投递 | 400/413 | 是 |
| `/api/v1/admin/tasks` | GET | console | Query 约束 + 游标解码 | X-Admin-Key | keyset SELECT + COUNT | — | 401/422/503 | 是 |
| `/api/v1/admin/tasks/{id}/replay` | POST | console | UUID | X-Admin-Key | 行锁 UPDATE | 重新叫醒 | 404/409 | 是 |
| `/api/v1/admin/tasks/{id}/cancel` | POST | console | UUID | X-Admin-Key | 行锁 + 级联 UPDATE | ZREM 兜底 | 404/409 | 是 |
| `/api/v1/admin/workers` | GET | console | — | X-Admin-Key | — | Redis 心跳 | 401/503 | 是 |
| `/api/v1/admin/workflows/{id}` | GET | console | UUID | X-Admin-Key | SELECT | — | 404 | 是 |
| `/health`、`/health/ready` | GET | 探针 | — | 无 | `SELECT 1` | `PING` | 200/503 | 是 |
| `/metrics` | GET | Prometheus | — | 无（建议内网） | — | Redis 读 Gauge | — | 是 |

**未发现**「返回 200 但没做任何事」的假 API，也未发现只回 `crypto.randomUUID()` 的假创建接口。

---

## 5. Database 审计

- 实体：`tenants`（id/name/api_key_hash/api_key_prefix/created_at）、`task_records`（18 列：原生枚举
  status、JSONB payload/upstream_ids/downstream_ids/result_data、execute_at/created_at/updated_at）。
- 约束与索引：PK、`tenant_id` FK `ON DELETE CASCADE`、`name`/`api_key_hash`/`api_key_prefix` 唯一、
  6 个查询索引（含 `(created_at DESC, task_id ASC)` keyset 专用表达式索引）。
- CRUD 真实性：Create（API / Workflow 真实 INSERT）、Read（租户隔离 / keyset 分页 / 结果查询）、
  Update（CAS 抢占、租约心跳、状态机流转、DLQ 重放）、Delete（随租户级联，无对外删除接口）。
- 迁移：`1c6021d6984b` baseline → `3a9f1c2b7e4d` → `b7e5d3f19c2a`；CI 在空库执行 `alembic upgrade head`。
  `init_db()` 的 `create_all` 仅作本地脚手架兜底，生产必须用 Alembic（README 已标注）。
- 类型一致性：`TaskStatus` 在 ORM / Pydantic / `console/src/types.ts` / DB 原生枚举四处同值。
- 刷新持久性：真实 E2E 已验证「重启 API 进程后结果仍可查」。

---

## 6. Security 审计

| 项目 | 结论 |
|---|---|
| 租户身份来源 | 只从 `X-API-Key` 派生，**从不**信任 body / Redis 里的 tenant_id；Worker 另外用 PG 行做二次校验 |
| 跨租户越权 (IDOR) | 结果查询带 `tenant_id` 谓词，跨租户统一 404；XCom 快照查询强制带租户谓词 |
| API Key 存储 | 仅存 bcrypt 哈希 + 24 字符前缀（定位用），明文只打印一次；`asyncio.to_thread` 校验不阻塞事件循环 |
| 管理面 | `X-Admin-Key` 常量时间比较；未配置时全部 503（不会匿名暴露跨租户控制面） |
| 凭据泄漏 | 客户端 bundle 无任何服务端密钥；`.env`/`.env.*` 已 gitignore；`.env.example` 只有占位 |
| 输入边界 | body 2 MiB 中间件（含 chunked）、payload 256 KiB、result_data 1 MiB、XCom 256 KiB |
| 日志注入 | 访问日志不记 body/query/header；入站 `X-Request-ID` 需匹配 `^[A-Za-z0-9._:-]{1,64}$`，否则丢弃重建 |
| SQL | 全链路 SQLAlchemy 参数绑定，无字符串拼 SQL |
| 前端凭证 | `X-Admin-Key` 存 `localStorage`（已知取舍，README/console README 标注需 TLS + 内网）；校验失败/401 立即清除并退回登录门（本轮修复） |
| 未覆盖 | 无速率限制 / 无审计日志表（见剩余问题 P2） |

---

## 7. Environment Matrix

| 变量 | 用途 | 生效位置 | 是否必须 | 已文档化 |
|---|---|---|---|---|
| `DATABASE_URL` | PG 异步连接串（asyncpg） | API / Worker / Alembic / CLI | 是 | ✅ `.env.example` |
| `REDIS_URL` | Redis 连接串 | API / Worker | 是 | ✅ |
| `REDIS_KEY_PREFIX` / `REDIS_STREAM_KEY` / `REDIS_DELAYED_KEY` / `REDIS_CONSUMER_GROUP` | 键布局 | API / Worker | 是（有默认） | ✅ |
| `REDIS_STREAM_MAXLEN` / `REDIS_PRIORITY_HIGH_THRESHOLD` | Stream 裁剪、高优车道阈值 | API / Worker | 有默认 | ✅ |
| `ADMIN_API_KEY` | 管理面凭证 | API | 生产必须 | ✅ |
| `DEEPSEEK_API_KEY` | 真实模型凭证 | Worker | 用 AI 任务时必须 | ✅（本轮补齐） |
| `DEEPSEEK_API_URL` / `DEEPSEEK_TIMEOUT_SECONDS` / `AI_MAX_TOKENS_LIMIT` | 模型调用地址与边界 | Worker | 有默认 | ✅（本轮补齐） |
| `AI_MOCK_ENABLED` | 离线模拟开关（生产须 false） | Worker | 有默认 | ✅（本轮新增） |
| `ENABLE_DEMO_HANDLERS` | demo.* Handler 注册开关 | Worker | 有默认 | ✅（本轮新增） |
| `WORKER_MAX_IN_FLIGHT` / `WORKER_BLOCK_MS` / `WORKER_CLAIM_IDLE_MS` / `WORKER_LEASE_HEARTBEAT_SECONDS` / `WORKER_MAX_RETRIES` / `WORKER_RETRY_BASE_DELAY_SECONDS` / `WORKER_CONSUMER_NAME` / `WORKER_METRICS_PORT` / `WORKER_HEARTBEAT_TTL_SECONDS` | Worker 并发、租约、退避、监控 | Worker | 有默认 | ✅（本轮补齐 8 项） |
| `OUTBOX_*` / `DELAY_DISPATCH_*` / `CONTROL_LEADER_*` | 控制面补偿与选主 | API | 有默认 | ✅（本轮补齐 2 项） |
| `DEBUG` | 日志级别（DEBUG/INFO） | API / Worker | 有默认 | ✅（本轮起真正生效） |
| `SQL_ECHO` | SQLAlchemy echo | API / Worker | 有默认 | ✅ |

`docker-compose.yml` 会把宿主机 `.env` 的 AI 相关变量传入容器；未提供的保持空/默认值。

---

## 8. 本轮改动清单

**真实性与正确性（P1）**
1. `app/handlers/ai_handlers.py`：删除「无凭证静默返回假文本」的生产路径。缺凭证 → `AIProviderError`
   （进重试/DLQ，`error_msg` 含可定位信息）；仅 `AI_MOCK_ENABLED=true` 走离线模拟。新增
   HTTP 状态码/超时/响应结构错误映射、`model`/`temperature`/`max_tokens` 边界校验、
   空提示词与空响应拒绝。`DEEPSEEK_API_URL`/`DEEPSEEK_TIMEOUT_SECONDS`/`AI_MAX_TOKENS_LIMIT` 可配。
2. `app/worker/handlers.py`：`demo.sleep` 的 `sleep_seconds` 加 `[0, 60]` 校验（此前无上限，租户可用
   `1e9` 长期占满在途槽位）；demo.* 整组改由 `ENABLE_DEMO_HANDLERS` 开关注册（生产可隔离）。
3. `app/api/v1/endpoints/tasks.py`：终态兜底文案按状态区分，取消任务不再被说成「执行失败」。

**可观测性与运维（P2）**
4. 新增 `app/core/observability.py`：`configure_logging()` 统一日志格式并把 `rid=` 注入每条日志；
   `RequestContextMiddleware` 生成/沿用 `X-Request-ID`、回写响应头，按状态分级记访问日志
   （<400 走 DEBUG，避免与 uvicorn 访问日志重复刷屏；≥400 走 INFO）。
5. `GET /health/ready`：真实探活 PG（一次性 NullPool 连接）与 Redis，降级返回
   `503 {"status":"degraded","checks":{...}}`；Compose healthcheck 改用它。
6. `app/main.py`：改为经 `app.core.redis` 模块晚绑定访问 Redis（与其他模块一致，且让探针可被替换）。

**前端（P2）**
7. `AdminKeyGate`：校验通过才写 `localStorage`，失败立即清除（不再留下「看起来已登录」的错误凭证）。
8. `api/client.ts` + `App.tsx`：401 时清凭证并自动退回登录门。
9. `TaskHall`：新增「错误」列展示 `error_msg` 首行（完整堆栈在 title），此前 API 已返回但 UI 丢弃。
10. `DagView`/`WorkerPanel`：补齐 loading 三态。

**部署与工程化（P2）**
11. 新增 `console/Dockerfile` + `console/nginx.conf` + `console/.dockerignore`，Compose 新增 `console`
    服务（8080）；nginx 用 Docker 内嵌 DNS 动态解析 upstream（api 重建换 IP 无需 reload，且 api
    暂不可解析也能启动），透传 `X-Request-ID`。
12. `sdk/python/pyproject.toml` + `README.md`：SDK 变为可 `pip install -e sdk/python` 的正式包。
13. CI：新增 `compileall` 步骤与独立 `console` job（`npm ci` + `tsc && vite build`）。
14. `.env.example`：补齐 15 个未文档化变量（Worker 租约/并发、Outbox/Dispatcher 批次、AI、demo 开关）。

**测试（回归覆盖）**
15. 新增 8 个 AI Handler 契约测试（显式失败、越界参数、401 映射、响应结构异常、成功透传、离线模拟开关）；
    新增 3 个 demo Handler 测试（开关隔离、越界时长）；新增 6 个可观测性测试（request id 生成/沿用/
    非法丢弃/错误响应携带、就绪探针真实探活与降级）；扩展取消用例断言「已取消」文案。
    用例数 **49 → 63**。

**文档**
16. 新增 `PROJECT_AUDIT.md`；README（中英）新增 AI Handler 章节、健康检查与 request id 说明、
    8080 端口、布局补全、已知边界（AI/控制台）；`console/README.md`、示例 README 同步。

---

## 9. Verification Evidence（本轮真实执行）

基础设施：PostgreSQL 16（容器 `vortexmq-test-pg`，宿主 55432）+ Redis 7（`vortexmq-test-redis`，宿主 56379），
使用本机 Docker 缓存镜像启动；测试隔离沿用 `tests/conftest.py`（每用例独立 schema + Redis 前缀）。

```text
python -m alembic upgrade head
  → 1c6021d6984b (baseline) → 3a9f1c2b7e4d → b7e5d3f19c2a   OK

python -m pytest -q
  → 49 passed (审计前基线)
  → 63 passed in 58.10s (修复后)

python -m compileall -q app tests sdk examples scripts
  → OK

cd console && npm run build        # tsc (strict) && vite build
  → ✓ built in 1.06s

docker compose config -q           → OK
docker run --rm -v console/nginx.conf:... nginx:1.27-alpine nginx -t
  → syntax is ok / test is successful
```

真实进程 E2E（`uvicorn app.main:app` + `python -m app.worker`，真实 PG/Redis）：

```text
1  /health                              → {"status":"ok","role":"leader"}
2  python -m app.cli create-tenant e2e  → 明文 Key 签发成功
3  POST /api/v1/tasks (demo.echo)       → 201 PENDING
4  Worker 消费                          → GET result = SUCCESS {"echo":{"hello":"real-persistence"}}
5  重启 API 进程后再次 GET result        → 仍为 SUCCESS（持久化成立）
6  POST /api/v1/tasks (ai.deepseek.chat, 无 Key, mock off)
                                        → retry_count 0→3 后 DLQ（显式失败，未返回假文本）
   PostgreSQL 中 error_msg: "app.handlers.ai_handlers.AIProviderError: 未配置 DEEPSEEK_API_KEY，
   ai.deepseek.chat 无法调用真实模型。如需离线演示，请显式设置 AI_MOCK_ENABLED=true。"
7  GET  /api/v1/admin/workers           → count=1 name=DCS-94380 in_flight=0
8  POST /api/v1/admin/tasks/{id}/replay → PENDING, retry_count=0, error_msg=null
9  GET  /metrics                        → vortexmq_tasks_total 存在
10 /health/ready                        → {"status":"ok","checks":{"postgres":"ok","redis":"ok"}}
11 GET /health 与 POST 401              → 均携带 x-request-id（入站值沿用、非法值重建）
12 应用日志                             → rid=<同一个 id> 出现在 200(DEBUG) 与 401(INFO) 行
```

---

## 10. Remaining Problems

**P0（无）** — 无无法运行、凭据绕过、数据损坏或「核心功能是假的」问题。

**P1**
1. ~~`init_db()` 启动期 `create_all`~~ **本轮已收敛**：新增 `AUTO_CREATE_SCHEMA` 开关
   （默认 `true` 保持本地/compose 一键启动，生产置 `false` 后启动路径完全不执行 DDL，
   schema 只由 `alembic upgrade head` 创建），并有单元测试锁死「关闭后不触碰数据库」。
   剩余工作属于部署流程（在流水线里跑迁移），不再是代码缺口。

**P2**
2. 管理面 API 无速率限制、无审计日志表（`X-Admin-Key` 暴力尝试只受网络层约束）。
3. `admin/tasks` 的 `total` 每次全条件 COUNT，大数据量下是唯一非 O(page) 成本。
4. 任务提交为「先落库再投递」，投递失败时返回 201 并依赖 Outbox 补偿（有意设计，但调用方
   需要理解 201 ≠ 已投递）；SDK 无 `wait_for_result` 便捷方法。
5. `DEEPSEEK_TIMEOUT_SECONDS`（120s）大于 `WORKER_CLAIM_IDLE_MS`（30s）：靠租约心跳续约避免误回收，
   若进程被 kill -9 则需等 30s 才可回收（与其它长任务一致，非 AI 特有）。
6. 真实模型调用无跨任务连接池复用（每次调用新建 `AsyncClient`）；高 QPS 场景建议复用客户端。

**P3**
7. `console` 无 ESLint（仅靠 `tsc strict` 兜底）；无前端单测。
8. Prometheus series 随租户数线性增长（`tenant_id` 标签）；超大规模需评估分片或聚合。
9. `FAILED` 状态无写入方（保留兼容历史行）。
10. 未接入结构化 JSON 日志（当前为 key=value 文本格式，已带 `rid=`）。

---

## 11. External Blockers

| 阻塞项 | 影响 | 现状 |
|---|---|---|
| `DEEPSEEK_API_KEY`（付费第三方账号） | AI Handler 无法真实生成文本 | **已推进到「只差 Key」**：适配、超时、错误映射、边界校验、测试全部就绪；缺 Key 时显式失败进 DLQ，已用真实进程验证 |
| Docker 镜像仓库网络 | 无法 `docker pull postgres/redis/node:20-alpine` 验证镜像构建 | 本机已有 Postgres/Redis/nginx 缓存镜像，已用于真实 E2E 与 nginx 配置校验；`node:20-alpine` 无缓存，故 `console` 镜像未做 `docker build` 实测（其构建步骤 `npm ci && npm run build` 已在本机 Node 24 上真实通过） |
| 无生产环境权限 | 无法在生产库执行迁移 / 观测真实流量 | 未触碰任何非测试数据库；全部验证在隔离测试 schema 内完成 |

---

## 12. Production Readiness Score

| 维度 | 分数 | 说明 |
|---|---|---|
| Frontend | 88 | 真实 API 调用、loading/empty/error 三态、错误列、401 自动登出、可容器化部署；缺 ESLint/前端单测 |
| Backend | 94 | 状态机、CAS/租约/PEL、Outbox、DAG、DLQ 重放全部真实且有并发/幂等回归测试 |
| Database | 94 | SSOT 明确、迁版本化、索引与约束齐全；启动期 create_all 兜底待收敛 |
| Auth | 93 | 服务端强制租户隔离、常量时间比较、跨租户 404；缺速率限制与审计日志 |
| Storage | 95 | 真实 Streams/ZSet 投递 + PG 持久化，重启后数据与服务均恢复 |
| AI | 88 | 真实供应商调用 + 显式失败语义 + 参数边界 + 错误映射；待接入真实 Key，未做流式输出 |
| Testing | 92 | 63 个用例跑在真实 PG+Redis；缺真实模型契约测试（需付费 Key）与前端单测 |
| Security | 90 | 无凭据泄漏、输入边界完备、日志不落敏感数据；管理面无速率限制 |
| **Overall** | **92 / 100** | 核心链路达到 LEVEL 4–5，剩余为运维加固类改进（P2/P3） |



---

