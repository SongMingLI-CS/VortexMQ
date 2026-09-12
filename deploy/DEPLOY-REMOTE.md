# VortexMQ · 云服务器部署手册

把 VortexMQ（FastAPI + PostgreSQL + Redis Streams + Worker + Prometheus/Grafana + Vite 控制台）
部署到云服务器，并与同机已部署项目（智汇于庄、LMS）**端口隔离**、互不影响。

## 0. 前置条件

| 项 | 要求 |
|----|------|
| 服务器 | Linux x86_64，≥ 2 vCPU / ≥ 4 GiB 内存 / ≥ 10 GiB 磁盘（7 个容器 + 镜像构建） |
| Docker | Docker Engine 20.10+ 与 Compose **≥ 2.24**（叠加层使用 `!override` 标签替换 ports） |
| 出网 | Docker Hub（或加速器）、PyPI、npm |

> 本仓库 `.gitattributes` 约定 `*.sh` / `*.yml` / `*.py` 为 LF。**部署脚本必须 LF**，
> CRLF 会让 Linux 报 `set: pipefail: invalid option name`；脚本 step 0 会自动规范化其余文件。

## 1. 端口规划（与同机项目隔离）

| 服务 | 容器端口 | 宿主映射 | 说明 |
|------|---------|---------|------|
| api | 8000 | **8000（公网）** | `/docs`、`/metrics`、业务 API |
| console | 80 | **8081（公网）** | 管理控制台 UI（nginx 同源反代 `/api`） |
| postgres | 5432 | `127.0.0.1:15432` | 不对外暴露 |
| redis | 6379 | `127.0.0.1:16379` | 6379 已被智汇于庄占用 |
| worker | 8001 | `127.0.0.1:18001` | Worker 指标 |
| prometheus | 9090 | `127.0.0.1:19090` | 仅本机 |
| grafana | 3000 | `127.0.0.1:13000` | 仅本机（口令见 `.env`） |

观测面默认只绑本机，用 SSH 隧道查看：

```bash
ssh -L 13000:127.0.0.1:13000 -L 19090:127.0.0.1:19090 ubuntu@<公网IP>
# 本机访问 http://127.0.0.1:13000 （Grafana） / http://127.0.0.1:19090 （Prometheus）
```

> **实现要点**：Compose 合并多文件时 `ports` 是**拼接**而非替换，因此
> `deploy/docker-compose.deploy.yml` 必须用 `!override` 强制替换，否则原始
> `5432:5432 / 6379:6379 / 8080:80 / 3000:3000` 仍会被绑定并直接冲突
> （已用 `docker compose config` 实测确认映射被正确替换）。

## 2. 部署

```powershell
python -m pip install paramiko                      # 一次性
$env:YZ_SSH_PASSWORD = '<实例密码>'                  # 密码仅经环境变量传递
python deploy/remote-deploy-runner.py --host <公网IP> --user ubuntu `
       --bundle vortexmq-deploy.tar.gz --remote-dir /opt/vortexmq
# 附加开关：--background / --tail-log 60 / --set-env K=V（可重复）/ --preflight-only
```

脚本六步（**幂等**，可重复执行；不会删除数据卷）：

| 步骤 | 内容 |
|------|------|
| 0 | 前置检查（编排文件 / docker / compose v2 / 磁盘）+ CRLF→LF 规范化 |
| 1 | Docker Hub 不通时自动写入镜像加速器（腾讯云内网源置首）并重启 Docker |
| 2 | 由 `.env.example` 生成 `.env`；**自动生成 `VORTEXMQ_PG_PASSWORD` / `ADMIN_API_KEY` / `GRAFANA_ADMIN_PASSWORD` 强随机值**（占位值 `admin`/`postgres`/`change-me*` 均会被替换） |
| 3 | 端口占用检查（8000 / 8081 / 15432 / 16379 / 18001 / 19090 / 13000） |
| 4 | `docker compose -f docker-compose.yml -f deploy/docker-compose.deploy.yml up -d --build` |
| 5 | 等待 `/health/ready`（**真连 PostgreSQL 并 PING Redis**）返回 200，再校验 `/health`、控制台首页、控制台 `/health`（同源反代） |
| 6 | 输出访问地址、SSH 隧道与运维命令 |

### 构建加速（应用源码零改动）

| 源 | 实测速度 | 本项目用法 |
|----|---------|-----------|
| `pypi.org` 49 KB/s | → 腾讯云 856 KB/s | `deploy/dockerfiles/vortexmq.Dockerfile`（api + worker，pip 换源，**失败自动回退官方源**） |
| npm 官方源 | → npmmirror | `deploy/dockerfiles/console.Dockerfile`（控制台，`npm ci --registry=…npmmirror.com`） |

两者由 `deploy/docker-compose.deploy.yml` 的 `build.dockerfile` 指向；
仓库根 `Dockerfile` 与 `console/Dockerfile` **保持不动**，本地开发路径不受影响。

## 3. 部署后使用

```bash
cd /opt/vortexmq
DC="docker compose --env-file .env -f docker-compose.yml -f deploy/docker-compose.deploy.yml"

# 1) 签发租户 API Key（明文只打印一次，请立即保存）
sudo $DC exec -T api python -m app.cli create-tenant default

# 2) 投递一个即时任务（demo.echo）验证「入队 → Worker 消费 → 结果落库」闭环
curl -sS -X POST http://127.0.0.1:8000/api/v1/tasks \
  -H 'Content-Type: application/json' -H 'X-API-Key: <上一步打印的 Key>' \
  -d '{"task_type":"demo.echo","payload":{"hello":"world"}}'
```

**控制台**：浏览器打开 `http://<公网IP>:8081/`，在页面内填写 **X-Admin-Key**
（值 = 服务器 `/opt/vortexmq/.env` 中的 `ADMIN_API_KEY`），即可查看任务大厅、DLQ 重放与 Worker 节点监控。

## 4. 运维命令

```bash
cd /opt/vortexmq
DC="docker compose --env-file .env -f docker-compose.yml -f deploy/docker-compose.deploy.yml"
sudo $DC ps
sudo $DC logs -f --tail=200 worker        # 观察消费、退避重试、DLQ
sudo $DC restart api
sudo $DC down                              # 停服务，保留 4 个数据卷
```

**更新发布**：本机改代码 → 重新打包上传 → 再跑一次 `deploy/remote-deploy.sh`（数据保留）。

## 5. 常见问题

| 现象 | 处理 |
|------|------|
| `port is already allocated` | 缺叠加层：必须带 `-f deploy/docker-compose.deploy.yml`；必要时显式指定 `API_PORT` / `CONSOLE_PORT` |
| ports 出现重复 / `!override` 报错 | Compose < 2.24 不支持 `!override`，请升级 compose 插件（本机与服务器均 ≥ 2.40） |
| `/api/v1/admin/**` 返回 503 | `ADMIN_API_KEY` 未设置；跑一次部署脚本会自动生成 |
| Grafana 用 admin/admin 登不上 | 口令已改为 `GRAFANA_ADMIN_PASSWORD`（见 `/opt/vortexmq/.env`） |
| 任务长期停在 PENDING | Worker 未消费：`logs -f worker`；`demo.*` 需要 `ENABLE_DEMO_HANDLERS=true` |
| `ai.deepseek.chat` 进入 DLQ | 未配 `DEEPSEEK_API_KEY`（设计如此：显式失败进入重试/DLQ，不返回假数据） |
| 控制台能打开但接口 404/502 | 控制台 nginx 反代 `api:8000` 失败 → 查 `api` 容器状态与 `console` 日志 |
| `/health/ready` 返回 503 | 依赖不可用：`{"status":"degraded","checks":{...}}` 会指出是 PostgreSQL 还是 Redis |

## 6. 部署记录

| 项 | 值 |
|----|----|
| 部署目录 | `/opt/vortexmq` |
| 部署包 | `vortexmq-deploy.tar.gz`（源码，不含 node_modules） |
| 部署日志 | `/opt/vortexmq/deploy.log`（使用 `--background` 时） |
| 公网端口 | 8000（API）、8081（控制台）—— 需在安全组放行 |
| 仅本机端口 | 15432（PG）/ 16379（Redis）/ 18001（Worker 指标）/ 19090（Prometheus）/ 13000（Grafana） |
| 口令 | `VORTEXMQ_PG_PASSWORD` / `ADMIN_API_KEY` / `GRAFANA_ADMIN_PASSWORD` 存于 `/opt/vortexmq/.env`（`chmod 600`） |
| 数据卷 | `vortexmq_pg_data` / `vortexmq_redis_data` / `vortexmq_prom_data` / `vortexmq_grafana_data` |
| 与同机项目 | 智汇于庄占 80 / 5433 / 6379；LMS 占 8080 与 3306(仅本机)；本栈占 8000 / 8081 + 本机端口，**无冲突** |
| 数据库 | 容器内自带 `postgres:16-alpine`（自建，不依赖外部托管库） |
| 验证结果 | `/health/ready` **200**（真连 PG + PING Redis）；`/health` 200；控制台首页 200；控制台 `/health` 200（同源反代） |
| 端到端闭环 | 签发租户 Key → 提交 `demo.echo` → Worker `开始执行任务/执行完成/已 XACK` → `GET /tasks/{id}/result` 返回 **200 SUCCESS** 且 `result_data` 正确；延迟任务（`execute_at`+3s）经 ZSet 通路亦 SUCCESS |
| 管理面 | `GET /api/v1/admin/workers` → **200**，返回 Worker 心跳（`in_flight=0`） |
| 租户 Key 存档 | `/opt/vortexmq/.tenant_keyout.txt`（600 权限，含明文 Key，仅本次签发；如需轮换：`create-tenant default --rotate`） |

