# 第九章 工程交付

> 接 [第八章](ch08-observability.md)。代码能在你笔记本跑，不等于别人能一键复现。本章是打包课。
>
> 编号 63–67。上一章：[可观测性](ch08-observability.md) · 回到 [目录](../KNOWLEDGE.zh-CN.md)

---

## 63. Docker Compose 多服务编排（入门）

### 容器是什么

容器像一个轻量的隔离小电脑：自己的文件系统、自己的进程树、自己的网卡，但和虚拟机相比几乎不跑完整操作系统。 **镜像** 是装机盘（本项目 `Dockerfile` 基于 `python:3.12-slim`，装好 requirements，拷入 `app/`）。 **容器** 是用镜像跑起来的一次实例。

手动 `docker run` 六次、每次记端口和依赖，会疯。 **Compose** 用一份 YAML 描述整个小集群：服务名、镜像或 build、环境变量、端口映射、卷、健康检查、依赖。

`docker compose up -d --build`：构建镜像，后台启动。服务名即 DNS 名：API 容器里连 `postgres:5432`、`redis:6379` 即可，不必知道对方 IP。

### 端口映射

```text
ports:
  - "8000:8000"
```

左边是你笔记本的端口，右边是容器内。浏览器访问左边。容器互相访问走服务名和 **右边**（容器内端口），不走左边。Prometheus scrape `api:8000` 是容器网络内，与你笔记本的 8000 映射是两件事。

### 数据卷

Postgres 的数据目录、Redis 的 AOF、Prometheus、Grafana 用 named volume。`compose down` 默认删容器和网络， **保留卷**。压测数据还在。要连数据一起删需要 `down -v`，本讲义不鼓励随手做。

`.dockerignore` 避免把 `.env`、`.git` 打进镜像。镜像里不应有密钥。

### 停下来想

API 和 Worker 用同一份 Dockerfile、同一份代码，靠 `command` 区分入口。改业务逻辑只需 build 一次镜像（Compose 会对两个服务用同一 build）。这是第 39 条在打包上的体现。

---

## 64. healthcheck 决定启动顺序（熟练）

### started vs healthy

```yaml
depends_on:
  postgres:
    condition: service_healthy
```

若只写 `depends_on: postgres`（老语法），Compose 只等 Postgres **容器启动**，不等它能接受连接。Postgres 启动后还要恢复 WAL、接受 TCP，中间十几秒 API 去连会失败退出，然后你看到「API 崩了」其实是抢跑。

`healthcheck` 在 Postgres 里跑 `pg_isready`，连续成功才标 healthy。API 自己的 healthcheck 打 `http://127.0.0.1:8000/health`（注意这里是 **容器内部** 的 127.0.0.1，合法）。Worker `depends_on: api` healthy，为了先让 `init_db` 建表。

Prometheus `depends_on` API healthy、Worker started：指标抓取晚几秒没关系。它 scrape 必须用 `api:8000` 不是 localhost，否则 target DOWN。这是第 59 条在网络上的对应。

### start_period

API healthcheck 有 `start_period: 15s`：刚启动的 15 秒失败不计。给 `init_db` 留时间，避免被判不健康而重启循环。

### 停下来想

循环依赖（A 等 B healthy，B 等 A healthy）会永远起不来。本项目是线性的：postgres/redis → api → worker → prometheus → grafana。画依赖图时不要构成环。

---

## 65. 12-factor：配置来自环境变量（入门）

### 那份备忘录里和本项目有关的几条

[12-factor](https://12factor.net/zh_cn/) 是一套云上应用的经验。本项目用到的：

| 因素 | 做法 |
|------|------|
| 配置在环境 | `Settings` 读环境变量和 `.env` |
| 进程无状态 | 状态在 Postgres/Redis，API 进程内存不保存任务 |
| 端口绑定 | uvicorn 听 8000 |
| 并发靠进程 | 加 Worker 容器，不靠线程池执行任务 |
| 日志打 stdout | logging 到标准输出，Compose logs 收集 |
| 一次性管理进程 | `python -m app.cli` 与 Web 进程分离 |

「无状态」不是「没有状态」，是 **进程崩溃可以再拉起，状态在外面**。这和第 36、39 条一致。

API Key 不再放环境变量：环境会进 Compose 文件、`docker inspect`、CI 日志。签发打印一次，比「12-factor 把密钥也塞进 env」更紧一点。

### 停下来想

DEBUG 默认 true 方便本地。Compose 覆盖 false。镜像里不要假设 DEBUG 关着——有人直接 `docker run` 不传环境。以 Settings 默认值会打 SQL echo，注意。

---

## 66. 异步压测客户端（熟练）

### 测谁，不要测自己的笔记本

压测脚本若用同步 `requests` 一个接一个发，测到的是脚本的速度。`asyncio` + `aiohttp` 同时挂起很多 HTTP，才能把压力送到 API。

**Semaphore** 把「正在进行的请求」限制在 `--concurrency`。无限并发会耗尽本机临时端口、文件描述符，失败率反映的是客户端，不是 VortexMQ。`TCPConnector.limit` 与并发对齐，同一道理。

### 70 / 20 / 10

| 种类 | 比例 | 锻炼谁 |
|------|------|--------|
| 即时 | 70% | Stream、Worker 吞吐、队列 Gauge |
| 延迟 5–45 秒 | 20% | ZSet、Dispatcher |
| force_fail | 10% | 退避、DLQ、failed 计数 |

多把 `--api-key` 可混合多租户，点亮公平轮询。脚本打的是提交接口，看 201，不负责等 SUCCESS。消化速度看 Grafana 和 Worker 日志。

默认 `--count 10000`。先用 200 试通再加大。毒药任务每个会执行多次，Worker 会比 10% 提交数更忙。

### 停下来想

压测时 bcrypt 会先打满 API CPU（第 6、46 条）。若你只想测队列，这是干扰；若你想测真实入口，这是真实。分开测需要 mock 鉴权，本项目没有提供。

---

## 67. Makefile 常用入口（入门）

### 短名

```makefile
up:
	docker compose up -d --build
```

`make up` 少打字、少记参数。`STRESS_ARGS` 允许 `make stress STRESS_ARGS='--api-key xxx --count 200'`。

Windows 默认可能没有 GNU Make。没有就直接敲右边的命令，效果相同。Git Bash、WSL、Chocolatey 都可以装 make。

目标：

| 目标 | 做什么 |
|------|--------|
| up | 构建并后台启动全部 |
| down | 停容器，保留卷 |
| logs-worker | 跟 Worker 日志 |
| stress | 跑压测脚本（需先有钥匙） |

Makefile 不是测试套件，不会在 `up` 后自动签发租户。仍要自己 `python -m app.cli create-tenant`。

### 停下来想

为什么不把 create-tenant 写进 Makefile 的 up 目标？自动签发会把明文打进 CI 日志，或每次 up 都试图创建已存在的租户而失败。保持手工，痛一次，记住钥匙在人手里。

---

## 第九章小结

- Compose 描述整个小集群；服务名当 DNS；卷保住数据。
- healthy 不是 started；scrape 不要写 localhost。
- 配置在环境，状态在 Postgres/Redis，日志在 stdout。
- 压测先限自己的并发；Make 只是短名。

---

回到 [目录与第零课](../KNOWLEDGE.zh-CN.md)。建议下一步：对着第五章的时间线，打开 `app/worker/processor.py` 从第一行读到 ACK。讲义到此结束，肌肉记忆从源码开始。
