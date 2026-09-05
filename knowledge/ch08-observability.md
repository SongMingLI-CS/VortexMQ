# 第八章 可观测性

> 接 [第七章](ch07-algo.md)。系统在跑，你得看得见。本项目用了指标和日志；没有做分布式追踪（没有把一次提交的跨进程故事自动串成火焰图）。
>
> 编号 58–62。上一章：[算法](ch07-algo.md) · 下一章：[工程](ch09-engineering.md)

可观测性三件套：

- **日志：** 某次 task_id 发生了什么（已经有）
- **指标：** 每秒多少成功、队列有多深（已经有）
- **追踪：** 一个请求穿过 API、Postgres、Redis、Worker 的时间轴（没有）

缺追踪时，靠日志里的 task_id 手工串。足够调试本仓库规模。

---

## 58. Counter、Gauge、Histogram（熟练）

### 三种尺子，不要混用

**Counter（计数器）。** 只增不减。进程重启从 0 开始（Prometheus 能用 rate 处理重启）。适合「发生了多少次」。本项目 `vortexmq_tasks_total` 标签：`status`（success/failed/dlq）、`tenant_id`、`task_type`。

注意 **failed 不是终态**：表示这次执行失败将重试。一个毒药任务会增加多次 failed，最后一次 dlq。用 `increase(vortexmq_tasks_total[5m])` 看速率，不要拿 Counter 的绝对值当「库里有多少失败任务」。库里的真相仍是 SELECT count。

**Gauge（测量仪）。** 可上可下。适合「现在有多少」。队列深度、活着的 Worker 数。错误用法：每个进程自己 inc/dec 队列深度——三个 API 各记各的。正确用法见第 59 条。

**Histogram（直方图）。** 把每次观测扔进预先划好的桶，例如 0.25s、0.5s、1s、…、30s。能回答「有百分之多少的任务超过 4 秒」，不能事后改桶再精确重算历史。模拟 sleep(3) 应落在 3–4 秒附近的桶。桶全挤在最左边或最右边，说明桶选错了。

`track_task_duration` 包住 `execute_simulated_job`，异常路径也会记录耗时——失败同样消耗时间，大盘应当看见。

### 停下来想

为什么没有「当前 PENDING 行数」这个 Gauge？它可以每 10 秒 `SELECT count`。本项目没做，避免扫表打 Postgres。队列 Gauge 用 Redis 长度近似「叫醒侧堆积」，不是账本侧堆积。两者含义不同，第 36 条末尾想过。

---

## 59. 全局 Gauge 必须现查（深入）

### 进程内计数的陷阱

API 副本 A 看到 XADD 100 次，B 看到 100 次。Prometheus 若把两个进程的 Gauge 加起来得到 200，但 Redis 里可能只有 150（有重叠、有消费）。Counter 用 rate 分进程加总是对的（事件发生在哪个进程就在哪记）；Gauge 表示全局水位，必须有一个共享的权威来源。

本项目权威是 Redis：

- 所有租户两条车道的 XLEN 之和 → stream 堆积
- 所有租户 ZSet 的 ZCARD 之和 → delayed 堆积
- 心跳 ZSet 里 score 足够新的成员数 → 活 Worker

抓取 `/metrics` 时 `refresh_runtime_gauges` 现算再 set。Worker 心跳：每次主循环 `ZADD` 自己的名字，分数为 Unix 时间。超过 15 秒没跳的先 `ZREMRANGEBYSCORE` 再 `ZCARD`。进程退出 `ZREM` 自己，避免优雅停机后大盘还显示 1。

Redis 短暂不可用：refresh 抛错被 `/metrics` 吃掉，仍返回进程内的 Counter/Histogram，避免 Prometheus 整次 scrape 失败导致大盘空洞。此时 Gauge 可能停在旧值。

### scrape 地址

Prometheus 配置必须写 Docker 服务名 `api:8000`、`worker:8001`。写 `localhost` 会打到 Prometheus 容器自己。见第九章第 64 条。

Worker 的 8001 不是 FastAPI，是 `prometheus_client.start_http_server`。它不会走 `refresh_runtime_gauges`。全局 Gauge 以 API 的 job 为准，Grafana 查询带 `job="vortexmq-api"`。

### 停下来想

两个 API 都 refresh 同一组 Gauge 名。Prometheus 分别 scrape 两个目标，会得到两份相近的全局值（都现查 Redis）。大盘应选一个 job 或用 `max`，不要 `sum`，否则 Worker 数翻倍。预置大盘已经按 job 过滤。

---

## 60. Grafana 大盘（入门）

### 它是什么

Prometheus 负责按时间存数字。Grafana 负责画。Compose 已预置数据源（指向 `prometheus:9090`）和大盘 JSON。浏览器开 `http://127.0.0.1:3000`，用户名密码 `admin/admin`。生产必须改密码；本地演示用默认。

你要看的面板：活 Worker、Stream/延迟堆积、每分钟处理速率、耗时。压测 70/20/10 会同时点亮它们：即时打 Stream，延迟打 ZSet，毒药打 failed 和 dlq 曲线。

刷新 5 秒。时间范围默认 now-15m。没有数据时先确认：Compose 是否起了 Prometheus、API `/metrics` 能否在容器网络里打开、是否已经有流量。

### 停下来想

Grafana 不产生数据。大盘空，去查 Prometheus targets 是否 DOWN，不要先改 JSON 面板。

---

## 61. 健康检查 /health（入门）

### 探活要轻

负载均衡和 Docker 每隔几秒问：你还活着吗？这个问题必须快、依赖少。本项目 `/health` 返回 JSON：`status=ok`、服务名、`role=leader|standby`。不查 Postgres 是否能 SELECT 1——Postgres 挂了，这个接口仍可能 200。这叫 **liveness**（进程还在跑）而不是 **readiness**（已经能正确办事）。

Compose 用它决定 API 是否 healthy，从而决定何时起 Worker、Prometheus。API 其实还没连上库时，lifespan 会在 `init_db` 卡住，health 还没挂上，探测失败，符合「没准备好」。起来之后库再挂，health 仍 ok，请求会 500。更严格的就绪检查可以以后加，注意不要让探测把数据库打满。

### 停下来想

role 字段能不能当「只有 Leader 才接流量」的依据？不要。所有 API 都应接提交。Leader 只是多跑了后台循环。把 Standby 摘出负载均衡，会浪费受理能力。

---

## 62. 结构化日志（入门）

### 让 grep 能工作

坏日志：`出错了`。好日志：`任务执行失败: tenant=default task_id=3fa8... task_type=email.send` 再加堆栈。

本项目用标准库 logging，格式带时间、级别、logger 名。关键字段用 `task_id=` 这种 `k=v`，方便 `docker compose logs worker | findstr task_id`。没有上 ELK，本机足够。

级别：

- INFO：正常投递、开始执行、ACK
- WARNING：退避、级联取消、越权丢弃
- ERROR：进 DLQ、写库失败、非法消息

不要把 API Key 打进日志。task_id、tenant 名可以。

`logger.exception` 会带堆栈，用在 except 块里，比 `logger.error(str(e))` 有用得多。

### 停下来想

DEBUG=true 时 SQLAlchemy `echo=True` 会打每条 SQL。本地有用，Compose 里关掉，否则压测日志会把磁盘写满，把真正的 ERROR 淹死。

---

## 第八章小结

- Counter 记事件，Gauge 记水位，Histogram 记分布。failed 是重试不是终态。
- 全局水位现查 Redis，不要分进程加减后 sum。
- /health 要轻；role 不是分流依据。
- 日志带 task_id，不带钥匙。

最后一章把整座房子打包：[第九章 工程交付](ch09-engineering.md)
