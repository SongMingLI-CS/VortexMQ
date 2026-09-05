# 第五章 分布式系统

> 接 [第四章](ch04-redis.md)。本章把账本和铃铛的零件组装成「多个进程、没有共享内存时如何不丢、不双跑」。建议和第 19、22、28、31 条对照着读。
>
> 编号 36–45。上一章：[Redis](ch04-redis.md) · 下一章：[安全](ch06-security.md)

「分布式」在本项目里非常具体：API 进程、Worker 进程、Postgres、Redis，四者之间只靠网络。没有一个全局锁能同时锁住它们四个。

---

## 36. 唯一事实来源 SSOT（深入）

### 为什么必须选一个

系统里每个能存数据的地方都会被有人当成「真相」：

- Stream 里有一条消息 → 「任务存在」
- ZSet 里有 member → 「还没到期」
- 行上 status=SUCCESS → 「做完了」

三份说法一旦打架，你没有裁判。 **SSOT（Single Source of Truth）** 就是事先指定裁判：本项目指定 PostgreSQL 的那一行。

Redis 里的一切降级为 **提示**：「去看那一行吧。」提示可以：

- 丢（XADD 失败、Lua 只 ZREM、MAXLEN 裁过狠、FLUSHALL）
- 重复（补投、双 Dispatcher）
- 迟到（Sweeper 30 秒后才补）

不允许的是：提示发明一行从未 COMMIT 的任务，或抹掉已经 COMMIT 的行。

### 冲突时怎么判

| 铃铛说 | 账本说 | 听谁 |
|--------|--------|------|
| 有消息 | 没有这行 | 账本：ACK 丢掉 |
| 有消息 | SUCCESS | 账本：ACK，不执行 |
| 有消息 | execute_at 在未来 | 账本：放回 ZSet |
| 没有消息 | PENDING 且很旧 | 账本：Sweeper 补铃 |
| ZSet 分数已到期 | 行上未到期 | 账本：Worker 仍不执行 |

「时钟以 Postgres / 应用 UTC 为准」是 SSOT 在时间上的推论。不要用 Redis 服务器时间去改状态机。

### 常见误解

- 「开了 AOF 就是双 SSOT。」见第 35 条。持久化和「谁拥有语义」不是一回事。
- 「SSOT 意味着 Redis 可以随便丢。」丢了要能修（Outbox）。SSOT 规定修的时候以行为准去重建铃铛，而不是以铃铛为准去删行。

### 停下来想

监控上的 `vortexmq_queue_size{queue="stream"}` 很大，但 Postgres 里 PENDING 很少。可能发生了什么？提示：Stream 里是已处理未裁剪的历史，或重复铃铛；堆积的叫号屏 ≠ 未完成的病历。看行的状态分布才是业务真相。

---

## 37. Outbox：双写补偿（深入）

### 两个系统没有共享事务

你无法写：

```text
BEGIN
  INSERT INTO task_records ...
  XADD stream ...
COMMIT   -- 让 Postgres 和 Redis 一起成功或一起失败
```

不存在这种 COMMIT。于是必有窗口：账本已提交，铃没按上。

**发件箱模式** 的标准做法：业务表（或单独 outbox 表）留下「待通知」记录，后台扫描发出去，成功后标记已发送。本项目让任务表兼任发件箱：

- 待通知 ≈ `status=PENDING` 且 `updated_at` 早于阈值
- 标记已发送 ≈ 刷新 `updated_at`（投递租约）

没有单独 outbox 表，少一张表的事务一致性问题，但也把「真正等执行的 PENDING」和「投递失败的 PENDING」混在同一状态里，靠时间戳区分。刚投成功的行 updated_at 是新的，不会立刻被扫。

### 完整时间线：XADD 失败

```text
T0   INSERT PENDING，COMMIT          行存在
T1   XADD 网络超时
T2   接口返回 201 + task_id          客人高兴
T3   0~30 秒内 Sweeper 看不见这行    updated_at 还新（create 时的 now）
     若 T0 的 updated_at 已超过 30 秒——刚创建就旧？几乎不会
     注意：create_task 的 created_at/updated_at 是插入时刻
     Redis 失败路径没有刷新 updated_at
T4   插入 30 秒后 + 最多一个扫描间隔 10 秒
T5   Sweeper SKIP LOCKED 认领，刷新 updated_at，COMMIT
T6   事务外 XADD，成功
T7   Worker 开始干活
```

最坏大约 40 秒任务才被第一次叫醒。对发邮件可接受；对「点击后 100ms 内必须执行」不可接受，那不该用这条补偿路径当主路径。主路径仍是 T1 成功的即时 XADD。

### 为什么失败仍 201

若 T1 失败就 500，客人重试 POST，`create_task` 再插一行，变成两个任务、两封邮件。201 的含义是「受理了」，不是「已经按铃成功」。调用方应以 task_id 查结果，不要用「没收到 201 就再 POST」以外的策略——没收到 201 仍可能已经插入（API 在返回前崩溃），这是第 26 条末尾的已知边界。

### 第二段：回收僵死 RUNNING

见第 41 条。同一循环 `sweep_outbox_once` 先扫 PENDING 再扫过期 RUNNING，代码在 `app/services/outbox.py`。

### 停下来想

Outbox 把 updated_at 当租约。Worker 执行时也会更新 updated_at（CAS 成 RUNNING）。若 Sweeper 误把「刚开始执行」当成「投递失败」？不会：执行中是 RUNNING 不是 PENDING。PENDING 扫描条件带 status。两类扫描分开，阈值还都 ≥ 30 秒。

---

## 38. 至少一次投递与幂等（深入）

### 三种口头承诺

| 承诺 | 丢 | 重复 | 本项目 |
|------|----|------|--------|
| 至多一次 | 可能 | 不 | 不能接受丢任务 |
| 恰好一次 | 不 | 不 | 跨两系统几乎做不到严格意义 |
| 至少一次 | 不（最终） | 可能 | 选择这个 |

「恰好一次」广告词背后通常是：至少一次 + 幂等，看起来像恰好一次。副作用真的做了两遍时，仍不是恰好一次。

### 幂等：做两次等于做一次

对「把 status 改成 SUCCESS」：第二次发现已是终态，ACK 走人，幂等。

对「发一封邮件」：两次调用会发出两封，除非邮件网关按 task_id 去重。本仓库模拟执行没有外部副作用，所以调度层幂等就够了。你换成真发邮件，必须自己做第五道闸。

### 四道闸，从外到内

```text
1. PEL          同一 message id 同时只在一个夹子上
2. 状态短路     SUCCESS/DLQ/WAITING → 只 ACK
3. CAS          只有 PENDING（或过期 RUNNING）能变成 RUNNING
4. 写终态行锁   仍是 RUNNING 才能 SUCCESS / 加 retry_count
```

第 1 道挡不掉「两个 message id」。第 2 道挡掉已经做完的。第 3 道挡掉并发执行。第 4 道挡掉并发改计数。少一道都会在某种故障组合下双跑或丢更新。

### 停下来想

「把 sleep(3) 换成向银行打款」时，哪一道闸失效了？提示：1–4 仍防双跑 **进入** execute 函数；若 execute 内部在 CAS 之后、写 SUCCESS 之前崩溃，认领后会再进 execute。银行接口必须能用 task_id 查询「这笔是否已打过」。

---

## 39. 控制面与数据面分离（熟练）

### 两个进程的职责表

| | 控制面 API | 数据面 Worker |
|--|------------|---------------|
| 接待 HTTP 业务 | 是 | 否 |
| 写 PENDING | 是 | 否（回收 RUNNING 时会改回 PENDING） |
| 按铃 / 补铃 / 到期搬运 | Leader 才跑 | 重试时 ZADD |
| 执行业务 | 否 | 是 |
| 写 SUCCESS/DLQ | 否 | 是 |
| 扩容手段 | 加 API 副本 | 加 Worker 副本 |
| 崩了的后果 | 暂不能提交；队列里的仍可消化 | 提交仍可；堆积增加 |

它们只共享 Postgres 和 Redis，不共享 Python 对象、不共享连接池。这就是「没有共享内存的合作」。

不要为了少起一个容器把 Worker 嵌进 API 进程：那时「只加执行力」会变成「API 和执行绑在一起扩」，而且执行卡住会拖垮受理。Compose 已经拆开。控制面选主也只存在于 API 进程里，Worker 不参与选主。

### 停下来想

三个 API、一个 Worker，和三个 Worker、一个 API，分别先撑不住哪一侧？提示：前者执行力不够 Stream 堆积；后者受理不够但消化快。压测哪一侧，就扩哪一侧。

---

## 40. Leader 租约选主（深入）

### 为什么要选

Sweeper 多个人跑：靠 SKIP LOCKED 仍正确，但会多打 Redis、多扫表。Dispatcher 多个人跑：靠 Lua 仍正确，但每秒多 N 倍 EVAL。浪费，不是撕裂。选主是 **效率**，不是正确性的唯一来源。正确性在第 21、22、31 条。记住这个分层，才不会把 SET NX 当成宗教。

### 循环

每个 API 进程的 lifespan 拉起 `run_control_plane`：

```text
每 3 秒：
  若我是 Leader → 续期
       续失败 → 我不再是，cancel Sweeper/Dispatcher
  若我不是 → SET NX 尝试当选
       成功 → create_task 两个后台循环
       失败 → 继续空转
```

`/health` 的 `role` 读内存里的 `_is_leader`，可能和 Redis 键有最多 3 秒的偏差。探活用，不要用它做精确的集群管理。

### 双主窗口

```text
Leader A 垃圾回收停顿 12 秒
键过期，B SET 成功，B 启动 Sweeper
A 醒来续期失败，停止 Sweeper
中间几秒两人的 Sweeper 都活着
```

SKIP LOCKED 让他们认领不同行。最坏重复 XADD，CAS 消化。状态机不撕。

### 停下来想

Worker 要不要选主？不要。执行本来就要多个人同时干。选主是针对「只该一个人做的扫描和搬运」。

---

## 41. 租约过期与僵死 RUNNING（深入）

### 三种死法

| 死法 | PEL | 账本 | 谁救 |
|------|-----|------|------|
| 优雅停机 | 当前条会 ACK | 写成终态 | 不需要救 |
| 进程崩溃，Redis 还在 | 在死者名下 | RUNNING | XAUTOCLAIM + CAS 过期 RUNNING |
| 进程崩溃且 Stream/PEL 没了 | 无 | RUNNING | Sweeper 改回 PENDING 再按铃 |

第三种极少（误删键、严重的 failover）。仍要覆盖：否则任务永远 RUNNING，客人一直 202。

### 阈值同盟

```text
WORKER_CLAIM_IDLE_MS = 30000     XAUTOCLAIM 空闲
Sweeper running stale = 同一值（代码里 ms 换成秒）
CAS 允许抢的 RUNNING 过期 = 同一值
```

三个数字必须一起改。业务执行上限必须小于它。模拟 3 秒 << 30 秒。

### 误伤活任务

作业跑 40 秒、阈值 30 秒：

```text
T0  CAS RUNNING
T30 Sweeper 改回 PENDING，XADD
T31 另一个 Worker CAS 成功（原 RUNNING 已旧），开始第二份执行
T40 第一个 Worker 写 SUCCESS
T41 第二个也写 SUCCESS 或失败路径
```

第 4 道闸（仍须 RUNNING 才能写 SUCCESS）可能让第一个成功、第二个发现不是 RUNNING。但业务函数已经跑了两遍。所以 **长作业必须先改阈值**。

### 停下来想

为什么回收时改回 PENDING 而不是直接留 RUNNING 让 CAS 抢？PEL 丢了时没有消息去触发 CAS。必须先有铃铛，Worker 才会走进 handle_message。改 PENDING + 按铃是「重新进入主路径」，而不是发明第三条执行入口。

---

## 42. 多租户公平轮询（深入）

### 饿死

全局一条 Stream、永远 XREADGROUP COUNT 1：谁提交得多谁占满 Worker。租户 B 的一封验证码排在租户 A 的 10 万封营销后面，可能几分钟出不去。

本项目每个租户自己的 Stream。Worker：

```text
tenants = 有过投递的租户列表（Redis SET，排序保证稳定）
cursor 在列表上转圈
每个租户：先读 :h 车道，再读普通车道
读到 1 条就处理，cursor 前进
一整圈都空：sleep BLOCK_MS
```

B 最多等「转一圈的时间」，而不是等 A 的队列见底。代价：租户很多时一圈要发很多次 XREADGROUP。活跃租户靠 `{vortex}:tenants` 登记，不会扫全世界。

这不是数学最优调度（没有按等待时间加权），是工程上可讲清楚的公平。

### 停下来想

新租户第一次投递才 SADD 进集合。从未投递的租户不会被轮询——本来也没有 Stream。租户被删了集合没清，会多几次空 XREADGROUP。已知小泄漏，可用定期对账修。

---

## 43. 时间轮（深入）

### 名字从哪来

操作系统、网络协议里有一种叫时间轮的结构：环形数组，指针每滴答走一格，格子上挂着到期事件。本项目 **没有** 自己实现环形数组，而是用 ZSet 的有序性 + 1 秒滴答，得到同一类效果：O(log N) 找到期项，而不是每秒全表扫描。

Dispatcher 每秒对每个租户 EVAL 一次，每租户最多搬 100 条。1 万条同时到期要 100 秒搬完吗？每秒每租户 100，一个租户 1 万条约 100 秒。这是刻意限流，避免瞬时 XADD 打爆 Stream 和 Worker。对「9:00 整点一万封邮件」会拖尾。要更快就加大 `DELAY_DISPATCH_BATCH_SIZE` 或缩短间隔，并观察 Worker 是否跟得上。

### 停下来想

滴答用 Postgres 的 `LISTEN/NOTIFY` 或 `SELECT … SKIP LOCKED` 当到期扫描，也能做。本项目把热路径放 Redis，把补偿放 Postgres，是第 36 条的再一次应用。

---

## 44. 指数退避与死信队列（熟练）

### 立刻重试的火灾

对端 500。你立刻连重三次，对端更 500。很多客户端一起这样，叫惊群。

**退避：** 失败后等待。**指数：** 等待乘 2。给对端（和自己的 CPU）喘口气。

代码：先 `retry_count + 1` 再代入 `base * 2^retry_count`。基数 5 秒。具体秒数以源码为准，不要背口算。

第三次失败（`next_retry` 达到 `WORKER_MAX_RETRIES` 即 3）进入 **DLQ**：不再自动执行，`error_msg` 留下堆栈（截断 8000 字）。人来看日志、看库、决定重放还是放弃。本项目没有「从 DLQ 捞回」的 HTTP 接口，属于边界。

工作流上 DLQ 会 BFS 取消 WAITING 子孙（第 55 条），避免永远等待。

失败路径仍遵守铁律 B：先改行（PENDING+新 execute_at 或 DLQ），再 ZADD（若重试），再 ACK。

### 停下来想

`force_fail` 的毒药任务在压测里占 10%。每个会失败 3 次再 DLQ，所以一次提交会在 Stream/ZSet 上出现多次叫醒。看 `tasks_total{status="failed"}` 会大于提交数。这不是 bug。

---

## 45. 优雅停机（深入）

### 把第 3、4 条组装起来

目标状态：停机后

- 没有「RUNNING 且已 ACK」的行
- 没有「SUCCESS 且未 ACK」的夹子（最多残留，认领可修，但优雅路径避免）
- Redis 心跳立刻摘掉，大盘不要多一个幽灵 Worker

实现：信号 → 只 set Event → 循环不再拉新 → 当前 handle_message 自然结束 → finally 清心跳、关池。

Windows 用 `call_soon_threadsafe`，见第 4 条。

`docker stop` 默认 10 秒。业务若改成睡 60 秒，必须同时加大 stop 宽限，否则仍会被 SIGKILL，落到第 41 条。

### 停下来想

优雅停机能不能替代 Outbox？不能。停机管的是「我知道我要走」；Outbox 管的是「我走得不明不白或 Redis 当时挂了」。

---

## 第五章小结

- 裁判是 Postgres 行。铃铛服从行。
- 双写没有共享事务，Outbox 用陈旧 PENDING 补铃。
- 至少一次 + 四道幂等闸。长作业副作用要自己去重。
- 选主为了效率；双主窗口靠 SKIP LOCKED/CAS。
- 公平按租户转圈；到期按秒搬运并限流。
- 停机只竖牌，做完当前条。

下一章讲房间之间的门锁：[第六章 安全与多租户](ch06-security.md)
