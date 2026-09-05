# 第三章 PostgreSQL 与 ORM

> 接 [第二章](ch02-web.md)。本章是账本课：表怎么存、事务在哪条边界切开、两个人同时改同一行时用什么原语。深入项（19–23）建议准备一张纸画时间线。
>
> 编号 16–25。上一章：[Web](ch02-web.md) · 下一章：[Redis](ch04-redis.md)

复习 [第零课](../KNOWLEDGE.zh-CN.md) 0.5：表、行、列、主键。本章所有「锁」都发生在 **行** 上，不是整张表（除非你写出了锁表的 SQL，本项目没有）。

---

## 16. SQLAlchemy 2.0 异步 ORM（熟练）

### SQL 是什么

跟数据库说话有一种专用语言，叫 SQL。例如：

```sql
INSERT INTO task_records (task_id, tenant_id, status, task_type, ...)
VALUES (...);

SELECT * FROM task_records
 WHERE task_id = '3fa8...' AND tenant_id = 'b2c4...';
```

你可以手写这些字符串。字符串里拼客人的输入，历史上催生了大量「SQL 注入」事故：客人在输入框里写一段能改变语句含义的字符。ORM 用参数绑定把「语句结构」和「值」分开，能挡住一类注入。本项目仍有少量手写 SQL（启动时补列），不拼接客人输入。

### ORM 把行变成对象

`TaskRecord` 类对应表 `task_records`。你写 `task.status = TaskStatus.SUCCESS`，提交时 SQLAlchemy 生成 UPDATE。你读 `task.payload["to"]`，不必先自己 `json.loads`。

**异步** 版本的引擎在执行时 `await`，把等待数据库的时间还给事件循环（第 2 条）。驱动必须是 `asyncpg`。连接串写成：

```text
postgresql+asyncpg://用户:密码@主机:5432/库名
```

少了 `+asyncpg`，异步引擎会不工作或走错驱动。Compose 里主机名是 `postgres`（服务名），不是 `localhost`。

### Session 是一次「工作单元」

`AsyncSession` 记住你这期间读过、改过哪些对象。`commit()` 把改动打到数据库并结束事务。`rollback()` 撤销。`expire_on_commit=False` 表示提交后对象上的属性仍可读取，不必立刻再 SELECT 一次——API 返回 `task_id` 时用得上。

每个 HTTP 请求一个 Session（`get_db`）。不要把 Session 存成全局变量跨请求用：事务会缠在一起，连接也无法归还。

Worker 不走 `get_db`，自己 `async with AsyncSessionLocal() as session`。注意 `handle_message` 里 **开了不止一个 Session**：抢 RUNNING 一次，写 SUCCESS/失败又一次。这是故意的短事务，见第 19 条。

### 启动时 create_all 加 ALTER

`init_db` 先按模型建尚不存在的表，再执行一串 `ALTER TABLE ... IF NOT EXISTS` 给旧库补列。这适合本地脚手架。多副本同时 ALTER 在生产上会难受，正式环境应换成 Alembic 迁移。讲义里把它当作「已知边界」，不是推荐做法。

### 停下来想

`autoflush=False` 是什么意思？提示：改了对象但还没 commit 时，默认有的配置会在下一次 SELECT 前先把改动刷到数据库。关掉后你必须自己 flush 或 commit。工作流唤醒下游前有一次 `await session.flush()`，就是为了让同事务里刚写的 SUCCESS 能被随后的 SELECT 看见。

---

## 17. UUID / JSONB / ENUM / TIMESTAMPTZ（熟练）

四个列类型，解决四个不同问题。不要混着记成「Postgres 的高级功能」。

### UUID 当主键

见第 5 条。SQLAlchemy 里 `UUID(as_uuid=True)` 表示：Python 侧用 `uuid.UUID` 对象，不是字符串。打印、比相等时更不容易和普通 str 搞混。

### JSONB：形状不固定的行李

邮件任务的 payload 有 `to`、`subject`；报表任务可能有 `month`。若为每种任务加列，表会宽到没法维护。JSONB 把一整份 JSON 存成二进制，可整包读写。

本项目还用 JSONB 存：

- `upstream_ids` / `downstream_ids`：UUID 列表。从库里读出来常常是字符串，所以有 `parse_uuid_list`。
- `result_data`：成功后的返回值。
- `payload` 里系统稍后注入的 `_vortex_sys`（第 51、57 条）。

改 JSONB 里的嵌套字段时，SQLAlchemy 有时看不出对象「脏了」，需要 `flag_modified(child, "payload")`。工作流注入 XCom 时就是这样做的。漏了这步，commit 可能不写回数据库，下游醒来发现没有上游结果。

### ENUM：状态机的铁栅栏

`task_status` 是 Postgres 原生枚举。写 `PENDNG` 会直接被数据库拒绝。比 VARCHAR + 应用层检查更硬。

代价：新增枚举值要用 `ALTER TYPE ... ADD VALUE`。`init_db` 里为 `WAITING`、`CANCELED` 做了这件事。枚举值一旦加进去，删除很痛苦，所以状态机要先想清楚再加。

Python 侧 `class TaskStatus(str, Enum)` 和数据库枚举同名同值，两边要对齐。

### TIMESTAMPTZ

`timestamptz` 存的是绝对瞬间，显示时按会话时区转。本项目会话与应用都按 UTC 走，减少「存进去是 8 点、读出来是 0 点」的惊吓。`server_default=func.now()` 表示插入时若没给值，用数据库的现在。`onupdate=func.now()` 在 ORM 更新对象时刷新 `updated_at`——但 Sweeper 会 **显式** 赋值 `updated_at`，因为那一戳是租约，不能依赖「有没有改其它字段」。

### 停下来想

为什么 Stream 消息不放整份 payload，而放在 JSONB 里？提示：铃铛可裁剪、可丢；行李必须跟行走。第四章第 26 条会再强调。

---

## 18. 按查询建索引（熟练）

### 没有索引时数据库在干什么

表有 100 万行。Sweeper 要找 `status = PENDING AND updated_at < 30 秒前`。没有索引时，数据库从第一行扫到最后一行，叫 **顺序扫描**。每 10 秒扫一遍，磁盘和 CPU 会先于业务被打满。

**索引** 像书末按主题排的目录：先定位到 PENDING 那一段，再在这段里按时间切。代价是：每次 INSERT/UPDATE 都要改目录，占磁盘。

### 本项目四条索引各自服务谁

| 索引列 | 谁在用 |
|--------|--------|
| tenant_id + status | 将来控制台「看我的失败任务」 |
| status + priority + created_at | 注释写明给后续按优先级抢占用 |
| status + updated_at | Outbox 扫陈旧 PENDING / RUNNING |
| workflow_id | 按图查询节点 |

复合索引的列顺序有讲究：条件里等值的列靠前，范围条件靠后。`status = PENDING AND updated_at < ?` 把 status 放前面是对的。

不要给 `payload` 整列建普通 B 树索引：JSONB 很大，而且 Sweeper 根本不按 payload 搜。

### 停下来想

若 Sweeper 的查询改成 `WHERE status = PENDING AND execute_at < now()`，现有索引还合适吗？提示：现有第三条索引的第二列是 `updated_at` 不是 `execute_at`。改查询往往要改索引，这就是「按查询建」。

---

## 19. 事务边界与提交顺序（深入）

### 事务是什么：用转账讲

你给同学转 50 元：你的账户减 50，同学加 50。若减完之后停电、加没做成，钱消失了。**事务** 把两步打包：要么都永久生效（COMMIT），要么都当作没发生（崩溃或 ROLLBACK）。

数据库用日志（WAL）保证：COMMIT 返回成功后，即使立刻断电，重启也能恢复到提交后的状态。这就是账本比铃铛硬的物理原因。

**未提交的改动，别的连接默认看不见。** 这叫隔离。所以「先插入再按铃」必须先 COMMIT：否则 Worker 可能读不到这行。

### 铁律 A：先提交账本，再按铃

`submit_task` 的顺序：

```text
1. create_task：INSERT + COMMIT     ← 行对全世界可见，状态 PENDING
2. schedule_wakeup：XADD 或 ZADD
3. 成功则刷新 updated_at 再 COMMIT  ← 投递租约，Sweeper 别立刻当成失败
4. 失败则记日志，接口仍返回这行     ← HTTP 201
```

对应代码：

```22:50:app/services/task_service.py
async def submit_task(...):
    record = await create_task(...)          # 内部已经 commit
    try:
        channel = await schedule_wakeup(...)
        record.updated_at = utcnow()
        await session.commit()
    except Exception:
        logger.exception("即时投递 Redis 失败...")
    return record
```

**反例时间线（先按铃再提交）：**

```text
T0  XADD，Worker 立刻读到 task_id
T1  Worker SELECT，行还不存在（事务没提交）
T2  Worker 以为毒丸，XACK 丢掉
T3  API 才 COMMIT
T4  账本上有一行 PENDING，铃铛已被摘掉
T5  只能等 Sweeper 最多约 40 秒后补铃
```

T2 那种「任务不存在就 ACK」在本项目是故意的（防毒丸堵 PEL）。先按铃会把合法任务当成毒丸。所以顺序不能反。

### 铁律 B：先写完终态，再 XACK

Worker 成功路径：

```text
1. CAS：PENDING → RUNNING，COMMIT
2. 睡 3 秒（模拟业务）
3. 新事务：RUNNING → SUCCESS，COMMIT
4. XACK
```

若 3 和 4 对调，4 成功 3 失败：铃铛没了，账本停在 RUNNING。PEL 救不了，因为已经 ACK。只能等第 41 条的僵死回收，而且要等满租约。业务若已产生外部副作用（真发出邮件），还会和回收后的重跑叠在一起。所以 ACK 必须在账本落定之后。

写库失败则 **不 ACK**，消息留在 PEL，30 秒后别人认领或自己重启后用 `0` 再读。

### 短事务：不要握着锁去等网络

一次事务从 BEGIN 到 COMMIT 之间，占着连接、可能占着行锁。本项目把「等 Redis」「睡 3 秒」放在事务外面：

- 抢 RUNNING 的 UPDATE 单独 commit
- 执行在 Session 之外
- 写 SUCCESS 再开一个 Session

若把 sleep(3) 放进持有 FOR UPDATE 的事务里，这行锁三秒，别的 Worker、Sweeper 都堵着。

### 常见误解

- 「数据库事务能包住 Redis。」不能。它们是两个系统，没有共享的 COMMIT。Outbox（第 37 条）就是承认这一点之后的补救。
- 「commit 很慢，我少 commit 几次。」该切的边界不切，锁和可见性会以更难查的方式出错。

### 停下来想

`create_task` 已经 commit 了，`submit_task` 里 Redis 成功后又 commit 一次刷新 `updated_at`。若第二次 commit 失败，会出现什么状态？提示：行仍是 PENDING，updated_at 可能仍很新或仍很旧，Sweeper 行为不同。这是双写世界里的灰色地带，最终靠 stale 窗口收敛。

---

## 20. SELECT FOR UPDATE：行锁（深入）

### 锁是什么

锁是一块「洗手间使用中」的牌子。**行锁** 只占这一行，表上其它行别人仍能改。`SELECT … FOR UPDATE` 的含义是：我读这行的同时挂牌，直到我的事务结束（COMMIT 或 ROLLBACK）。

别的事务：

- 普通 SELECT（不加锁）在默认隔离级别下仍可能读到旧版本（MVCC），不一定等。
- 另一个 `FOR UPDATE` 或 UPDATE 这行，会等。

本项目需要的是第二种：两个 Worker 不能同时把子任务从 WAITING 改成 PENDING。

### 唤醒下游为什么要锁子任务

工作流：B 依赖 A 和 C。A、C 几乎同时 SUCCESS。

```text
Worker1（刚做完 A）          Worker2（刚做完 C）
读 B，看到 WAITING            读 B，看到 WAITING
读上游：A 已成功（自己刚写）   读上游：C 已成功
     C 可能还没提交 → 不齐？   A 可能还没提交 → 不齐？
```

若 A、C 都已提交，不加锁的时间线：

```text
两者都看到 WAITING 且上游齐
两者都 UPDATE B 为 PENDING
两者都 XADD                 ← 两声铃
```

CAS 会让 B 只执行一次，但 Stream 上多一条垃圾。加锁后：

```text
Worker1 SELECT B FOR UPDATE，拿到锁
Worker2 SELECT B FOR UPDATE，等待
Worker1 看到上游齐，改 PENDING，COMMIT（放锁）
Worker2 被唤醒，再读 B，已经是 PENDING，跳过
```

### 只锁子任务，不锁上游

上游已经是终态，再锁上游容易和「对方正在 UPDATE 自己那一行写成 SUCCESS」形成环（第 23 条）。本事务里刚写的父任务 SUCCESS，先 `flush`，后续 SELECT 能看见自己的写入。另一方若尚未提交，这边会看到对方仍非 SUCCESS，于是不唤醒，把机会留给后提交的那一方。这是正确的：不能在上游还没提交时提前放行。

### 停下来想

普通 SELECT 看到 WAITING，然后另开一个事务去 UPDATE，中间没有锁。这叫「读和写之间有缝」。FOR UPDATE 把读和后续写放进同一事务，缝没了。CAS（第 22 条）是另一种缝的缝法：不先读，直接带条件更新。两者本项目都用，场景不同。

---

## 21. SKIP LOCKED：遇锁跳过（深入）

### 排队 vs 跳过

普通 `FOR UPDATE`：你要的行被人锁了，你坐在门口等。适合「我就想改这一行」。

Sweeper 的需求是：「给我任意 20 行陈旧 PENDING」。并不指定哪 20 行。这时等是浪费：等的期间你本可以去处理没人锁的行。

`SKIP LOCKED`：这行有人拿着？跳过。拿下一批没牌子的。

### 两个扫描者的时间线

假设选主短暂双主，Sweeper A、B 同时跑（第 40 条承认这可能发生）：

**不用 SKIP LOCKED：**

```text
A 锁住 id=1..20
B 想选同一批，阻塞
A Redis 写完，COMMIT
B 醒来，可能再选到 1..20，再 XADD 一遍
```

**用 SKIP LOCKED：**

```text
A 锁住 1..20
B 跳过 1..20，拿到 21..40
两人并行，没有重复认领
```

即使后来双写同一 task_id 的铃铛（其它路径），CAS 仍在。SKIP LOCKED 减少的是 **扫描层的重复和互相堵死**。

### 事务必须短，且不要在锁内 await Redis

认领过程：

```text
BEGIN
SELECT … FOR UPDATE SKIP LOCKED   -- 锁住最多 20 行
把 updated_at 打成 now            -- 租约刷新
COMMIT                            -- 放锁
-- 事务外 --
逐个 schedule_wakeup
```

若把 XADD 放进事务里：Redis 抖动 5 秒，这 20 行锁 5 秒，Worker 若要改同一行会等。连接池里的连接也占着。注释写得很狠：禁止这样做。

Redis 这次又失败：updated_at 已经新了，要再等一个 stale 窗口（默认 30 秒）才会被扫到。这是节流，避免对已出问题的 Redis 打出重试风暴。

### 停下来想

SKIP LOCKED 会不会饿死某几行（永远被跳过）？在本项目里：锁只持有极短时间，不存在长期跳过。若有人把锁持有做成跨分钟，才会饿死。所以「短事务」和 SKIP LOCKED 是配套的。

---

## 22. CAS：比较并交换（深入）

### 先读后写的缝

```text
Worker1 读到 PENDING
Worker2 读到 PENDING
Worker1 UPDATE RUNNING
Worker2 UPDATE RUNNING     ← 也成功了，两个人都去执行
```

**CAS** 把「看」和「改」合成一条 UPDATE，数据库保证这条语句执行期间没有别人插进来改同一行：

```text
UPDATE … SET status=RUNNING
 WHERE task_id=?
   AND (status=PENDING
        OR (status=RUNNING AND updated_at 足够旧))
 RETURNING task_id
```

返回一行：我抢到了。返回空：没抢到，不要执行。

对应 `claim_task_for_execution`。`RETURNING` 让 Python 不必再 SELECT 一次来问「改到了没有」。

### 为什么 PEL 不够

PEL 保证：**同一条 Stream 消息 ID** 同时只在一个消费者的夹子上。

但下列情况会产生 **另一条** 消息 ID、同一个 task_id：

- Outbox 补投
- Dispatcher 和提交路径各 XADD 一次（窗口很小但仍可能）
- Lua 只 XADD 没 ZREM 再被 Sweeper 补

消费者组把它们当两件不同的事。CAS 认账本，不认铃铛编号。这是「至少一次 + 幂等」的核心闸门（第 38 条）。

### 过期 RUNNING 也可抢

```text
条件 1：现在是 PENDING          → 正常领取
条件 2：现在是 RUNNING 且 updated_at 早于 (现在 - 30秒)
                                → 原消费者可能死了，允许回收
其它：SUCCESS/DLQ/WAITING/新鲜 RUNNING
                                → 返回空
```

新鲜 RUNNING 排除掉，否则任务睡到第 2 秒时，另一个 Worker 会把它再跑一遍。30 秒来自 `WORKER_CLAIM_IDLE_MS`，必须和 XAUTOCLAIM 的空闲阈值一致，也必须大于正常执行时间。模拟任务 3 秒，余量很大。你改成 5 分钟作业时，两个地方一起改。

### 写终态时还有一道 FOR UPDATE

CAS 抢到后，执行可能成功或失败。写 SUCCESS 时再次 `get_task_for_update`，并检查仍是 RUNNING。防止：租约到期后别人也 CAS 成功、两人同时写终态把 retry_count 各加一次。第二道闸在 `_persist_success` / `_persist_failure`。

四道闸清单见第 38 条。本章先吃透 CAS 这一道。

### 停下来想

CAS 的 UPDATE 不先 SELECT。这叫乐观：假设竞争不多，冲突时谁都改不到 0 行以外的东西。若竞争极热（同一 task_id 每秒 100 次叫醒），大量 UPDATE 打到同一行，仍正确，只是浪费。那时该问的是：为什么同一任务会被叫醒 100 次？

---

## 23. 锁顺序：防止 AB-BA 死锁（深入）

### 死锁的最小例子

```text
事务 1：锁住行 A，想再锁行 B
事务 2：锁住行 B，想再锁行 A
```

两人永远等。Postgres 会在一段时间后挑死一个，抛出死锁错误。那是事故：被挑死的那次唤醒或取消会失败，靠重试才能恢复。

### 规则：全局同一顺序

「先锁 UUID 字符串更小的那一行」。唤醒下游和取消子孙都 `sorted(..., key=str)`。规则本身没有业务含义， **一致** 才有含义。

交叉场景：Worker1 成功唤醒，按序锁子任务；Worker2 因父任务 DLQ 取消子孙，按同一顺序锁。不会出现一个从 A 到 B、一个从 B 到 A。

### 少拿锁也是预防

只锁子任务、不锁上游；取消时先无锁 BFS 收集 ID，再按序加锁。持有的锁越少、时间越短，死锁窗口越小。

### 停下来想

Python 的 `sort` 和 Postgres 的 UUID 比较顺序是否永远相同？本项目按 **字符串** 排，两边都把 UUID 当字符串看，一致。若一边按 UUID 二进制、一边按字符串，可能出现不一致。读代码时看到 `key=str` 不要随手删掉。

---

## 24. 连接池与 pool_pre_ping（入门）

### 连接很贵

每次 TCP 握手 + 认证 + 可能的 TLS，比 `SELECT 1` 本身贵得多。进程启动时准备一小池连接（几条到几十条），用完归还，下个请求借用。

池耗尽时，新请求会等连接。Session 泄漏（打开不关）会把池耗尽，所有接口卡住。所以 `get_db` 用 `async with` 保证退出即还。

### 死连接

防火墙、Docker 网络、Postgres 重启，都可能让池里的连接其实已经断了。你用它时会得到奇怪的错误。`pool_pre_ping=True`：借出前先发一个很轻的探测，死了就丢掉重连。多一次往返，换少一次诡异故障。

### 停下来想

Worker 和 API 各有自己的引擎和池，因为它们是两个进程。池大小互不影响。Postgres 的 `max_connections` 必须大于「所有进程池之和」，否则启动时看起来健康、一压测就连不上。

---

## 25. 外键 ON DELETE CASCADE（入门）

### 引用完整性

`task_records.tenant_id` 必须指向真实存在的 `tenants.id`。否则会出现「任务属于一个不存在的公司」。

删除租户时有两种哲学：

- **RESTRICT：** 还有任务就不许删租户。很安全，但运维删测试租户会失败。
- **CASCADE：** 删租户时数据库自动删它名下的任务。本项目用这个。

SQLAlchemy 侧 `relationship(..., cascade="all, delete-orphan")` 管的是「通过 ORM 删 Tenant 对象时」。数据库侧 `ondelete="CASCADE"` 管的是「直接 SQL DELETE tenants」。两边都写，避免只走一条路时留下孤儿。

### 停下来想

CASCADE 会不会误删大量任务？会。删租户是高权限操作，本项目甚至没有 HTTP 删除租户接口。危险操作放 CLI、放人的手里，比放在网上好。

---

## 第三章小结

- ORM 让行变成对象；事务边界比对象语法更重要。
- 先 COMMIT 账本再按铃；先写终态再 ACK。
- FOR UPDATE 堵住读与写之间的缝；SKIP LOCKED 让扫描者互不排队；CAS 让抢执行权变成一条 UPDATE。
- 大家按同一 UUID 字符串顺序拿锁。

下一章进入铃铛房：[第四章 Redis 与消息](ch04-redis.md)
