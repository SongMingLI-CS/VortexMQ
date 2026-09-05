# 第四章 Redis 与消息管道

> 接 [第三章](ch03-postgres.md)。本章是铃铛课。Redis 极快，本项目只用它叫醒 Worker，不当账本。每一条命令都问：丢了怎么办、重复了怎么办。
>
> 编号 26–35。上一章：[PostgreSQL](ch03-postgres.md) · 下一章：[分布式](ch05-distributed.md)

把 Redis 想成厨房里的三样东西：

- **Stream**：叫号屏，新号追加在末尾
- **ZSet**：按时间排的预约本
- **一个带过期时间的钥匙扣**：选主锁

它们都可以停电。停电后看第三章的账本。

---

## 26. Stream 与 XADD（熟练）

### 日志，不是队列黑板

很多人听过「队列」：先进先出的管子，读走就没了。Redis Stream **不是** 那种管子。它更像一本只能在末尾写的日志：写下的条目还在，直到你裁剪或删除。多个消费者组可以读同一本日志而不互相消灭条目。

`XADD stream * field value ...` 追加一条。`*` 表示请 Redis 生成 ID。ID 形如 `1710000000000-0`：毫秒时间戳加序号。后写入的 ID 更大，可以「从某个 ID 之后读」。

### 本项目一条消息只有三个字段

```text
task_id=3fa8...
tenant_id=b2c4...
priority=0
```

没有收件人、没有邮件正文。那些在 PostgreSQL 的 JSONB 里。原因：

1. Stream 会按 MAXLEN 裁旧条目。行李不能跟叫号屏一起被裁掉。
2. 消息越小，XADD 越快，内存越省。
3. Worker 反正要读行（要 CAS、要看状态），不存在「省一次读库所以把 payload 放进 Redis」的甜头。

### MAXLEN 近似裁剪

`maxlen=100000, approximate=True` 对应 Redis 的 `MAXLEN ~ 100000`。波浪号表示不必精确到 100000，允许 Redis 用更便宜的方式裁。只 ACK 不删条目时，日志仍会变长；MAXLEN 在 XADD 时顺手砍尾巴。

裁掉的是 **已处理的叫号历史**。PEL 里尚未 ACK 的条目不会因为你口头说「我处理完了」而单独消失——ACK 只改组的进度。MAXLEN 裁的是 Stream 本体。极端情况下裁得太狠，理论上可能碰到还没读的条目，本项目 10 万的上限对单机演示足够；生产要按堆积速度算。

### 两条车道

`priority >= 50` 写入 `{tenant}:vortex:tasks:stream:h`，否则写入不带 `:h` 的键。Worker 对每个租户先读 `:h` 再读普通。这不是全局插队，只在租户内部让紧急任务不排在十万封营销邮件后面。阈值可配：`REDIS_PRIORITY_HIGH_THRESHOLD`。

### 停下来想

XADD 成功、进程在返回前崩溃。调用方可能没收到 201。调用方重试 POST 会插入 **另一行** 任务。这是「至少一次受理」。接口幂等（同一请求只插一行）本项目没做，需要调用方自己带去重键。属于已知边界。

---

## 27. 消费者组：XGROUP / XREADGROUP（深入）

### 不要每人自己从头读日志

若三个 Worker 各自 `XREAD` 同一条 Stream，每条任务会被做三次。营销邮件发三遍。

**消费者组** 是 Redis 提供的「几个工人共用一本日志、每条新消息只分给其中一人」的机制。组名本项目固定为 `vortex:workers`。组要先创建：

```text
XGROUP CREATE stream vortex:workers 0 MKSTREAM
```

`MKSTREAM`：Stream 还不存在就建空的。已经存在组则报 `BUSYGROUP`，代码里忽略。每个租户的普通车道和高优车道都要各建一次组，因为它们是不同的 Stream 键。

### `>` 和 `0`

`XREADGROUP GROUP vortex:workers 消费者名 STREAMS stream >`

`>` 表示「这个组里还从未分配给任何人的新条目」。读成功的瞬间，条目进入该消费者的 PEL（下一条）。

`XREADGROUP ... STREAMS stream 0` 表示「属于我、还在 PEL 里的」。Worker 启动时用这个排空旧债：上次同名消费者崩溃前没 ACK 的活。

主循环的策略：优先用 `>` 拉新；这一圈没新消息，再去 XAUTOCLAIM 别人的过期 PEL。不要只 CLAIM 不拉新，否则空闲时全员抢死人的债，活人的新单没人接。

### 公平轮询在组之上

消费者组本身不理解「租户」。本项目有很多 Stream（每租户两条）。Worker 自己维护租户列表 cursor，转圈读。组只保证 **单条 Stream 内** 不重复分发新消息。跨 Stream 的公平是第五章第 42 条。

### 停下来想

组 ID 用 `0` 创建表示「从 Stream 开头」。本项目创建时 Stream 往往是空的，等价于从现在开始。若对已经有历史的 Stream 误用 `0`，会把旧历史当新任务再投一遍。CAS 会挡住执行，但仍会掀起一阵空跑。`ensure_consumer_group` 只在组不存在时创建，避免反复从 0 读。

---

## 28. PEL 与 XACK（深入）

### 夹子

PEL = Pending Entries List。每个（组，消费者）一对有一份清单：我拿走了但还没说做完的消息 ID。

```text
XREADGROUP >     从日志领一张单，夹到自己夹子上
XACK             从夹子拿掉（组认为这张单结束了）
```

没 ACK 之前，同一条消息 ID 不会再以 `>` 发给组内别人。它只在你的夹子上（或被 CLAIM 改挂）。

### 何时 ACK：对照第三章铁律 B

| 情况 | ACK 吗 | 为什么 |
|------|--------|--------|
| 缺少 task_id / 非法 UUID | 是 | 不 ACK 会永远堵夹子，这个消费者读不动后面 |
| 库里没有这行 | 是 | 毒丸或先铃后账的残渣；再留着也没有行可执行 |
| tenant_id 与行不一致 | 是 | 越权，丢掉并打日志 |
| 已是终态 / WAITING | 是 | 重复叫醒，不要再执行 |
| 未到 execute_at | 是 | 已放回 ZSet，这条铃的使命结束 |
| CAS 没抢到 | 是 | 别人在跑或已跑完 |
| 执行成功且写 SUCCESS 成功 | 是 | 账本已落定 |
| 失败路径写 PENDING/DLQ 成功 | 是 | 账本已落定；重试靠新的 ZADD 铃 |
| 写库失败 | **否** | 留夹子，等认领或重启 |

「毒丸必须 ACK」和「写库失败不得 ACK」同时成立，靠的是分类：前者没有合法账本可对齐，后者有。

### ACK 必须打在读到的那个键上

Worker 可能从 `:h` 车道读到消息。`xack(stream_key, group, message_id)` 的 `stream_key` 必须是那条车道。写到普通车道上是 ACK 空气，原夹子仍占着。`handle_message` 因此要求调用方传入 `stream_key`。

### 停下来想

ACK 之后用 `XDEL` 删条目本项目没做，靠 MAXLEN 批量裁。单独 XDEL 更精确但多一次往返。10 万上限下选择了简单。

---

## 29. XAUTOCLAIM：认领别人丢掉的单（深入）

### 厨师昏倒了

消费者名 `host-99` 的 PEL 里有一条消息，进程已经被杀。组不会自动把单转给别人。一直等到：

```text
XAUTOCLAIM stream vortex:workers 我的名字 30000 0-0 COUNT 1
```

含义：把该组里 **空闲至少 30000 毫秒** 的 PEL 条目，改挂到我名下，从 ID `0-0` 开始找，最多 1 条。空闲时间从这条消息上次被读或被认领起算。

认领成功后，这条消息就像我刚刚 XREADGROUP 到的一样，进入我的 `handle_message`。随后的 CAS 会看到 RUNNING 过期或仍是 PENDING，决定是否执行。

### 阈值太短的灾难

执行要睡 3 秒。若阈值 1 秒：

```text
T0 Worker1 读到，开始睡
T1 1 秒后 Worker2 CLAIM 走
T2 两人同时执行同一任务
```

CAS 对新鲜 RUNNING 会拒绝 Worker2。但若 Worker1 还没 CAS 完、或你把 CAS 放在 sleep 之后（本项目 CAS 在 sleep 之前，还好），窗口不同。阈值必须覆盖 **从读到到 ACK** 的最长时间，而不仅仅是业务 sleep。本项目 30 秒覆盖 3 秒业务 + 写库，余量充足。

### 和 Sweeper 回收 RUNNING 的关系

CLAIM 救的是 **铃铛还在 PEL 里** 的情况。若 Stream 被误删、PEL 没了，CLAIM 无计可施，账本上的 RUNNING 靠第 41 条。两条路覆盖两种丢失。

### 停下来想

本项目一次 CLAIM 1 条，而且按租户轮询。为什么不一次 CLAIM 100 条？提示：停机更及时；避免一个 Worker 吞下所有死人的债导致不公平；handle_message 目前是串行的。

---

## 30. ZSet 延迟队列（深入）

### 有序集合

ZSet 里每个成员有一个 **分数（score）**，Redis 按分数排序。本项目：

- 成员：`{task_id}|{priority}`，例如 `3fa8...-...|0`
- 分数：`execute_at` 的 Unix 时间戳（浮点秒）

「已经到期」= 分数 ≤ 现在。`ZRANGEBYSCORE delayed -inf now LIMIT 0 100` 取出最多 100 个。不必扫 PostgreSQL。

### 为什么还要在行上存 execute_at

ZSet 是索引。Redis 重启丢了 AOF、有人 DEL 了键、Lua 只 ZREM 没 XADD，索引会坏。行上的 `execute_at` 是真相。Worker 读到 Stream 消息后仍用 **行上的时间** 判断是否执行；没到点就再 ZADD 回去。Sweeper 看到陈旧 PENDING 也会按行上的时间决定 XADD 还是 ZADD。

### 提交时的分流

`schedule_wakeup`：

```text
execute_at <= now  → XADD Stream（即时）
execute_at >  now  → ZADD ZSet（预约）
```

HTTP 传入的 `2026-08-18T16:00:00Z` 先经 `as_utc`。客人电脑是北京时间时，应送带时区的字符串，否则 naive 时间会被当成 UTC，差 8 小时。

### 成员里为什么带 priority

Dispatcher 的 Lua 要把到期任务送进普通车道还是 `:h` 车道。ZSet 的键已经按租户切开，成员里不必再带 tenant_id（旧格式曾带，Lua 里仍兼容末段为数字的写法）。priority 留在成员里，避免再读一次 PostgreSQL。

同一 task_id 以不同 priority 出现两次？正常路径会先 ZREM 再可能再次 ZADD。重试时 retry 会带当前行上的 priority。不要手工往 ZSet 塞脏成员。

### 停下来想

ZSet 的分数用毫秒会更精确吗？Python `timestamp()` 是浮点秒，亚毫秒也会进分数。Dispatcher 每秒才跑一次，精度瓶颈在 1 秒节拍，不在分数。把间隔改成 0.1 秒会更「准时」，也更耗 CPU。本项目选 1 秒，是产品选择。

---

## 31. Lua EVAL：三条命令当一条（深入）

### 竞态

两条命令之间，别人的命令可以插进来。Redis 本身单线程执行命令，但 **你的客户端发的三条命令** 中间可以夹杂另一个客户端的命令。

```text
Dispatcher A: ZRANGEBYSCORE 得到 [任务1, 任务2]
Dispatcher B: ZRANGEBYSCORE 得到 [任务1, 任务2]   ← A 还没删
A: XADD 任务1；ZREM 任务1
B: XADD 任务1；ZREM 任务1     ← 任务1 进 Stream 两次
```

消费者组帮不上忙：两个 XADD 两个 ID。

### EVAL 把循环放到 Redis 线程里

脚本期间 Redis 不插跑别人的命令（同一实例上）。对单个租户的 delayed + 两条 Stream，循环是原子的：**一个 member 只被搬走一次**。

KEYS 必须同槽，见第 32 条。ARGV 传入 now、批量、MAXLEN、高优阈值、tenant_id。

### Lua 不是事务，不会回滚

脚本第 50 个 member 时 Redis 崩溃：前 49 个可能已经 XADD+ZREM，第 50 个可能停在只做了一半。Redis 不把脚本当数据库事务来 UNDO。

| 一半状态 | 后果 | 谁来修 |
|----------|------|--------|
| XADD 了，没 ZREM | 下次还会再 XADD | CAS 消化重复铃 |
| ZREM 了，没 XADD | 这次铃没响 | Sweeper 按行补 |
| 都做完 | 正常 | |

所以 Lua 降低 **并发重复搬走**，不提供「全成或全不成」。账本仍是最后的网。

### 停下来想

为什么不用 Redis 事务 MULTI/EXEC？MULTI 管的是排队命令一次性执行，但 `ZRANGEBYSCORE` 的结果要在客户端看完再决定 XADD 哪些——中间已经回到客户端，缝又出现了。Lua 才能根据刚读到的 members 立刻写。这是「需要读结果来决定写」时必须用脚本的原因。

---

## 32. Hash Tag、Cluster 槽、CROSSSLOT（深入）

### Cluster 把键切开

Redis Cluster 有 16384 个槽。每个键经哈希函数进一个槽。槽分配到不同机器。一条命令或脚本碰到的所有 KEY 必须在同一槽，否则节点不知道该在哪台机器上跑，报 `CROSSSLOT`。

**Hash Tag：** 键名里 `{...}` 花括号内部的子串用来算槽，而不是整个键。因此：

```text
{abc}:stream
{abc}:delayed
```

槽相同。`{abc}` 和 `{xyz}` 通常不同。

### 本项目的布局

数据面：`{tenant_id}:vortex:tasks:stream` 等，租户内三键同槽，Lua 合法。租户之间不同槽，可以分散到 Cluster 多机——这是「按租户分片」的含义。

控制面：`{vortex}:tenants`、`{vortex}:leader`、`{vortex}:metrics:workers` 固定另一槽。登记租户用单独 `SADD`，因为不能在租户 Lua 里碰 `{vortex}`。

### 本地是单机，为什么还写 Tag

Compose 单机没有槽。现在就按 Cluster 规则起键，以后迁 Cluster 不用改逻辑。反过来，若现在把全局 delayed 和各租户 stream 写进同一 Lua，单机没事，一上 Cluster 立刻 CROSSSLOT。注释里的「不要再把全局 ZSet 和各租户 Stream 写进同一个 Lua」是有人踩过的坑。

pipeline 统计各租户 XLEN 时 `transaction=False`：不要用 MULTI 包跨槽命令。

### 停下来想

tenant_id 是 UUID，几乎每个租户一个槽分布。租户极少时，槽会很不均匀，Cluster 的「均匀分散」甜头不明显。这套布局的价值在租户多的时候，以及「禁止跨租户 Lua」的纪律。

---

## 33. SET NX PX：租约锁（深入）

### 互斥，但必须能自动解开

多个 API 副本只要一个跑 Sweeper。需要互斥。若用「SET 一个键表示我是 Leader」且永不过期：持有者被 kill -9，键还在，永远没人扫表。所以必须带过期：

```text
SET {vortex}:leader <token> NX PX 10000
```

- **NX：** 不存在才成功。成功者当选。
- **PX 10000：** 10 秒后键消失。持有者死了，最多 10 秒后别人能 SET 成功。
- **value 是 token：** 续期和释放时认人，见第 34 条。

这叫 **租约**：不是买断的锁，是「我活着会续，死了自动作废」。

### 不是共识算法

教科书里的 Raft/Paxos 能在网络分区时保证最多一个 Leader（或停止服务）。单 Key SET NX 在分区时可能：旧 Leader 以为自己还持有（其实 TTL 到了），新 Leader 已经当选，短暂双主。本项目 **接受** 这个窗口，正确性放在 SKIP LOCKED 和 CAS 上。注释写了「不是 Redlock」。不要把它宣传成金融级互斥。

TTL 必须明显大于续约间隔（10s vs 3s）。网络抖 4 秒不应丢锁；抖 11 秒应当丢，让别人接手。

### 停下来想

若续约间隔改成 9 秒、TTL 10 秒，几乎任何 GC 停顿都会丢锁，Sweeper 会像走马灯一样在节点间跳。两个参数要一起改。

---

## 34. 比较后续期、比较后删除（熟练）

### 误续别人的锁

```text
T0 我 SET 成功，token=A
T1 我卡了 11 秒，键过期
T2 别人 SET 成功，token=B，当选
T3 我醒来，PEXPIRE leader 10000
    ← 若无比较，把别人的锁续成我的时间，两人混乱
```

续期脚本：

```lua
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
```

只有值仍是我的 token 才续。否则返回 0，Python 侧 `_is_leader=False`，停掉 Sweeper。

释放同理：不能 DEL 一个已经属于别人的键。进程退出走这条；若来不及走，靠 TTL。

token 含 hostname-pid-随机，避免 pid 复用。见第 7 条。

### 停下来想

GET 和 PEXPIRE 为什么也必须放 Lua 而不能分两条命令？提示：中间别人可能刚好抢到并写入新 token，你的 PEXPIRE 会续到别人头上。和第 31 条同一类缝。

---

## 35. AOF 持久化（入门）

### Redis 怎么把内存落到磁盘

默认 Redis 主要在内存。可选：

- **RDB：** 周期性拍快照。可能丢最后几分钟。
- **AOF：** 每个写命令追加到文件，重启回放。Compose 开了 `--appendonly yes`。

AOF 让「容器重启后延迟队列还在」成为可能。它 **不能** 挡：

- 有人执行 `FLUSHALL`
- 错误的 XACK（逻辑错误，不是掉电）
- MAXLEN 裁掉的历史
- 磁盘满、文件损坏

所以设计原则不变：任务是否存在只问 PostgreSQL。AOF 是铃铛房的黑匣子，降低重启成本，不升级铃铛的法律地位。

### 停下来想

Postgres 也有 WAL。为什么 Postgres 的 WAL 就能当 SSOT，Redis 的 AOF 不能？提示：不是日志格式更高级，是 **我们选择谁拥有状态机**。两份日志都能扛掉电；不能扛的是双写时以谁为准。选一个当真相，另一个当提示。

---

## 第四章小结

- Stream 是叫号日志；消息只带 ID；MAXLEN 裁历史。
- 消费者组 + PEL + ACK 管「同一条消息 ID」；CAS 管「同一个 task_id」。
- ZSet 是预约本；Lua 原子搬走；搬丢了看账本。
- 选主是带 TTL 的钥匙扣，续期前先认 token。
- Hash Tag 让租户内三键能进同一 Lua。

下一章把这些零件组装成分布式行为：[第五章 分布式系统](ch05-distributed.md)
