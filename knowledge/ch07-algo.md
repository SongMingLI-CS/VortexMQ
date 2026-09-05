# 第七章 算法与工作流

> 接 [第六章](ch06-security.md)。本章用一张有向图把多步任务连起来。图论只需要课内两种走法：按入度消点（Kahn），按层向外搜（BFS）。
>
> 编号 53–57。上一章：[安全](ch06-security.md) · 下一章：[可观测性](ch08-observability.md)

先把词对齐：

- **节点：** 一步任务。请求里叫 `node_id`（小名，如 `"extract"`），入库后换成真正的 `task_id`。
- **有向边：** 箭头 A→B 表示 B 依赖 A。请求里写在 B 的 `depends_on: ["extract"]`。
- **入度：** 有多少箭头指进来。入度 0 的是起始任务，可以立刻 PENDING。
- **环：** 跟着箭头能走回自己。互相等待，永远做不完。

Redis 仍然只叫醒已经变成 PENDING 的节点。WAITING 的节点安静地躺在账本上。

---

## 53. Kahn 拓扑排序（熟练）

### 算法用大白话

1. 统计每个点的入度。
2. 把入度为 0 的点放进队列。
3. 拿出一个点，记入结果顺序；把它指出去的每条边删掉（邻居入度减 1）；邻居入度变成 0 就入队。
4. 重复直到队列空。

若结果里的点数等于全部点数，这是一个合法先后顺序（可能有多种，取决于队列里谁先出）。本项目提交时不按这个顺序执行——执行顺序由「父任务何时 SUCCESS」决定。Kahn 在这里的职责是 **证明无环，并在有环时指出剩余点**。

### 手算例子

节点：闹钟 A，洗脸 W，吃饭 E。边：A→W，A→E。（depends_on：W 和 E 都依赖 A）

```text
入度：A=0, W=1, E=1
队列：[A]
拿出 A，顺序=[A]，W、E 入度变 0
队列：[W, E]
拿出 W、E，顺序=[A, W, E] 或 [A, E, W]
点数 3=3，无环
```

复杂度 O(V+E)：每个点入队一次，每条边减一次入度。上限 256 个节点，毫秒级。

实现里还要：跳过重复的父节点、禁止自己依赖自己、父节点必须在 nodes 列表里。这些在减入度之前检查，失败抛 `WorkflowValidationError`，API 变 400，一行都不入库。

### 停下来想

Kahn 的顺序是 A,W,E。若 W 其实还依赖一个很慢的外部条件（execute_at 在未来），会不会按顺序被立刻执行？不会。Kahn 不负责调度。W 仍按自己的 execute_at 走 ZSet。拓扑序只证明「不存在画不出来的先后」。

---

## 54. 环检测（熟练）

### 为什么环是致命的

A 依赖 B，B 依赖 A。两者都 WAITING，都等对方 SUCCESS。没有起始 PENDING，Dispatcher 无事可做，客人永远 202。必须在提交时拒绝。

Kahn 结束后 `len(order) != len(nodes)`，剩下入度仍 >0 的点就是环上的（或依赖环的）。错误信息把它们列出来，方便改 `depends_on`。

其它非法图：

| 问题 | 例子 |
|------|------|
| node_id 重复 | 两个 `"extract"` |
| 引用不存在 | depends_on: `["没有这个点"]` |
| 自己依赖自己 | extract depends_on extract |

空图不允许（`min_length=1`）。单节点无依赖：整张图就是一个普通任务，合法。

**失败原子性：** 先校验再 `add_all` + `COMMIT`。环不会留下半张图。这和第 19 条一致：要么整张图在账本上，要么没有。

### 停下来想

三个点 A→B→C→A 是环。Kahn 从一开始没有任何入度 0 的点，队列空，order 空，三个点都被报出来。若 D 依赖 A，D 也会剩在里面（依赖环，自己不是环上的最小圈）。信息是「涉及节点」，不一定是最小环。对人够用。

---

## 55. BFS 级联取消（熟练）

### 为什么要蔓延

ETL：extract → transform → load。extract 三次失败进 DLQ。transform、load 若仍 WAITING，会永远等。取消是把「不可能发生的等待」标成 CANCELED，查询接口走 400 而不是无限 202。

已经 RUNNING 或 SUCCESS 的子孙不改：跑到一半的不一定能撤；已成功的不要事后改写历史。只动 WAITING。

### BFS：水波一层层

广度优先：先直接下游，再下游的下游。用队列：

```text
起点 failed 的 downstream_ids 入队
当队列不空：
  拿出一个 ID
  若见过则跳过（防菱形重复访问）
  把它的下游再入队
```

菱形：A 的两个子都指向 D，D 只应被访问一次。`visited` 集合保证。

收集阶段 **不加锁**，只读 ID。边在提交后不变，读到的图是稳定的。然后按 UUID 字符串排序，逐个 FOR UPDATE：仍是 WAITING 则改 CANCELED。锁顺序与唤醒相同（第 23 条）。跨租户 ID 直接跳过。

### 停下来想

父任务 FAILED 重试（回到 PENDING）会取消子孙吗？不会。代码只在进入 **DLQ** 时 `cancel_descendants`。重试期间子孙继续等，这是对的：父还可能成功。

---

## 56. 指数退避公式（入门）

### 公式

```text
delay = WORKER_RETRY_BASE_DELAY_SECONDS * (2 ** retry_count)
next_execute_at = now + delay
```

发生在 `retry_count` 已经 +1 之后。基数默认 5。

| 失败后的 retry_count | 等待（基数 5） |
|----------------------|----------------|
| 1 | 10 秒 |
| 2 | 20 秒 |
| 3 | 不再等，进 DLQ |

口算若与源码不一致，以 `compute_next_execute_at` 为准。

指数增长让暂时故障有恢复窗口，又不会像「每次等 1 小时」那么慢才进 DLQ。本项目没有加 **抖动（jitter）**：很多任务同一毫秒失败会同一毫秒醒来。任务量大时可能要加 `random * delay`。已知边界。

等待不是 `sleep` 在 Worker 里——那会占着一个进程。而是改行上 `execute_at`，ZADD，ACK，Worker 去干别的。到期由 Dispatcher 再叫醒。这是第 30、43、44 条的交汇。

### 停下来想

基数改成 0.001 秒，三次重试几乎瞬间打满对端，指数失去意义。阈值和基数都是生产参数，不是魔法常数。

---

## 57. XCom：上游结果交给下游（熟练）

### 流水线要传篮子

transform 需要 extract 的产出。HTTP 再查一次父任务结果也行，但会多一次往返、还要处理父任务不属于你的情况。提交图时把边写下，成功时把 `result_data` 塞进子任务，子任务执行时 payload 里已经有篮子。

名字 XCom 来自 Airflow：cross-communication。本项目实现非常小：一个 dict，键是父 task_id 字符串。

### 注入发生在同一事务里

父任务写成 SUCCESS 之后、COMMIT 之前，`awaken_downstream`：

1. 按序锁子任务
2. 子任务须 WAITING、同租户
3. 所有上游 status==SUCCESS（快照查询带租户谓词）
4. 写入 `_vortex_sys.upstream_results`
5. 子任务 WAITING→PENDING
6. COMMIT
7. 事务外对就绪的子任务 schedule_wakeup

第 3 步：A、C 两个父，只完成 A 时不会放行 B。后完成的那个父会再走进 awaken，那时才齐。FOR UPDATE 保证不会两人各 XADD 一次（第 20 条）。

模拟执行返回 `{"output": "data_from_任务类型"}`。真业务应把有意义的 dict 放进 `result_data`。下游从系统栏读取，不要假设位置。

### 停下来想

下游 execute_at 在未来，被唤醒成 PENDING 后走 schedule_wakeup，仍可能进 ZSet。XCom 已经在行上。到点执行时能读到。不必把「数据到达」和「时间到达」绑死。

---

## 第七章小结

- 提交时 Kahn 证明无环；有环 400，一行不留。
- WAITING 安静躺账本；全员 SUCCESS 才 PENDING 并按铃。
- DLQ 才 BFS 取消 WAITING 子孙；重试不取消。
- 退避改时间戳，不占 Worker 睡觉。
- XCom 在同一事务注入，带租户谓词。

下一章看墙上的表：[第八章 可观测性](ch08-observability.md)
