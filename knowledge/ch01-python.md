# 第一章 Python 与运行时

> 接 [第零课](../KNOWLEDGE.zh-CN.md)。本章解决：这份说明（源码）是怎么被一台电脑跑起来的，以及为什么本项目几乎处处写 `async` / `await`。
>
> 编号 1–8。下一章：[Web API](ch02-web.md)

Python 是本项目的工作语言。源码是给人看的说明；解释器按说明驱动进程做事。你暂时不必能独立写出这些代码，但要能看懂下面这种句子在干什么。

---

## 1. Python 3.10+ 类型系统（入门）

### 从你会的东西讲起

数学作业会写「设 x 为整数」。编程里也可以写「这个盒子里只能放某一种东西」。Python 早期很宽松：盒子没有标签，放整数、放字符串都不会立刻报错，直到某一天你把两个电话号码当数字加起来，程序才在运行时炸掉。

**类型标注** 就是把标签写在盒子旁边。它主要给人和工具看。解释器默认不靠它拦运行（除非你另外跑检查器），但编辑器会用红线告诉你：你把任务 ID 传进了租户参数。

### 本项目里你实际会看到的写法

```python
tenant_id: UUID          # 这个值是 UUID
status: TaskStatus       # 只能是状态枚举里的某一个
payload: dict[str, Any]  # 字典：键是字符串，值随便
hashed: str | None       # 要么是字符串，要么是空
```

竖线 `|` 表示「或者」。`str | None` 读作「字符串或者空」。空在 Python 里叫 `None`，表示「这里没有值」，不是空字符串 `""`，也不是数字 0。把「没填执行时间」写成 `None`，和写成 `"None"` 这个单词，是两件完全不同的事。

`dict[str, Any]` 里的 `Any` 表示「值的类型我懒得在这里收紧」。`payload` 因任务而异，所以用它。门口仍有体积和保留键检查（第 11、50、51 条）。

### 为什么是 3.10+

`str | None` 这种写法从 3.10 才成为正式语法。更老的版本要写 `Optional[str]`，还得从 `typing` 导入。本仓库按 3.10 来写，所以环境也要求 3.10 以上。Docker 镜像用的是 3.12，本地只要不低于 3.10 即可。

### 如果没有它会怎样

两个 UUID 在屏幕上都是一长串带横线的十六进制。人眼分不清哪个是任务、哪个是租户。没有标注时，函数参数写反，可能直到生产上把 A 公司的任务写进 B 公司名下才被发现。标注不能替代第 49 条的租户谓词，但它能在开发阶段拦住一类低级错误。

### 常见误解

- 「有类型标注，运行时就会自动检查。」不一定。Python 仍是动态语言。FastAPI / Pydantic 会检查 **进门口的 JSON**；函数内部你自己写的赋值，解释器不会仅仅因为标注而拒绝。
- 「类型是给编译器优化用的。」在本项目里，类型首先是给人读的合同。

### 在 VortexMQ 里

几乎每个 `.py` 文件。读代码时把冒号后面的名字当注释：先读标签，再读逻辑。从 `app/models/task.py` 的 `TaskRecord` 开始最合适，每个字段都有标注和 `comment=`。

### 停下来想

`execute_at: datetime | None = None` 这一个字段里出现了两个 `None`，含义一样吗？提示：一个是类型里的「允许空」，一个是默认值「调用方没传就当空」。

---

## 2. asyncio 协程与 Task（熟练）

### 从你会的东西讲起

你在食堂打饭。窗口只有一个阿姨。如果她必须盯着一锅面煮满三分钟才接下一位，队伍会排到门外。聪明的做法是：面下锅以后，先给下一个人打菜，到点再回来捞面。

计算机里，「等面熟」对应：等数据库回包、等 Redis 回包、`sleep(3)`。这些时间里 CPU 几乎闲着。**协程** 就是让一条执行线索在「等待」时去推进别的已经等完的事。

### 三个词必须分开

**`async def` 函数（协程函数）。** 调用它不会马上把里面的代码跑完，而是得到一个「还没开始或尚未跑完的工作」。要真正推进它，通常要 `await`，或把它包成 Task。

**`await xxx()`。** 这句话的完整意思是：「开始做 xxx；若 xxx 需要等待外部，把窗口让出去；xxx 完成后，从下一行继续。」没有 `await` 的 `async def` 调用，工作可能根本没被安排执行。

**`asyncio.create_task(...)`。** 「请另起一张小票盯着这件事，我先去问客人下一句。」当前函数继续往下走，小票在后台推进。API 启动时用它拉起控制面选主循环，就是这个意思。

### 用一条时间线看 API 进程

API 进程内部同时有：

- 正在处理的若干个 HTTP 请求（每个请求是一段协程）
- 一个控制面循环（选主、必要时跑 Sweeper / Dispatcher）

简化到毫秒级：

```text
时刻    HTTP 请求 A              HTTP 请求 B           控制面
0ms     开始校验 Key
5ms     await 查数据库 ──────►  让出
5ms                             开始校验 Key
12ms                            await 查数据库 ──────► 让出
12ms                                                   await 续 Leader 锁
20ms    数据库回来，插入任务
25ms    await XADD Redis ─────► 让出
25ms                            数据库回来，插入任务
```

同一条线程（同一个阿姨）在几毫秒内为两桌客人和后厨定时器都服务了。这不是两颗 CPU 同时算哈希，是 **等待期间的交替**。

### 和「开很多线程」比

每个 HTTP 开一条操作系统线程也可以。线程有独立的调用栈，切换由操作系统做，能利用多核。代价是：几百条线程占内存、切换贵、共享数据要加锁。等 Redis 这种场景，线程大部分时间在睡，浪费。协程在等网络时几乎不占额外线程。

本项目选择：主路径协程；bcrypt 这种 CPU 活丢线程池（第 6 条）。

### Worker 里的协程是另一副样子

打开 `app/worker/main.py` 会发现：主循环读到一条消息后 `await handle_message(...)`，做完再读下一条。它 **没有** 同时执行 100 个任务。协程在这里的价值是：等 PostgreSQL、等 Redis 时不把进程卡死，停机信号有机会被处理，心跳能写出去。真正的水平扩展靠 **多开 Worker 进程**，不是在一个 Worker 里堆协程。

### 常见误解

- 「async 等于更快。」对 CPU 密集计算，async 通常不会更快，有时更慢。它快在「同时等很多外部系统」。
- 「await 会开新线程。」默认不会。
- 「写了 async def 就是并发了。」没有 await 让出、也没有 create_task，它仍是一段普通顺序代码。

### 在 VortexMQ 里

- `app/main.py`：`control_task = asyncio.create_task(run_control_plane())`
- `app/worker/main.py`：整条消费循环
- 所有 `await session.commit()`、`await redis.xadd(...)`

### 停下来想

如果把 Worker 的 `await asyncio.sleep(3)` 改成不带 await 的死循环空转 3 秒（拼命占 CPU），API 会受影响吗？Worker 自己呢？提示：它们是两个进程。

---

## 3. 协作式取消与 CancelledError（深入）

### 从第 2 条往下走

取消一段协程，有两种暴力程度：

1. **抢占式。** 操作系统立刻打断。正在执行的最后一条指令可能停在任何地方。
2. **协作式。** 发一张「请停」的条子。协程跑到下一个 `await`（一个可以歇脚的点）才看到条子，抛出 `CancelledError`。

asyncio 的 `task.cancel()` 是第二种。它不是立刻把内存抹掉，而是：下次让出时间片时，把取消变成异常。

### 为什么本项目必须用协作式

想象 Worker 正在做：

```text
1. CAS 成功，行已经是 RUNNING
2. sleep(3) 到第 2 秒
3. 此时有人 docker stop
```

若整条协程被瞬间取消：

- 账本停在 RUNNING
- Redis 还没 XACK，消息在 PEL 里

这其实还有救（PEL + 认领，第 29、41 条）。更糟的是另一种：已经 XACK 还没写成 SUCCESS。协作式取消的目标是： **正在 handle_message 里的那一条，尽量跑完写库和 ACK**；新的 XREADGROUP 不要再发。

API 侧同理：Sweeper 可能正锁着一批行。取消时要让循环在 `await sleep` 处干净地退出，并释放 Leader 锁。

### 必须把 CancelledError 再抛出去

Python 里 `except Exception` 会把 `CancelledError` 也抓住（在 3.8 之后 CancelledError 继承 BaseException，3.8 之前情况不同；本项目按「不要用裸 except 吞掉取消」来写）。错误写法：

```python
try:
    await sweep_outbox_once()
except Exception:
    logger.exception("出错了")   # 若这里把取消也吞了，停机指令失效
```

正确写法是本仓库这样：先单独 `except asyncio.CancelledError: raise`，再捕其它异常。`raise` 表示「我知道这是取消，我不处理，继续往外传」。

`contextlib.suppress(asyncio.CancelledError)` 用在「我已经 cancel 了，现在 await 它结束，不让这个异常再吓到 lifespan」。语义是：我预期这里会看到 CancelledError，这是正常下班，不是事故。

### 时间线：API 进程收到停机

```text
T0  uvicorn 开始关闭，lifespan 进入 finally
T1  control_task.cancel()     ← 只是贴条子
T2  控制面循环正 await sleep(3)，醒来看到条子，抛 CancelledError
T3  finally 里停止 Sweeper / Dispatcher 两个子 Task（同样 cancel）
T4  release_leader()          ← 把 Redis 锁还给别人
T5  close_redis()；engine.dispose()
```

若 T1 之后直接杀进程，T4 可能发生不了。锁还有 TTL，最多 10 秒后别人也能抢到，所以不是世界末日，但 failover 会变慢。

### 常见误解

- 「cancel() 返回后，后台任务已经停了。」不一定。需要 await 那个 Task，等到它真正结束。
- 「捕获所有异常是健壮。」在协程世界里，它可能让你关不了机。

### 在 VortexMQ 里

`app/main.py` 的 lifespan；`app/services/outbox.py`、`delay_dispatcher.py`、`control_plane.py` 的循环。Worker 的优雅停机主要不是靠 cancel 当前 handle_message，而是靠停机 Event，见第 4、45 条。

### 停下来想

为什么 Worker 不在收到 SIGTERM 时 `cancel` 当前的 `handle_message`？提示：sleep(3) 的 await 点会响应取消，业务会停在 RUNNING 且未 ACK。

---

## 4. 信号处理：Unix 与 Windows（熟练）

### 从你会的东西讲起

你在终端按 Ctrl+C，并不是 Python 自己侦测了键盘。操作系统向这个进程发送一种叫 **信号** 的短消息。常见的：

| 信号 | 谁发出 | 默认后果 |
|------|--------|----------|
| SIGINT | Ctrl+C | 中断进程，Python 里常变成 KeyboardInterrupt |
| SIGTERM | `docker stop`、`kill pid` | 请求退出 |
| SIGKILL | `kill -9` | 立刻死，进程无法捕获 |

能捕获的是前两个。SIGKILL 无法捕获，所以第 41 条必须假设「有时来不及道别」。

### 本项目要改默认行为

默认 SIGINT 会变成 KeyboardInterrupt，`asyncio.run` 取消全部任务——第 3 条里那个糟糕局面。本项目改成：信号处理函数 **只做一件事**：把 `stop_event` 设为「已停机」。主循环每次转一圈都看这块牌子，看见了就不再 `XREADGROUP`，但当前 `handle_message` 继续。

这叫 **把信号变成协作式标志**，而不是在信号函数里直接关连接。信号函数应当极短：操作系统可能在很尴尬的时刻插入它。

### 为什么 Unix 和 Windows 要写两套

在 Linux/macOS 上，asyncio 提供 `loop.add_signal_handler(sig, callback)`。回调保证在事件循环线程执行，可以直接改 `stop_event`。

Windows 的事件循环不支持这套 API。退回 `signal.signal`。回调可能跑在别的线程。若在那个线程直接 `stop_event.set()`，和事件循环并发改同一块状态，理论上有竞态。所以用 `loop.call_soon_threadsafe(request_stop)`：请事件循环线程稍后执行 `request_stop`。这是「跨线程拜托」的标准做法。

### 时间线：你按了 Ctrl+C

```text
T0  内核投递 SIGINT
T1  request_stop()：若牌子还没竖上，set()
T2  Worker 可能正阻塞在 XREADGROUP 或 sleep
T3  阻塞结束（最多大约 WORKER_BLOCK_MS，默认 5 秒）
T4  循环顶部看到 stop_event，跳出，不再拉新任务
T5  若循环体内已经拿到一条消息，handle_message 仍会跑完
T6  清心跳、关 Redis、关数据库
```

从按键到进程消失，可能有几秒。这是故意的。`docker stop` 默认给大约 10 秒 SIGTERM，然后才 SIGKILL。那 10 秒就是留给这段收尾的。

### 常见误解

- 「信号处理函数里可以随便 await Redis。」不要。信号上下文极敏感，本项目故意只改 Event。
- 「Windows 上没 SIGTERM。」Docker 里仍可能发。本仓库两边都注册了。

### 在 VortexMQ 里

`app/worker/main.py` 的 `install_shutdown_signals`。读的时候注意 `sys.platform == "win32"` 分支。

### 停下来想

若 `WORKER_BLOCK_MS` 改成 60 秒，优雅停机的最坏等待会怎样？提示：看时间线 T3。

---

## 5. uuid / datetime / secrets / logging / argparse（入门）

五个标准库，放在一条里是因为它们都「小而处处在」，但彼此无关。请分开记。

### uuid：几乎不撞车的身份证

**自增整数** 1、2、3 很好记，但：

- 两台服务器同时插入，可能都想用 18。
- 把测试库的数据合到正式库，ID 会打架。
- 客人若能猜到下一个 ID 是 19，就可以去撞别人的任务号（还要配合第 49 条才真正危险，但少一个可预测性总更好）。

**UUID** 是 128 位随机（本项目用的版本）标识符，写成 `3fa85f64-5717-4562-b3fc-2c963f66afa6` 这种。两台机器各自生成，撞车概率低到可以忽略。代价是：不好记、占的空间比整数大、索引比连续整数略不友好。对本项目这些代价可接受。

任务、租户、工作流、Leader token 里的随机段，都建立在「不要靠人来保证唯一」上。

### datetime：必须带时区

世界上有「北京时间 8 点」和「伦敦时间 8 点」，它们不是同一瞬间。Python 有两种时间对象：

- **naive**：不带时区。你以为它是本地时间，同事以为它是 UTC。
- **aware**：带时区。可以正确换算。

naive 和 aware 比大小，Python 3 会抛异常。若你先把 tzinfo 剥掉再比，可能静默比错：延迟任务在未到期时被执行，或永远不执行。

本项目规定：**一律 UTC，一律 aware。** `utcnow()` 生成；`as_utc()` 把外来时间拧到 UTC。`execute_at` 从 JSON 进来时可能带 `Z`（表示 UTC），也可能带 `+08:00`，都先拧过来再存、再和「现在」比。

见 `app/core/clock.py`，一共十来行，建议读熟。

### secrets：不可预测的随机

`random` 模块用的是伪随机，种子若被猜到，后续数列可被推算。API Key 若可被推算，bcrypt 也救不了——攻击者直接拿钥匙来。

`secrets.token_urlsafe(32)` 向操作系统要密码学安全的随机字节，再编码成 URL 里能用的字符。前面加 `vxk_` 只是方便人眼认出「这是 VortexMQ 的钥匙」，不增加强度。

### logging：给未来的自己搜

`print` 会和真正的响应、进度条混在一起，没有级别，容器一多就找不到。`logging` 可以：

- 带时间、级别、模块名
- 按级别过滤（生产只看 WARNING 以上）
- 写成「任务已投递: task_id=… channel=…」这种能 grep 的行

本项目关键路径都带 `task_id=`。出了 DLQ，用这个 ID 把提交、执行、重试串起来。

### argparse：命令行的 Pydantic

`python -m app.cli create-tenant default --rotate` 里：

- `create-tenant` 是子命令
- `default` 是位置参数（租户名）
- `--rotate` 是开关

`argparse` 负责：没写对时打印帮助并退出。见 `app/cli.py` 的 `build_parser`。

### 停下来想

为什么 API Key 不用 uuid 当钥匙？UUID 也够随机。提示：uuid 的文本格式固定、可能被误当成 task_id；`vxk_` 前缀让日志和配置里更好认，也便于第 47 条做前缀截取。

---

## 6. asyncio.to_thread：把 CPU 密集活搬出事件循环（熟练）

### 问题：阿姨在心算

第 2 条里，阿姨之所以能服务很多桌，是因为「等面熟」不占脑力。若突然要她当场做一道很难的心算（bcrypt 就是故意很难的心算），整条队伍停住：HTTP 请求堆积，选主循环也不再续约，Leader 锁可能过期。

bcrypt 慢是安全特性，不是 bug（第 46 条）。所以不能「换一个快哈希就好了」。

### 做法

```python
matched = await asyncio.to_thread(verify_api_key, api_key, tenant.api_key_hash)
```

含义：请线程池里的某个工人算 bcrypt；协程在这里等结果；**事件循环线程去服务别人**。工人算完，结果回来，协程从下一行继续。

这和 `create_task` 不同：`create_task` 仍在事件循环里跑协程；`to_thread` 是真的换一条线程做 CPU 活。

### 什么该丢、什么不该丢

| 该丢到 to_thread | 不该丢 |
|------------------|--------|
| bcrypt、压缩、大 JSON 的纯 CPU 解析 | 已经是 async 的数据库驱动、redis.asyncio |
| 阻塞的第三方同步库 | 极短的纯计算（开销可能比计算本身大） |

把 `await session.execute(...)` 再包一层 to_thread 没有意义：asyncpg 已经不会堵住循环。

### 在 VortexMQ 里

目前主要就是 `app/crud/tenant.py` 这一处。每个带 API Key 的请求都会走它。压测时你会感觉到：鉴权比插库更「重」，这是故意的。

### 停下来想

若有人用错误 Key 狂轰 API，bcrypt 会不会成为武器？提示：慢哈希保护钥匙，也消耗 CPU。这是第 50 条之外另一类滥用。本项目没有做按 IP 限流，属于已知边界。

---

## 7. hostname-pid：给进程起不会撞的名字（入门）

### 为什么要名字

Redis 消费者组把「未确认的消息」记在 **消费者名** 下面（第 28 条）。选主锁的值也是「我是谁」的 token（第 33 条）。名字撞车等于两个人共用一个衣帽柜号。

### 组合方式

- **hostname**：容器或电脑的名字。不同机器通常不同。
- **pid**：同一台机器上操作系统保证当前活着的进程号不重复。进程退出后 pid 可能被复用，所以这不是永恒身份证，只是「这一次运行」的名字。
- Leader token 再加 `uuid4` 的一小段，避免 pid 复用造成「新进程以为自己还是旧 Leader」。

Worker 允许用环境变量 `WORKER_CONSUMER_NAME` 写死。只在你清楚「全局只有一个这个名字」时才用。Compose 默认不写，让它自动生成。

### 重启会发生什么

若消费者名固定，新进程启动会先看到旧 PEL 里的债，这是好事：第 27 条用 `XREADGROUP … 0` 排空自己的旧债。若每次重启名字都变，旧债会留在已死的名字下，直到空闲超过 30 秒被 `XAUTOCLAIM`。两种都行，本项目两种都覆盖了。

### 停下来想

两个 Worker 容器若被你手动设成同一个 `WORKER_CONSUMER_NAME`，最坏会出现什么？提示：PEL 归属互相覆盖。

---

## 8. `python -m` 模块入口（入门）

### 两种启动方式的差别

```bash
python app/worker/main.py     # 把这个文件当「顶层脚本」
python -m app.worker          # 把 app 当包，跑 worker 这个模块
```

第一种容易把 `import app.xxx` 搞乱：Python 不知道 `app` 包在哪，`sys.path` 取决于你在哪个目录敲命令。第二种明确说：请按包的方式加载，入口是 `app/worker/__main__.py`（一个模块当 `python -m` 目标时，解释器会找 `__main__.py`）。

`app/worker/__main__.py` 只有几行：调用 `main()`。真正逻辑在 `app/worker/main.py`。CLI 同理：`python -m app.cli` 走进 `app/cli.py` 的 `if __name__ == "__main__"`。

Docker 里 Worker 的 command 就是 `python -m app.worker`。API 不走这条，而是 `uvicorn app.main:app`：uvicorn 知道怎么从包路径加载 ASGI 应用（第 10 条）。

### `__name__ == "__main__"` 是什么

一个文件被别人 import 时，`__name__` 是模块名（如 `app.cli`）。直接当入口跑时，`__name__` 是 `"__main__"`。这个判断防止「我只是被导入，却把整个 Worker 循环跑起来」。

### 停下来想

为什么 `create-tenant` 不做成 HTTP 接口，而做成 CLI？提示：签发钥匙的人应该已经能进服务器；少一个能在网上撞的入口。

---

## 第一章小结

- 类型是给人看的标签；3.10 的 `|` 表示「或者」。
- 协程擅长「同时等待」，不擅长「同时拼命算」。
- 取消和停机必须协作，否则账本和铃铛会对不齐。
- 信号只竖牌子；bcrypt 请去线程池；进程用 hostname-pid 认领自己的夹子。

下一章把客人递进来的纸条拆开：[第二章 Web API](ch02-web.md)
