# 第二章 Web API

> 接 [第一章](ch01-python.md)。本章解决：客人的 HTTP 纸条如何变成「已校验的租户 + 合法的任务形状」，以及为什么状态码不能随便用。
>
> 编号 9–15。上一章：[Python](ch01-python.md) · 下一章：[PostgreSQL](ch03-postgres.md)

建议先复习 [第零课](../KNOWLEDGE.zh-CN.md) 的 0.3、0.4：HTTP 纸条的结构、JSON 是什么。

---

## 9. FastAPI 依赖注入（熟练）

### 从你会的东西讲起

学校里每间教室都自己查学生证，一定会发生：某间教室忘了查，或者每间教室查的规则不一样。更好的做法是 **进楼先过门卫**，教室只接收「已经登记过的人」。

Web 接口也一样。每个函数都自己读 `X-API-Key`、自己开数据库，复制粘贴几次后，新接口就会漏掉鉴权。

**依赖注入** 的意思是：接口函数在参数上声明「我需要什么」，框架在调用你之前，先去准备这些东西，准备失败就不要调用你。

### 一次 POST 实际的调用栈

客人打到 `POST /api/v1/tasks`。FastAPI 大致按这个顺序工作：

```text
1. 找到 create_task 这个处理函数
2. 看到参数 db: AsyncSession = Depends(get_db)
   → 调用 get_db()，打开一个数据库 Session
3. 看到参数 tenant: Tenant = Depends(get_current_tenant)
   → get_current_tenant 自己也 Depends(get_db)，复用同一次请求的 Session
   → 读 Header「X-API-Key」
   → 查库、bcrypt（第 46、47 条）
   → 失败：直接 401，create_task 根本不会运行
4. 把 JSON 身体填进 TaskCreateRequest（第 11 条）
   → 失败：422 或 413，同样不会运行业务
5. 这才调用 create_task(payload, db, tenant)
```

注意第 3 步：`create_task` 的签名里 **没有** `tenant_id` 这个可由客人填写的字段。租户对象是门卫塞进来的。这是第 48 条在框架层的落地。

### 依赖可以嵌套

`get_current_tenant` 内部再次 `Depends(get_db)`。FastAPI 保证同一请求里 `get_db` 只跑一套：不会打开两个 Session 再对不上事务。请求结束，`get_db` 的 `async with` 退出，Session 关闭。即使业务函数漏了 close，连接也会还回去。这和第三章第 24 条的连接池连在一起。

### 如果不用依赖注入

你仍能把鉴权写成一个普通函数，每个接口第一行手动调用。能工作。缺点是：新人加接口时会忘；测试时很难说「给我一个假租户」——依赖注入可以用框架的 override 在测试里替换 `get_current_tenant`。本仓库还没有完整测试套件，但门已经按这个形状开好。

### 常见误解

- 「Depends 是全局单例。」不是。默认每个请求跑一次。
- 「Header 写在 JSON 里也行。」本项目的 `Header(..., alias="X-API-Key")` 只认头。身体里即使有 `X-API-Key` 字段也没用。

### 在 VortexMQ 里

```9:23:app/api/deps.py
async def get_current_tenant(
    x_api_key: str = Header(..., alias="X-API-Key", description="租户 API Key"),
    db: AsyncSession = Depends(get_db),
) -> Tenant:
    ...
```

任务接口和工作流接口都 Depends 它。见 `app/api/v1/endpoints/tasks.py`、`workflows.py`。

### 停下来想

若有人做一个「内部管理员接口」忘了 Depends `get_current_tenant`，最坏泄露什么？提示：本项目没有另做登录态，忘了鉴权等于对全网开放。

---

## 10. ASGI 与 uvicorn（入门）

### 三层饼

| 层 | 是谁 | 干什么 |
|----|------|--------|
| 应用 | FastAPI 的 `app` 对象 | 路由、校验、你的业务函数 |
| 服务器 | uvicorn | 听端口、收 TCP 字节、切成 HTTP 请求、再把响应写回 |
| 协议 | ASGI | 应用和服务器之间的插座标准 |

没有 uvicorn（或同等 ASGI 服务器），FastAPI 只是一个 Python 对象，不会有人来敲门。没有 FastAPI，uvicorn 不知道 `/api/v1/tasks` 该叫谁。

WSGI 是更老的同步插座（Flask 传统栈）。ASGI 能挂协程，和第一章的 asyncio 对齐。你看到 `uvicorn app.main:app` 里的冒号：左边是模块路径 `app.main`，右边是那个模块里的变量名 `app`。

### `host 0.0.0.0`

容器有自己的网卡。若 uvicorn 听 `127.0.0.1`，只有容器内部的「自己」能连上自己，你在笔记本浏览器里访问映射出来的 8000 会失败。`0.0.0.0` 表示「所有网卡」。Docker 端口映射才能把外面的敲门转进来。

这不等于把服务暴露到公网。还要看你笔记本的防火墙、以及 Compose 有没有把端口映射到宿主机。本项目 Compose 映射了 8000，所以本地能访问。真正上线时前面通常还有一层反向代理，那是第九章范围之外的事。

### reload

`app/main.py` 底部 `uvicorn.run(..., reload=True)` 只适合本地改代码自动重启。Docker 里 command 没有开 reload：生产热重载会丢请求、状态混乱。

### 停下来想

Worker 为什么不用 uvicorn 接待业务 HTTP？提示：Worker 的 8001 只给 Prometheus 抓指标，用的是 prometheus_client 自己的小 HTTP 服务，不加载 FastAPI 路由。

---

## 11. Pydantic v2 请求校验（熟练）

### 从你会的东西讲起

网线对面是陌生人。JSON 里可能：

- 缺 `task_type`
- `priority` 是 `"很高"` 这种字符串
- `priority` 是 -1 或 99999
- `payload` 是 20MB 的小说
- 多一个你不认识的字段

若直接塞进数据库，轻则脏数据，重则进程内存爆掉。

**Pydantic** 用一个类描述合法形状。FastAPI 在依赖注入之后、业务函数之前，把 JSON 填进这个类。填失败，业务函数不运行。

### 本项目的合同

`TaskCreateRequest` 规定：

| 字段 | 规则 |
|------|------|
| task_type | 必填，长度 1–128 |
| payload | 默认空对象 `{}`；另有 Mixin 检查 |
| priority | 默认 0，必须 0–100 的整数 |
| execute_at | 可选。有则必须是能解析的时间 |

`PayloadGuardMixin` 再做两件事（第 50、51 条详细讲）：顶层禁止 `_vortex_sys`；序列化后不超过 256KiB。

### 校验失败是 422 还是 413

普通形状错误（缺字段、类型不对）→ **422**，身体里是一列错误细节，方便调用方改请求。

体积超限走同一套 validator，但 `app/main.py` 的异常处理器认出特殊标记 `payload_too_large`，改成 **413**。原因：413 在 HTTP 里专门表示「你给的东西太大」，网关、客户端、监控会按「体积问题」而不是「字段写错」来分类。

### extra 字段怎么办

Pydantic v2 默认忽略请求里多出来的未知字段（本项目 Schema 没有开 forbid）。调用方多写 `"foo": 1` 不会 422，但也不会存进任务行——只有 Schema 里的字段会进入 `create_task`。这是一种温和策略：向前兼容，不把拼写错误当致命。拼错 `task_type` 成 `tasktype` 则会 422，因为必填字段缺失。

### 常见误解

- 「数据库约束已经够了，不必 Pydantic。」数据库在更里层，报错更难翻译给客人。能在门口用 round-trip 的 JSON 语言拒绝，就不要让 Postgres 冒出一串 `CheckViolation`。
- 「校验等于安全。」校验形状，不校验「这个人是不是在发垃圾邮件」。安全还要靠第 46–52 条。

### 在 VortexMQ 里

`app/schemas/task.py`、`app/schemas/workflow.py`、`app/core/payload.py`。工作流每个节点都复用同一套 payload 守卫。

### 停下来想

为什么 `priority` 要有上限 100，而不是越大越优先一直到百万？提示：防止有人用 2^31-1 抢占全部调度；也给「高优车道阈值 50」留出刻度。

---

## 12. pydantic-settings：配置来自环境（入门）

### 同一份代码，三套地址

| 场景 | PostgreSQL 在哪 |
|------|-----------------|
| 你笔记本直接跑 | `localhost:5432` |
| Docker Compose | 服务名 `postgres:5432` |
| 以后的云上机器 | 云厂商给的内网地址 |

若把 `localhost` 写死在源码里，镜像一封，Compose 里的 API 会去连容器自己的 5432——那里没有 Postgres。

**环境变量** 是进程启动时操作系统塞进来的一堆「名字=值」。Docker 的 `environment:`、本地的 `.env` 文件、云平台的密钥服务，最后都变成环境变量。`pydantic-settings` 的 `BaseSettings` 按字段名去读它们。

`Settings` 里写了默认值，是为了「什么都不配也能在开发机试」。Compose 通过 `x-app-env` 覆盖成容器内地址。`DEBUG` 在 Compose 里是 `false`，少打 SQL 流水。

### `.env` 不要提交密钥

`.env.example` 是模板，可以进 git。真正的 `.env` 在 `.gitignore` 里。API Key 连 `.env` 都不进了——签发命令打印一次（第 46 条）。

`extra="ignore"` 表示：环境里多出来的、Settings 不认识的变量，忽略，不要启动失败。这样你在 shell 里残留的无关变量不会绊倒应用。

### 停下来想

`DATABASE_URL` 里带着 `postgres:postgres` 这种用户名密码。本地 Compose 能接受；公开仓库里若有人把生产密码写进 compose 文件并 push，会发生什么？提示：配置与密钥仍可能被误提交，12-factor 只是把问题从「写死在代码」缩小到「写在配置」，不是自动安全。

---

## 13. HTTP 语义状态码（熟练）

### 为什么要三位数，不能全 200

调用方是机器。机器写代码的方式是：

```text
若 201：把 task_id 存下来
若 202：睡 1 秒再 GET
若 401：去找人换钥匙，不要重试同一把
若 429/5xx：过一会儿再试
若 400：请求本身有问题，再试一万次也没用
```

若你永远返回 200，再在 JSON 里写 `"ok": false`，每家调用方都要发明自己的解析规则，有人会漏看，有人会把失败当成功。

### 本项目用到的码，逐个讲清

**200 OK。** GET 结果且任务已 SUCCESS。身体里有 `result_data`。这是「你要的东西在这里」。

**201 Created。** POST 受理成功。任务未必执行完，甚至 Redis 都可能还没按到铃（第 37 条），但账本上已经有行。201 强调「新建了一个资源」，对应那个 `task_id`。

**202 Accepted。** GET 结果时任务仍在 PENDING / RUNNING / WAITING。不是错误。自动化应当稍后重试 GET，而不是把 202 当失败告警。

**400 Bad Request。** 工作流图不合法（环、缺节点）；或结果查询时任务已经 DLQ / CANCELED。语义是「按你这个请求，我无法按成功路径给你结果」。

**401 Unauthorized。** 钥匙无效。注意 HTTP 历史命名有点别扭：401 其实更接近「未认证」，403 才是「认证了但没权限」。本项目没有登录态，无效 Key 一律 401。跨租户查任务走 404 不走 403，见第 49 条。

**404 Not Found。** 没有这行，或不是你的行。

**413 Payload Too Large。** 见第 11、50 条。

**422 Unprocessable Entity。** JSON 形状过不了 Pydantic。

### 不要混淆的两对

| 看起来像 | 其实 |
|----------|------|
| Redis 失败仍 201 | 任务已在账本上，补偿后台做。对调用方「已受理」为真 |
| 查询 202 | 受理早就成功了，只是还没做完。不要再 POST 一遍，否则会多一行任务 |

### 在 VortexMQ 里

`app/api/v1/endpoints/tasks.py` 把状态集合分成 `_IN_FLIGHT` 和 `_FAILED`。读这个文件时对照上面这张表。

### 停下来想

压测脚本若把 202 当成失败计入错误率，大盘会怎样？提示：查询接口和提交接口不是同一个。压测脚本目前打的是提交 URL，看的是 201。

---

## 14. OpenAPI 与 `/docs`（入门）

### 说明书可以自动生成

你在 Schema 和路由上写的类型、`summary`、`description`、`examples`，FastAPI 会汇总成一份 **OpenAPI** JSON。`/docs` 是这份 JSON 的可视化：每个接口一张卡片，能填参数、点 Execute。

这对大一非常有用：还不会 curl 时，先在浏览器里试。注意：

1. 点右上角 Authorize 或每个接口的 Header 栏，填 `X-API-Key`。不填会 401。
2. `/docs` 在浏览器里跑，请求仍打到同一进程的 8000。服务没起来时页面可能打开但 Execute 失败。
3. `include_in_schema=False` 的接口不会出现在文档里。本项目的 `/metrics` 就是这样：给 Prometheus 看的，不是给人点的。

`/redoc` 是另一套更像说明书的视图，同样自动生成。本讲义不展开。

### 停下来想

为什么不把 `create-tenant` 放进 `/docs`？与第 8 条同一答案：签发钥匙不应当是公开 HTTP。

---

## 15. Schema 与 ORM 分层（熟练）

### 两套形状，不是偷懒重复

| | Schema `app/schemas/` | ORM `app/models/` |
|--|----------------------|-------------------|
| 给谁看 | 网线对面的调用方 | 数据库 |
| 例子 | TaskCreateRequest 没有 retry_count | TaskRecord 有 retry_count、error_msg、api 不该随便改的列 |
| 校验 | 长度、范围、保留键 | 列类型、非空、外键 |
| 变的原因 | 「我们想让调用方多传一个字段」 | 「我们要加一列给 Sweeper 用」 |

若让 FastAPI 直接 `response_model=TaskRecord`，默认可能把 `error_msg`、内部 JSON 结构、甚至将来误加的敏感列一并序列化出去。`TaskCreateResponse` 白名单了：`task_id`、`tenant_id`、`status`、`task_type`、`priority`、`execute_at`、`created_at`。`from_attributes=True` 表示：可以从 ORM 对象读这些属性来填充 Schema，而不是把 ORM 整颗扔出去。

### 分层在目录上的体现

```text
endpoints/   只跟 Schema 和 service 说话，不写 SQL
schemas/     请求/响应合同
services/    编排：先落库再按铃
crud/        SQL
models/      表
```

新人加字段时问自己：这是给客人看的，还是只给内部状态机用的？答案决定改 Schema 还是只改 Model。

### 常见误解

- 「两套类违反 DRY。」重复的是名字，不是职责。合并它们会让「对外合同」和「存储细节」绑死，一次迁库就得改 API。
- 「CRUD 层应当返回 Schema。」本项目 crud 返回 ORM，由 endpoint 再 `model_validate`。也可以反过来，但不要两头各转一次搞丢字段。

### 停下来想

`retry_count` 为什么不让 POST 传入？提示：否则调用方可以提交「我已经失败 2 次」来操纵何时进 DLQ。

---

## 第二章小结

- 鉴权是门卫，用 Depends，不要让每个柜台自己查证。
- uvicorn 听 0.0.0.0，FastAPI 才从网线对面可达。
- 不信任 JSON：Pydantic 在门口拦形状；状态码给机器当分支条件。
- Schema 是成绩单，ORM 是学籍档案，不要合成一张纸。

下一章进入账本：[第三章 PostgreSQL 与 ORM](ch03-postgres.md)
