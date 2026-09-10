# VortexMQ Python Client SDK

调用 VortexMQ 租户 API（`X-API-Key` 鉴权）的轻量客户端：

- `VortexMQClient`：同步客户端（`httpx.Client`）；
- `AsyncVortexMQClient`：异步客户端（`httpx.AsyncClient`）；
- `Workflow` / `WorkflowNode`：Fluent DAG 构建器，负责把依赖关系翻译成
  `POST /api/v1/workflows` 的 JSON；图的合法性（环、缺边、重复 node_id）仍由服务端校验。

## 安装

```bash
# 仓库内使用（开发）
pip install -e sdk/python

# 或不安装，直接把 sdk/python 放进 PYTHONPATH
export PYTHONPATH=sdk/python        # Windows PowerShell: $env:PYTHONPATH = "sdk/python"
```

## 用法

```python
from datetime import datetime, timedelta, timezone

from vortexmq_client import VortexMQClient, Workflow

client = VortexMQClient("http://127.0.0.1:8000", "<your-api-key>")

# 即时任务
task_id = client.submit_task("demo.echo", {"hello": "world"})

# 延迟任务：execute_at 未到时只进延迟 ZSet
task_id = client.submit_task(
    "demo.echo",
    {"hello": "later"},
    execute_at=datetime.now(timezone.utc) + timedelta(hours=1),
)

# 轮询结果：SUCCESS(200) 带 result_data；处理中(202) 带 status；失败(400)/不存在(404) 抛 HTTPStatusError
print(client.get_task_result(task_id))

# DAG：node_b 依赖 node_a，服务端保证拓扑序与 XCom 注入
wf = Workflow()
node_a = wf.add_node("node_a", "etl.extract", {"source": "db"})
wf.add_node("node_b", "etl.transform", {}, depends_on=[node_a])
task_ids = client.submit_workflow(wf)
```

异步调用把同一组方法换成 `await`，并用 `async with` 管理连接池：

```python
from vortexmq_client import AsyncVortexMQClient

async with AsyncVortexMQClient("http://127.0.0.1:8000", api_key) as client:
    task_id = await client.submit_task("demo.echo", {"hello": "async"})
```
