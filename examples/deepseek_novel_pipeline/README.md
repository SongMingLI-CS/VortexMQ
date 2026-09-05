# DeepSeek 网文生成流水线 Showcase

用 VortexMQ 的 Python SDK 与 `ai.deepseek.chat` Handler 编排一个两节点 DAG：

```
Outline_Agent ──(XCom 大纲)──> Chapter1_Agent
```

- **Outline_Agent**：请求 `ai.deepseek.chat` 生成《赛博修仙传》分章大纲。
- **Chapter1_Agent**：依赖大纲节点，通过 XCom 把大纲注入提示词模板（`{output}` 占位符），撰写第一章。

## 前置条件

1. 启动 VortexMQ 栈（PostgreSQL + Redis + API + Worker）：

   ```bash
   docker compose up -d --build
   # 或本地分别启动: uvicorn app.main:app & python -m app.worker
   ```

2. 签发租户 API Key（明文只打印一次）：

   ```bash
   python -m app.cli create-tenant demo
   ```

3. （可选）配置真实 DeepSeek：在 `.env` 或环境变量中设置 `DEEPSEEK_API_KEY`。

## 运行

脚本依赖 `sdk/python` 下的 `vortexmq_client`，需先把它加入 `PYTHONPATH`（或日后
`pip install` SDK 包后直接使用）：

```bash
# Windows PowerShell
$env:PYTHONPATH = "sdk/python"
python examples/deepseek_novel_pipeline/main.py --api-key <your-api-key>

# Linux / macOS
PYTHONPATH=sdk/python python examples/deepseek_novel_pipeline/main.py --api-key <your-api-key>

# 可选: --base-url http://127.0.0.1:8000
```

- **未配置 `DEEPSEEK_API_KEY`**：Worker 自动走 **Mock 路径**（`await asyncio.sleep(1)`
  后返回模拟网文段落），保证流水线随时可测、可演示。
- **配置了 `DEEPSEEK_API_KEY`**：走真实 DeepSeek `chat/completions` 接口。

脚本会依次打印「大纲生成中…」「第一章生成中…」的轮询状态，并最终输出大纲与第一章正文。
