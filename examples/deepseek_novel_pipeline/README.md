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

3. 配置模型调用方式（二选一）：

   - **真实调用**（推荐）：在 `.env` 或环境变量中设置 `DEEPSEEK_API_KEY`；
   - **离线演示**：显式设置 `AI_MOCK_ENABLED=true`（无需 Key，返回模拟文本）。

   两者都没有时 `ai.deepseek.chat` 会显式失败（`AIProviderError`），任务进入
   重试 / DLQ —— 这是刻意设计，避免把假文本当成模型输出。


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

- **配置了 `DEEPSEEK_API_KEY`**：走真实 DeepSeek `chat/completions` 接口。
- **未配置 Key 但设置了 `AI_MOCK_ENABLED=true`**：返回模拟网文段落，无需外部
  依赖也能完整演示 DAG 与 XCom 注入。
- **两者都没有**：任务显式失败并进入重试 / DLQ，绝不会把假文本当作模型输出。

脚本会依次打印「大纲生成中…」「第一章生成中…」的轮询状态，并最终输出大纲与第一章正文。
