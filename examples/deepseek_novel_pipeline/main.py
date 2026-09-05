"""《赛博修仙传》多智能体网文生成流水线 Showcase。

用 VortexMQ Python SDK 编排一个两节点 DAG：

    Outline_Agent ──(XCom 大纲)──> Chapter1_Agent

两个节点都使用 ``ai.deepseek.chat`` Handler。Outline 的结果会经 XCom 注入
第一章的提示词模板（``{output}`` 占位符），演示多节点上下文传递。

运行：
    python examples/deepseek_novel_pipeline/main.py --api-key <your-api-key>
    # 可选: --base-url http://127.0.0.1:8000

未配置 ``DEEPSEEK_API_KEY`` 时，Worker 自动走 Mock 路径（sleep 1s 返回模拟
文本），无需真实 LLM 也能完整跑通。
"""

from __future__ import annotations

import argparse
import os
import time
from typing import Any

import httpx

from vortexmq_client import VortexMQClient, Workflow

OUTLINE_PROMPT = (
    "你是一位资深网文作者。请为修仙题材小说《赛博修仙传》撰写一份分章大纲，"
    "必须包含：世界观设定、主角人设、前三章的情节走向。"
)

CHAPTER1_PROMPT = (
    "请根据以下大纲撰写《赛博修仙传》第一章正文（800 字左右，节奏明快）：\n\n"
    "{output}"
)

WIDTH = 72


def _divider(char: str = "-") -> None:
    print(char * WIDTH)


def _banner() -> None:
    _divider("=")
    print("《赛博修仙传》多智能体网文生成流水线".center(WIDTH))
    _divider("=")


def _fetch_result(
    client: VortexMQClient,
    task_id: str,
    label: str,
    timeout_seconds: int = 180,
) -> dict[str, Any]:
    """轮询任务结果，直到 SUCCESS；失败 / 取消 / 超时则打印错误并退出。"""
    deadline = time.monotonic() + timeout_seconds
    while True:
        if time.monotonic() > deadline:
            _divider()
            print(f"[{label}] 等待超过 {timeout_seconds}s 仍未完成，请检查 Worker 是否在线。")
            raise SystemExit(1)

        try:
            result = client.get_task_result(task_id)
        except httpx.HTTPStatusError as exc:
            _divider()
            print(f"[{label}] 任务失败/取消 (HTTP {exc.response.status_code}):")
            print(exc.response.text)
            raise SystemExit(1) from exc

        status = result.get("status")
        if status == "SUCCESS":
            return result.get("result_data") or {}
        print(f"[{label}] 生成中... 状态={status}")
        time.sleep(2)


def main() -> None:
    parser = argparse.ArgumentParser(description="VortexMQ 多智能体网文生成 Showcase")
    parser.add_argument(
        "--base-url",
        default=os.getenv("VORTEXMQ_BASE_URL", "http://127.0.0.1:8000"),
        help="VortexMQ API 地址",
    )
    parser.add_argument(
        "--api-key",
        default=os.getenv("VORTEXMQ_API_KEY", ""),
        help="租户 API Key（或设置 VORTEXMQ_API_KEY）",
    )
    args = parser.parse_args()

    if not args.api_key:
        parser.error("缺少 API Key：请用 --api-key 或设置 VORTEXMQ_API_KEY")

    _banner()
    client = VortexMQClient(args.base_url, args.api_key)

    # 构建 DAG：大纲 -> 第一章（第一章依赖大纲，经 XCom 注入 {output}）
    wf = Workflow()
    outline = wf.add_node(
        "Outline_Agent", "ai.deepseek.chat", {"user_prompt_template": OUTLINE_PROMPT}
    )
    wf.add_node(
        "Chapter1_Agent",
        "ai.deepseek.chat",
        {"user_prompt_template": CHAPTER1_PROMPT},
        depends_on=[outline],
    )

    print("提交工作流: Outline_Agent -> Chapter1_Agent")
    task_ids = client.submit_workflow(wf)
    outline_id, chapter1_id = task_ids[0], task_ids[1]
    print(f"工作流已受理: 大纲任务={outline_id}")
    print(f"              第一章任务={chapter1_id}")
    _divider()

    # 大纲节点
    outline_data = _fetch_result(client, outline_id, "Outline_Agent")
    _divider()
    print("[Outline_Agent] 大纲产出：")
    print(outline_data.get("output", "<空>"))
    _divider()

    # 第一章节点（依赖大纲，大纲 SUCCESS 后 Worker 才唤醒它）
    chapter_data = _fetch_result(client, chapter1_id, "Chapter1_Agent")
    _divider()
    print("[Chapter1_Agent] 第一章产出：")
    print(chapter_data.get("output", "<空>"))
    _divider("=")
    print("流水线完成".center(WIDTH))
    _divider("=")


if __name__ == "__main__":
    main()
