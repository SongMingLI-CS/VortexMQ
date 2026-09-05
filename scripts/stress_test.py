#!/usr/bin/env python3
"""
VortexMQ 高并发混合压测客户端。

设计目标：客户端本身不要成为瓶颈。
- asyncio + aiohttp 全异步发请求
- Semaphore 把同时进行的 HTTP 限制在 --concurrency
- TCPConnector.limit 与并发数对齐，避免把本机 ephemeral port / fd 打满

流量配比（模拟真实业务，同时点亮监控大盘）：
- 70% 即时任务：立刻 XADD 进 Stream，打满 Worker 消费
- 20% 延迟任务：5~45s 后到期，压 ZSet + Delay Dispatcher
- 10% 毒药任务：force_fail=true，走指数退避并最终进入 DLQ

用法：
    python scripts/stress_test.py --api-key <签发时打印的明文 Key>
    python scripts/stress_test.py --api-key KEY1 --api-key KEY2 --count 1000
    或设置环境变量 VORTEXMQ_API_KEYS=key1,key2
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

IMMEDIATE_TASK_TYPES = ("email.send", "order.sync", "report.export", "sms.notify")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="VortexMQ 混合流量压测")
    parser.add_argument("--count", type=int, default=10000, help="总任务数（默认 10000）")
    parser.add_argument("--concurrency", type=int, default=200, help="并发协程上限（默认 200）")
    parser.add_argument(
        "--url",
        default="http://localhost:8000/api/v1/tasks",
        help="接收任务 API 地址",
    )
    parser.add_argument("--timeout", type=float, default=30.0, help="单次请求超时秒数")
    parser.add_argument(
        "--api-key",
        action="append",
        dest="api_keys",
        default=None,
        help="租户 API Key，可重复传入以混合多租户流量。也可用环境变量 VORTEXMQ_API_KEYS",
    )
    return parser.parse_args()


def pick_traffic_kind() -> str:
    """按 70/20/10 抽一种流量。"""
    roll = random.random()
    if roll < 0.70:
        return "immediate"
    if roll < 0.90:
        return "delayed"
    return "poison"


def build_payload(kind: str, api_keys: tuple[str, ...]) -> tuple[dict[str, Any], str]:
    """
    构造请求体。

    immediate：无 execute_at，走 Stream 即时消费，用于打满 Worker、观察队列深度。
    delayed：execute_at 落在 5~45 秒后，验证时间轮 Dispatcher 的到期搬运吞吐。
    poison：payload.force_fail=true，Worker 必失败，观察退避重试与 DLQ 曲线。
    """
    api_key = random.choice(api_keys)
    if kind == "immediate":
        body: dict[str, Any] = {
            "task_type": random.choice(IMMEDIATE_TASK_TYPES),
            "payload": {"source": "stress", "kind": "immediate"},
            "priority": random.randint(0, 20),
        }
        return body, api_key

    if kind == "delayed":
        delay_seconds = random.randint(5, 45)
        execute_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        body = {
            "task_type": "delay.wakeup",
            "payload": {"source": "stress", "kind": "delayed", "delay_seconds": delay_seconds},
            "priority": random.randint(0, 20),
            "execute_at": execute_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }
        return body, api_key

    body = {
        "task_type": "chaos.poison",
        "payload": {"force_fail": True, "source": "stress", "kind": "poison"},
        "priority": 0,
    }
    return body, api_key


def render_bar(done: int, total: int, width: int = 28) -> str:
    if total <= 0:
        return "-" * width
    filled = int(width * done / total)
    return "#" * filled + "-" * (width - filled)


class Stats:
    __slots__ = ("ok", "err", "done")

    def __init__(self) -> None:
        self.ok = 0
        self.err = 0
        self.done = 0

    def mark(self, success: bool) -> None:
        # asyncio 单线程，整数自增无需锁
        self.done += 1
        if success:
            self.ok += 1
        else:
            self.err += 1


async def post_one(
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
    url: str,
    stats: Stats,
    api_keys: tuple[str, ...],
) -> None:
    kind = pick_traffic_kind()
    body, api_key = build_payload(kind, api_keys)
    headers = {"Content-Type": "application/json", "X-API-Key": api_key}
    async with semaphore:
        try:
            async with session.post(url, json=body, headers=headers) as resp:
                stats.mark(resp.status == 201)
                # 读完 body，连接才能回池，避免悬挂
                await resp.read()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            stats.mark(False)


async def report_progress(stats: Stats, total: int, started_at: float, stop: asyncio.Event) -> None:
    """后台刷新进度条和瞬时 TPS，避免在每个请求里 print 造成 IO 抖动。"""
    last_done = 0
    last_ts = started_at
    while not stop.is_set():
        await asyncio.sleep(0.2)
        now = time.monotonic()
        elapsed = max(now - started_at, 1e-6)
        window = max(now - last_ts, 1e-6)
        instant_tps = (stats.done - last_done) / window
        avg_tps = stats.done / elapsed
        bar = render_bar(stats.done, total)
        pct = 100.0 * stats.done / total if total else 100.0
        line = (
            f"\r[{bar}] {stats.done}/{total} {pct:5.1f}%  "
            f"TPS={instant_tps:7.1f}  avg={avg_tps:7.1f}  "
            f"ok={stats.ok} err={stats.err}   "
        )
        sys.stdout.write(line)
        sys.stdout.flush()
        last_done = stats.done
        last_ts = now


async def run_stress(args: argparse.Namespace) -> None:
    if args.count <= 0:
        raise SystemExit("--count 必须大于 0")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency 必须大于 0")

    env_keys = [
        item.strip()
        for item in os.environ.get("VORTEXMQ_API_KEYS", "").split(",")
        if item.strip()
    ]
    api_keys = tuple(args.api_keys or env_keys)
    if not api_keys:
        raise SystemExit(
            "未提供 API Key。先运行 python -m app.cli create-tenant default，"
            "再使用 --api-key 或环境变量 VORTEXMQ_API_KEYS"
        )

    concurrency = min(args.concurrency, args.count)
    stats = Stats()
    stop = asyncio.Event()
    semaphore = asyncio.Semaphore(concurrency)
    timeout = aiohttp.ClientTimeout(total=args.timeout)
    connector = aiohttp.TCPConnector(
        limit=concurrency,
        limit_per_host=concurrency,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
    )

    print(
        f"VortexMQ stress  url={args.url}  count={args.count}  "
        f"concurrency={concurrency}  keys={len(api_keys)}  "
        f"mix=70% immediate / 20% delayed / 10% poison"
    )

    started_at = time.monotonic()
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        reporter = asyncio.create_task(report_progress(stats, args.count, started_at, stop))
        try:
            await asyncio.gather(
                *(post_one(session, semaphore, args.url, stats, api_keys) for _ in range(args.count))
            )
        finally:
            stop.set()
            await reporter

    elapsed = max(time.monotonic() - started_at, 1e-6)
    print()
    print(
        f"完成  ok={stats.ok}  err={stats.err}  "
        f"elapsed={elapsed:.2f}s  avg_tps={stats.done / elapsed:.1f}"
    )
    if stats.err:
        print("存在失败请求：检查 API 是否就绪、三个压测租户是否已种子写入。")


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(run_stress(args))
    except KeyboardInterrupt:
        print("\n已中断压测")


if __name__ == "__main__":
    main()
