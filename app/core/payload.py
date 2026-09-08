"""任务 payload 边界：系统保留命名空间与体积上限。"""

from __future__ import annotations

import json
from typing import Any

from pydantic import field_validator

# Worker / DAG 注入专用，用户 payload 顶层禁止出现。
VORTEX_SYS_KEY = "_vortex_sys"
VORTEX_UPSTREAM_RESULTS_KEY = "upstream_results"

# 防御 OOM 打爆进程：单条 payload JSON 上限 256KiB。
MAX_PAYLOAD_BYTES = 256 * 1024
PAYLOAD_TOO_LARGE = "payload_too_large"

# 全局 HTTP 请求体上限：即使没有 Content-Length（chunked）也按接收字节数限流，
# 在 Pydantic 解析之前拦截，防止超大 body 耗尽内存。远大于单字段 256KiB 上限。
MAX_HTTP_BODY_BYTES = 2 * 1024 * 1024

# Handler 返回值（result_data）上限：防止 JSONB 行无界膨胀。超限按任务失败处理，
# 由现有退避 / DLQ 管道显式暴露给调用方，而不是静默截断业务数据。
MAX_RESULT_DATA_BYTES = 1024 * 1024

# 注入下游 payload 的 XCom（_vortex_sys.upstream_results）序列化体积预算：
# 单条 result_data 已有上限，但扇入很大的节点仍可能撑爆 JSONB。超预算时丢弃
# 超出的上游结果并告警（worker 日志），保证系统边界优先于尽善尽美的 XCom。
MAX_XCOM_INJECT_BYTES = 256 * 1024


def payload_size_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def validate_user_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """拒绝系统保留键，并限制序列化体积。"""
    if VORTEX_SYS_KEY in payload:
        raise ValueError("payload 禁止包含保留键 _vortex_sys")
    # 防御 OOM 打爆进程
    if payload_size_bytes(payload) > MAX_PAYLOAD_BYTES:
        raise ValueError(PAYLOAD_TOO_LARGE)
    return payload


def ensure_result_data_within_limit(result_data: dict[str, Any]) -> None:
    """Handler 返回值体积闸门：超限抛错，交由失败 / DLQ 管道显式接管。"""
    size = payload_size_bytes(result_data)
    if size > MAX_RESULT_DATA_BYTES:
        raise ValueError(
            f"result_data 超过上限 {MAX_RESULT_DATA_BYTES} 字节（实际 {size}）"
        )


class PayloadGuardMixin:
    """给 Task / Workflow 节点的 payload 字段复用同一套校验。"""

    @field_validator("payload")
    @classmethod
    def guard_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_user_payload(value)
