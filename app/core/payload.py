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


class PayloadGuardMixin:
    """给 Task / Workflow 节点的 payload 字段复用同一套校验。"""

    @field_validator("payload")
    @classmethod
    def guard_payload(cls, value: dict[str, Any]) -> dict[str, Any]:
        return validate_user_payload(value)
