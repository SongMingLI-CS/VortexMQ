"""
凭证与载荷安全原语。

API Key 只以 bcrypt 哈希落库；明文仅在签发时打印一次。
bcrypt 带盐，不能用哈希做 SQL 等值查询，因此另存 api_key_prefix 做定位。
"""

from __future__ import annotations

import secrets

from passlib.context import CryptContext

# bcrypt 哈希约 60 字节；列宽 128 预留算法升级。
API_KEY_PREFIX_LEN = 24
API_KEY_HASH_MAX_LEN = 128

_pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def generate_api_key() -> str:
    """生成带前缀的随机 API Key。vxk_ 便于识别，后半段足够长以保持前缀唯一。"""
    return "vxk_" + secrets.token_urlsafe(32)


def api_key_prefix(plain: str) -> str:
    """从明文截取查找前缀；不能单独当凭证。"""
    return plain[:API_KEY_PREFIX_LEN]


def hash_api_key(plain: str) -> str:
    """bcrypt 哈希。禁止把返回值当查找键。"""
    return _pwd_context.hash(plain)


def verify_api_key(plain: str, hashed: str) -> bool:
    """恒定时间校验明文与哈希。格式损坏时视为不匹配。"""
    try:
        return _pwd_context.verify(plain, hashed)
    except (ValueError, TypeError):
        return False
