"""
VortexMQ 管理命令。

签发租户密钥（明文只打印一次）：
    python -m app.cli create-tenant default
    python -m app.cli create-tenant alpha --rotate
"""

from __future__ import annotations

import argparse
import asyncio
import sys

from app.core.bootstrap import TenantAlreadyExistsError, create_tenant
from app.core.database import AsyncSessionLocal, init_db


async def _create_tenant(name: str, rotate: bool) -> int:
    await init_db()
    async with AsyncSessionLocal() as session:
        try:
            tenant, plaintext = await create_tenant(session, name, rotate=rotate)
        except TenantAlreadyExistsError as exc:
            print(str(exc), file=sys.stderr)
            return 1

    action = "已轮换密钥" if rotate else "已创建"
    print(f"租户{action}: name={tenant.name} id={tenant.id}")
    print("API Key（仅显示一次，请立即保存，数据库只存 bcrypt 哈希）：")
    print(plaintext)
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="VortexMQ 管理命令")
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create-tenant", help="创建租户并打印一次性 API Key")
    create.add_argument("name", help="租户名称，全局唯一")
    create.add_argument(
        "--rotate",
        action="store_true",
        help="若租户已存在则轮换密钥，旧 Key 立即失效",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "create-tenant":
        raise SystemExit(asyncio.run(_create_tenant(args.name, args.rotate)))
    parser.error(f"未知命令: {args.command}")


if __name__ == "__main__":
    main()
