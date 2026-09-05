from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from pydantic import ValidationError
from sqlalchemy.engine import make_url

from etf_sentinel.config import UnsafeConfigurationError, get_settings


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etf-sentinel")
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init-db", help="创建本地表；生产环境应使用 Alembic")
    subparsers.add_parser("demo", help="运行确定性 Demo 端到端流水线")
    subparsers.add_parser("serve", help="使用经安全门禁校验的地址启动 Web")
    subparsers.add_parser("check-config", help="验证第一阶段安全配置")
    reset = subparsers.add_parser("reset-demo", help="仅清空当前工作区的本地 Demo 库")
    reset.add_argument(
        "--confirm-delete-local-demo",
        action="store_true",
        help="确认删除可丢弃的工作区 SQLite Demo 数据",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        settings = get_settings()
        if args.command == "check-config":
            print(
                json.dumps({"ok": True, "trading_mode": settings.trading_mode}, ensure_ascii=False)
            )
            return
        if args.command == "serve":
            import uvicorn

            uvicorn.run(
                "etf_sentinel.main:app",
                host=settings.app_host,
                port=settings.app_port,
            )
            return
        from etf_sentinel.database import Base, SessionLocal, engine

        if args.command == "init-db":
            Base.metadata.create_all(engine)
            print("数据库表已就绪。")
            return
        if args.command == "reset-demo":
            if settings.app_env != "demo":
                raise UnsafeConfigurationError("reset-demo 仅允许 APP_ENV=demo。")
            if not args.confirm_delete_local_demo:
                raise UnsafeConfigurationError(
                    "reset-demo 需要 --confirm-delete-local-demo 显式确认。"
                )
            database_url = make_url(settings.database_url)
            expected = (Path.cwd() / "var" / "etf_sentinel.db").resolve()
            actual = Path(database_url.database or "").resolve()
            if not database_url.drivername.startswith("sqlite") or actual != expected:
                raise UnsafeConfigurationError(
                    "reset-demo 只允许当前工作区 var/etf_sentinel.db；"
                    "拒绝删除其他 SQLite 路径或服务器数据库。"
                )
            Base.metadata.drop_all(engine)
            Base.metadata.create_all(engine)
            print(f"Demo 数据库已清空并重建：{actual}；此操作不可恢复。")
            return
        from etf_sentinel.services.pipeline import run_demo_pipeline

        Base.metadata.create_all(engine)
        with SessionLocal() as session:
            result = run_demo_pipeline(session, settings)
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, default=str))
    except (UnsafeConfigurationError, ValidationError) as exc:
        print(f"配置被安全门禁拒绝：{exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
