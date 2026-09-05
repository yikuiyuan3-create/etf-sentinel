#!/usr/bin/env python3
"""Prepare a private Demo copy; start it only after explicit --execute.

This launcher has no live-data, broker, order, reset, delete, or stop interface.
It never uses the installed plugin directory as an application workspace.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import stat
import subprocess
import sys
from pathlib import Path
from typing import Any

PLUGIN_ROOT = Path(__file__).resolve().parents[1]
RUNTIME_ROOT = PLUGIN_ROOT / "runtime"
MARKER = ".etf-sentinel-plugin-workspace.json"
REQUIRED_FILES = (
    "docker-compose.yml",
    "Dockerfile",
    "pyproject.toml",
    "uv.lock",
    "README.md",
    "alembic.ini",
    "alembic/env.py",
    "scripts/docker-entrypoint.sh",
    "src/etf_sentinel/__init__.py",
    "src/etf_sentinel/config.py",
    "src/etf_sentinel/main.py",
    "src/etf_sentinel/tasks.py",
)
DISABLED_FLAGS = (
    "ENABLE_PUBLIC_SERVICE",
    "ENABLE_PAID_SUBSCRIPTIONS",
    "ENABLE_PURCHASE_REDIRECT",
    "ENABLE_THIRD_PARTY_FUNDS",
    "ENABLE_LIVE_TRADING",
    "ENABLE_ORDER_ENTRY",
    "TWELVE_DATA_ENABLED",
    "GDELT_ENABLED",
    "EMAIL_ALERTS_ENABLED",
    "WEBHOOK_ALERTS_ENABLED",
    "AUTH_ENABLED",
)
SYSTEM_ENV = (
    "PATH",
    "HOME",
    "USER",
    "LOGNAME",
    "TMPDIR",
    "LANG",
    "LC_ALL",
    "DOCKER_HOST",
    "DOCKER_CONTEXT",
    "DOCKER_CONFIG",
    "DOCKER_CERT_PATH",
    "DOCKER_TLS_VERIFY",
)
SERVICES = {"postgres", "redis", "migrate", "seed", "web", "worker", "beat"}
FALSE_VALUES = {"", "0", "false", "no", "off"}


class LauncherError(Exception):
    """Safe public error: deliberately contains no input or subprocess output."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def fail(code: str, message: str) -> None:
    raise LauncherError(code, message)


def assert_no_symlinks(path: Path) -> None:
    """Inspect every existing component before resolving or opening a path."""
    for component in (path, *path.parents):
        try:
            metadata = component.lstat()
        except FileNotFoundError:
            continue
        if stat.S_ISLNK(metadata.st_mode):
            fail("SYMLINK_REFUSED", "路径或文件包含软链接，已拒绝操作。")


def workspace_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute() or ".." in path.parts:
        fail("INVALID_WORKSPACE", "工作目录必须是明确的绝对路径，不能包含上级跳转。")
    assert_no_symlinks(path)
    path = path.absolute()
    if path == Path(path.anchor) or path == Path.home().absolute():
        fail("UNSAFE_WORKSPACE", "不能使用根目录或用户主目录。")
    if path == PLUGIN_ROOT or PLUGIN_ROOT in path.parents:
        fail("PLUGIN_CACHE_REFUSED", "必须使用插件目录之外的独立工作目录。")
    return path


def regular_bytes(path: Path) -> bytes:
    assert_no_symlinks(path)
    if not stat.S_ISREG(path.lstat().st_mode):
        fail("NONREGULAR_FILE", "只允许普通文件和目录。")
    descriptor = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    with os.fdopen(descriptor, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            fail("NONREGULAR_FILE", "只允许普通文件和目录。")
        return stream.read()


def files_in(root: Path) -> dict[str, Path]:
    assert_no_symlinks(root)
    if not root.is_dir():
        fail("RUNTIME_MISSING", "未找到完整的随插件打包运行时。")
    result: dict[str, Path] = {}
    for current, directories, filenames in os.walk(root, followlinks=False):
        for name in (*directories, *filenames):
            item = Path(current) / name
            metadata = item.lstat()
            if stat.S_ISLNK(metadata.st_mode):
                fail("SYMLINK_REFUSED", "路径或文件包含软链接，已拒绝操作。")
            if not (stat.S_ISREG(metadata.st_mode) or stat.S_ISDIR(metadata.st_mode)):
                fail("NONREGULAR_FILE", "只允许普通文件和目录。")
        for name in filenames:
            item = Path(current) / name
            result[item.relative_to(root).as_posix()] = item
    return result


def check_dotenv(root: Path) -> None:
    env_file = root / ".env"
    assert_no_symlinks(env_file)
    if env_file.exists() and (not env_file.is_file() or env_file.stat().st_size):
        fail("DOTENV_REFUSED", "发现非空或非普通 .env；请使用新建的纯 Demo 工作副本。")


def validate_compose_text(content: str) -> None:
    # The runtime is bundled and subsequently hash-checked. This preflight also
    # protects PLAN mode without requiring Docker or a YAML dependency.
    expected = r'^    ports:\s*\n      - "127\.0\.0\.1:\$\{APP_PORT:-8000\}:8000"\s*$'
    if len(re.findall(r"^\s*ports:", content, re.MULTILINE)) != 1:
        fail("UNSAFE_COMPOSE", "Compose 只能发布一个本机 Web 端口。")
    if not re.search(expected, content, re.MULTILINE):
        fail("UNSAFE_COMPOSE", "Compose Web 端口必须显式绑定 127.0.0.1。")
    if re.search(r"^\s*(network_mode|include|extends|container_name):", content, re.MULTILINE):
        fail("UNSAFE_COMPOSE", "不允许共享主机网络、外部扩展或固定容器名称。")


def validate_runtime(root: Path) -> dict[str, Path]:
    result = files_in(root)
    if any(name not in result for name in REQUIRED_FILES):
        fail("RUNTIME_INCOMPLETE", "随插件打包的运行时文件不完整，无法启动。")
    if not any(name.startswith("alembic/versions/") and name.endswith(".py") for name in result):
        fail("RUNTIME_INCOMPLETE", "运行时缺少数据库迁移。")
    check_dotenv(root)
    if MARKER in result:
        fail("RUNTIME_INVALID", "运行时不能包含工作目录生成标记。")
    validate_compose_text(regular_bytes(root / "docker-compose.yml").decode("utf-8"))
    return result


def prepare(workspace: Path) -> dict[str, Any]:
    if workspace.exists():
        fail("WORKSPACE_EXISTS", "工作目录已经存在；不会覆盖或合并任何文件。")
    if not workspace.parent.is_dir():
        fail("PARENT_MISSING", "工作目录的父目录必须已经存在。")
    source_files = validate_runtime(RUNTIME_ROOT)
    workspace.mkdir(mode=0o700)
    manifest: dict[str, str] = {}
    for relative, source in sorted(source_files.items()):
        content = regular_bytes(source)
        destination = workspace / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        assert_no_symlinks(destination.parent)
        mode = 0o755 if source.stat().st_mode & stat.S_IXUSR else 0o644
        descriptor = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, mode)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
        manifest[relative] = hashlib.sha256(content).hexdigest()
    marker = {
        "schema_version": 1,
        "plugin_id": "etf-sentinel",
        "workspace": str(workspace),
        "data_mode": "DEMO_FIXTURE",
        "trading_mode": "paper",
        "files": manifest,
    }
    with (workspace / MARKER).open("x", encoding="utf-8") as stream:
        json.dump(marker, stream, ensure_ascii=False, sort_keys=True, indent=2)
        stream.write("\n")
    return {
        "status": "PREPARED",
        "data_mode": "DEMO_FIXTURE",
        "trading_mode": "paper",
        "workspace": str(workspace),
        "copied_files": len(manifest),
        "started": False,
    }


def verify_prepared(workspace: Path) -> None:
    if not workspace.is_dir():
        fail("NOT_PREPARED", "请先用 prepare 创建独立 Demo 工作副本。")
    check_dotenv(workspace)
    try:
        marker = json.loads(regular_bytes(workspace / MARKER))
    except (FileNotFoundError, ValueError, UnicodeError):
        fail("NOT_PREPARED", "工作目录缺少有效的插件生成标记。")
    if not isinstance(marker, dict) or any(
        (
            marker.get("schema_version") != 1,
            marker.get("plugin_id") != "etf-sentinel",
            marker.get("workspace") != str(workspace),
            marker.get("trading_mode") != "paper",
            marker.get("data_mode") != "DEMO_FIXTURE",
            not isinstance(marker.get("files"), dict),
        )
    ):
        fail("NOT_PREPARED", "工作目录生成标记无效或属于其他路径。")
    files = files_in(workspace)
    files.pop(MARKER, None)
    expected = marker["files"]
    if ".env" not in expected:
        files.pop(".env", None)  # An empty optional .env is harmless.
    if not expected or files.keys() != expected.keys():
        fail("RUNTIME_MODIFIED", "工作副本文件发生变化；请重新准备独立副本。")
    if any(name not in expected for name in REQUIRED_FILES):
        fail("RUNTIME_INCOMPLETE", "工作副本缺少必要运行时文件。")
    for relative, path in files.items():
        if hashlib.sha256(regular_bytes(path)).hexdigest() != expected[relative]:
            fail("RUNTIME_MODIFIED", "工作副本文件校验失败；不会执行已修改的运行时。")
    validate_compose_text(regular_bytes(workspace / "docker-compose.yml").decode("utf-8"))


def isolated_environment(port: int, interval_hours: int) -> dict[str, str]:
    incoming: dict[str, list[str]] = {}
    for name, value in os.environ.items():
        incoming.setdefault(name.upper(), []).append(value)
    if any(value.strip().lower() != "paper" for value in incoming.get("TRADING_MODE", [])):
        fail("LIVE_MODE_REFUSED", "TRADING_MODE 只能为 paper，已拒绝启动。")
    if any(value.strip() != "demo_fixture" for value in incoming.get("MARKET_DATA_PROVIDER", [])):
        fail("LIVE_PROVIDER_REFUSED", "真实数据提供方尚未授权，已拒绝启动。")
    if any(value.strip().lower() not in {"demo", "test"} for value in incoming.get("APP_ENV", [])):
        fail("PRODUCTION_ENV_REFUSED", "本插件启动器仅支持独立 Demo。")
    if any(
        value.strip().lower() not in FALSE_VALUES
        for name in DISABLED_FLAGS
        for value in incoming.get(name, [])
    ):
        fail("UNSAFE_FLAG", "发现真实数据、外部服务或交易功能开关，已拒绝启动。")
    environment = {name: os.environ[name] for name in SYSTEM_ENV if name in os.environ}
    host = environment.get("DOCKER_HOST", "")
    if host and not local_docker_endpoint(host):
        fail("REMOTE_DOCKER_REFUSED", "仅允许本机 Docker 套接字，不支持远程 Docker 主机。")
    environment.update({name: "false" for name in DISABLED_FLAGS})
    environment.update(
        {
            "APP_ENV": "demo",
            "TRADING_MODE": "paper",
            "MARKET_DATA_PROVIDER": "demo_fixture",
            "APP_PORT": str(port),
            "MONITORING_INTERVAL_HOURS": str(interval_hours),
            "GLOBAL_KILL_SWITCH": "false",
            "CONTAINER_LOCALHOST_BOUND": "true",
            "COMPOSE_DISABLE_ENV_FILE": "1",
            "PYTHONDONTWRITEBYTECODE": "1",
        }
    )
    return environment


def local_docker_endpoint(value: Any) -> bool:
    return isinstance(value, str) and value.startswith(("unix:///", "npipe:////./pipe/"))


def verify_local_docker(workspace: Path, environment: dict[str, str]) -> None:
    inspected = run_compose(["docker", "context", "inspect"], workspace, environment, 20)
    try:
        contexts = json.loads(inspected.stdout)
        if not isinstance(contexts, list) or len(contexts) != 1:
            fail("INVALID_DOCKER_CONTEXT", "无法唯一确认 Docker 的本机运行环境。")
        endpoint = contexts[0]["Endpoints"]["docker"]["Host"]
    except (ValueError, KeyError, TypeError):
        fail("INVALID_DOCKER_CONTEXT", "无法验证当前 Docker context 的连接地址。")
    # DOCKER_CONTEXT takes precedence over DOCKER_HOST. With no explicit
    # context, DOCKER_HOST is the effective endpoint used by the Docker CLI.
    if environment.get("DOCKER_HOST") and not environment.get("DOCKER_CONTEXT"):
        endpoint = environment["DOCKER_HOST"]
    if not local_docker_endpoint(endpoint):
        fail("REMOTE_DOCKER_REFUSED", "当前 Docker context 指向远程主机，已拒绝启动。")


def validate_resolved_compose(config: Any, project: str, port: int) -> None:
    if not isinstance(config, dict) or config.get("name") != project:
        fail("UNSAFE_COMPOSE", "Compose 项目名称校验失败。")
    services = config.get("services", {})
    if not isinstance(services, dict) or set(services) != SERVICES:
        fail("UNSAFE_COMPOSE", "Compose 服务范围校验失败。")
    for name, service in services.items():
        if not isinstance(service, dict):
            fail("UNSAFE_COMPOSE", "Compose 服务格式无效。")
        if (
            service.get("network_mode")
            or service.get("privileged")
            or service.get("container_name")
        ):
            fail("UNSAFE_COMPOSE", "Compose 含不允许的容器权限或网络设置。")
        if any(volume.get("type") != "volume" for volume in service.get("volumes", [])):
            fail("UNSAFE_COMPOSE", "Compose 只允许独立命名数据卷，不允许主机目录绑定。")
        if name == "web":
            ports = service.get("ports", [])
            if len(ports) != 1 or not isinstance(ports[0], dict):
                fail("UNSAFE_COMPOSE", "Web 端口数量无效。")
            binding = ports[0]
            if any(
                (
                    binding.get("host_ip") != "127.0.0.1",
                    str(binding.get("published")) != str(port),
                    binding.get("target") != 8000,
                    binding.get("protocol", "tcp") != "tcp",
                )
            ):
                fail("UNSAFE_COMPOSE", "Web 端口没有按指定参数绑定本机地址。")
        elif service.get("ports"):
            fail("UNSAFE_COMPOSE", "非 Web 服务不得发布端口。")
        if name not in {"postgres", "redis"}:
            settings = service.get("environment", {})
            if (
                settings.get("TRADING_MODE") != "paper"
                or settings.get("MARKET_DATA_PROVIDER") != "demo_fixture"
                or settings.get("APP_ENV") != "demo"
                or any(
                    str(settings.get(flag, "false")).lower() not in FALSE_VALUES
                    for flag in DISABLED_FLAGS
                )
            ):
                fail("UNSAFE_COMPOSE", "容器环境越过 Demo/paper 边界。")
    for group in ("volumes", "networks"):
        for key, value in config.get(group, {}).items():
            if (
                not isinstance(value, dict)
                or value.get("external")
                or value.get("name") != f"{project}_{key}"
            ):
                fail("UNSAFE_COMPOSE", "Compose 数据卷或网络未与现有项目隔离。")


def run_compose(
    command: list[str], workspace: Path, environment: dict[str, str], timeout: int
) -> subprocess.CompletedProcess[str]:
    try:
        # All callers build fixed argv lists; no shell or arbitrary Compose flags.
        result = subprocess.run(  # noqa: S603 - fixed argv, filtered env, no shell
            command,
            cwd=workspace,
            env=environment,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        fail("DOCKER_NOT_FOUND", "未找到 Docker；请先在本机安装并启动 Docker/Colima。")
    except subprocess.TimeoutExpired:
        fail("DOCKER_TIMEOUT", "Docker 操作超时；可能已有部分资源，请人工核查，不会自动删除。")
    if result.returncode:
        fail("DOCKER_FAILED", "Docker 操作失败；不会输出可能含敏感信息的构建日志，请在本机核查。")
    return result


def start(
    workspace: Path, port: int = 18081, interval_hours: int = 1, execute: bool = False
) -> dict[str, Any]:
    if not isinstance(port, int) or isinstance(port, bool) or not 1024 <= port <= 65535:
        fail("INVALID_PORT", "端口必须是 1024–65535 的整数。")
    if type(interval_hours) is not int or interval_hours not in {1, 2}:
        fail("INVALID_INTERVAL", "更新检查间隔只能为 1 小时或 2 小时。")
    environment = isolated_environment(port, interval_hours)
    verify_prepared(workspace)
    project = "etf-sentinel-plugin-" + hashlib.sha256(str(workspace).encode()).hexdigest()[:10]
    prefix = [
        "docker",
        "compose",
        "--project-name",
        project,
        "--project-directory",
        str(workspace),
        "--file",
        str(workspace / "docker-compose.yml"),
    ]
    command = prefix + ["up", "--build", "--detach"]
    initial_monitor_command = prefix + [
        "exec",
        "-T",
        "worker",
        "celery",
        "-A",
        "etf_sentinel.tasks:celery_app",
        "call",
        "etf_sentinel.hourly_monitor",
    ]
    result: dict[str, Any] = {
        "status": "PLAN",
        "data_mode": "DEMO_FIXTURE",
        "trading_mode": "paper",
        "workspace": str(workspace),
        "project_name": project,
        "url": f"http://127.0.0.1:{port}/",
        "interval_hours": interval_hours,
        "command": command,
        "initial_monitor_command": initial_monitor_command,
        "initial_monitor_enqueued": False,
        "started": False,
        "notice": "仅更新固定演示数据的健康与风险分析；不获取真实行情，不提供真实交易。",
    }
    if execute:
        verify_local_docker(workspace, environment)
        resolved = run_compose(prefix + ["config", "--format", "json"], workspace, environment, 60)
        try:
            config = json.loads(resolved.stdout)
        except ValueError:
            fail("INVALID_COMPOSE_CONFIG", "Docker Compose 返回了不可验证的配置。")
        validate_resolved_compose(config, project, port)
        verify_prepared(workspace)
        run_compose(command, workspace, environment, 1200)
        try:
            run_compose(initial_monitor_command, workspace, environment, 30)
        except LauncherError:
            fail(
                "INITIAL_MONITOR_DISPATCH_FAILED",
                "初始健康分析入队未能确认；服务可能已经启动，请人工核查。不会自动停止或清理资源。",
            )
        result.update(status="STARTED", started=True, initial_monitor_enqueued=True)
        result["initial_monitor_completion"] = "NOT_CHECKED"
        result["notice"] = (
            "Compose 启动命令成功，初始健康分析已入队但未等待或确认完成；"
            "不获取真实行情、不训练模型、不产生模拟成交。应用健康与业务验收需另行检查。"
        )
    return result


class SafeParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        fail("INVALID_ARGUMENTS", "参数无效；仅支持 prepare/start、绝对工作目录及已列明的选项。")


def main(argv: list[str] | None = None) -> int:
    parser = SafeParser(
        description="ETF Sentinel 独立 DEMO_FIXTURE/paper 启动器", allow_abbrev=False
    )
    subcommands = parser.add_subparsers(dest="action", required=True, parser_class=SafeParser)
    prepare_parser = subcommands.add_parser("prepare", allow_abbrev=False)
    prepare_parser.add_argument("--workspace", required=True)
    start_parser = subcommands.add_parser("start", allow_abbrev=False)
    start_parser.add_argument("--workspace", required=True)
    start_parser.add_argument("--port", type=int, default=18081)
    start_parser.add_argument("--interval-hours", type=int, choices=(1, 2), default=1)
    start_parser.add_argument("--execute", action="store_true")
    try:
        args = parser.parse_args(argv)
        workspace = workspace_path(args.workspace)
        result = (
            prepare(workspace)
            if args.action == "prepare"
            else start(
                workspace,
                args.port,
                args.interval_hours,
                args.execute,
            )
        )
        print(json.dumps(result, ensure_ascii=False, sort_keys=True))
        return 0
    except LauncherError as error:
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "code": error.code,
                    "message": str(error),
                    "data_mode": "DEMO_FIXTURE",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2
    except (OSError, UnicodeError, ValueError, TypeError, AttributeError):
        print(
            json.dumps(
                {
                    "status": "ERROR",
                    "code": "LOCAL_OPERATION_FAILED",
                    "message": "本机文件或配置校验失败；不会覆盖、删除或启动未经验证的目录。",
                    "data_mode": "DEMO_FIXTURE",
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
