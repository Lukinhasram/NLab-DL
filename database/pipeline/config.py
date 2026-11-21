"""Shared configuration and database connection helpers for the nLab MariaDB pipeline."""

from __future__ import annotations

import argparse
import os
import time
from pathlib import Path
from typing import Any

import pymysql

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - optional if env vars are exported directly
    load_dotenv = None

REPO_ROOT = Path(__file__).resolve().parents[2]

if load_dotenv is not None:
    load_dotenv(REPO_ROOT / ".env")


def _resolve_path(raw: str) -> Path:
    p = Path(raw)
    return p if p.is_absolute() else (REPO_ROOT / p).resolve()


def get_default_db_config() -> dict[str, Any]:
    return {
        "host": os.getenv("MARIADB_HOST", "127.0.0.1"),
        "port": int(os.getenv("MARIADB_PORT", "3307")),
        "user": os.getenv("MARIADB_USER", "root"),
        "password": os.getenv("MARIADB_PASSWORD", "senhadoida"),
        "database": os.getenv("MARIADB_DATABASE", "nlab_db"),
        "charset": "utf8mb4",
    }


def get_content_dir() -> Path:
    return _resolve_path(os.getenv("NLAB_CONTENT_DIR", "data/nlab-content"))


def get_dump_file() -> Path:
    return _resolve_path(os.getenv("MARIADB_DUMP_FILE", "data/dumps/dump_completo.sql"))


def get_container_name() -> str:
    return os.getenv("MARIADB_CONTAINER", "nlab_mariadb")


def get_pages_root() -> Path:
    return get_content_dir() / "pages"


def get_git_log_file() -> Path:
    raw = os.getenv("NLAB_GIT_LOG_FILE")
    if raw:
        return _resolve_path(raw)
    return get_content_dir() / "full_log.txt"


def add_db_args(parser: argparse.ArgumentParser) -> None:
    defaults = get_default_db_config()
    group = parser.add_argument_group("MariaDB connection")
    group.add_argument("--host", default=defaults["host"], help=f"MariaDB host (default: {defaults['host']})")
    group.add_argument("--port", type=int, default=defaults["port"], help=f"MariaDB port (default: {defaults['port']})")
    group.add_argument("--user", default=defaults["user"], help=f"MariaDB user (default: {defaults['user']})")
    group.add_argument("--password", default=defaults["password"], help="MariaDB password (default: from env)")
    group.add_argument("--database", default=defaults["database"], help=f"MariaDB database (default: {defaults['database']})")


def db_config_from_args(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "host": args.host,
        "port": args.port,
        "user": args.user,
        "password": args.password,
        "database": args.database,
        "charset": "utf8mb4",
    }


def connect_db(db_config: dict[str, Any], retries: int = 5, delay: float = 2.0, **kwargs: Any) -> pymysql.connections.Connection:
    last_exc: Exception | None = None
    params = dict(db_config)
    params.update(kwargs)
    for attempt in range(1, retries + 1):
        try:
            return pymysql.connect(**params)
        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(delay)
    assert last_exc is not None
    raise last_exc
