#!/usr/bin/env python3
"""
Restore the complete nLab MariaDB database directly from a SQL dump file
(default: data/dumps/dump_completo.sql) and verify final table row counts.

Streams the dump in chunks into the native `mariadb` client inside the running
Docker container (or a local `mariadb`/`mysql` binary with --use-local-client)
while displaying live progress (MB, %, throughput, ETA).

Examples:
  # Restore default dump_completo.sql into the running nlab_mariadb container
  python database/pipeline/restore_dump.py

  # Drop & recreate nlab_db before restoring (clean idempotent reload)
  python database/pipeline/restore_dump.py --recreate-db

  # Restore a custom dump file
  python database/pipeline/restore_dump.py --dump-file data/dumps/dump_completo.sql
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from check_cardinality import run_check_cardinality
from config import (
    add_db_args,
    connect_db,
    db_config_from_args,
    get_container_name,
    get_dump_file,
)

CHUNK_BYTES = 4 * 1024 * 1024  # 4 MiB streaming buffer


def _build_client_cmd(
    db_config: dict,
    container: str,
    use_local_client: bool,
) -> list[str]:
    user = db_config["user"]
    password = db_config["password"]
    database = db_config["database"]

    if use_local_client:
        binary = shutil.which("mariadb") or shutil.which("mysql")
        if not binary:
            raise FileNotFoundError(
                "Neither `mariadb` nor `mysql` CLI was found in PATH. "
                "Omit --use-local-client to stream via `docker exec`."
            )
        cmd = [
            binary,
            f"-h{db_config['host']}",
            f"-P{db_config['port']}",
            f"-u{user}",
            "--default-character-set=utf8mb4",
            database,
        ]
        if password:
            cmd.insert(4, f"-p{password}")
        return cmd

    cmd = [
        "docker",
        "exec",
        "-i",
        container,
        "mariadb",
        f"-u{user}",
        "--default-character-set=utf8mb4",
        database,
    ]
    if password:
        cmd.insert(6, f"-p{password}")
    return cmd


def recreate_database(db_config: dict) -> None:
    db_name = db_config["database"]
    server_cfg = dict(db_config)
    server_cfg.pop("database", None)

    print(f"Dropping and recreating database `{db_name}`...", file=sys.stderr)
    conn = connect_db(server_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{db_name}`")
            cur.execute(
                f"CREATE DATABASE `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()
    finally:
        conn.close()


def ensure_database_exists(db_config: dict) -> None:
    db_name = db_config["database"]
    server_cfg = dict(db_config)
    server_cfg.pop("database", None)

    conn = connect_db(server_cfg)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE DATABASE IF NOT EXISTS `{db_name}` "
                "CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
            )
        conn.commit()
    finally:
        conn.close()


def run_restore_dump(
    db_config: dict,
    dump_file: Path,
    container: str = "nlab_mariadb",
    use_local_client: bool = False,
    recreate_db: bool = False,
    verify: bool = True,
) -> None:
    if not dump_file.exists():
        raise FileNotFoundError(f"Dump file not found: {dump_file}")

    total_bytes = dump_file.stat().st_size
    total_gb = total_bytes / (1024 ** 3)
    print(f"Dump file: {dump_file} ({total_gb:.2f} GiB)", file=sys.stderr)

    if recreate_db:
        recreate_database(db_config)
    else:
        ensure_database_exists(db_config)

    cmd = _build_client_cmd(db_config, container=container, use_local_client=use_local_client)
    target_desc = "local CLI" if use_local_client else f"docker container `{container}`"
    print(f"Streaming dump into MariaDB via {target_desc}...", file=sys.stderr)

    t0 = time.time()
    sent_bytes = 0
    last_log = t0

    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE, stderr=subprocess.PIPE)
    assert proc.stdin is not None and proc.stderr is not None

    try:
        with open(dump_file, "rb") as f:
            while True:
                chunk = f.read(CHUNK_BYTES)
                if not chunk:
                    break
                proc.stdin.write(chunk)
                sent_bytes += len(chunk)

                now = time.time()
                if now - last_log >= 2.0 or sent_bytes == total_bytes:
                    elapsed = max(0.001, now - t0)
                    pct = (sent_bytes / total_bytes) * 100.0 if total_bytes else 100.0
                    mb_s = (sent_bytes / (1024 * 1024)) / elapsed
                    eta_s = (total_bytes - sent_bytes) / max(1.0, sent_bytes / elapsed)
                    print(
                        f"\r  {sent_bytes / (1024 ** 3):.2f} / {total_gb:.2f} GiB "
                        f"({pct:5.1f}%) | {mb_s:5.1f} MiB/s | ETA {eta_s:5.0f}s   ",
                        end="",
                        file=sys.stderr,
                        flush=True,
                    )
                    last_log = now
        proc.stdin.close()
    except BrokenPipeError:
        pass

    stderr_out = proc.stderr.read().decode("utf-8", errors="replace").strip()
    rc = proc.wait()
    print("", file=sys.stderr)

    if rc != 0:
        raise RuntimeError(f"MariaDB restore failed (exit code {rc}):\n{stderr_out}")

    elapsed = time.time() - t0
    print(
        f"Restore completed in {elapsed:.1f}s ({elapsed / 60:.2f} min).",
        file=sys.stderr,
    )

    if verify:
        print("\nVerifying table cardinality...", file=sys.stderr)
        run_check_cardinality(db_config=db_config)


def main() -> None:
    parser = argparse.ArgumentParser(description="Restore nlab_db from a MariaDB SQL dump file")
    add_db_args(parser)
    parser.add_argument(
        "--dump-file",
        type=Path,
        default=get_dump_file(),
        help=f"Path to SQL dump file (default: {get_dump_file()})",
    )
    parser.add_argument(
        "--container",
        default=get_container_name(),
        help=f"Docker container name for MariaDB (default: {get_container_name()})",
    )
    parser.add_argument(
        "--use-local-client",
        action="store_true",
        help="Use local `mariadb`/`mysql` binary instead of `docker exec`",
    )
    parser.add_argument(
        "--recreate-db",
        action="store_true",
        help="Drop and recreate the target database before restoring",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip running check_cardinality.py after restore",
    )
    args = parser.parse_args()

    run_restore_dump(
        db_config=db_config_from_args(args),
        dump_file=args.dump_file,
        container=args.container,
        use_local_client=args.use_local_client,
        recreate_db=args.recreate_db,
        verify=not args.no_verify,
    )


if __name__ == "__main__":
    main()
