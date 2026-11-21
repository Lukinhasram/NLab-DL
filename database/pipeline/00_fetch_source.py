#!/usr/bin/env python3
"""
Step 0: Fetch upstream nLab content mirror and export full git revision log.

Clones https://github.com/ncatlab/nlab-content.git with full commit/blob history
(no partial-clone filter) and generates `full_log.txt` for `02_etl_revisions.py`.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import get_content_dir, get_git_log_file

DEFAULT_REPO_URL = "https://github.com/ncatlab/nlab-content.git"


def generate_git_log(content_dir: Path, log_file: Path) -> None:
    print(f"Generating git log at {log_file}...", file=sys.stderr)
    log_file.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "git",
        "log",
        "--format=commit%x09%H%x09%an%x09%ae%x09%ad",
        "--date=iso-strict",
        "--name-only",
        "--",
        "pages",
    ]
    with open(log_file, "wb") as out:
        subprocess.run(cmd, cwd=str(content_dir), stdout=out, check=True)
    size_mb = log_file.stat().st_size / (1024 * 1024)
    print(f"Git log generated ({size_mb:.1f} MB).", file=sys.stderr)


def fetch_source(
    content_dir: Path,
    log_file: Path,
    repo_url: str = DEFAULT_REPO_URL,
    update: bool = False,
    force_log: bool = False,
) -> None:
    git_dir = content_dir / ".git"
    if not git_dir.exists():
        content_dir.parent.mkdir(parents=True, exist_ok=True)
        print(f"Cloning {repo_url} into {content_dir} (full history)...", file=sys.stderr)
        subprocess.run(["git", "clone", repo_url, str(content_dir)], check=True)
    elif update:
        print(f"Updating existing repository at {content_dir}...", file=sys.stderr)
        subprocess.run(["git", "pull", "--ff-only"], cwd=str(content_dir), check=True)
    else:
        print(f"Repository already present at {content_dir} (use --update to pull).", file=sys.stderr)

    if force_log or update or not log_file.exists():
        generate_git_log(content_dir, log_file)
    else:
        print(f"Git log already exists at {log_file} (use --force-log to regenerate).", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Clone nlab-content and generate full_log.txt")
    parser.add_argument("--repo-url", default=DEFAULT_REPO_URL, help="Upstream git URL")
    parser.add_argument("--content-dir", type=Path, default=get_content_dir(), help="Local clone directory")
    parser.add_argument("--log-file", type=Path, default=get_git_log_file(), help="Output path for full_log.txt")
    parser.add_argument("--update", action="store_true", help="Run git pull if repository already exists")
    parser.add_argument("--force-log", action="store_true", help="Regenerate full_log.txt even if it exists")
    args = parser.parse_args()

    fetch_source(
        content_dir=args.content_dir,
        log_file=args.log_file,
        repo_url=args.repo_url,
        update=args.update,
        force_log=args.force_log,
    )


if __name__ == "__main__":
    main()
