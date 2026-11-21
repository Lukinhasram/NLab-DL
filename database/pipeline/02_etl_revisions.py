#!/usr/bin/env python3
"""
Step 2: Reconstructs `revisions` from the real git commit history of nlab-content.

Steps:
  1. Parse full_log.txt (auto-generating it if missing) into chronological
     (date_iso, page_id, commit_hash, author, rel_path) tuples.
  2. Filter out any page_id not present in `pages` so FK integrity is preserved.
  3. Stream content blobs using a persistent `git cat-file --batch` process
     (with a dedicated writer thread to avoid pipe deadlock).
  4. Bulk-insert into `revisions` in resumable chunks until complete.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import threading
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args, get_content_dir, get_git_log_file

WEB_ID = 1
PATH_RE = re.compile(r"^pages/(?:\d/){4}(\d+)/content\.md$")
BATCH = 300
DEFAULT_CHUNK_SIZE = 15000


def ensure_git_log(content_dir: Path, log_file: Path) -> None:
    if log_file.exists():
        return
    if not (content_dir / ".git").exists():
        raise FileNotFoundError(
            f"Git repository not found at {content_dir}. Run 00_fetch_source.py first."
        )
    print(f"Log file not found at {log_file}; generating via git log...", file=sys.stderr)
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


def parse_log(log_file: Path, valid_page_ids: set[int] | None = None) -> tuple[list[tuple[str, int, str, str, str]], int]:
    """Returns (chronological entries, skipped_orphan_count)."""
    entries: list[tuple[str, int, str, str, str]] = []
    skipped_orphans = 0
    cur_hash = cur_author = cur_date = ""

    with open(log_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.rstrip("\r\n")
            if line.startswith("commit\t"):
                parts = line.split("\t")
                cur_hash = parts[1] if len(parts) > 1 else ""
                cur_author = parts[2] if len(parts) > 2 else ""
                cur_date = parts[4] if len(parts) > 4 else ""
            elif not line.strip():
                continue
            else:
                m = PATH_RE.match(line)
                if m:
                    page_id = int(m.group(1))
                    if valid_page_ids is not None and page_id not in valid_page_ids:
                        skipped_orphans += 1
                        continue
                    entries.append((cur_date, page_id, cur_hash, cur_author, line))

    entries.sort(key=lambda t: t[0])
    return entries, skipped_orphans


def batch_cat_file(repo_dir: Path, pairs: list[tuple[str, str]]):
    """Yields blob bytes in the same order as `pairs` using `git cat-file --batch`."""
    proc = subprocess.Popen(
        ["git", "cat-file", "--batch"],
        cwd=str(repo_dir),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
    )
    assert proc.stdin is not None and proc.stdout is not None

    def writer() -> None:
        for commit_hash, rel_path in pairs:
            proc.stdin.write(f"{commit_hash}:{rel_path}\n".encode("utf-8"))
        proc.stdin.close()

    t = threading.Thread(target=writer, daemon=True)
    t.start()

    out = proc.stdout
    for _ in pairs:
        header = out.readline().decode("utf-8", errors="replace")
        if not header or header.endswith("missing\n"):
            yield b""
            continue
        _, _, size_s = header.split()
        size = int(size_s)
        content = out.read(size)
        out.read(1)  # trailing newline
        yield content

    t.join()
    proc.wait()


def run_etl_revisions(
    db_config: dict,
    content_dir: Path,
    log_file: Path,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    max_chunks: int | None = None,
    truncate: bool = False,
) -> None:
    ensure_git_log(content_dir, log_file)

    conn = connect_db(db_config)
    cur = conn.cursor()

    try:
        if truncate:
            print("Truncating `revisions` before loading full git history...", file=sys.stderr)
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("TRUNCATE TABLE revisions")
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()

        cur.execute("SELECT id FROM pages")
        valid_page_ids = {row[0] for row in cur.fetchall()}

        cur.execute("SELECT COUNT(*) FROM revisions")
        start_offset = cur.fetchone()[0]

        print(f"Parsing git log ({log_file})...", file=sys.stderr)
        entries, skipped_orphans = parse_log(log_file, valid_page_ids=valid_page_ids)
        total = len(entries)
        print(
            f"  {total:,} revisions matched existing pages "
            f"({skipped_orphans:,} orphan revisions skipped); resuming from offset {start_offset:,}",
            file=sys.stderr,
        )

        if start_offset >= total:
            print("Revisions already complete.", file=sys.stderr)
            return

        chunks_done = 0
        offset = start_offset
        while offset < total:
            if max_chunks is not None and chunks_done >= max_chunks:
                print(
                    f"Reached --max-chunks={max_chunks}. Resume later from offset {offset:,}/{total:,}.",
                    file=sys.stderr,
                )
                break

            chunk = entries[offset : offset + chunk_size]
            print(
                f"Processing chunk {chunks_done + 1} ({offset:,}..{offset + len(chunk):,} / {total:,})...",
                file=sys.stderr,
            )

            pairs = [(h, path) for (_, _, h, _, path) in chunk]
            rows = []
            inserted = 0

            for (date_iso, page_id, _, author, _), content_bytes in zip(
                chunk, batch_cat_file(content_dir, pairs)
            ):
                content = content_bytes.replace(b"\x00", b"").decode("utf-8", errors="replace")
                dt = date_iso.replace("T", " ")[:19]
                rows.append((dt, dt, dt, page_id, content, author[:60], WEB_ID))

                if len(rows) >= BATCH:
                    cur.executemany(
                        "INSERT INTO revisions (created_at, updated_at, revised_at, page_id, content, author, web_id) "
                        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                        rows,
                    )
                    conn.commit()
                    inserted += len(rows)
                    print(f"  {offset + inserted:,}/{total:,}", file=sys.stderr)
                    rows = []

            if rows:
                cur.executemany(
                    "INSERT INTO revisions (created_at, updated_at, revised_at, page_id, content, author, web_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    rows,
                )
                conn.commit()
                inserted += len(rows)

            offset += inserted
            chunks_done += 1
            print(f"Chunk {chunks_done} done ({offset:,}/{total:,}).", file=sys.stderr)

        if offset >= total:
            print(f"Step 2 (revisions ETL) complete: {offset:,} revisions loaded.", file=sys.stderr)
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 2: Load full git revision history into MariaDB")
    add_db_args(parser)
    parser.add_argument("--content-dir", type=Path, default=get_content_dir(), help="Path to nlab-content git repo")
    parser.add_argument("--log-file", type=Path, default=get_git_log_file(), help="Path to full_log.txt")
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE, help="Revisions per chunk (default: 15000)")
    parser.add_argument("--max-chunks", type=int, default=None, help="Optional cap on chunks per invocation")
    parser.add_argument("--truncate", action="store_true", help="Truncate `revisions` before loading")
    args = parser.parse_args()

    run_etl_revisions(
        db_config=db_config_from_args(args),
        content_dir=args.content_dir,
        log_file=args.log_file,
        chunk_size=args.chunk_size,
        max_chunks=args.max_chunks,
        truncate=args.truncate,
    )


if __name__ == "__main__":
    main()
