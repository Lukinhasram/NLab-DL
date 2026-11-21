#!/usr/bin/env python3
"""
Step 1: Base ETL from nlab-content (git mirror) into the MariaDB core schema.

Populates:
  - webs             : 1 row for the "nLab" web (id=1)
  - pages            : id preserved from folder name in nlab-content (matches prod IDs)
  - revisions        : optional 1 snapshot per page (omit via --skip-revisions when
                       Step 2 will load the full git commit history)
  - wiki_references  : parsed from [[...]] wiki markup using nLab link_type codes:
                       L=linked, W=wanted, R=redirect, I=include
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args, get_pages_root

BATCH = 2000
REV_BATCH = 200
WEB_ID = 1

LINK_RE = re.compile(r"\[\[(?!!)([^\]|]+)(?:\|[^\]]*)?\]\]")
INCLUDE_RE = re.compile(r"\[\[!include\s+([^\]]+)\]\]", re.IGNORECASE)
REDIRECT_RE = re.compile(r"\[\[!redirects\s+([^\]]+)\]\]", re.IGNORECASE)


def read_text(path: str | Path) -> str:
    with open(path, "rb") as f:
        raw = f.read()
    return raw.replace(b"\x00", b"").decode("utf-8", errors="replace")


def walk_pages(pages_root: Path):
    """Yield (page_id:int, name:str, content:str) for every page dir under pages_root."""
    for dirpath, dirnames, filenames in os.walk(pages_root):
        if "name" in filenames and "content.md" in filenames:
            leaf = os.path.basename(dirpath)
            if not leaf.isdigit():
                continue
            name = read_text(os.path.join(dirpath, "name")).strip()[:255]
            content = read_text(os.path.join(dirpath, "content.md"))
            yield int(leaf), name, content
            dirnames[:] = []


def run_etl_base(
    db_config: dict,
    pages_root: Path,
    skip_revisions: bool = False,
    reset: bool = False,
) -> None:
    if not pages_root.exists():
        raise FileNotFoundError(
            f"Pages directory not found: {pages_root}. Run 00_fetch_source.py first."
        )

    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    print(f"Pass 1: reading all pages from {pages_root}...", file=sys.stderr)
    pages: dict[int, tuple[str, str]] = {}
    for pid, name, content in walk_pages(pages_root):
        pages[pid] = (name, content)
    print(f"  {len(pages):,} pages read", file=sys.stderr)

    print("Pass 2: extracting links/includes/redirects...", file=sys.stderr)
    redirects_by_page: dict[int, list[str]] = {}
    includes_by_page: dict[int, list[str]] = {}
    links_by_page: dict[int, list[str]] = {}
    alias_to_page: dict[str, int] = {}

    for pid, (_, content) in pages.items():
        reds = [m.group(1).strip()[:255] for m in REDIRECT_RE.finditer(content) if m.group(1).strip()]
        incs = [m.group(1).strip()[:255] for m in INCLUDE_RE.finditer(content) if m.group(1).strip()]
        stripped = REDIRECT_RE.sub(" ", INCLUDE_RE.sub(" ", content))
        lks = [m.group(1).strip()[:255] for m in LINK_RE.finditer(stripped) if m.group(1).strip()]

        redirects_by_page[pid] = reds
        includes_by_page[pid] = incs
        links_by_page[pid] = lks
        for r in reds:
            alias_to_page[r] = pid

    real_names = {name for name, _ in pages.values()}
    resolvable = real_names | set(alias_to_page.keys())

    print("Connecting to MariaDB...", file=sys.stderr)
    conn = connect_db(db_config)
    cur = conn.cursor()

    try:
        if reset:
            print("Resetting core tables (webs, pages, revisions, wiki_references, wiki_files)...", file=sys.stderr)
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            for tbl in ("wiki_references", "revisions", "wiki_files", "pages", "webs"):
                cur.execute(f"TRUNCATE TABLE `{tbl}`")
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            conn.commit()

        print("Inserting web row...", file=sys.stderr)
        cur.execute(
            "INSERT INTO webs (id, created_at, updated_at, name, address, markup) "
            "VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE updated_at = VALUES(updated_at)",
            (WEB_ID, now, now, "nLab", "nlab", "markdownMML"),
        )
        conn.commit()

        print("Inserting pages...", file=sys.stderr)
        page_rows = [(pid, now, now, WEB_ID, name) for pid, (name, _) in pages.items()]
        for i in range(0, len(page_rows), BATCH):
            cur.executemany(
                "INSERT INTO pages (id, created_at, updated_at, web_id, name) VALUES (%s, %s, %s, %s, %s)",
                page_rows[i : i + BATCH],
            )
            conn.commit()
            print(f"  pages {min(i + BATCH, len(page_rows)):,}/{len(page_rows):,}", file=sys.stderr)

        if not skip_revisions:
            print("Inserting snapshot revisions (1 current snapshot per page)...", file=sys.stderr)
            rev_rows = [
                (now, now, now, pid, content, "nlab-content-mirror", WEB_ID)
                for pid, (_, content) in pages.items()
            ]
            for i in range(0, len(rev_rows), REV_BATCH):
                cur.executemany(
                    "INSERT INTO revisions (created_at, updated_at, revised_at, page_id, content, author, web_id) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s)",
                    rev_rows[i : i + REV_BATCH],
                )
                conn.commit()
                print(f"  revisions {min(i + REV_BATCH, len(rev_rows)):,}/{len(rev_rows):,}", file=sys.stderr)
        else:
            print("Skipping snapshot revisions (--skip-revisions set; Step 2 will load git history).", file=sys.stderr)

        print("Inserting wiki_references (L/W/R/I)...", file=sys.stderr)
        ref_rows = []
        for pid in pages:
            for r in redirects_by_page[pid]:
                ref_rows.append((now, now, pid, r, "R"))
            for inc in includes_by_page[pid]:
                ref_rows.append((now, now, pid, inc, "I"))
            for lk in links_by_page[pid]:
                link_type = "L" if lk in resolvable else "W"
                ref_rows.append((now, now, pid, lk, link_type))

        for i in range(0, len(ref_rows), BATCH):
            cur.executemany(
                "INSERT INTO wiki_references (created_at, updated_at, page_id, referenced_name, link_type) "
                "VALUES (%s, %s, %s, %s, %s)",
                ref_rows[i : i + BATCH],
            )
            conn.commit()
            print(f"  wiki_references {min(i + BATCH, len(ref_rows)):,}/{len(ref_rows):,}", file=sys.stderr)

        print("Updating webs.count_pages...", file=sys.stderr)
        cur.execute("UPDATE webs SET count_pages=%s WHERE id=%s", (len(pages), WEB_ID))
        conn.commit()
        print("Step 1 (base ETL) complete.", file=sys.stderr)
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 1: Base ETL from nlab-content into MariaDB")
    add_db_args(parser)
    parser.add_argument("--pages-root", type=Path, default=get_pages_root(), help="Path to nlab-content/pages")
    parser.add_argument(
        "--skip-revisions",
        action="store_true",
        help="Skip inserting 1 snapshot revision per page (recommended before running 02_etl_revisions.py)",
    )
    parser.add_argument("--reset", action="store_true", help="Truncate core tables before inserting")
    args = parser.parse_args()

    run_etl_base(
        db_config=db_config_from_args(args),
        pages_root=args.pages_root,
        skip_revisions=args.skip_revisions,
        reset=args.reset,
    )


if __name__ == "__main__":
    main()
