#!/usr/bin/env python3
"""
Step 4: Adds `C` (Category) and `F`/`E` (File exists/wanted) rows to `wiki_references`.

Replicates upstream nLab parsing logic:
  - lib/chunks/category.rb              -> Category chunk (link_type 'C')
  - script/src/renderer/link_block.py   -> file_link_processor (link_type 'F' or 'E')

Checks `wiki_files` to classify each file reference as 'F' (exists) or 'E' (wanted).
Idempotent: clears existing C/F/E rows before inserting.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args, get_pages_root

BATCH = 2000

CATEGORY_RE = re.compile(r"^:?category\s*:(.*)$", re.IGNORECASE | re.MULTILINE)
LINK_INNER_RE = re.compile(r"\[\[([^\]]+)\]\]")
FILE_TYPES = {"file", "pic", "cdf", "audio", "video"}


def read_text(path: str | Path) -> str:
    with open(path, "rb") as f:
        raw = f.read()
    return raw.replace(b"\x00", b"").decode("utf-8", errors="replace")


def walk_pages(pages_root: Path):
    for dirpath, dirnames, filenames in os.walk(pages_root):
        if "name" in filenames and "content.md" in filenames:
            leaf = os.path.basename(dirpath)
            if leaf.isdigit():
                content = read_text(os.path.join(dirpath, "content.md"))
                yield int(leaf), content
            dirnames[:] = []


def extract_categories(content: str) -> list[str]:
    cats = []
    for m in CATEGORY_RE.finditer(content):
        for c in m.group(1).split(","):
            c = c.strip()[:255]
            if c:
                cats.append(c)
    return cats


def extract_file_link(inner: str) -> tuple[str, str] | None:
    parts = inner.split("|")
    if len(parts) > 1:
        page_name = parts[0].strip()
        link_text = parts[1].strip()
    else:
        page_name = None
        link_text = parts[0].strip()

    if ":" not in link_text:
        return None
    file_name_from_linktext, file_type = link_text.rsplit(":", 1)
    file_type = file_type.strip().lower()
    if file_type not in FILE_TYPES:
        return None

    if page_name is not None:
        if ":" in page_name:
            return None
        return page_name.strip()[:255], file_type
    return file_name_from_linktext.strip()[:255], file_type


def extract_file_links(content: str) -> list[str]:
    files = []
    for m in LINK_INNER_RE.finditer(content):
        r = extract_file_link(m.group(1))
        if r and r[0]:
            files.append(r[0])
    return files


def run_etl_extra_reftypes(db_config: dict, pages_root: Path) -> None:
    if not pages_root.exists():
        raise FileNotFoundError(
            f"Pages directory not found: {pages_root}. Run 00_fetch_source.py first."
        )

    print(f"Reading pages from {pages_root} and extracting C/F/E references...", file=sys.stderr)
    ref_rows_cat: list[tuple[int, str, str]] = []
    file_refs: list[tuple[int, str]] = []
    n_pages = 0
    for pid, content in walk_pages(pages_root):
        n_pages += 1
        for cat in extract_categories(content):
            ref_rows_cat.append((pid, cat, "C"))
        for fname in extract_file_links(content):
            file_refs.append((pid, fname))

    print(
        f"  {n_pages:,} pages, {len(ref_rows_cat):,} category refs, {len(file_refs):,} file refs",
        file=sys.stderr,
    )

    conn = connect_db(db_config)
    cur = conn.cursor()
    try:
        cur.execute("SELECT file_name FROM wiki_files")
        existing_files = {row[0] for row in cur.fetchall()}
        print(
            f"  {len(existing_files):,} known files in wiki_files -> classifying F/E accordingly",
            file=sys.stderr,
        )

        ref_rows = list(ref_rows_cat)
        n_f = n_e = 0
        for pid, fname in file_refs:
            if fname in existing_files:
                ref_rows.append((pid, fname, "F"))
                n_f += 1
            else:
                ref_rows.append((pid, fname, "E"))
                n_e += 1
        print(f"  -> F: {n_f:,}, E: {n_e:,}", file=sys.stderr)

        print("Clearing any previous C/F/E rows (idempotent re-run)...", file=sys.stderr)
        cur.execute("DELETE FROM wiki_references WHERE link_type IN ('C', 'F', 'E')")
        conn.commit()

        print("Inserting C/F/E rows...", file=sys.stderr)
        for i in range(0, len(ref_rows), BATCH):
            chunk = ref_rows[i : i + BATCH]
            cur.executemany(
                "INSERT INTO wiki_references (created_at, updated_at, page_id, referenced_name, link_type) "
                "VALUES (NOW(), NOW(), %s, %s, %s)",
                chunk,
            )
            conn.commit()
            print(f"  {min(i + BATCH, len(ref_rows)):,}/{len(ref_rows):,}", file=sys.stderr)

        print("Step 4 (extra reference types) complete.", file=sys.stderr)
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 4: Extract C/F/E wiki_references")
    add_db_args(parser)
    parser.add_argument("--pages-root", type=Path, default=get_pages_root(), help="Path to nlab-content/pages")
    args = parser.parse_args()

    run_etl_extra_reftypes(db_config=db_config_from_args(args), pages_root=args.pages_root)


if __name__ == "__main__":
    main()
