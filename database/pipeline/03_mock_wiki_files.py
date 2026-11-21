#!/usr/bin/env python3
"""
Step 3: Populates `wiki_files` from the earliest citation of each file in `revisions`.

Because upstream nLab does not publish upload metadata, this step derives a
coherent synthetic upload timestamp and description from the first revision that
references each [[...:file|pic|cdf|audio|video]] link.

Run Step 4 (`04_etl_extra_reftypes.py`) afterwards so F/E references are
classified against the populated `wiki_files` table.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pymysql

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args

WEB_ID = 1
BATCH = 2000

LINK_INNER_RE = re.compile(r"\[\[([^\]]+)\]\]")
FILE_TYPES = {"file", "pic", "cdf", "audio", "video"}
GENERIC_DESCRIPTIONS = {"pdf", "here", "djvu", "pdf-format", "ps", "link", "this"}


def extract_file_link(inner: str) -> tuple[str, str, str | None] | None:
    """Returns (file_name, file_type, inline_description_or_None)."""
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
        desc = file_name_from_linktext.strip() or None
        return page_name.strip()[:255], file_type, desc
    return file_name_from_linktext.strip()[:255], file_type, None


def extract_file_refs(content: str) -> list[tuple[str, str | None]]:
    out = []
    for m in LINK_INNER_RE.finditer(content):
        r = extract_file_link(m.group(1))
        if r and r[0]:
            out.append((r[0], r[2]))
    return out


def column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone()[0] > 0


def run_mock_wiki_files(db_config: dict) -> None:
    conn = connect_db(db_config)
    try:
        meta_cur = conn.cursor()
        has_author_col = column_exists(meta_cur, "revisions", "author")
        meta_cur.close()

        read_cur = conn.cursor(pymysql.cursors.SSCursor)
        print("Streaming all revisions in chronological order...", file=sys.stderr)
        if has_author_col:
            read_cur.execute(
                "SELECT revised_at, page_id, author, content FROM revisions ORDER BY revised_at ASC"
            )
        else:
            read_cur.execute(
                "SELECT r.revised_at, r.page_id, COALESCE(u.name, 'unknown'), r.content "
                "FROM revisions r LEFT JOIN users u ON u.id = r.author_id "
                "ORDER BY r.revised_at ASC"
            )

        first_seen: dict[str, tuple] = {}
        best_desc: dict[str, str] = {}
        n_rows = 0
        for revised_at, page_id, author, content in read_cur:
            n_rows += 1
            if not content:
                continue
            for fname, desc in extract_file_refs(content):
                if fname not in first_seen:
                    first_seen[fname] = (revised_at, page_id, author or "unknown")
                if desc:
                    cur_best = best_desc.get(fname)
                    is_better = cur_best is None or (
                        desc.lower() not in GENERIC_DESCRIPTIONS
                        and (
                            cur_best.lower() in GENERIC_DESCRIPTIONS
                            or len(desc) > len(cur_best)
                        )
                    )
                    if is_better:
                        best_desc[fname] = desc
            if n_rows % 20000 == 0:
                print(
                    f"  processed {n_rows:,} revisions, {len(first_seen):,} distinct files found",
                    file=sys.stderr,
                )

        read_cur.close()
        print(
            f"Done streaming: {n_rows:,} revisions processed, {len(first_seen):,} distinct file names found.",
            file=sys.stderr,
        )

        name_cur = conn.cursor()
        name_cur.execute("SELECT id, name FROM pages")
        page_names = {pid: name for pid, name in name_cur.fetchall()}
        name_cur.close()

        write_cur = conn.cursor()
        print("Clearing wiki_files (idempotent re-run)...", file=sys.stderr)
        write_cur.execute("SET FOREIGN_KEY_CHECKS=0")
        write_cur.execute("DELETE FROM wiki_files")
        write_cur.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.commit()

        print("Inserting mocked wiki_files rows...", file=sys.stderr)
        rows = []
        for fname, (revised_at, page_id, author) in first_seen.items():
            page_name = page_names.get(page_id, f"page #{page_id}")
            desc = best_desc.get(fname)
            base = (
                f"mock: primeira citação em '{page_name}' por {author} "
                f"em {revised_at.strftime('%Y-%m-%d')}"
            )
            description = f'{base} — descrição do link: "{desc}"' if desc else base
            rows.append((revised_at, revised_at, WEB_ID, fname[:255], description[:255]))

        for i in range(0, len(rows), BATCH):
            write_cur.executemany(
                "INSERT INTO wiki_files (created_at, updated_at, web_id, file_name, description) "
                "VALUES (%s, %s, %s, %s, %s)",
                rows[i : i + BATCH],
            )
            conn.commit()
            print(f"  {min(i + BATCH, len(rows)):,}/{len(rows):,}", file=sys.stderr)

        write_cur.close()
        print("Step 3 (mock wiki_files) complete.", file=sys.stderr)
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 3: Populate wiki_files from revision citations")
    add_db_args(parser)
    args = parser.parse_args()
    run_mock_wiki_files(db_config=db_config_from_args(args))


if __name__ == "__main__":
    main()
