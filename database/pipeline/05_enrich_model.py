#!/usr/bin/env python3
"""
Step 5: Enriches the core nLab MariaDB schema into the final 7-table relational model.

Transformations applied (idempotent):
  1. `users` (new table)       - distinct authors aggregated from `revisions`
  2. `categories` (new table)  - distinct 'C' references aggregated from `wiki_references`
  3. `wiki_files.file_type`    - backfilled from wikitext markup (file/pic/cdf/audio/video)
  4. `pages` metrics           - adds `revision_count`, `first_revised_at`, `last_revised_at`,
                                 and syncs `pages.created_at`/`updated_at` and `webs.created_at`/`updated_at`
  5. `wiki_references` FKs     - adds & resolves `target_page_id`, `category_id`, `file_id`
  6. `revisions.author_id`     - normalizes `revisions.author` -> `revisions.author_id` (FK -> users)
  7. Foreign keys              - adds all 8 InnoDB FOREIGN KEY constraints
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import pymysql

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args, get_pages_root

LINK_INNER_RE = re.compile(r"\[\[([^\]]+)\]\]")
FILE_TYPES = {"file", "pic", "cdf", "audio", "video"}


def extract_file_type_pairs(content: str) -> list[tuple[str, str]]:
    out = []
    for m in LINK_INNER_RE.finditer(content):
        inner = m.group(1)
        parts = inner.split("|")
        if len(parts) > 1:
            page_name, link_text = parts[0].strip(), parts[1].strip()
        else:
            page_name, link_text = None, parts[0].strip()
        if ":" not in link_text:
            continue
        fname_from_lt, ftype = link_text.rsplit(":", 1)
        ftype = ftype.strip().lower()
        if ftype not in FILE_TYPES:
            continue
        if page_name is not None:
            if ":" in page_name:
                continue
            out.append((page_name.strip()[:255], ftype))
        else:
            out.append((fname_from_lt.strip()[:255], ftype))
    return out


def run(cur, sql: str, why: str) -> None:
    print(f"-- {why}", file=sys.stderr)
    cur.execute(sql)


def column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone()[0] > 0


def fk_exists(cur, table: str, fk_name: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.table_constraints "
        "WHERE table_schema = DATABASE() AND table_name = %s "
        "AND constraint_name = %s AND constraint_type = 'FOREIGN KEY'",
        (table, fk_name),
    )
    return cur.fetchone()[0] > 0


def run_enrich_model(db_config: dict, pages_root: Path) -> None:
    conn = connect_db(db_config)
    cur = conn.cursor()

    try:
        run(cur, "SET FOREIGN_KEY_CHECKS=0", "disabling FK checks for users/categories rebuild")

        # ---- 1. users -------------------------------------------------------
        author_col_exists = column_exists(cur, "revisions", "author")

        if author_col_exists:
            run(cur, "DROP TABLE IF EXISTS users", "drop users (initial migration)")
            run(
                cur,
                """
                CREATE TABLE users (
                    id INT NOT NULL AUTO_INCREMENT,
                    name VARCHAR(60) NOT NULL,
                    total_revisions INT NOT NULL DEFAULT 0,
                    pages_created INT NOT NULL DEFAULT 0,
                    first_edit_at DATETIME NOT NULL,
                    last_edit_at DATETIME NOT NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY index_users_on_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """,
                "creating users table",
            )
            run(
                cur,
                """
                INSERT INTO users (name, total_revisions, first_edit_at, last_edit_at)
                SELECT author, COUNT(*), MIN(revised_at), MAX(revised_at)
                FROM revisions
                WHERE author IS NOT NULL AND author <> ''
                GROUP BY author
                """,
                "populating users from revisions.author",
            )
            conn.commit()
        else:
            run(
                cur,
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INT NOT NULL AUTO_INCREMENT,
                    name VARCHAR(60) NOT NULL,
                    total_revisions INT NOT NULL DEFAULT 0,
                    pages_created INT NOT NULL DEFAULT 0,
                    first_edit_at DATETIME NOT NULL,
                    last_edit_at DATETIME NOT NULL,
                    PRIMARY KEY (id),
                    UNIQUE KEY index_users_on_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
                """,
                "ensuring users table exists (preserving stable IDs)",
            )
            run(
                cur,
                """
                UPDATE users u
                JOIN (
                    SELECT author_id, COUNT(*) n, MIN(revised_at) mn, MAX(revised_at) mx
                    FROM revisions WHERE author_id IS NOT NULL GROUP BY author_id
                ) r ON r.author_id = u.id
                SET u.total_revisions = r.n, u.first_edit_at = r.mn, u.last_edit_at = r.mx
                """,
                "refreshing users aggregates via author_id",
            )
            conn.commit()

        if author_col_exists:
            run(cur, "DROP TEMPORARY TABLE IF EXISTS first_page_author", "clearing temp table")
            run(
                cur,
                """
                CREATE TEMPORARY TABLE first_page_author AS
                SELECT page_id, SUBSTRING_INDEX(
                    GROUP_CONCAT(author ORDER BY revised_at ASC), ',', 1
                ) AS first_author
                FROM revisions
                GROUP BY page_id
                """,
                "computing first author per page (by name)",
            )
            run(
                cur,
                """
                UPDATE users u
                JOIN (
                    SELECT first_author, COUNT(*) n FROM first_page_author GROUP BY first_author
                ) t ON t.first_author = u.name
                SET u.pages_created = t.n
                """,
                "populating users.pages_created",
            )
        else:
            run(cur, "DROP TEMPORARY TABLE IF EXISTS first_page_author", "clearing temp table")
            run(
                cur,
                """
                CREATE TEMPORARY TABLE first_page_author AS
                SELECT page_id, SUBSTRING_INDEX(
                    GROUP_CONCAT(author_id ORDER BY revised_at ASC), ',', 1
                ) AS first_author_id
                FROM revisions
                WHERE author_id IS NOT NULL
                GROUP BY page_id
                """,
                "computing first author per page (by author_id)",
            )
            run(
                cur,
                """
                UPDATE users u
                JOIN (
                    SELECT first_author_id, COUNT(*) n FROM first_page_author GROUP BY first_author_id
                ) t ON t.first_author_id = u.id
                SET u.pages_created = t.n
                """,
                "refreshing users.pages_created",
            )
        conn.commit()

        # ---- 2. categories --------------------------------------------------
        run(cur, "DROP TABLE IF EXISTS categories", "drop categories (idempotent)")
        run(
            cur,
            """
            CREATE TABLE categories (
                id INT NOT NULL AUTO_INCREMENT,
                name VARCHAR(255) NOT NULL,
                page_count INT NOT NULL DEFAULT 0,
                PRIMARY KEY (id),
                UNIQUE KEY index_categories_on_name (name)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
            """,
            "creating categories table",
        )
        run(
            cur,
            """
            INSERT INTO categories (name, page_count)
            SELECT referenced_name, COUNT(DISTINCT page_id)
            FROM wiki_references
            WHERE link_type = 'C'
            GROUP BY referenced_name
            """,
            "populating categories from wiki_references (C)",
        )
        conn.commit()

        run(cur, "SET FOREIGN_KEY_CHECKS=1", "re-enabling FK checks")

        # ---- 3. wiki_files.file_type ----------------------------------------
        if not column_exists(cur, "wiki_files", "file_type"):
            run(
                cur,
                "ALTER TABLE wiki_files ADD COLUMN file_type VARCHAR(10) DEFAULT NULL AFTER file_name",
                "adding wiki_files.file_type",
            )
            conn.commit()

        if pages_root.exists():
            print(f"Scanning {pages_root} to backfill wiki_files.file_type...", file=sys.stderr)
            type_votes: dict[str, dict[str, int]] = {}
            for dirpath, dirnames, filenames in os.walk(pages_root):
                if "content.md" in filenames and os.path.basename(dirpath).isdigit():
                    with open(os.path.join(dirpath, "content.md"), "rb") as f:
                        content = f.read().replace(b"\x00", b"").decode("utf-8", errors="replace")
                    for fname, ftype in extract_file_type_pairs(content):
                        type_votes.setdefault(fname, {}).setdefault(ftype, 0)
                        type_votes[fname][ftype] += 1
                    dirnames[:] = []

            rows = [
                (max(votes.items(), key=lambda kv: kv[1])[0], fname)
                for fname, votes in type_votes.items()
            ]
            if rows:
                cur.executemany("UPDATE wiki_files SET file_type=%s WHERE file_name=%s", rows)
                conn.commit()
            print(f"  file_type backfilled for {len(rows):,} file names", file=sys.stderr)
        else:
            print(f"  Skipping file_type scan ({pages_root} not found).", file=sys.stderr)

        # ---- 4. pages: denormalized revision metrics & real timestamps ------
        if not column_exists(cur, "pages", "revision_count"):
            run(
                cur,
                """
                ALTER TABLE pages
                    ADD COLUMN revision_count INT NOT NULL DEFAULT 0,
                    ADD COLUMN first_revised_at DATETIME DEFAULT NULL,
                    ADD COLUMN last_revised_at DATETIME DEFAULT NULL
                """,
                "adding denormalized revision metrics to pages",
            )
            conn.commit()

        run(
            cur,
            """
            UPDATE pages p
            JOIN (
                SELECT page_id, COUNT(*) n, MIN(revised_at) first_rev, MAX(revised_at) last_rev
                FROM revisions GROUP BY page_id
            ) r ON r.page_id = p.id
            SET p.revision_count = r.n,
                p.first_revised_at = r.first_rev,
                p.last_revised_at = r.last_rev,
                p.created_at = r.first_rev,
                p.updated_at = r.last_rev
            """,
            "populating pages metrics & timestamps from revisions",
        )
        conn.commit()

        run(
            cur,
            """
            UPDATE webs w
            JOIN (
                SELECT web_id, MIN(revised_at) first_rev, MAX(revised_at) last_rev
                FROM revisions GROUP BY web_id
            ) r ON r.web_id = w.id
            SET w.created_at = r.first_rev,
                w.updated_at = r.last_rev
            """,
            "syncing webs created_at/updated_at from revisions",
        )
        conn.commit()

        # ---- 5. wiki_references: target_page_id / category_id / file_id -----
        if not column_exists(cur, "wiki_references", "target_page_id"):
            run(
                cur,
                """
                ALTER TABLE wiki_references
                    ADD COLUMN target_page_id INT DEFAULT NULL,
                    ADD COLUMN category_id INT DEFAULT NULL,
                    ADD COLUMN file_id INT DEFAULT NULL
                """,
                "adding target_page_id/category_id/file_id to wiki_references",
            )
            conn.commit()

        run(
            cur,
            """
            UPDATE wiki_references wr
            JOIN pages p ON p.name = wr.referenced_name
            JOIN pages src ON src.id = wr.page_id AND src.web_id = p.web_id
            SET wr.target_page_id = p.id
            WHERE wr.link_type IN ('L', 'I')
            """,
            "resolving target_page_id for links (L) and includes (I) via direct name match",
        )
        conn.commit()

        run(
            cur,
            """
            UPDATE wiki_references wr
            JOIN wiki_references redir ON redir.referenced_name = wr.referenced_name AND redir.link_type = 'R'
            SET wr.target_page_id = redir.page_id
            WHERE wr.link_type IN ('L', 'I') AND wr.target_page_id IS NULL
            """,
            "resolving remaining target_page_id via redirect aliases (R)",
        )
        conn.commit()

        run(
            cur,
            """
            UPDATE wiki_references wr
            JOIN categories c ON c.name = wr.referenced_name
            SET wr.category_id = c.id
            WHERE wr.link_type = 'C'
            """,
            "resolving category_id",
        )
        conn.commit()

        run(
            cur,
            """
            UPDATE wiki_references wr
            JOIN wiki_files wf ON wf.file_name = wr.referenced_name
            SET wr.file_id = wf.id
            WHERE wr.link_type IN ('F', 'E')
            """,
            "resolving file_id",
        )
        conn.commit()

        # ---- 6. revisions.author -> revisions.author_id ---------------------
        if column_exists(cur, "revisions", "author"):
            if not column_exists(cur, "revisions", "author_id"):
                run(
                    cur,
                    "ALTER TABLE revisions ADD COLUMN author_id INT DEFAULT NULL AFTER author",
                    "adding revisions.author_id",
                )
                conn.commit()

            run(
                cur,
                """
                UPDATE revisions r
                JOIN users u ON u.name = r.author
                SET r.author_id = u.id
                """,
                "resolving revisions.author_id from users",
            )
            conn.commit()

            run(
                cur,
                "ALTER TABLE revisions DROP COLUMN author",
                "dropping redundant revisions.author column",
            )
            conn.commit()
        else:
            print("-- revisions.author already migrated to author_id; skipping.", file=sys.stderr)

        # ---- 7. Foreign key constraints -------------------------------------
        fks = [
            ("pages", "fk_pages_web", "web_id", "webs(id)"),
            ("wiki_files", "fk_wiki_files_web", "web_id", "webs(id)"),
            ("revisions", "fk_revisions_page", "page_id", "pages(id)"),
            ("revisions", "fk_revisions_author", "author_id", "users(id)"),
            ("wiki_references", "fk_wr_page", "page_id", "pages(id)"),
            ("wiki_references", "fk_wr_target_page", "target_page_id", "pages(id)"),
            ("wiki_references", "fk_wr_category", "category_id", "categories(id)"),
            ("wiki_references", "fk_wr_file", "file_id", "wiki_files(id)"),
        ]
        for table, fk_name, col, ref in fks:
            if fk_exists(cur, table, fk_name):
                print(f"-- FK {fk_name} already exists; skipping.", file=sys.stderr)
                continue
            try:
                run(
                    cur,
                    f"ALTER TABLE {table} ADD CONSTRAINT {fk_name} FOREIGN KEY ({col}) REFERENCES {ref}",
                    f"adding FK {table}.{col} -> {ref}",
                )
                conn.commit()
            except pymysql.err.Error as exc:
                print(f"  WARNING: FK {table}.{col} -> {ref} failed: {exc}", file=sys.stderr)
                conn.rollback()

        print("Step 5 (model enrichment) complete.", file=sys.stderr)
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 5: Enrich MariaDB schema with users, categories, metrics & FKs")
    add_db_args(parser)
    parser.add_argument("--pages-root", type=Path, default=get_pages_root(), help="Path to nlab-content/pages")
    args = parser.parse_args()

    run_enrich_model(db_config=db_config_from_args(args), pages_root=args.pages_root)


if __name__ == "__main__":
    main()
