#!/usr/bin/env python3
"""
Step 6 (Optional): Mocks coherent temporal metadata for the enriched nLab model.

Use this only if you loaded snapshot-only revisions (without full git history) or
explicitly want to regenerate monotonic random timestamps across revisions, pages,
and wiki_references.

Sequence:
  1. Randomizes `revisions.created_at` per page in monotonic chronological order
  2. Derives `pages.first_revised_at` / `pages.last_revised_at` from `revisions.created_at`
  3. Sets `pages.created_at = first_revised_at` (or seeded fallback date)
  4. Sets `pages.updated_at = last_revised_at` (or NULL if no revisions)
  5. Syncs `wiki_references.created_at = pages.created_at` for each owning page
"""

from __future__ import annotations

import argparse
import random
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args

BATCH = 500


def column_exists(cur, table: str, column: str) -> bool:
    cur.execute(
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_schema = DATABASE() AND table_name = %s AND column_name = %s",
        (table, column),
    )
    return cur.fetchone()[0] > 0


def seeded_date(page_id: int, fallback_year: int = 2008) -> datetime:
    rng = random.Random(page_id)
    start = datetime(fallback_year, 1, 1)
    end = datetime(2025, 12, 31)
    span_days = (end - start).days
    return start + timedelta(days=rng.randint(0, span_days))


def generate_revision_dates(page_id: int, count: int) -> list[datetime]:
    if count <= 0:
        return []
    rng = random.Random(page_id)
    base = seeded_date(page_id)
    if count == 1:
        return [base]

    max_span_days = rng.randint(7, 3650)
    offsets = []
    cursor = 0
    for i in range(count):
        if i == count - 1:
            cursor = max_span_days
        else:
            remaining = count - i - 1
            step = rng.randint(1, max(1, max_span_days // max(1, remaining)))
            cursor = min(max_span_days, cursor + step)
        offsets.append(cursor)
    return [base + timedelta(days=d) for d in sorted(offsets)]


def fmt_dt(dt: datetime | None) -> str | None:
    return dt.strftime("%Y-%m-%d %H:%M:%S") if dt is not None else None


def run_fix_temporal_meta(db_config: dict) -> None:
    conn = connect_db(db_config)
    cur = conn.cursor()

    try:
        if not column_exists(cur, "pages", "first_revised_at"):
            cur.execute("ALTER TABLE pages ADD COLUMN first_revised_at DATETIME DEFAULT NULL AFTER revision_count")
            conn.commit()
        if not column_exists(cur, "pages", "last_revised_at"):
            cur.execute("ALTER TABLE pages ADD COLUMN last_revised_at DATETIME DEFAULT NULL AFTER first_revised_at")
            conn.commit()

        print("Reading revisions grouped by page...", file=sys.stderr)
        cur.execute("SELECT id, page_id FROM revisions ORDER BY page_id, id ASC")
        revisions_by_page: dict[int, list[int]] = defaultdict(list)
        for rev_id, page_id in cur.fetchall():
            revisions_by_page[page_id].append(rev_id)

        revision_updates = []
        for page_id in sorted(revisions_by_page.keys()):
            rev_ids = revisions_by_page[page_id]
            dates = generate_revision_dates(page_id, len(rev_ids))
            for rev_id, dt in zip(rev_ids, dates):
                revision_updates.append((fmt_dt(dt), rev_id))

        if revision_updates:
            print(f"Updating {len(revision_updates):,} revisions with coherent monotonic dates...", file=sys.stderr)
            for i in range(0, len(revision_updates), BATCH):
                cur.executemany("UPDATE revisions SET created_at=%s WHERE id=%s", revision_updates[i : i + BATCH])
                conn.commit()

        print("Deriving pages.first_revised_at / last_revised_at / created_at / updated_at...", file=sys.stderr)
        cur.execute(
            "SELECT page_id, MIN(created_at), MAX(created_at) FROM revisions GROUP BY page_id"
        )
        first_by_page = {}
        last_by_page = {}
        for page_id, first_rev, last_rev in cur.fetchall():
            first_by_page[page_id] = first_rev
            last_by_page[page_id] = last_rev

        all_page_ids = set(revisions_by_page.keys())
        cur.execute("SELECT id FROM pages ORDER BY id ASC")
        for (page_id,) in cur.fetchall():
            all_page_ids.add(page_id)

        page_rows = []
        for page_id in sorted(all_page_ids):
            first_rev = first_by_page.get(page_id)
            last_rev = last_by_page.get(page_id)
            if first_rev is not None and last_rev is not None:
                created_at, updated_at = first_rev, last_rev
            elif first_rev is not None:
                created_at, updated_at = first_rev, None
            else:
                created_at, updated_at = seeded_date(page_id), None

            page_rows.append(
                (fmt_dt(first_rev), fmt_dt(last_rev), fmt_dt(created_at), fmt_dt(updated_at), page_id)
            )

        if page_rows:
            for i in range(0, len(page_rows), BATCH):
                cur.executemany(
                    "UPDATE pages SET first_revised_at=%s, last_revised_at=%s, created_at=%s, updated_at=%s WHERE id=%s",
                    page_rows[i : i + BATCH],
                )
                conn.commit()

        print("Syncing wiki_references.created_at with owning page...", file=sys.stderr)
        cur.execute("SELECT id, created_at FROM pages ORDER BY id ASC")
        page_created_at = {page_id: created_at for page_id, created_at in cur.fetchall()}

        cur.execute("SELECT id, page_id FROM wiki_references ORDER BY page_id, id ASC")
        wiki_ref_updates = [(page_created_at.get(page_id), ref_id) for ref_id, page_id in cur.fetchall()]

        if wiki_ref_updates:
            for i in range(0, len(wiki_ref_updates), BATCH):
                cur.executemany("UPDATE wiki_references SET created_at=%s WHERE id=%s", wiki_ref_updates[i : i + BATCH])
                conn.commit()

        print("Step 6 (temporal metadata sync) complete.", file=sys.stderr)
    finally:
        cur.close()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 6 (Optional): Mock coherent temporal metadata")
    add_db_args(parser)
    args = parser.parse_args()
    run_fix_temporal_meta(db_config=db_config_from_args(args))


if __name__ == "__main__":
    main()
