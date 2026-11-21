#!/usr/bin/env python3
"""
End-to-end orchestrator to rebuild the complete nLab MariaDB database.

Runs the full pipeline in order:
  0. Fetch upstream nlab-content git mirror & generate full_log.txt
  1. Base ETL: webs, pages, wiki_references (L/W/R/I)
  2. Revisions ETL: full chronological git commit history
  3. Mock wiki_files from earliest citation in revisions
  4. Extra wiki_references: categories (C) and file links (F/E)
  5. Enrich model: users, categories, file_type, page metrics, resolved FKs & constraints
  6. (Optional) Synthetic temporal metadata sync (--with-temporal-mock)
  7. Scale to 1,000,000 pages (or --target-pages N; skip with --skip-scale)
  8. Print final table cardinality

Examples:
  # Fast path: restore directly from data/dumps/dump_completo.sql
  python database/pipeline/rebuild_db.py --from-dump

  # Full rebuild from scratch up to 1M pages
  python database/pipeline/rebuild_db.py

  # Rebuild real nLab data only (~20.7k pages, ~206k revisions), without synthetic scaling
  python database/pipeline/rebuild_db.py --skip-scale

  # Fast dev mode (snapshot revisions only, no git history walk)
  python database/pipeline/rebuild_db.py --snapshot-only --skip-scale
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from pathlib import Path

PIPELINE_DIR = Path(__file__).resolve().parent
if str(PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(PIPELINE_DIR))

from config import (
    add_db_args,
    db_config_from_args,
    get_container_name,
    get_content_dir,
    get_dump_file,
    get_git_log_file,
    get_pages_root,
)


def _load_module(filename: str):
    path = PIPELINE_DIR / filename
    spec = importlib.util.spec_from_file_location(path.stem, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def banner(title: str) -> None:
    print(f"\n{'=' * 72}", file=sys.stderr)
    print(f"  {title}", file=sys.stderr)
    print(f"{'=' * 72}", file=sys.stderr)


def main() -> None:
    default_target = int(os.getenv("TARGET_PAGES", "1000000"))

    parser = argparse.ArgumentParser(description="Rebuild the complete nLab MariaDB database")
    add_db_args(parser)
    parser.add_argument("--from-dump", action="store_true", help="Restore directly from SQL dump instead of running ETL from scratch")
    parser.add_argument("--dump-file", type=Path, default=get_dump_file(), help=f"Path to SQL dump for --from-dump (default: {get_dump_file()})")
    parser.add_argument("--container", default=get_container_name(), help=f"MariaDB Docker container name for --from-dump (default: {get_container_name()})")
    parser.add_argument("--use-local-client", action="store_true", help="Use local mariadb/mysql CLI instead of docker exec for --from-dump")
    parser.add_argument("--content-dir", type=Path, default=get_content_dir(), help="Path to nlab-content clone")
    parser.add_argument("--log-file", type=Path, default=get_git_log_file(), help="Path to full_log.txt")
    parser.add_argument("--target-pages", type=int, default=default_target, help=f"Target pages for Step 7 (default: {default_target})")
    parser.add_argument("--batch-size", type=int, default=10000, help="Batch size for Step 7 (default: 10000)")
    parser.add_argument("--skip-fetch", action="store_true", help="Skip Step 0 (assume nlab-content is already cloned)")
    parser.add_argument("--snapshot-only", action="store_true", help="Load 1 snapshot revision per page instead of full git history")
    parser.add_argument("--with-temporal-mock", action="store_true", help="Run optional Step 6 temporal metadata mock")
    parser.add_argument("--skip-scale", action="store_true", help="Skip Step 7 (keep only real nLab pages)")
    parser.add_argument("--reset", action="store_true", help="Drop/recreate DB (with --from-dump) or truncate core tables in Step 1")
    args = parser.parse_args()

    db_config = db_config_from_args(args)
    t_start = time.time()

    if args.from_dump:
        banner(f"Restoring MariaDB from dump: {args.dump_file}")
        mr = _load_module("restore_dump.py")
        mr.run_restore_dump(
            db_config=db_config,
            dump_file=args.dump_file,
            container=args.container,
            use_local_client=args.use_local_client,
            recreate_db=args.reset,
            verify=True,
        )
        return

    pages_root = args.content_dir / "pages" if args.content_dir != get_content_dir() else get_pages_root()

    if not args.skip_fetch:
        banner("Step 0: Fetch nlab-content source & git log")
        m0 = _load_module("00_fetch_source.py")
        m0.fetch_source(content_dir=args.content_dir, log_file=args.log_file)

    banner("Step 1: Base ETL (webs, pages, wiki_references L/W/R/I)")
    m1 = _load_module("01_etl_base.py")
    m1.run_etl_base(
        db_config=db_config,
        pages_root=pages_root,
        skip_revisions=not args.snapshot_only,
        reset=args.reset,
    )

    if not args.snapshot_only:
        banner("Step 2: Full git revision history ETL")
        m2 = _load_module("02_etl_revisions.py")
        m2.run_etl_revisions(
            db_config=db_config,
            content_dir=args.content_dir,
            log_file=args.log_file,
            truncate=args.reset,
        )

    banner("Step 3: Mock wiki_files from earliest revision citations")
    m3 = _load_module("03_mock_wiki_files.py")
    m3.run_mock_wiki_files(db_config=db_config)

    banner("Step 4: Extract C (category) and F/E (file) wiki_references")
    m4 = _load_module("04_etl_extra_reftypes.py")
    m4.run_etl_extra_reftypes(db_config=db_config, pages_root=pages_root)

    banner("Step 5: Enrich relational model (users, categories, metrics & FKs)")
    m5 = _load_module("05_enrich_model.py")
    m5.run_enrich_model(db_config=db_config, pages_root=pages_root)

    if args.with_temporal_mock:
        banner("Step 6 (Optional): Mock coherent temporal metadata")
        m6 = _load_module("06_fix_temporal_meta.py")
        m6.run_fix_temporal_meta(db_config=db_config)

    if not args.skip_scale:
        banner(f"Step 7: Scale database to {args.target_pages:,} pages")
        m7 = _load_module("07_generate_1m_pages.py")
        m7.run_generate_pages(
            db_config=db_config,
            target_pages=args.target_pages,
            batch_size=args.batch_size,
        )

    banner("Verification: Final table cardinality")
    mc = _load_module("check_cardinality.py")
    mc.run_check_cardinality(db_config=db_config)

    elapsed = time.time() - t_start
    print(f"\nFull database rebuild completed in {elapsed:.1f}s ({elapsed / 60:.2f} min).", file=sys.stderr)


if __name__ == "__main__":
    main()
