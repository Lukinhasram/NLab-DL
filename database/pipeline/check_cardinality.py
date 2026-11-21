#!/usr/bin/env python3
"""
Prints exact row counts (cardinality) for every base table in the MariaDB schema.

Usage:
  python database/pipeline/check_cardinality.py
  python database/pipeline/check_cardinality.py --format csv
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

if str(Path(__file__).resolve().parent) not in sys.path:
    sys.path.insert(0, str(Path(__file__).resolve().parent))

from config import add_db_args, connect_db, db_config_from_args


def get_table_names(conn, schema: str, include_views: bool = False) -> list[str]:
    cur = conn.cursor()
    sql = """
        SELECT TABLE_NAME
        FROM information_schema.tables
        WHERE TABLE_SCHEMA = %s
    """
    if not include_views:
        sql += " AND TABLE_TYPE = 'BASE TABLE'"
    sql += " ORDER BY TABLE_NAME ASC"
    cur.execute(sql, (schema,))
    return [row[0] for row in cur.fetchall()]


def get_table_count(conn, table_name: str) -> int:
    cur = conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM `{table_name}`")
    row = cur.fetchone()
    return int(row[0]) if row else 0


def print_table_counts(rows: list[dict[str, Any]], fmt: str = "text") -> None:
    if fmt == "csv":
        writer = csv.DictWriter(sys.stdout, fieldnames=["table_name", "row_count"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
        return

    print(f"{'table_name':<30} {'row_count':>14}")
    print("-" * 46)
    for row in rows:
        print(f"{row['table_name']:<30} {row['row_count']:>14,}")


def run_check_cardinality(db_config: dict, fmt: str = "text") -> list[dict[str, Any]]:
    conn = connect_db(db_config)
    try:
        tables = get_table_names(conn, db_config["database"])
        rows = [{"table_name": t, "row_count": get_table_count(conn, t)} for t in tables]
        rows.sort(key=lambda x: (-x["row_count"], x["table_name"]))
        print_table_counts(rows, fmt=fmt)
        return rows
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Check MariaDB table row counts (cardinality)")
    add_db_args(parser)
    parser.add_argument("--format", choices=["text", "csv"], default="text", help="Output format (default: text)")
    args = parser.parse_args()
    run_check_cardinality(db_config=db_config_from_args(args), fmt=args.format)


if __name__ == "__main__":
    main()
