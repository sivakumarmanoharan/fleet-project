"""
Load generated files into Bronze.

    1. PUT every file in data/backfill/<table>/ to @LANDING/<table>/
    2. Run sql/02_bronze.sql: create the raw tables, then COPY INTO each one
    3. Suspend the warehouse straight away instead of waiting 60 seconds

Safe to re-run. PUT skips files already in the stage (OVERWRITE = FALSE) and
COPY INTO skips files it already loaded, so a second run loads nothing new.

Usage
    python generator/load_bronze.py
    python generator/load_bronze.py --source data/backfill
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from snowflake_conn import ROOT, connect

SQL_FILE = ROOT / "sql" / "02_bronze.sql"


def upload(cur, source: Path) -> None:
    folders = sorted(p for p in source.iterdir() if p.is_dir() and not p.name.startswith("_"))
    if not folders:
        sys.exit(f"No table folders in {source}. Run generator/backfill.py first.")
    print("Uploading to @LANDING")
    for folder in folders:
        pattern = folder.resolve().as_posix() + "/*"
        rows = cur.execute(
            f"PUT 'file://{pattern}' @LANDING/{folder.name}/ "
            "AUTO_COMPRESS = TRUE OVERWRITE = FALSE PARALLEL = 4"
        ).fetchall()
        # PUT result columns: source, target, source_size, target_size, ..., status, message
        statuses = [r[6] for r in rows]
        print(f"  {folder.name:<22}{statuses.count('UPLOADED'):>4} uploaded"
              f"{statuses.count('SKIPPED'):>6} already there")


def run_sql(conn) -> None:
    print(f"\nRunning {SQL_FILE.relative_to(ROOT)}")
    sql = SQL_FILE.read_text(encoding="utf-8")
    last = None
    for cur in conn.execute_string(sql, remove_comments=True):
        query = " ".join(cur.query.split())
        if query.upper().startswith("COPY INTO"):
            table = query.split()[2]
            cols = [d[0].lower() for d in cur.description]
            rows = cur.fetchall()
            if cols == ["status"]:  # "Copy executed with 0 files processed."
                print(f"  COPY {table:<20}   0 new files")
                continue
            loaded = sum(r[cols.index("rows_loaded")] for r in rows)
            errors = sum(r[cols.index("errors_seen")] for r in rows)
            print(f"  COPY {table:<20}{len(rows):>4} new files{loaded:>10,} rows  {errors} errors")
        last = cur
    print("\nBronze row counts")
    for table, count in last.fetchall():
        print(f"  {table:<22}{count:>10,}")


def main():
    ap = argparse.ArgumentParser(description="Upload generated files and load them into Bronze.")
    ap.add_argument("--source", type=Path, default=ROOT / "data" / "backfill")
    args = ap.parse_args()

    started = time.time()
    with connect() as conn:
        cur = conn.cursor()
        upload(cur, args.source)
        run_sql(conn)
        try:
            cur.execute("ALTER WAREHOUSE FLEET_WH SUSPEND")
            print("\nWarehouse FLEET_WH suspended.")
        except Exception:
            print("\nWarehouse FLEET_WH already suspended.")
    print(f"Done in {time.time() - started:.0f} s")


if __name__ == "__main__":
    main()
