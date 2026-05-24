"""
Migration script: convert all_history.csv → year-partitioned Parquet store.

New layout: data/history_parquet/year=YYYY/holdings.parquet

Usage:
    python scripts/migrate_to_parquet.py
    python scripts/migrate_to_parquet.py --source data/all_history.csv
    python scripts/migrate_to_parquet.py --dry-run
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

# Platform encoding fix (Windows CI)
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = REPO_ROOT / "data" / "all_history.csv"
DEFAULT_DEST = REPO_ROOT / "data" / "history_parquet"


def migrate(
    source: Path = DEFAULT_SOURCE,
    dest: Path = DEFAULT_DEST,
    dry_run: bool = False,
) -> None:
    """Convert all_history.csv to year-partitioned Parquet."""
    if not source.exists():
        print(f"ERROR: Source file not found: {source}")
        sys.exit(1)

    print(f"Reading: {source}")
    df = pd.read_csv(source)
    print(f"  {len(df):,} rows · {df['ETF_Ticker'].nunique()} ETFs")

    # Parse dates
    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    df = df.dropna(subset=["Holdings_As_Of"])
    df["year"] = df["Holdings_As_Of"].dt.year

    years = sorted(df["year"].unique())
    print(f"  Years: {years[0]} -> {years[-1]} ({len(years)} years)")

    if dry_run:
        print("\n--dry-run: would write:")
        for yr in years:
            yr_df = df[df["year"] == yr]
            out_path = dest / f"year={yr}" / "holdings.parquet"
            print(f"  {out_path}: {len(yr_df):,} rows")
        return

    total_written = 0
    for yr in years:
        yr_df = df[df["year"] == yr].drop(columns=["year"]).copy()
        out_path = dest / f"year={yr}" / "holdings.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # If partition already exists, merge (dedup by key)
        if out_path.exists():
            existing = pd.read_parquet(out_path)
            combined = pd.concat([existing, yr_df], ignore_index=True)
            key_cols = ["ETF_Ticker", "ticker", "Holdings_As_Of"]
            combined = combined.drop_duplicates(subset=key_cols, keep="last")
            yr_df = combined

        yr_df.to_parquet(out_path, index=False, compression="snappy")
        total_written += len(yr_df)
        print(f"  Wrote {out_path.name}: {len(yr_df):,} rows → {out_path}")

    print(f"\nMigration complete: {total_written:,} rows across {len(years)} year partitions")
    print(f"Output: {dest}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Migrate all_history.csv to year-partitioned Parquet store"
    )
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help=f"Source CSV file (default: {DEFAULT_SOURCE})")
    parser.add_argument("--dest", default=str(DEFAULT_DEST),
                        help=f"Destination directory (default: {DEFAULT_DEST})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without writing")
    args = parser.parse_args(argv)

    migrate(
        source=Path(args.source),
        dest=Path(args.dest),
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
