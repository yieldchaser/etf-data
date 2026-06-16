"""
Reconstruct data/all_history.csv from the year-partitioned Parquet store.

This is the PRE-SCRAPE hydration step in the history-isolation pipeline.
It inverts scripts/migrate_to_parquet.py: read every partition under
data/history_parquet/year=*/holdings.parquet, concatenate, and write the
result to data/all_history.csv — the exact file scraper.py expects to find
on disk so it can append today's scrape and dedup against history.

Why this exists:
    data/all_history.csv is being un-tracked from Git (the daily append was
    the single biggest driver of .git growth). The committed, SHA-256-guarded
    Parquet store is now the durable history. Each scrape run hydrates the CSV
    from Parquet before scraper.py runs, and migrate_to_parquet.py folds the
    appended rows back into Parquet afterwards. scraper.py itself is unchanged.

CSV contract (critical — do not drift):
    scraper.py reads this file with pandas.read_csv(..., keep_default_na=False,
    na_values=[]). Under that reader, an empty ticker cell is the STRING ""
    (not NaN). pandas to_csv serializes a NaN cell as an empty field, so the
    round-trip is: Parquet NaN → CSV empty cell → scraper reads "". We therefore
    write the DataFrame as-is (NaN preserved) and let to_csv do its default
    serialization. NEVER substitute a literal "" here — that would survive one
    write/read cycle but produce a different on-disk file than migrate emitted,
    breaking the byte-stability that validate_zero_loss.py proves.

Ordering:
    Partitions are sorted by year ascending, then within-year rows are left in
    their stored order. Because scraper dedups with keep="last" on
    (ETF_Ticker, ticker, Holdings_As_Of), the only thing that matters for
    correctness is that LATER scrapes appear AFTER earlier ones within the
    file — which the ascending-year concat + migrate's append semantics
    guarantee.

Usage:
    python scripts/hydrate_csv_from_parquet.py
    python scripts/hydrate_csv_from_parquet.py --dry-run     # plan only, no write
    python scripts/hydrate_csv_from_parquet.py --verify      # hydrate to temp,
                                                             # diff vs existing CSV
Exit codes:
    0  success (or dry-run printed its plan)
    1  no parquet store / no partitions found (refuse to write an empty CSV)
    2  --verify found a content mismatch vs the existing CSV
"""
from __future__ import annotations

import argparse
import filecmp
import shutil
import sys
import tempfile
from pathlib import Path

import pandas as pd

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT  = Path(__file__).resolve().parent.parent
STORE_DIR  = REPO_ROOT / "data" / "history_parquet"
TARGET_CSV = REPO_ROOT / "data" / "all_history.csv"

# Column order must match the pipeline schema scraper.py and migrate use.
# migrate_to_parquet.py writes exactly these (after dropping its helper
# 'year' column), so the partition already carries them in this order —
# but we assert explicitly to fail loud on any schema drift.
EXPECTED_COLS = ["ETF_Ticker", "ticker", "name", "weight",
                 "Holdings_As_Of", "Date_Scraped"]


def _discover_partitions(store: Path) -> list[Path]:
    """Return sorted list of year-partition parquet files, ascending by year."""
    parts = sorted(store.glob("year=*/holdings.parquet"))
    # Defensive sort by the integer year in the dir name, in case a lexicographic
    # sort ever diverges from numeric order (it won't for 4-digit years, but
    # this is cheap insurance against a future 5-digit year edge case).
    def _year(p: Path) -> int:
        try:
            return int(p.parent.name.split("=", 1)[1])
        except (ValueError, IndexError):
            return 0
    return sorted(parts, key=_year)


def reconstruct(store: Path = STORE_DIR) -> pd.DataFrame:
    """Read all partitions, concat, normalise Holdings_As_Of back to a string.

    Parquet stores Holdings_As_Of as datetime64[ns] (migrate coerces it before
    writing). The on-disk CSV stores it as a plain YYYY-MM-DD string. We restore
    the string form so the hydrated file matches what migrate originally read.
    """
    if not store.exists():
        raise FileNotFoundError(
            f"Parquet store not found at {store}. Cannot hydrate all_history.csv "
            f"— the durable history is absent. Refusing to write an empty CSV."
        )
    parts = _discover_partitions(store)
    if not parts:
        raise FileNotFoundError(
            f"No partitions (year=*/holdings.parquet) under {store}. "
            f"Store exists but is empty. Refusing to write an empty CSV."
        )

    frames = []
    for p in parts:
        df = pd.read_parquet(p)
        if list(df.columns) != EXPECTED_COLS:
            # Tolerate extra trailing columns but never silently drop a known one.
            missing = [c for c in EXPECTED_COLS if c not in df.columns]
            if missing:
                raise ValueError(
                    f"Partition {p} is missing required columns {missing}. "
                    f"Schema drift — refuse to hydrate a malformed file."
                )
            df = df[EXPECTED_COLS]
        frames.append(df)
        print(f"  read {p.parent.name}/holdings.parquet: {len(df):,} rows")

    combined = pd.concat(frames, ignore_index=True)

    # Restore YYYY-MM-DD string form for the as-of date (CSV-native).
    combined["Holdings_As_Of"] = pd.to_datetime(
        combined["Holdings_As_Of"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Enforce canonical column order.
    return combined[EXPECTED_COLS]


def write_csv(df: pd.DataFrame, target: Path = TARGET_CSV) -> None:
    """Write the hydrated DataFrame to the target CSV path.

    index=False matches scraper.py's own to_csv call (line 598 of scraper.py).
    We do NOT pass na_rep — pandas default (empty field) is what makes the
    NaN-ticker cash-collateral rows round-trip as "" under scraper's
    keep_default_na=False reader.
    """
    target.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(target, index=False)


def _fingerprint(df: pd.DataFrame) -> dict:
    """Cheap summary for the dry-run / verify log lines."""
    return {
        "rows": int(len(df)),
        "etfs": int(df["ETF_Ticker"].nunique()) if len(df) else 0,
        "asof_dates": int(df["Holdings_As_Of"].nunique()) if len(df) else 0,
        "weight_sum": float(pd.to_numeric(df["weight"], errors="coerce").sum()) if len(df) else 0.0,
        "nan_ticker_rows": int(df["ticker"].isna().sum()),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Hydrate data/all_history.csv from the Parquet store (pre-scrape step)."
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Print the plan + fingerprint; write nothing.")
    parser.add_argument("--verify", action="store_true",
                        help="Hydrate to a temp file and diff against the existing CSV. "
                             "Exit 2 on any content difference. No write to TARGET_CSV.")
    args = parser.parse_args(argv)

    print(f"Hydrating from: {STORE_DIR}")
    print(f"Target CSV:     {TARGET_CSV}")

    try:
        df = reconstruct()
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        return 1

    fp = _fingerprint(df)
    print(f"\nReconstructed: {fp['rows']:,} rows · {fp['etfs']} ETFs · "
          f"{fp['asof_dates']} as-of dates · weight Σ={fp['weight_sum']:.6f} · "
          f"{fp['nan_ticker_rows']} NaN-ticker rows (will serialize as empty cells)")

    if args.dry_run:
        print(f"\n--dry-run: would write {fp['rows']:,} rows → {TARGET_CSV}")
        return 0

    if args.verify:
        if not TARGET_CSV.exists():
            print(f"\n--verify: {TARGET_CSV} does not exist — nothing to diff against.")
            return 2
        with tempfile.TemporaryDirectory(prefix="hydrate_verify_") as tmp:
            tmp_csv = Path(tmp) / "all_history.csv"
            write_csv(df, tmp_csv)
            # Byte-compare. Both files are deterministic given the same source
            # partition + same pandas version, so filecmp is sufficient and
            # stricter than a row-by-row check.
            if filecmp.cmp(TARGET_CSV, tmp_csv, shallow=False):
                print(f"\n--verify: ✓ hydrated CSV is byte-identical to existing {TARGET_CSV}")
                return 0
            print(f"\n--verify: ✗ hydrated CSV DIFFERS from existing {TARGET_CSV}")
            print("  This is expected after the first live migrate folds the CSV "
                  "into the store; afterwards it should be stable.")
            return 2

    write_csv(df, TARGET_CSV)
    print(f"\n✓ Wrote {fp['rows']:,} rows → {TARGET_CSV}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
