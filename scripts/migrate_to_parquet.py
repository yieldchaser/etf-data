"""
Migration script: convert all_history.csv → year-partitioned Parquet store.

Layout: data/history_parquet/year=YYYY/holdings.parquet
        data/history_parquet/CHECKSUMS.json   (manifest — see below)

This script is the maintainer of the **permanent year-partitioned archive**.
The working CSV (data/all_history.csv) is allowed to be a rolling recent
window; the parquet store is the durable, append-only history.

Immutability contract (Part 3 of the v2 charter):

    Past-year partitions (year < current_year) are immutable. Once a year
    closes, its `holdings.parquet` MUST NOT change byte-for-byte. The
    `CHECKSUMS.json` manifest at the store root records the SHA-256 of
    each partition; `tests/test_parquet_immutability.py` recomputes those
    hashes on every CI run and fails if any prior-year partition has
    drifted.

    Current-year partition is mutable (it grows daily as new scrapes land).

    To intentionally fix a historical mistake, pass --allow-historical-rewrite.
    Any such rewrite logs a loud warning and updates the manifest.

Usage:
    python scripts/migrate_to_parquet.py
    python scripts/migrate_to_parquet.py --source data/all_history.csv
    python scripts/migrate_to_parquet.py --dry-run
    python scripts/migrate_to_parquet.py --verify-only          # check manifest, no writes
    python scripts/migrate_to_parquet.py --allow-historical-rewrite   # force-rewrite past years
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Platform encoding fix (Windows CI)
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT     = Path(__file__).resolve().parent.parent
DEFAULT_SOURCE = REPO_ROOT / "data" / "all_history.csv"
DEFAULT_DEST   = REPO_ROOT / "data" / "history_parquet"
MANIFEST_NAME  = "CHECKSUMS.json"


# ─── Checksum + manifest helpers ─────────────────────────────────────────────

def _sha256_of(path: Path) -> str:
    """Streaming SHA-256 of a file, hex-encoded."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def _load_manifest(dest: Path) -> dict:
    """Load CHECKSUMS.json or return an empty manifest skeleton."""
    path = dest / MANIFEST_NAME
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Could not parse {path} ({e}). Starting fresh manifest.")
    return {
        "schema_version":  1,
        "generated_at":    "",
        "partitions":      {},   # "2026": {sha256, rows, first_date, last_date, etfs, file_size}
    }


def _write_manifest(dest: Path, manifest: dict) -> None:
    """Atomic-write the manifest with ISO timestamp."""
    manifest["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    path = dest / MANIFEST_NAME
    tmp  = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def _partition_summary(parquet_path: Path, year: int) -> dict:
    """Compute the manifest record for a partition: sha256 + row stats."""
    df = pd.read_parquet(parquet_path)
    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    return {
        "sha256":     _sha256_of(parquet_path),
        "rows":       int(len(df)),
        "etfs":       int(df["ETF_Ticker"].nunique()) if "ETF_Ticker" in df.columns else 0,
        "first_date": df["Holdings_As_Of"].min().strftime("%Y-%m-%d") if not df.empty else None,
        "last_date":  df["Holdings_As_Of"].max().strftime("%Y-%m-%d") if not df.empty else None,
        "file_size":  parquet_path.stat().st_size,
        "year":       year,
    }


# ─── Verify-only path ─────────────────────────────────────────────────────────

def verify(dest: Path = DEFAULT_DEST) -> int:
    """
    Recompute SHA-256 for each partition listed in the manifest and report
    drift. Returns 0 if all OK or no manifest exists; 1 on any drift.

    Drift in current-year partitions is informational only; drift in
    prior-year partitions is a HARD FAILURE — those partitions are
    contractually immutable.
    """
    manifest = _load_manifest(dest)
    if not manifest.get("partitions"):
        print(f"  No manifest at {dest / MANIFEST_NAME} — nothing to verify (system not yet bootstrapped).")
        return 0

    current_year = datetime.now(timezone.utc).year
    drift_past:    list[tuple[str, str, str]] = []   # (year, expected, actual)
    drift_current: list[tuple[str, str, str]] = []
    ok_past, ok_current = 0, 0

    for year_str, record in manifest["partitions"].items():
        try:
            year = int(year_str)
        except ValueError:
            continue
        parquet_path = dest / f"year={year_str}" / "holdings.parquet"
        if not parquet_path.exists():
            print(f"  ::error:: partition file missing: {parquet_path}")
            if year < current_year:
                drift_past.append((year_str, record.get("sha256", ""), "<missing>"))
            else:
                drift_current.append((year_str, record.get("sha256", ""), "<missing>"))
            continue

        actual = _sha256_of(parquet_path)
        expected = record.get("sha256", "")
        if actual == expected:
            if year < current_year:
                ok_past += 1
            else:
                ok_current += 1
            continue
        if year < current_year:
            drift_past.append((year_str, expected, actual))
        else:
            drift_current.append((year_str, expected, actual))

    print(f"\n  Past-year partitions:    OK={ok_past}, drift={len(drift_past)}")
    print(f"  Current-year partitions: OK={ok_current}, drift={len(drift_current)} (drift is allowed)")

    if drift_past:
        print("\n  ❌ IMMUTABILITY VIOLATION — past-year partitions changed:")
        for yr, exp, act in drift_past:
            print(f"    year={yr}: expected sha256={exp[:16]}…  got={act[:16] if act != '<missing>' else act}")
        print("\n  These years are contractually frozen. If a fix is genuinely required,")
        print("  re-run with --allow-historical-rewrite (this is a privileged operation).")
        return 1

    if drift_current:
        print("\n  Current-year partition has drifted vs manifest (this is normal — re-run migrate to refresh).")

    return 0


# ─── Migration path ──────────────────────────────────────────────────────────

def migrate(
    source: Path = DEFAULT_SOURCE,
    dest:   Path = DEFAULT_DEST,
    dry_run: bool = False,
    allow_historical_rewrite: bool = False,
) -> int:
    """Convert all_history.csv to year-partitioned Parquet, immutability-guarded."""
    if not source.exists():
        print(f"ERROR: Source file not found: {source}")
        return 1

    print(f"Reading: {source}")
    # keep_default_na=False aligns with the bridge's reader convention
    # (scraper.py:193/362/599): "NA"/"NULL" tickers stay verbatim strings,
    # keeping composite keys stable and hydrate byte-stability intact.
    df = pd.read_csv(source, keep_default_na=False, na_values=[])
    print(f"  {len(df):,} rows · {df['ETF_Ticker'].nunique()} ETFs")

    df["Holdings_As_Of"] = pd.to_datetime(df["Holdings_As_Of"], errors="coerce")
    df = df.dropna(subset=["Holdings_As_Of"])
    df["year"] = df["Holdings_As_Of"].dt.year
    years = sorted(df["year"].unique())
    print(f"  Years in CSV: {years}")

    current_year = datetime.now(timezone.utc).year
    manifest     = _load_manifest(dest)

    if dry_run:
        print("\n--dry-run: would write:")
        for yr in years:
            yr_df = df[df["year"] == yr]
            out_path = dest / f"year={yr}" / "holdings.parquet"
            note = "current-year (mutable)" if int(yr) == current_year else \
                   ("PAST-YEAR (would be REWRITTEN — requires --allow-historical-rewrite)"
                    if str(yr) in manifest.get("partitions", {}) and not allow_historical_rewrite
                    else "past-year (new partition, allowed)")
            print(f"  {out_path}: {len(yr_df):,} rows  [{note}]")
        return 0

    total_written = 0
    refusals: list[int] = []
    refused_frames: list = []

    for yr in years:
        yr_int   = int(yr)
        yr_str   = str(yr_int)
        yr_df    = df[df["year"] == yr].drop(columns=["year"]).copy()
        out_path = dest / f"year={yr_int}" / "holdings.parquet"
        out_path.parent.mkdir(parents=True, exist_ok=True)

        # ── Immutability guard ──────────────────────────────────────────
        # A past-year partition is locked once the manifest records it.
        # We REFUSE to overwrite it unless the operator explicitly opts in.
        is_past_year      = yr_int < current_year
        manifest_record   = manifest.get("partitions", {}).get(yr_str)
        is_locked         = is_past_year and manifest_record is not None
        if is_locked and not allow_historical_rewrite:
            print(f"  LOCKED  year={yr_int}: past-year partition is contractually immutable. "
                  f"Skipping write (use --allow-historical-rewrite to override).")
            refusals.append(yr_int)
            refused_frames.append(yr_df)
            continue
        if is_locked and allow_historical_rewrite:
            print(f"  ⚠️  REWRITING year={yr_int}: past-year partition (manifest will be updated). "
                  f"This is a privileged operation.")

        # ── Merge with existing partition (dedup by composite key) ──────
        # Same logic as before: scraper may emit duplicates when retried;
        # composite key (ETF, ticker, Holdings_As_Of) ensures we keep the
        # latest scrape for any given (ETF, ticker, day) combination.
        if out_path.exists():
            try:
                existing = pd.read_parquet(out_path)
                combined = pd.concat([existing, yr_df], ignore_index=True)
                key_cols = ["ETF_Ticker", "ticker", "Holdings_As_Of"]
                combined = combined.drop_duplicates(subset=key_cols, keep="last")
                yr_df = combined
            except Exception as e:
                print(f"  WARNING: could not merge with existing {out_path}: {e}")

        yr_df.to_parquet(out_path, index=False, compression="snappy")
        total_written += len(yr_df)

        # ── Update manifest ──────────────────────────────────────────────
        manifest.setdefault("partitions", {})[yr_str] = _partition_summary(out_path, yr_int)
        marker = "current" if yr_int == current_year else "past"
        print(f"  Wrote   year={yr_int}: {len(yr_df):,} rows → {out_path}  [{marker}]")

    # Schema bookkeeping & ISO timestamp
    manifest.setdefault("schema_version", 1)
    _write_manifest(dest, manifest)

    if refusals and refused_frames:
        # Zero-loss guarantee: locked-year rows survive in a committed sidecar.
        refused_df = pd.concat(refused_frames, ignore_index=True)
        spill = source.parent / "all_history_refused.csv"
        if spill.exists():
            prev = pd.read_csv(spill, keep_default_na=False, na_values=[])
            refused_df = pd.concat([prev, refused_df], ignore_index=True)
            if {"ETF_Ticker", "ticker", "Holdings_As_Of"} <= set(refused_df.columns):
                refused_df = refused_df.drop_duplicates(
                    subset=["ETF_Ticker", "ticker", "Holdings_As_Of"], keep="last"
                )
        refused_df.to_csv(spill, index=False)
        print(f"\n  ⚠️  {len(refused_df):,} locked-year row(s) spilled to {spill}")
        print(f"      Fold them with:  python scripts/migrate_to_parquet.py "
              f"--source {spill} --allow-historical-rewrite")

    print(f"\nMigration complete: {total_written:,} rows; "
          f"{len(refusals)} past-year partition(s) skipped (immutable).")
    print(f"Output:   {dest}")
    print(f"Manifest: {dest / MANIFEST_NAME}")
    return 0


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Migrate all_history.csv → year-partitioned, immutability-guarded Parquet store"
    )
    parser.add_argument("--source", default=str(DEFAULT_SOURCE),
                        help=f"Source CSV file (default: {DEFAULT_SOURCE})")
    parser.add_argument("--dest", default=str(DEFAULT_DEST),
                        help=f"Destination directory (default: {DEFAULT_DEST})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without writing")
    parser.add_argument("--verify-only", action="store_true",
                        help="Recompute hashes vs manifest; exit 1 on past-year drift")
    parser.add_argument("--allow-historical-rewrite", action="store_true",
                        help="Permit overwriting past-year partitions (privileged)")
    args = parser.parse_args(argv)

    dest = Path(args.dest)

    if args.verify_only:
        return verify(dest=dest)

    return migrate(
        source=Path(args.source),
        dest=dest,
        dry_run=args.dry_run,
        allow_historical_rewrite=args.allow_historical_rewrite,
    )


if __name__ == "__main__":
    sys.exit(main())
