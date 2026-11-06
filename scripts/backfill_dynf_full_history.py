"""
Backfill full daily historical holdings for DYNF (iShares U.S. Equity Factor Rotation Active ETF).

Inception Date: 2019-03-19
Portfolio ID:   307283

This script:
1. Generates all US trading days from 2019-03-19 to present.
2. Checks local cache (data/.cache_backfill/DYNF_{YYYYMMDD}.parquet).
3. Downloads missing dates via polite multi-threaded connection-pooled calls to BlackRock's Varnish CDN.
4. Cleans each snapshot through scraper.clean_dataframe.
5. Merges into data/history_parquet/year={YYYY}/holdings.parquet for each year (2019..2026).
6. Atomically updates data/history_parquet/CHECKSUMS.json.
7. Verifies archive integrity.
"""
from __future__ import annotations

import argparse
import io
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Platform encoding fix
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scraper import clean_dataframe, roll_to_prev_biz_day, _US_HOLIDAYS
from scripts.migrate_to_parquet import (
    DEFAULT_DEST,
    _load_manifest,
    _partition_summary,
    _write_manifest,
    verify,
)

HEADERS = {
    "Accept": "*/*",
    "Referer": "https://www.blackrock.com/",
    "Origin": "https://www.blackrock.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}

TICKER = "DYNF"
PORTFOLIO_ID = "307283"
CACHE_DIR = REPO_ROOT / "data" / ".cache_backfill"


def make_session(pool_size: int = 15) -> requests.Session:
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retries,
        pool_connections=pool_size,
        pool_maxsize=pool_size,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(HEADERS)
    return session


def fetch_and_clean_day(session: requests.Session, d_str: str) -> tuple[pd.DataFrame | None, str]:
    cache_path = CACHE_DIR / f"{TICKER}_{d_str}.parquet"
    if cache_path.exists():
        try:
            df = pd.read_parquet(cache_path)
            if not df.empty and "Holdings_As_Of" in df.columns:
                aod = str(df["Holdings_As_Of"].iloc[0])[:10]
                return df, aod
        except Exception:
            pass

    url = (
        "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/"
        "get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares"
        f"&locale=en_US&portfolioId={PORTFOLIO_ID}&userType=individual&asOfDate={d_str}&component=holdings"
    )

    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200 or len(r.content) < 3000:
            return None, d_str

        text = r.text
        lines = text.splitlines()
        hidx = next((i for i, ln in enumerate(lines) if ln.strip().startswith("Ticker,")), None)
        if hidx is None:
            return None, d_str

        aod = ""
        for meta_line in lines[:hidx]:
            if "as of" in meta_line.lower() or "date" in meta_line.lower():
                parts = meta_line.split(",", 1)
                if len(parts) == 2:
                    raw_d = parts[1].strip().strip('"')
                    for fmt in (
                        "%B %d, %Y",
                        "%b %d, %Y",
                        "%m/%d/%Y",
                        "%Y-%m-%d",
                        "%B %d %Y",
                        "%b %d %Y",
                    ):
                        try:
                            aod = datetime.strptime(raw_d, fmt).strftime("%Y-%m-%d")
                            break
                        except ValueError:
                            pass
                if aod:
                    break

        if not aod:
            aod = datetime.strptime(d_str, "%Y%m%d").strftime("%Y-%m-%d")

        aod = roll_to_prev_biz_day(aod)
        df_raw = pd.read_csv(io.StringIO(text), skiprows=hidx)
        df_clean = clean_dataframe(df_raw, TICKER, aod, weight_unit="percent")
        if df_clean is not None and not df_clean.empty:
            df_clean["Holdings_As_Of"] = pd.to_datetime(df_clean["Holdings_As_Of"], errors="coerce")
            df_clean.to_parquet(cache_path, index=False, compression="snappy")
            return df_clean, aod

        return None, d_str
    except Exception:
        return None, d_str


def run_backfill(
    start_date: str = "2019-03-19",
    end_date: str = "2026-11-06",
    max_workers: int = 10,
    skip_parquet_merge: bool = False,
):
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    dr = pd.date_range(start_date, end_date, freq="B")
    trading_days = [
        d.strftime("%Y%m%d") for d in dr if d.strftime("%Y-%m-%d") not in _US_HOLIDAYS
    ]

    print(f"Total candidate trading days ({start_date} to {end_date}): {len(trading_days)}", flush=True)

    cached_count = sum(1 for d in trading_days if (CACHE_DIR / f"{TICKER}_{d}.parquet").exists())
    needed = len(trading_days) - cached_count
    print(f"Already cached: {cached_count} | Needed: {needed}", flush=True)

    session = make_session(pool_size=max_workers + 5)
    results: dict[str, pd.DataFrame] = {}

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_and_clean_day, session, d): d for d in trading_days}
        completed = 0
        for fut in as_completed(futures):
            completed += 1
            d_str = futures[fut]
            df, aod = fut.result()
            if df is not None and not df.empty:
                results[aod] = df

            if completed % 100 == 0 or completed == len(trading_days):
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                print(
                    f"[{completed}/{len(trading_days)}] Processed ({rate:.1f} days/s) - "
                    f"Valid snapshots so far: {len(results)}",
                    flush=True,
                )

    print(
        f"\nDownload & extraction complete: {len(results)} distinct trading days in {time.time()-t0:.1f}s.",
        flush=True,
    )

    if not results:
        print("No valid results found. Exiting.")
        return

    # Master DataFrame for DYNF
    all_dynf = pd.concat(list(results.values()), ignore_index=True)
    all_dynf["Holdings_As_Of"] = pd.to_datetime(all_dynf["Holdings_As_Of"], errors="coerce")
    all_dynf.drop_duplicates(subset=["ETF_Ticker", "ticker", "Holdings_As_Of"], keep="last", inplace=True)
    all_dynf.sort_values(by=["Holdings_As_Of", "weight"], ascending=[True, False], inplace=True)

    print(f"Total unique DYNF records across all dates: {len(all_dynf):,}", flush=True)
    print(f"Earliest snapshot: {all_dynf['Holdings_As_Of'].min().strftime('%Y-%m-%d')}")
    print(f"Latest snapshot:   {all_dynf['Holdings_As_Of'].max().strftime('%Y-%m-%d')}")

    scratch_csv = REPO_ROOT / "scratch" / "dynf_full_history_2019_2026.csv"
    scratch_csv.parent.mkdir(parents=True, exist_ok=True)
    all_dynf.to_csv(scratch_csv, index=False)
    print(f"Saved full DYNF consolidated CSV to {scratch_csv}")

    if skip_parquet_merge:
        print("Skipping parquet merge as requested.")
        return

    # Group and merge into year-partitioned parquet
    dest_dir = DEFAULT_DEST
    dest_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest(dest_dir)

    all_dynf["year"] = all_dynf["Holdings_As_Of"].dt.year
    years = [int(y) for y in sorted(all_dynf["year"].dropna().unique())]

    print(f"\nMerging DYNF into Parquet store across years: {years}", flush=True)

    for yr in years:
        yr = int(yr)
        yr_dynf = all_dynf[all_dynf["year"] == yr].drop(columns=["year"])
        part_dir = dest_dir / f"year={yr}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = part_dir / "holdings.parquet"

        if part_file.exists():
            existing = pd.read_parquet(part_file)
            existing["Holdings_As_Of"] = pd.to_datetime(existing["Holdings_As_Of"], errors="coerce")
            # Remove any older DYNF rows and replace with full daily DYNF set
            existing_non_dynf = existing[existing["ETF_Ticker"] != TICKER]
            combined = pd.concat([existing_non_dynf, yr_dynf], ignore_index=True)
        else:
            combined = yr_dynf

        combined.drop_duplicates(subset=["ETF_Ticker", "ticker", "Holdings_As_Of"], keep="last", inplace=True)
        combined.sort_values(by=["Holdings_As_Of", "ETF_Ticker", "weight"], ascending=[True, True, False], inplace=True)

        combined.to_parquet(part_file, index=False, compression="snappy")
        summary = _partition_summary(part_file, int(yr))
        # Ensure native python types in summary
        summary = {k: int(v) if isinstance(v, (pd.Int64Dtype, np.integer)) else v for k, v in summary.items()}
        manifest["partitions"][str(yr)] = summary
        print(
            f"  Year {yr}: {summary['rows']:,} rows ({summary['etfs']} ETFs, "
            f"{summary['first_date']} -> {summary['last_date']}, {summary['file_size']/1024/1024:.2f} MB)"
        )

    _write_manifest(dest_dir, manifest)
    print(f"\nUpdated CHECKSUMS.json at {dest_dir / 'CHECKSUMS.json'}")

    print("\nRunning verification...")
    v_code = verify(dest_dir)
    if v_code == 0:
        print("Manifest verification: PERFECT MATCH (0 drift).")
    else:
        print("Manifest verification reported code:", v_code)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backfill full daily history for DYNF")
    parser.add_argument("--start", default="2019-03-19", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end", default="2026-11-06", help="End date (YYYY-MM-DD)")
    parser.add_argument("--workers", type=int, default=10, help="Concurrency workers")
    parser.add_argument("--skip-merge", action="store_true", help="Skip parquet store merge")
    args = parser.parse_args()

    run_backfill(
        start_date=args.start,
        end_date=args.end,
        max_workers=args.workers,
        skip_parquet_merge=args.skip_merge,
    )
