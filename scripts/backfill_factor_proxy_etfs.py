"""
Backfill historical holdings for BlackRock iShares factor proxy ETFs:
  - MTUM: MSCI USA Momentum Factor ETF (portfolio_id: 251614)
  - QUAL: MSCI USA Quality Factor ETF (portfolio_id: 256101)
  - VLUE: MSCI USA Value Factor ETF (portfolio_id: 251616)
  - USMV: MSCI USA Min Vol Factor ETF (portfolio_id: 239695)
  - SIZE: MSCI USA Size Factor ETF (portfolio_id: 251465)
  - IWB:  Russell 1000 ETF Benchmark (portfolio_id: 239707)

Downloads raw holdings directly from BlackRock Varnish CDN API, parses and cleans
constituent records, caches raw responses locally, and writes clean Parquet archives
to data/factor_proxies/{ticker}.parquet and all_factor_proxies.parquet.
"""

from __future__ import annotations

import argparse
import datetime as dt
import io
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_OUT_DIR = REPO_ROOT / "data" / "factor_proxies"
CACHE_DIR = DEFAULT_OUT_DIR / "cache"

PROXY_ETFS = {
    "MTUM": "251614",
    "QUAL": "256101",
    "VLUE": "251616",
    "USMV": "239695",
    "SIZE": "251465",
    "IWB":  "239707",
}

# Inception dates (earliest constituent holdings availability on BlackRock CDN):
# - IWB:  Russell 1000 benchmark constituent holdings archive begins June 2008
# - USMV: MSCI USA Min Vol Factor ETF inception October 2011
# - MTUM: MSCI USA Momentum Factor ETF inception April 2013
# - VLUE: MSCI USA Value Factor ETF inception April 2013
# - SIZE: MSCI USA Size Factor ETF inception April 2013
# - QUAL: MSCI USA Quality Factor ETF inception July 2013
ETF_INCEPTION_DATES: Dict[str, dt.date] = {
    "IWB":  dt.date(2008, 6, 1),
    "USMV": dt.date(2011, 10, 1),
    "MTUM": dt.date(2013, 4, 1),
    "VLUE": dt.date(2013, 4, 1),
    "SIZE": dt.date(2013, 4, 1),
    "QUAL": dt.date(2013, 7, 1),
}

HEADERS = {
    "Accept": "*/*",
    "Referer": "https://www.blackrock.com/",
    "Origin": "https://www.blackrock.com",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
}

API_TEMPLATE = (
    "https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/"
    "get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares"
    "&locale=en_US&portfolioId={portfolio_id}&userType=individual&asOfDate={dstr}&component=holdings"
)


def generate_target_dates(
    start_date: dt.date,
    end_date: dt.date,
    cadence: str = "weekly_and_month_end",
) -> List[dt.date]:
    """
    Generate target dates to sample.
    cadence options:
      - 'all': every weekday
      - 'weekly_and_month_end': every Friday, last business day of every month,
         and every business day for the most recent 60 calendar days.
    """
    dates: Set[dt.date] = set()
    cur = start_date
    recent_cutoff = end_date - dt.timedelta(days=60)

    while cur <= end_date:
        if cur.weekday() < 5:  # Monday through Friday
            if cadence == "all":
                dates.add(cur)
            else:
                # Add if Friday
                if cur.weekday() == 4:
                    dates.add(cur)
                # Add if recent window
                if cur >= recent_cutoff:
                    dates.add(cur)
                # Add if month end business day (next business day is next month)
                next_day = cur + dt.timedelta(days=1)
                while next_day.weekday() >= 5:
                    next_day += dt.timedelta(days=1)
                if next_day.month != cur.month:
                    dates.add(cur)
        cur += dt.timedelta(days=1)

    return sorted(dates)


def fetch_and_cache_date(
    ticker: str,
    portfolio_id: str,
    date_val: dt.date,
    session: requests.Session,
) -> Tuple[str, dt.date, bool, Optional[str]]:
    """
    Fetch raw CSV for a given ticker and date, checking cache first.
    Returns (ticker, date_val, is_valid, raw_text).
    """
    dstr = date_val.strftime("%Y%m%d")
    cache_subdir = CACHE_DIR / ticker
    cache_subdir.mkdir(parents=True, exist_ok=True)
    cache_file = cache_subdir / f"{dstr}.csv"
    empty_marker = cache_subdir / f"{dstr}.empty"

    if empty_marker.exists():
        return ticker, date_val, False, None

    if cache_file.exists():
        try:
            content = cache_file.read_text(encoding="utf-8")
            if len(content) > 1000 and "Ticker," in content:
                return ticker, date_val, True, content
        except Exception:
            pass

    url = API_TEMPLATE.format(portfolio_id=portfolio_id, dstr=dstr)
    for attempt in range(3):
        try:
            resp = session.get(url, headers=HEADERS, timeout=25)
            if resp.status_code == 200:
                text = resp.text
                if len(text) > 1000 and "Ticker," in text:
                    cache_file.write_text(text, encoding="utf-8")
                    return ticker, date_val, True, text
                else:
                    # Blackrock stub/empty response on holidays/weekends
                    empty_marker.write_text("EMPTY", encoding="utf-8")
                    return ticker, date_val, False, None
            elif resp.status_code in (404, 400):
                empty_marker.write_text(f"HTTP_{resp.status_code}", encoding="utf-8")
                return ticker, date_val, False, None
        except Exception:
            time.sleep(1.0 + attempt * 0.5)

    return ticker, date_val, False, None


def parse_holdings_csv(
    ticker: str,
    target_date: dt.date,
    raw_text: str,
) -> Optional[pd.DataFrame]:
    """
    Parse BlackRock holdings CSV into normalized DataFrame.
    """
    lines = raw_text.splitlines()
    hidx = next((i for i, ln in enumerate(lines) if ln.strip().startswith("Ticker,")), None)
    if hidx is None:
        return None

    # Detect actual as-of date from metadata lines if present
    as_of = target_date.strftime("%Y-%m-%d")
    for meta_line in lines[:hidx]:
        if re.search(r"fund holdings date|holdings date|as of date|asofdate", meta_line, re.I):
            parts = meta_line.split(",", 1)
            if len(parts) == 2:
                raw_d = parts[1].strip().strip('"')
                for fmt in ("%B %d, %Y", "%b %d, %Y", "%m/%d/%Y", "%Y-%m-%d", "%B %d %Y", "%b %d %Y"):
                    try:
                        as_of = dt.datetime.strptime(raw_d, fmt).strftime("%Y-%m-%d")
                        break
                    except ValueError:
                        pass
                break

    try:
        df = pd.read_csv(io.StringIO(raw_text), skiprows=hidx)
    except Exception:
        return None

    if df.empty or "Ticker" not in df.columns:
        return None

    # Standardize column mappings
    rename_map = {}
    for col in df.columns:
        clow = col.lower()
        if clow == "ticker":
            rename_map[col] = "ticker"
        elif clow == "name":
            rename_map[col] = "name"
        elif "sector" in clow:
            rename_map[col] = "sector"
        elif "weight" in clow:
            rename_map[col] = "weight"
        elif "quantity" in clow or "shares" in clow:
            rename_map[col] = "shares"
        elif "market value" in clow:
            rename_map[col] = "market_value"
        elif "asset class" in clow:
            rename_map[col] = "asset_class"

    df = df.rename(columns=rename_map)

    # Filter out cash, derivatives, non-equity
    if "asset_class" in df.columns:
        df = df[df["asset_class"].astype(str).str.lower().isin(["equity", "stock", "fund", "etf"])]

    df = df.dropna(subset=["ticker"]).copy()
    df["ticker"] = df["ticker"].astype(str).str.strip().str.upper()
    df = df[df["ticker"].str.len() > 0]
    df = df[~df["ticker"].isin(["-", "N/A", "CASH", "USD"])]

    if df.empty:
        return None

    # Clean numeric fields
    if "weight" in df.columns:
        df["weight"] = pd.to_numeric(
            df["weight"].astype(str).str.replace(",", "").str.replace("%", "").str.strip(),
            errors="coerce"
        ).fillna(0.0)
        # Convert percent to decimal 0..1 if max > 1.0
        if df["weight"].max() > 1.0:
            df["weight"] = df["weight"] / 100.0

    if "shares" in df.columns:
        df["shares"] = pd.to_numeric(
            df["shares"].astype(str).str.replace(",", "").str.strip(),
            errors="coerce"
        ).fillna(0.0)
    else:
        df["shares"] = 0.0

    if "market_value" in df.columns:
        df["market_value"] = pd.to_numeric(
            df["market_value"].astype(str).str.replace(",", "").str.strip(),
            errors="coerce"
        ).fillna(0.0)
    else:
        df["market_value"] = 0.0

    if "sector" not in df.columns:
        df["sector"] = "Unknown"
    else:
        df["sector"] = df["sector"].fillna("Unknown").astype(str).str.strip()

    if "name" not in df.columns:
        df["name"] = df["ticker"]
    else:
        df["name"] = df["name"].fillna(df["ticker"]).astype(str).str.strip()

    df["ETF_Ticker"] = ticker
    df["Holdings_As_Of"] = pd.to_datetime(as_of)

    keep_cols = [
        "ETF_Ticker",
        "ticker",
        "name",
        "sector",
        "weight",
        "shares",
        "market_value",
        "Holdings_As_Of",
    ]
    return df[keep_cols]


def run_backfill(
    tickers: List[str],
    start_date: Optional[dt.date],
    end_date: dt.date,
    cadence: str = "weekly_and_month_end",
    workers: int = 8,
    out_dir: Path = DEFAULT_OUT_DIR,
) -> None:
    """
    Execute multi-threaded backfill and generate Parquet archives.
    If start_date is None, each ETF starts from its inception date in ETF_INCEPTION_DATES.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=workers * 2, pool_maxsize=workers * 2)
    session.mount("https://", adapter)

    all_dfs: Dict[str, List[pd.DataFrame]] = {t: [] for t in tickers}

    tasks = []
    print(f"Planning target dates up to {end_date} (cadence={cadence}):")
    for ticker in tickers:
        pid = PROXY_ETFS[ticker]
        t_start = start_date if start_date is not None else ETF_INCEPTION_DATES.get(ticker, dt.date(2013, 1, 1))
        t_dates = generate_target_dates(t_start, end_date, cadence)
        print(f"  {ticker} (portfolio {pid}): {len(t_dates)} dates ({t_start} to {end_date})")
        for d in t_dates:
            tasks.append((ticker, pid, d))

    total_tasks = len(tasks)
    print(f"\nTotal download tasks: {total_tasks} across {len(tickers)} ETFs with {workers} workers")

    completed = 0
    valid_count = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {
            executor.submit(fetch_and_cache_date, t, pid, d, session): (t, d)
            for (t, pid, d) in tasks
        }

        for future in as_completed(future_map):
            completed += 1
            ticker, date_val = future_map[future]
            try:
                t, d, is_valid, raw_text = future.result()
                if is_valid and raw_text:
                    valid_count += 1
                    df_parsed = parse_holdings_csv(t, d, raw_text)
                    if df_parsed is not None and not df_parsed.empty:
                        all_dfs[t].append(df_parsed)
            except Exception as exc:
                print(f"Error fetching {ticker} on {date_val}: {exc}")

            if completed % 100 == 0 or completed == total_tasks:
                elapsed = time.time() - t0
                rate = completed / elapsed if elapsed > 0 else 0
                print(
                    f"Progress: {completed}/{total_tasks} ({completed*100/total_tasks:.1f}%) "
                    f"| Valid files: {valid_count} | Rate: {rate:.1f} req/s"
                )

    # Save per-ETF parquet files
    combined_list = []
    print("\nWriting Parquet archives:")
    for ticker in tickers:
        dfs = all_dfs[ticker]
        if dfs:
            full_df = pd.concat(dfs, ignore_index=True)
            full_df = full_df.drop_duplicates(subset=["ETF_Ticker", "ticker", "Holdings_As_Of"])
            full_df = full_df.sort_values(by=["Holdings_As_Of", "weight"], ascending=[True, False])
            out_file = out_dir / f"{ticker}.parquet"
            full_df.to_parquet(out_file, index=False, engine="pyarrow", compression="snappy")
            num_dates = full_df["Holdings_As_Of"].nunique()
            print(f"  {ticker}: {len(full_df)} rows, {num_dates} dates -> {out_file.name}")
            combined_list.append(full_df)
        else:
            print(f"  {ticker}: 0 rows found")

    if combined_list:
        all_proxies_df = pd.concat(combined_list, ignore_index=True)
        all_out = out_dir / "all_factor_proxies.parquet"
        all_proxies_df.to_parquet(all_out, index=False, engine="pyarrow", compression="snappy")
        print(f"\nConsolidated archive: {len(all_proxies_df)} rows across {len(combined_list)} ETFs -> {all_out.name}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill iShares factor proxy ETF holdings")
    parser.add_argument(
        "--start-date",
        default="inception",
        help="Start date (YYYY-MM-DD) or 'inception' (default) to backfill each ETF from its earliest available history",
    )
    parser.add_argument("--end-date", default=dt.date.today().strftime("%Y-%m-%d"), help="End date (YYYY-MM-DD)")
    parser.add_argument("--cadence", default="weekly_and_month_end", choices=["weekly_and_month_end", "all"])
    parser.add_argument("--workers", type=int, default=8, help="Number of concurrent workers")
    parser.add_argument("--tickers", nargs="+", default=list(PROXY_ETFS.keys()), help="ETFs to backfill")
    parser.add_argument("--out-dir", default=str(DEFAULT_OUT_DIR), help="Output directory")

    args = parser.parse_args()
    if args.start_date.lower() == "inception":
        start_d = None
    else:
        start_d = dt.date.fromisoformat(args.start_date)
    end_d = dt.date.fromisoformat(args.end_date)
    out_path = Path(args.out_dir)

    run_backfill(
        tickers=args.tickers,
        start_date=start_d,
        end_date=end_d,
        cadence=args.cadence,
        workers=args.workers,
        out_dir=out_path,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
