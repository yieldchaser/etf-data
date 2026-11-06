"""
BlackRock Historical Holdings Backfill Script.

Pulls historical portfolio snapshots for iShares / BlackRock ETFs via the
public Varnish CDN API and merges them into the permanent Parquet store.

Usage:
    python scripts/backfill_blackrock_history.py
"""
import os
import sys
import json
import time
import io
import re
import requests
import pandas as pd
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

sys.path.insert(0, os.path.abspath('.'))
from scraper import clean_dataframe, PIPELINE_SCHEMA_COLS, roll_to_prev_biz_day, TODAY, _US_HOLIDAYS
from scripts.migrate_to_parquet import _partition_summary, _write_manifest, _load_manifest

HEADERS = {
    'Accept': '*/*',
    'Referer': 'https://www.blackrock.com/',
    'Origin':  'https://www.blackrock.com',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
}

CACHE_DIR = os.path.join('data', '.cache_backfill')
os.makedirs(CACHE_DIR, exist_ok=True)

thread_local = threading.local()

def get_session():
    if not hasattr(thread_local, "session"):
        s = requests.Session()
        s.headers.update(HEADERS)
        thread_local.session = s
    return thread_local.session

def fetch_historical_snapshot(ticker: str, portfolio_id: str, target_date_str: str) -> tuple[pd.DataFrame | None, str]:
    d_clean = target_date_str.replace('-', '')
    cache_path = os.path.join(CACHE_DIR, f"{ticker}_{d_clean}.parquet")
    if os.path.exists(cache_path):
        try:
            df = pd.read_parquet(cache_path)
            aod = str(df['Holdings_As_Of'].iloc[0])[:10] if not df.empty else target_date_str
            return df, aod
        except Exception:
            pass

    session = get_session()
    url = (
        'https://www.blackrock.com/varnish-api/blk-one01-product-data/product-data/api/v1/'
        f'get-fund-document?appType=PRODUCT_PAGE&appSubType=ISHARES&targetSite=us-ishares'
        f'&locale=en_US&portfolioId={portfolio_id}&userType=individual&asOfDate={d_clean}&component=holdings'
    )
    try:
        r = session.get(url, timeout=15)
        if r.status_code != 200 or len(r.content) < 3000:
            return None, target_date_str
        lines = r.text.splitlines()
        hidx = next((i for i, ln in enumerate(lines) if ln.strip().startswith('Ticker,')), None)
        if hidx is None:
            return None, target_date_str

        aod = ''
        for meta_line in lines[:hidx]:
            if re.search(r'fund holdings date|holdings date|as of date|as-of date|asofdate', meta_line, re.I):
                parts = meta_line.split(',', 1)
                if len(parts) == 2:
                    raw_d = parts[1].strip().strip('"')
                    for fmt in ('%B %d, %Y', '%b %d, %Y', '%m/%d/%Y', '%Y-%m-%d', '%B %d %Y', '%b %d %Y'):
                        try:
                            aod = datetime.strptime(raw_d, fmt).strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            pass
                if aod:
                    break
        if not aod:
            for meta_line in lines[:hidx]:
                m2 = re.search(r'([A-Za-z]+ \d{1,2},? \d{4})', meta_line)
                if m2:
                    raw_d = m2.group(1).replace(',', '')
                    for fmt in ('%B %d %Y', '%b %d %Y'):
                        try:
                            aod = datetime.strptime(raw_d, fmt).strftime('%Y-%m-%d')
                            break
                        except ValueError:
                            pass
                    if aod:
                        break
        if not aod:
            try:
                aod = datetime.strptime(d_clean, '%Y%m%d').strftime('%Y-%m-%d')
            except Exception:
                aod = target_date_str

        aod = roll_to_prev_biz_day(aod)
        df_raw = pd.read_csv(io.StringIO(r.text), skiprows=hidx)
        df_clean = clean_dataframe(df_raw, ticker, aod, weight_unit='percent')
        if df_clean is not None and not df_clean.empty:
            df_clean['Holdings_As_Of'] = pd.to_datetime(df_clean['Holdings_As_Of'], errors='coerce')
            df_clean.to_parquet(cache_path, index=False)
            return df_clean, aod
        return None, target_date_str
    except Exception:
        return None, target_date_str

def backfill():
    with open('config.json', 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    blackrock_funds = [
        item for item in cfg
        if item.get('scraper_type') == 'blackrock_api' and item.get('portfolio_id')
    ]
    # Also include VLUE and USMV
    blackrock_funds.append({'ticker': 'VLUE', 'portfolio_id': '251616'})
    blackrock_funds.append({'ticker': 'USMV', 'portfolio_id': '239695'})

    print(f"Total BlackRock funds to backfill: {len(blackrock_funds)}", flush=True)

    # Core flagship factor & benchmark ETFs get dense monthly history
    flagship_tickers = {
        'QUAL', 'MTUM', 'VLUE', 'USMV', 'SIZE', 'IVV', 'IJH', 'IJR', 'IWM', 'SOXX', 'EFA', 'IEMG'
    }

    # Dense dates for flagships (all 2024, 2025, 2026 month-ends)
    flagship_dates = [
        # 2026 month-ends
        '20260130', '20260227', '20260331', '20260430', '20260529',
        '20260630', '20260731', '20260831', '20260930', '20261030',
        # 2025 month-ends
        '20250131', '20250228', '20250331', '20250430', '20250530',
        '20250630', '20250731', '20250829', '20250930', '20251031',
        '20251128', '20251231',
        # 2024 quarter-ends & month-ends
        '20240328', '20240628', '20240930', '20241231',
        # 2023 year-end
        '20231229',
    ]

    # Milestone dates for ALL other BlackRock ETFs (quarter-ends & year-ends)
    broad_dates = [
        '20260331', '20260630', '20260930',
        '20250630', '20251231',
        '20240628', '20241231',
    ]

    tasks = []
    for fund in blackrock_funds:
        t = fund['ticker']
        pid = fund['portfolio_id']
        dates_to_query = flagship_dates if t in flagship_tickers else broad_dates
        for d in dates_to_query:
            tasks.append((t, pid, d))

    print(f"Total historical snapshot tasks scheduled: {len(tasks)}", flush=True)

    fetched_frames = []
    success_count = 0
    fail_count = 0
    t0 = time.time()

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {
            executor.submit(fetch_historical_snapshot, t, pid, d): (t, d)
            for t, pid, d in tasks
        }
        for i, fut in enumerate(as_completed(futures)):
            t, d = futures[fut]
            df, aod = fut.result()
            if df is not None and not df.empty:
                fetched_frames.append(df)
                success_count += 1
            else:
                fail_count += 1
            if (i + 1) % 100 == 0 or (i + 1) == len(tasks):
                rate = (i + 1) / (time.time() - t0)
                print(f"[{i+1}/{len(tasks)}] Done: {success_count} snapshots ok, {fail_count} missed | {rate:.1f} tasks/sec", flush=True)

    print(f"\nHistorical fetch complete: {success_count} snapshots retrieved in {time.time()-t0:.1f}s", flush=True)

    if not fetched_frames:
        print("No historical frames retrieved.")
        return

    # Consolidate all backfilled historical frames
    print("Consolidating backfilled frames...", flush=True)
    all_history_df = pd.concat(fetched_frames, ignore_index=True)
    all_history_df['Holdings_As_Of'] = pd.to_datetime(all_history_df['Holdings_As_Of'], errors='coerce')
    all_history_df.dropna(subset=['Holdings_As_Of'], inplace=True)
    all_history_df.drop_duplicates(subset=['ETF_Ticker', 'ticker', 'Holdings_As_Of'], keep='last', inplace=True)
    print(f"Total deduplicated historical rows: {len(all_history_df):,}", flush=True)

    # Group by year and partition into data/history_parquet/year=YYYY/holdings.parquet
    parquet_dir = Path('data') / 'history_parquet'
    all_history_df['year'] = all_history_df['Holdings_As_Of'].dt.year

    for yr, group in all_history_df.groupby('year'):
        yr_int = int(yr)
        yr_dir = parquet_dir / f'year={yr_int}'
        yr_dir.mkdir(parents=True, exist_ok=True)
        yr_parquet = yr_dir / 'holdings.parquet'
        group_to_write = group[PIPELINE_SCHEMA_COLS].copy()
        group_to_write['Holdings_As_Of'] = pd.to_datetime(group_to_write['Holdings_As_Of'], errors='coerce')

        if yr_parquet.exists():
            existing_df = pd.read_parquet(yr_parquet)
            existing_df['Holdings_As_Of'] = pd.to_datetime(existing_df['Holdings_As_Of'], errors='coerce')
            combined = pd.concat([existing_df, group_to_write], ignore_index=True)
            combined.drop_duplicates(subset=['ETF_Ticker', 'ticker', 'Holdings_As_Of'], keep='last', inplace=True)
        else:
            combined = group_to_write

        combined.to_parquet(yr_parquet, engine='pyarrow', compression='snappy', index=False)
        print(f"  year={yr_int}: {len(combined):,} rows saved to {yr_parquet}")

    # Recompute CHECKSUMS.json using official migrate_to_parquet helpers
    print("\nRefreshing CHECKSUMS.json manifest...", flush=True)
    manifest = _load_manifest(parquet_dir)
    manifest['partitions'] = {}
    for yr_folder in sorted(os.listdir(parquet_dir)):
        p_path = parquet_dir / yr_folder
        if p_path.is_dir() and yr_folder.startswith('year='):
            yr_int = int(yr_folder.split('=')[1])
            yr_str = str(yr_int)
            p_file = p_path / 'holdings.parquet'
            if p_file.exists():
                manifest['partitions'][yr_str] = _partition_summary(p_file, yr_int)

    manifest['schema_version'] = 1
    _write_manifest(parquet_dir, manifest)
    print("CHECKSUMS.json successfully refreshed!")

if __name__ == '__main__':
    backfill()
