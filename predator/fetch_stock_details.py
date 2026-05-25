"""
Predator Protocol — Stock Details Builder
=========================================
Fetches:
  1. Company descriptions (longBusinessSummary) — cached in data/company_descriptions.json
     to avoid rate limits on repeated builds.
  2. 2-year daily adjusted-close history (for 1M/3M/6M/YTD/1Y/ALL time selectors in UI)

For each ticker in docs/data/leaderboard.json, writes a minified JSON file to:
  docs/data/details/{TICKER}.json

Output schema per file:
{
  "ticker": "AAPL",
  "company": "Apple Inc.",
  "description": "Apple Inc. designs, manufactures...",
  "prices": [["2024-05-25", 189.34], ...]   // ascending by date
}

Usage:
    python -m predator.fetch_stock_details                  # incremental (uses cache)
    python -m predator.fetch_stock_details --refresh-desc   # re-fetch all descriptions
    python -m predator.fetch_stock_details --tickers AAPL,MSFT  # specific tickers
    python -m predator.fetch_stock_details --dry-run        # print plan, no fetch

The description cache (data/company_descriptions.json) should be committed to git so
CI builds never need to re-fetch the full universe every run.
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# ─── Platform encoding fix ────────────────────────────────────────────────────
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

REPO_ROOT         = Path(__file__).resolve().parent.parent
LEADERBOARD_PATH  = REPO_ROOT / "docs" / "data" / "leaderboard.json"
DESC_CACHE_PATH   = REPO_ROOT / "data" / "company_descriptions.json"
DETAILS_DIR       = REPO_ROOT / "docs" / "data" / "details"

PRICE_PERIOD      = "2y"      # 2 years of daily data (covers 1M/3M/6M/YTD/1Y/ALL)
CHUNK_SIZE        = 500       # tickers per yf.download() batch
DESC_WORKERS      = 5         # concurrent threads for description fetching
DESC_DELAY        = 0.4       # seconds between individual description requests (rate limit)
PRICE_CHUNK_DELAY = 2.0       # seconds between bulk download chunks


# ─── Safe JSON helpers ────────────────────────────────────────────────────────

class _SafeEncoder(json.JSONEncoder):
    def iterencode(self, o, _one_shot=False):
        return super().iterencode(self._sanitise(o), _one_shot)

    def _sanitise(self, obj):
        if isinstance(obj, float):
            return None if (math.isnan(obj) or math.isinf(obj)) else obj
        if isinstance(obj, dict):
            return {k: self._sanitise(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [self._sanitise(v) for v in obj]
        return obj


def _dumps(obj: Any, **kwargs) -> str:
    kwargs.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kwargs)


def _write_json_atomic(path: Path, text: str) -> None:
    """Write JSON atomically using a .tmp file to avoid corrupting partial writes."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# ─── Load leaderboard tickers ─────────────────────────────────────────────────

def _load_leaderboard_tickers() -> dict[str, str]:
    """Load {ticker: company} map from leaderboard.json."""
    if not LEADERBOARD_PATH.exists():
        print(f"  ERROR: {LEADERBOARD_PATH} not found. Run predator.build first.")
        return {}
    try:
        lb = json.loads(LEADERBOARD_PATH.read_text(encoding="utf-8"))
        return {r["ticker"]: r.get("company", "") for r in lb if r.get("ticker")}
    except Exception as e:
        print(f"  ERROR: Could not read leaderboard.json: {e}")
        return {}


# ─── Description cache ────────────────────────────────────────────────────────

def _load_desc_cache() -> dict[str, str]:
    """Load the description cache (ticker -> description string)."""
    if DESC_CACHE_PATH.exists():
        try:
            return json.loads(DESC_CACHE_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Could not read description cache ({e}). Starting fresh.")
    return {}


def _save_desc_cache(cache: dict[str, str]) -> None:
    """Persist the description cache back to disk."""
    DESC_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(DESC_CACHE_PATH, _dumps(cache, indent=2, sort_keys=True))


def _fetch_description_one(ticker: str, company: str) -> tuple[str, str]:
    """
    Fetch a single ticker's longBusinessSummary from yfinance.
    Returns (ticker, description_string).
    Falls back to a generic description on error.
    """
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        desc = info.get("longBusinessSummary") or info.get("description") or ""
        if desc and len(desc) > 10:
            # Truncate very long descriptions at a sentence boundary
            if len(desc) > 600:
                sentences = desc.split(". ")
                truncated = ""
                for s in sentences:
                    if len(truncated) + len(s) > 580:
                        break
                    truncated += s + ". "
                desc = truncated.strip()
            return ticker, desc
    except Exception:
        pass
    # Generic fallback
    company_name = company or ticker
    return ticker, f"{company_name} ({ticker}) is a holding tracked across smart-beta ETFs by the Predator Protocol conviction scanner."


def _fetch_descriptions_bulk(
    to_fetch: dict[str, str],
    cache: dict[str, str],
    workers: int = DESC_WORKERS,
) -> dict[str, str]:
    """
    Fetch descriptions for all tickers in `to_fetch` (those not already in cache).
    Uses a thread pool to speed up the I/O-bound requests.
    Returns the updated full cache.
    """
    if not to_fetch:
        return cache

    print(f"\n  Fetching descriptions for {len(to_fetch)} new tickers ({workers} threads)…")
    fetched = 0
    failed  = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(_fetch_description_one, ticker, company): ticker
            for ticker, company in to_fetch.items()
        }
        for future in as_completed(futures):
            try:
                ticker, desc = future.result()
                cache[ticker] = desc
                fetched += 1
                if fetched % 50 == 0:
                    print(f"    {fetched}/{len(to_fetch)} descriptions fetched…")
                    _save_desc_cache(cache)   # periodic save
                time.sleep(DESC_DELAY / workers)  # throttle
            except Exception as e:
                t = futures[future]
                print(f"    WARN: {t} description failed: {e}")
                failed += 1

    print(f"  Descriptions: {fetched} fetched, {failed} failed")
    return cache


# ─── Price fetching ───────────────────────────────────────────────────────────

def _fetch_prices_bulk(tickers: list[str]) -> dict[str, list[list]]:
    """
    Fetch 2-year daily adjusted-close for all tickers in chunks.
    Returns {ticker: [[date_str, close], ...]} (ascending).
    """
    try:
        import yfinance as yf
    except ImportError:
        print("  ERROR: yfinance not installed. Run: pip install yfinance")
        return {}

    results: dict[str, list[list]] = {}
    chunks = [tickers[i:i + CHUNK_SIZE] for i in range(0, len(tickers), CHUNK_SIZE)]
    print(f"\n  Fetching {PERIOD_LABEL} daily prices for {len(tickers)} tickers in {len(chunks)} chunk(s)…")

    for chunk_idx, chunk in enumerate(chunks):
        print(f"  Chunk {chunk_idx + 1}/{len(chunks)}: {len(chunk)} tickers…")
        try:
            df = yf.download(
                chunk,
                period=PRICE_PERIOD,
                interval="1d",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if df is None or df.empty:
                print(f"    WARNING: Empty response for chunk {chunk_idx + 1}")
                continue

            # Navigate the MultiIndex — shape: (dates) x MultiIndex(Price, Ticker)
            if isinstance(df.columns, pd.MultiIndex):
                close_df = df["Close"] if "Close" in df.columns.get_level_values(0) else df.iloc[:, 0]
            else:
                close_df = df

            close_df = close_df.dropna(how="all")

            for ticker in chunk:
                # Handle both single-ticker (Series) and multi-ticker (DataFrame) cases
                if isinstance(close_df, pd.Series):
                    series = close_df.dropna()
                elif ticker in close_df.columns:
                    series = close_df[ticker].dropna()
                else:
                    continue

                prices = [
                    [pd.Timestamp(dt).strftime("%Y-%m-%d"), round(float(val), 4)]
                    for dt, val in series.items()
                    if not (isinstance(val, float) and (math.isnan(val) or math.isinf(val)))
                ]
                prices.sort(key=lambda x: x[0])
                if prices:
                    results[ticker] = prices

        except Exception as e:
            print(f"    ERROR in chunk {chunk_idx + 1}: {e}")

        if chunk_idx < len(chunks) - 1:
            time.sleep(PRICE_CHUNK_DELAY)

    print(f"  Price data: {len(results)}/{len(tickers)} tickers succeeded")
    return results


PERIOD_LABEL = "2-year"


# ─── Main builder ─────────────────────────────────────────────────────────────

def build_details(
    tickers_filter: list[str] | None = None,
    refresh_descriptions: bool = False,
    dry_run: bool = False,
) -> None:
    print("\n" + "═" * 60)
    print("  Predator Protocol — Stock Details Builder")
    print("═" * 60)

    # 1. Load leaderboard ticker universe
    all_tickers = _load_leaderboard_tickers()
    if not all_tickers:
        print("  No tickers found. Aborting.")
        return

    # Apply filter
    if tickers_filter:
        all_tickers = {t: c for t, c in all_tickers.items() if t in tickers_filter}
    print(f"\n  Universe: {len(all_tickers)} tickers")

    if dry_run:
        print("\n  --dry-run: exiting without fetching.")
        return

    # 2. Description cache
    desc_cache = {} if refresh_descriptions else _load_desc_cache()
    missing_desc = {t: c for t, c in all_tickers.items() if t not in desc_cache}
    print(f"  Descriptions: {len(desc_cache)} cached, {len(missing_desc)} to fetch")

    if missing_desc:
        desc_cache = _fetch_descriptions_bulk(missing_desc, desc_cache)
        _save_desc_cache(desc_cache)
        print(f"  Description cache saved → {DESC_CACHE_PATH}")

    # 3. Fetch prices for all tickers
    ticker_list = sorted(all_tickers.keys())
    prices_by_ticker = _fetch_prices_bulk(ticker_list)

    # 4. Write detail files
    DETAILS_DIR.mkdir(parents=True, exist_ok=True)
    written = 0
    skipped = 0
    generated_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for ticker, company in all_tickers.items():
        prices = prices_by_ticker.get(ticker, [])
        desc   = desc_cache.get(ticker, "")

        if not prices:
            skipped += 1
            continue

        payload = {
            "ticker":      ticker,
            "company":     company,
            "description": desc,
            "prices":      prices,
            "generated":   generated_utc,
        }
        out_path = DETAILS_DIR / f"{ticker}.json"
        _write_json_atomic(out_path, _dumps(payload, separators=(",", ":")))
        written += 1

    print(f"\n✓ Details: {written} files written to {DETAILS_DIR}/")
    if skipped:
        print(f"  {skipped} tickers skipped (no price data)")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="predator.fetch_stock_details",
        description="Fetch company descriptions and 2-year daily prices for the leaderboard universe.",
    )
    parser.add_argument("--refresh-desc", action="store_true",
                        help="Re-fetch all descriptions (ignores cache).")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without fetching.")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers to process (default: full universe).")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    tickers_filter = None
    if args.tickers:
        tickers_filter = [t.strip().upper() for t in args.tickers.split(",")]
        print(f"  Filter: {', '.join(tickers_filter)}")

    build_details(
        tickers_filter=tickers_filter,
        refresh_descriptions=args.refresh_desc,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
