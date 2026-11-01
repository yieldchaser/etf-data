#!/usr/bin/env python3
"""Build and maintain daily ETF OHLCV history cache from Yahoo Finance v8 Chart API.

This script:
1. Dynamically discovers active instruments from `docs/data/flows/catalog.json`.
2. Fetches full or incremental daily OHLCV bars from Yahoo Finance Chart API.
3. Cleans, sanitizes, and geometrically validates bars (deduplication, price bounds, positive volume).
4. Merges with existing cache in `docs/data/flows/etf_history.json` when present.
5. Saves a compact, high-performance JSON artifact for client-side Canvas 2D studies.
"""

from __future__ import annotations

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CATALOG_PATH = REPO_ROOT / "docs" / "data" / "flows" / "catalog.json"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "docs" / "data" / "flows" / "etf_history.json"

DEFAULT_PERIOD1 = 1451606400  # 2016-01-01 00:00:00 UTC
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
}


def round_price(val: float) -> float:
    """Format price to sensible precision without float noise."""
    if val >= 1.0:
        return round(val, 2)
    return round(val, 4)


def fetch_yahoo_chart_data(
    ticker: str,
    period1: int,
    period2: int,
    retries: int = 3,
    backoff: float = 2.0,
) -> list[dict[str, Any]]:
    """Fetch and parse daily OHLCV bars for a single ticker with exponential backoff."""
    url = (
        f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
        f"?period1={period1}&period2={period2}&interval=1d&includePrePost=false"
    )
    req = urllib.request.Request(url, headers=HEADERS)
    ctx = ssl.create_default_context()

    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=15) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                result = data.get("chart", {}).get("result")
                if not result:
                    error_info = data.get("chart", {}).get("error", {})
                    msg = error_info.get("description", "No result array in chart response")
                    raise ValueError(f"Yahoo Chart API error: {msg}")

                chart_obj = result[0]
                timestamps = chart_obj.get("timestamp", [])
                quotes = chart_obj.get("indicators", {}).get("quote", [{}])[0]
                opens = quotes.get("open", [])
                highs = quotes.get("high", [])
                lows = quotes.get("low", [])
                closes = quotes.get("close", [])
                volumes = quotes.get("volume", [])

                bars_by_date: dict[str, dict[str, Any]] = {}
                for idx, ts in enumerate(timestamps):
                    if ts is None:
                        continue
                    o = opens[idx] if idx < len(opens) else None
                    h = highs[idx] if idx < len(highs) else None
                    l = lows[idx] if idx < len(lows) else None
                    c = closes[idx] if idx < len(closes) else None
                    v = volumes[idx] if idx < len(volumes) else None

                    # Strict null / non-positive checks
                    if c is None or v is None or c <= 0 or v <= 0:
                        continue

                    # Fallbacks for missing partial fields
                    o = o if (o is not None and o > 0) else c
                    h = h if (h is not None and h > 0) else max(o, c)
                    l = l if (l is not None and l > 0) else min(o, c)

                    # Geometric sanity repair
                    real_high = max(h, o, c, l)
                    real_low = min(l, o, c, h)

                    date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d")
                    bars_by_date[date_str] = {
                        "d": date_str,
                        "o": round_price(o),
                        "h": round_price(real_high),
                        "l": round_price(real_low),
                        "c": round_price(c),
                        "v": int(round(v)),
                    }

                # Return sorted chronologically
                clean_bars = sorted(bars_by_date.values(), key=lambda x: x["d"])
                return clean_bars

        except Exception as exc:
            last_exc = exc
            if attempt < retries:
                time.sleep(backoff * attempt)

    raise RuntimeError(f"Failed to fetch {ticker} after {retries} attempts: {last_exc}")


def load_catalog_tickers(catalog_path: Path) -> list[str]:
    """Dynamically load tickers from catalog.json without hardcoding."""
    if not catalog_path.is_file():
        raise FileNotFoundError(f"Catalog file not found at {catalog_path}")
    raw = json.loads(catalog_path.read_text(encoding="utf-8"))
    instruments = raw.get("instruments", [])
    tickers = [item["ticker"] for item in instruments if "ticker" in item]
    if not tickers:
        raise ValueError(f"No instruments found in catalog at {catalog_path}")
    return tickers


def load_existing_history(output_path: Path) -> dict[str, list[dict[str, Any]]]:
    """Load existing history cache if present."""
    if not output_path.is_file():
        return {}
    try:
        content = json.loads(output_path.read_text(encoding="utf-8"))
        return content.get("history", {})
    except Exception as exc:
        print(f"Notice: Could not parse existing cache ({exc}), will rebuild from scratch.")
        return {}


def main() -> int:
    parser = argparse.ArgumentParser(description="Build daily ETF OHLCV history cache from Yahoo Finance.")
    parser.add_argument("--catalog-path", type=Path, default=DEFAULT_CATALOG_PATH, help="Path to catalog.json")
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH, help="Path to etf_history.json")
    parser.add_argument("--force-all", action="store_true", help="Force complete refetch from 2016 for all tickers")
    parser.add_argument("--lookback-days", type=int, default=30, help="Incremental lookback days if cache exists")
    parser.add_argument("--workers", type=int, default=6, help="Concurrent worker threads")
    parser.add_argument("--min-bars", type=int, default=10, help="Minimum bars required for valid instrument")
    args = parser.parse_args()

    tickers = load_catalog_tickers(args.catalog_path)
    print(f"Loaded {len(tickers)} dynamic ETF tickers from {args.catalog_path}")

    existing_history = {} if args.force_all else load_existing_history(args.output_path)
    now_ts = int(datetime.now(timezone.utc).timestamp()) + 86400  # Buffer 1 day

    results: dict[str, list[dict[str, Any]]] = {}
    errors: dict[str, str] = {}

    def process_ticker(ticker: str) -> tuple[str, list[dict[str, Any]], str | None]:
        cached_bars = existing_history.get(ticker, [])
        if cached_bars and not args.force_all:
            # Incremental fetch: calculate start timestamp based on lookback
            # Convert last bar date to timestamp or use lookback_days
            last_date_str = cached_bars[-1]["d"]
            try:
                last_dt = datetime.strptime(last_date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                start_ts = max(DEFAULT_PERIOD1, int(last_dt.timestamp()) - (args.lookback_days * 86400))
            except Exception:
                start_ts = now_ts - (args.lookback_days * 86400)
        else:
            start_ts = DEFAULT_PERIOD1

        try:
            new_bars = fetch_yahoo_chart_data(ticker, period1=start_ts, period2=now_ts)
            if cached_bars and not args.force_all:
                merged: dict[str, dict[str, Any]] = {b["d"]: b for b in cached_bars}
                for b in new_bars:
                    merged[b["d"]] = b
                final_bars = sorted(merged.values(), key=lambda x: x["d"])
            else:
                final_bars = new_bars

            return ticker, final_bars, None
        except Exception as exc:
            # If incremental failed and cached_bars exist, fall back to keeping cached_bars
            if cached_bars:
                print(f"Warning: Fetch failed for {ticker} ({exc}), retaining {len(cached_bars)} cached bars.")
                return ticker, cached_bars, None
            return ticker, [], str(exc)

    t0 = time.time()
    print(f"Fetching OHLCV for {len(tickers)} ETFs with {args.workers} workers...")
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_ticker, t): t for t in tickers}
        done_count = 0
        for fut in as_completed(futures):
            t, bars, err = fut.result()
            done_count += 1
            if err:
                errors[t] = err
                print(f"[{done_count}/{len(tickers)}] [FAIL] {t}: {err}")
            else:
                results[t] = bars
                if done_count % 20 == 0 or done_count == len(tickers):
                    print(f"[{done_count}/{len(tickers)}] [OK] Processed {t} ({len(bars)} bars)")

    elapsed = time.time() - t0
    print(f"\nFetch completed in {elapsed:.1f}s. Success: {len(results)}, Errors: {len(errors)}")

    if errors:
        print(f"Warning: {len(errors)} tickers had fetch errors: {list(errors.keys())}")
        if len(results) < len(tickers) * 0.9:
            print("Error: Too many tickers failed to fetch OHLCV data.", file=sys.stderr)
            return 1

    payload = {
        "schema_version": 1,
        "updated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "instrument_count": len(results),
        "history": results,
    }

    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(payload, separators=(",", ":"))
    args.output_path.write_text(serialized, encoding="utf-8")
    file_size_mb = args.output_path.stat().st_size / (1024 * 1024)
    print(f"Wrote {len(results)} instrument histories to {args.output_path} ({file_size_mb:.2f} MB)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
