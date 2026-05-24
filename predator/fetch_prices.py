"""
Predator Protocol — Portfolio Lab Price Data Builder
=====================================================
Fetches monthly adjusted-close (total-return, dividends reinvested) for the
backtestable universe and writes docs/data/prices.json.

Universe (§22.2 defaults):
  - All ETFs from config.yaml (30 ETFs)
  - Key indices/assets already in market_returns.json (S&P 500, Gold, etc.)
  - A curated set of benchmark ETFs (SPY, IWM, EWJ, QQQ, GLD, TLT, BND)

Output shape:
{
  "generated_utc": "...",
  "asof": "YYYY-MM",
  "tickers": {
    "SPY": {
      "name": "SPDR S&P 500 ETF",
      "category": "equity",
      "monthly": [["2000-01", 148.23], ...]   // adjusted-close EOP
    },
    ...
  }
}

Usage:
    python -m predator.fetch_prices                    # incremental update
    python -m predator.fetch_prices --full-refresh     # fetch full history
    python -m predator.fetch_prices --dry-run          # print plan, no write
    python -m predator.fetch_prices --tickers SPY,QQQ  # specific tickers only
"""
from __future__ import annotations

import argparse
import json
import math
import sys
import time
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

REPO_ROOT   = Path(__file__).resolve().parent.parent
CONFIG_PATH = REPO_ROOT / "config.yaml"
OUTPUT_PATH = REPO_ROOT / "docs" / "data" / "prices.json"

# ─── Backtestable universe ────────────────────────────────────────────────────
# Curated set of benchmark + asset-class ETFs for the Portfolio Lab.
# ETFs from config.yaml are added dynamically at runtime.
BENCHMARK_UNIVERSE: dict[str, dict[str, str]] = {
    # Broad market benchmarks
    "SPY":  {"name": "S&P 500 ETF (SPY)",          "category": "equity"},
    "QQQ":  {"name": "Nasdaq-100 ETF (QQQ)",        "category": "equity"},
    "IWM":  {"name": "Russell 2000 ETF (IWM)",      "category": "equity"},
    "EWJ":  {"name": "Japan ETF (EWJ)",             "category": "equity"},
    "EEM":  {"name": "Emerging Markets ETF (EEM)",  "category": "equity"},
    "VEA":  {"name": "Developed Markets ETF (VEA)", "category": "equity"},
    # Fixed income
    "TLT":  {"name": "20Y Treasury ETF (TLT)",      "category": "bonds"},
    "IEF":  {"name": "7-10Y Treasury ETF (IEF)",    "category": "bonds"},
    "BND":  {"name": "Total Bond Market (BND)",     "category": "bonds"},
    "HYG":  {"name": "High Yield Bond ETF (HYG)",   "category": "bonds"},
    "LQD":  {"name": "Investment Grade Corp (LQD)", "category": "bonds"},
    # Commodities / alternatives
    "GLD":  {"name": "Gold ETF (GLD)",              "category": "commodities"},
    "SLV":  {"name": "Silver ETF (SLV)",            "category": "commodities"},
    "USO":  {"name": "Oil ETF (USO)",               "category": "commodities"},
    "DJP":  {"name": "Commodity Index (DJP)",       "category": "commodities"},
    # Real estate
    "VNQ":  {"name": "Real Estate ETF (VNQ)",       "category": "real_estate"},
    # Inverse / leveraged (for stress testing)
    "DWSH": {"name": "AdvisorShares Dorsey Wright Short (DWSH)", "category": "equity"},
    "SQQQ": {"name": "ProShares UltraPro Short QQQ (SQQQ)",     "category": "equity"},
    "SPXU": {"name": "ProShares UltraPro Short S&P500 (SPXU)",  "category": "equity"},
}


# ─── Safe JSON encoder ────────────────────────────────────────────────────────

import os


def _write_json_atomic(path: Path, text: str) -> None:
    """Write JSON atomically: write to .tmp then os.replace to avoid truncated files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


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


# ─── Load ETF universe from config.yaml ──────────────────────────────────────

def _load_etf_universe() -> dict[str, dict[str, str]]:
    """Load ETF tickers from config.yaml and return as {ticker: {name, category}}."""
    try:
        import yaml
        cfg = yaml.safe_load(CONFIG_PATH.read_text())
        result = {}
        for etf in cfg.get("etfs", []):
            ticker = etf["ticker"]
            tier = etf.get("tier", "equity")
            result[ticker] = {
                "name": f"{ticker} ({tier})",
                "category": tier.lower(),
            }
        return result
    except Exception as e:
        print(f"  WARNING: Could not load config.yaml: {e}")
        return {}


# ─── yfinance fetcher ─────────────────────────────────────────────────────────

def _fetch_monthly_adjusted(
    ticker: str,
    full_refresh: bool = False,
    existing_monthly: list[list] | None = None,
) -> list[list]:
    """
    Fetch monthly adjusted-close (total return, dividends reinvested) from yfinance.

    Returns sorted [[YYYY-MM, price], ...] list.
    On incremental runs, only fetches trailing 6 months and merges.
    """
    try:
        import yfinance as yf
    except ImportError:
        print(f"    ERROR: yfinance not installed. Run: pip install yfinance")
        return []

    try:
        if full_refresh or not existing_monthly:
            data = yf.download(ticker, period="max", interval="1mo",
                               auto_adjust=True, progress=False)
        else:
            # Incremental: fetch only from last cached month (+ 2 month buffer)
            last_ym = existing_monthly[-1][0]  # e.g. "2026-04"
            yr, mo = int(last_ym[:4]), int(last_ym[5:7])
            # Go back 2 months for overlap/revision safety
            mo -= 2
            if mo <= 0:
                mo += 12
                yr -= 1
            start_date = f"{yr:04d}-{mo:02d}-01"
            data = yf.download(ticker, start=start_date, interval="1mo",
                               auto_adjust=True, progress=False)
    except Exception as e:
        print(f"    ERROR fetching {ticker}: {e}")
        return existing_monthly or []

    if data is None or data.empty:
        return existing_monthly or []

    # Extract Close (auto_adjust=True gives total-return adjusted close)
    if isinstance(data.columns, pd.MultiIndex):
        close = data[("Close", ticker)] if ("Close", ticker) in data.columns else data["Close"].iloc[:, 0]
    else:
        close = data["Close"]

    close = close.dropna()

    # Convert to [[YYYY-MM, value], ...] list
    new_monthly = []
    for dt, val in close.items():
        ym = pd.Timestamp(dt).strftime("%Y-%m")
        new_monthly.append([ym, round(float(val), 4)])
    new_monthly.sort(key=lambda x: x[0])

    if not new_monthly:
        return existing_monthly or []

    # Merge with existing (new data takes priority)
    if existing_monthly and not full_refresh:
        existing_dict = {m: v for m, v in existing_monthly}
        for ym, val in new_monthly:
            existing_dict[ym] = val
        merged = sorted([[ym, v] for ym, v in existing_dict.items()], key=lambda x: x[0])
        return merged

    return new_monthly


# ─── Load / save ─────────────────────────────────────────────────────────────

def _load_existing() -> dict[str, Any]:
    if OUTPUT_PATH.exists():
        try:
            return json.loads(OUTPUT_PATH.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  WARNING: Could not parse existing prices.json ({e}). Starting fresh.")
    return {"generated_utc": "", "asof": "", "tickers": {}}


# ─── Main ─────────────────────────────────────────────────────────────────────

def fetch_prices(
    tickers_filter: list[str] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Fetch monthly adjusted-close for the full backtestable universe.

    Args:
        tickers_filter: If provided, only fetch these tickers.
        full_refresh: Ignore existing data and fetch full history.
        dry_run: Print plan without fetching.

    Returns:
        The output dict (also written to OUTPUT_PATH unless dry_run).
    """
    # Build full universe: ETFs from config + benchmark set
    etf_universe = _load_etf_universe()
    full_universe = {**BENCHMARK_UNIVERSE, **etf_universe}

    # Apply filter
    if tickers_filter:
        full_universe = {t: v for t, v in full_universe.items() if t in tickers_filter}

    print(f"\nFetch plan ({'FULL REFRESH' if full_refresh else 'INCREMENTAL'}):")
    print(f"  Universe: {len(full_universe)} tickers")
    for t in sorted(full_universe.keys()):
        print(f"    {t:8s} — {full_universe[t]['name']}")

    if dry_run:
        print("\n--dry-run: exiting without fetching.")
        return {}

    # Load existing data
    existing = _load_existing()
    existing_tickers = existing.get("tickers", {})

    output_tickers: dict[str, Any] = {}
    failed: list[str] = []

    for i, (ticker, meta) in enumerate(sorted(full_universe.items())):
        existing_data = existing_tickers.get(ticker, {})
        existing_monthly = existing_data.get("monthly", [])

        print(f"\n  [{i+1}/{len(full_universe)}] {ticker} — {meta['name']}"
              f" (cached: {len(existing_monthly)} months)")

        monthly = _fetch_monthly_adjusted(ticker, full_refresh, existing_monthly)

        if monthly:
            output_tickers[ticker] = {
                "name":     meta["name"],
                "category": meta["category"],
                "first":    monthly[0][0],
                "last":     monthly[-1][0],
                "monthly":  monthly,
            }
            print(f"    OK: {len(monthly)} months ({monthly[0][0]} → {monthly[-1][0]})")
        else:
            print(f"    FAILED: no data — preserving existing if available")
            failed.append(ticker)
            if existing_monthly:
                output_tickers[ticker] = existing_data
                print(f"    Using cached: {len(existing_monthly)} months")

        # Rate-limit pause
        if i < len(full_universe) - 1:
            time.sleep(0.3)

    # Compute asof
    all_lasts = [v["last"] for v in output_tickers.values() if v.get("last")]
    asof = max(all_lasts) if all_lasts else datetime.now().strftime("%Y-%m")

    output = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "asof":          asof,
        "tickers":       output_tickers,
    }

    # Write
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(OUTPUT_PATH, _dumps(output, separators=(",", ":")))
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\nWrote: {OUTPUT_PATH} ({size_kb:.1f} KB)")
    print(f"  asof: {asof}")
    print(f"  tickers: {len(output_tickers)} ({len(failed)} failed)")
    if failed:
        print(f"  failed: {', '.join(failed)}")

    return output


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="predator.fetch_prices",
        description="Fetch monthly adjusted-close for Portfolio Lab backtester",
    )
    parser.add_argument("--full-refresh", action="store_true",
                        help="Ignore cache and fetch full history.")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print plan without fetching.")
    parser.add_argument("--tickers", type=str, default=None,
                        help="Comma-separated tickers to fetch (default: full universe).")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    print("═" * 60)
    print("  Predator Protocol — Portfolio Lab Price Data Builder")
    print("═" * 60)

    tickers_filter = None
    if args.tickers:
        tickers_filter = [t.strip().upper() for t in args.tickers.split(",")]
        print(f"\nFiltering to: {', '.join(tickers_filter)}")

    fetch_prices(
        tickers_filter=tickers_filter,
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
