"""
Markets History — monthly close data for broad market indices, metals, and energy.

Fetches end-of-period monthly data from FRED (primary) and yfinance (fallback
for international indices without FRED series). Writes the consolidated output
to docs/data/market_returns.json for the Predator Protocol dashboard.

Usage:
    python -m predator.markets_history
    python -m predator.markets_history --full-refresh
    python -m predator.markets_history --assets sp500,gold --dry-run
    python -m predator.markets_history --validate-only
"""
from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

# ─── Platform encoding fix (Windows CI) ──────────────────────────────────────
if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass


# ─── Safe JSON encoder (NaN → null) ─────────────────────────────────────────

class _SafeEncoder(json.JSONEncoder):
    """Encode NaN / ±Inf floats as JSON null instead of invalid bare NaN."""

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
    """json.dumps using _SafeEncoder (NaN → null)."""
    kwargs.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kwargs)


# ─── Asset Registry ──────────────────────────────────────────────────────────

ASSET_REGISTRY: list[dict[str, str]] = [
    # Equities
    {
        "key": "sp500", "name": "S&P 500", "category": "equity",
        "native_ccy": "USD", "source_type": "fred", "series_id": "SP500",
        "return_type": "price", "notes": "weekly → EOP monthly",
    },
    {
        "key": "nasdaq", "name": "NASDAQ Composite", "category": "equity",
        "native_ccy": "USD", "source_type": "fred", "series_id": "NASDAQCOM",
        "return_type": "price", "notes": "",
    },
    {
        "key": "nasdaq100", "name": "NASDAQ-100", "category": "equity",
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "^NDX",
        "return_type": "price", "notes": "no FRED series",
    },
    {
        "key": "djia", "name": "Dow Jones Industrial Average", "category": "equity",
        "native_ccy": "USD", "source_type": "fred", "series_id": "DJIA",
        "return_type": "price", "notes": "",
    },
    {
        "key": "wilshire5000", "name": "Wilshire 5000 Total Market", "category": "equity",
        "native_ccy": "USD", "source_type": "fred", "series_id": "WILL5000IND",
        "return_type": "price", "notes": "",
    },
    {
        "key": "dax", "name": "DAX (Germany)", "category": "equity",
        "native_ccy": "EUR", "source_type": "yfinance", "series_id": "^GDAXI",
        "return_type": "price", "notes": "no FRED",
    },
    {
        "key": "nikkei", "name": "Nikkei 225 (Japan)", "category": "equity",
        "native_ccy": "JPY", "source_type": "yfinance", "series_id": "^N225",
        "return_type": "price", "notes": "no FRED",
    },
    {
        "key": "sensex", "name": "BSE Sensex (India)", "category": "equity",
        "native_ccy": "INR", "source_type": "yfinance", "series_id": "^BSESN",
        "return_type": "price", "notes": "no FRED",
    },
    # Precious Metals
    {
        "key": "gold", "name": "Gold (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "GOLDAMGBD228NLBM",
        "return_type": "price", "notes": "daily → EOP monthly",
    },
    {
        "key": "silver", "name": "Silver (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "SLVPRUSD",
        "return_type": "price", "notes": "",
    },
    {
        "key": "platinum", "name": "Platinum (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PPLTMUSDM",
        "return_type": "price", "notes": "",
    },
    {
        "key": "palladium", "name": "Palladium (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PPALMUSDM",
        "return_type": "price", "notes": "",
    },
    # Energy
    {
        "key": "wti_crude", "name": "WTI Crude Oil (USD/bbl)", "category": "energy",
        "native_ccy": "USD", "source_type": "fred", "series_id": "DCOILWTICO",
        "return_type": "price", "notes": "daily → EOP monthly",
    },
    {
        "key": "brent_crude", "name": "Brent Crude Oil (USD/bbl)", "category": "energy",
        "native_ccy": "USD", "source_type": "fred", "series_id": "DCOILBRENTEU",
        "return_type": "price", "notes": "daily → EOP monthly",
    },
    {
        "key": "natural_gas", "name": "Natural Gas (USD/MMBtu)", "category": "energy",
        "native_ccy": "USD", "source_type": "fred", "series_id": "MHHNGSP",
        "return_type": "price", "notes": "",
    },
]

# Auxiliary series (needed for derived calculations and toggles)
AUX_REGISTRY: list[dict[str, str]] = [
    {
        "key": "us_cpi", "name": "US CPI (All Urban)", "category": "auxiliary",
        "native_ccy": "USD", "source_type": "fred", "series_id": "CPIAUCSL",
        "return_type": "level", "notes": "for real-return adjustment",
    },
    {
        "key": "usdinr", "name": "USD/INR Exchange Rate", "category": "auxiliary",
        "native_ccy": "INR", "source_type": "fred", "series_id": "DEXINUS",
        "return_type": "rate", "notes": "daily → EOP monthly",
    },
    {
        "key": "us_tbill_yld", "name": "3-Month Treasury Bill Rate", "category": "auxiliary",
        "native_ccy": "USD", "source_type": "fred", "series_id": "TB3MS",
        "return_type": "yield", "notes": "secondary market rate",
    },
]

# Combined for iteration
ALL_SERIES = ASSET_REGISTRY + AUX_REGISTRY

# Cache directory for parquet files
CACHE_DIR = Path("data/markets_history")
# Output path
OUTPUT_PATH = Path("docs/data/market_returns.json")
# Trailing months to refetch on incremental runs
INCREMENTAL_TRAILING_MONTHS = 3


# ─── FRED Fetcher ────────────────────────────────────────────────────────────

def _get_fred_client():
    """Initialise fredapi.Fred with API key from env. Returns None on failure."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # python-dotenv optional at runtime if env var already set

    api_key = os.environ.get("FRED_API_KEY")
    if not api_key:
        print("ERROR: FRED_API_KEY environment variable not set.")
        print("  Set it in your shell or create a .env file with FRED_API_KEY=your_key")
        return None

    try:
        from fredapi import Fred
    except ImportError:
        print("ERROR: fredapi package not installed. Run: pip install fredapi")
        return None

    return Fred(api_key=api_key)


def _fetch_fred_series(
    fred,
    series_id: str,
    full_refresh: bool = False,
    existing: pd.Series | None = None,
) -> pd.Series:
    """
    Fetch a FRED series with monthly EOP aggregation.

    Uses exponential backoff on 429 (rate limit) errors.
    On incremental runs, only fetches trailing 3 months and merges.
    """
    kwargs: dict[str, Any] = {
        "frequency": "m",
        "aggregation_method": "eop",
    }

    # Incremental: restrict observation_start to trailing window
    if not full_refresh and existing is not None and not existing.empty:
        last_date = existing.index.max()
        start = last_date - pd.DateOffset(months=INCREMENTAL_TRAILING_MONTHS)
        kwargs["observation_start"] = start.strftime("%Y-%m-%d")

    max_retries = 5
    for attempt in range(max_retries + 1):
        try:
            data = fred.get_series(series_id, **kwargs)
            break
        except Exception as e:
            err_str = str(e)
            # Rate limit (429) — exponential backoff
            if "429" in err_str or "Too Many Requests" in err_str:
                if attempt >= max_retries:
                    print(f"    FAILED after {max_retries} retries: {series_id}")
                    return pd.Series(dtype=float)
                wait = min(2 ** attempt * 0.5, 30)
                print(f"    Rate limited on {series_id}, waiting {wait:.1f}s (attempt {attempt + 1})")
                time.sleep(wait)
            else:
                print(f"    ERROR fetching {series_id}: {e}")
                return pd.Series(dtype=float)

    if data is None or data.empty:
        return pd.Series(dtype=float)

    # Merge with existing if incremental
    if not full_refresh and existing is not None and not existing.empty:
        combined = pd.concat([existing, data])
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined.sort_index()

    return data


# ─── yfinance Fetcher ────────────────────────────────────────────────────────

def _fetch_yfinance_series(
    ticker: str,
    full_refresh: bool = False,
    existing: pd.Series | None = None,
) -> pd.Series:
    """
    Fetch monthly close data from yfinance for international indices.

    Returns a Series indexed by period-end dates with monthly close values.
    """
    try:
        import yfinance as yf
    except ImportError:
        print(f"    ERROR: yfinance not installed. Run: pip install yfinance")
        return pd.Series(dtype=float)

    try:
        data = yf.download(ticker, period="max", interval="1mo", progress=False)
    except Exception as e:
        print(f"    ERROR fetching {ticker} from yfinance: {e}")
        return pd.Series(dtype=float)

    if data is None or data.empty:
        return pd.Series(dtype=float)

    # Extract Close column (handle MultiIndex columns from yfinance)
    if isinstance(data.columns, pd.MultiIndex):
        close = data[("Close", ticker)] if ("Close", ticker) in data.columns else data["Close"].iloc[:, 0]
    else:
        close = data["Close"]

    close = close.dropna()

    # Merge with existing if incremental
    if not full_refresh and existing is not None and not existing.empty:
        combined = pd.concat([existing, close])
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined.sort_index()

    return close


# ─── Cache (Parquet) ─────────────────────────────────────────────────────────

def _read_cache(key: str) -> pd.Series | None:
    """Read cached parquet for a series. Returns None if not found."""
    path = CACHE_DIR / f"{key}.parquet"
    if not path.exists():
        return None
    try:
        df = pd.read_parquet(path)
        if "value" in df.columns:
            s = df["value"]
            s.index = pd.to_datetime(df.index)
            return s
        return None
    except Exception:
        return None


def _write_cache(key: str, data: pd.Series) -> None:
    """Write series to parquet cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"value": data})
    df.to_parquet(CACHE_DIR / f"{key}.parquet")


# ─── Series → YYYY-MM dict ──────────────────────────────────────────────────

def _series_to_monthly_dict(data: pd.Series) -> dict[str, float]:
    """
    Convert a pandas Series with datetime index to {YYYY-MM: value} dict.

    Rounds values to 4 decimal places. Drops NaN entries.
    """
    if data.empty:
        return {}
    data = data.dropna()
    out = {}
    for dt, val in data.items():
        key = pd.Timestamp(dt).strftime("%Y-%m")
        out[key] = round(float(val), 4)
    return out


# ─── Main Fetch Logic ────────────────────────────────────────────────────────

def fetch_all(
    assets: list[str] | None = None,
    full_refresh: bool = False,
    dry_run: bool = False,
) -> dict[str, pd.Series]:
    """
    Fetch all (or filtered) series from FRED/yfinance.

    Args:
        assets: If provided, only fetch these asset keys.
        full_refresh: If True, ignore cache and fetch full history.
        dry_run: If True, print the plan and return empty dict.

    Returns:
        Dict mapping series key → pandas Series of monthly close values.
    """
    # Filter registry if --assets specified
    series_to_fetch = ALL_SERIES
    if assets:
        asset_set = set(a.lower().strip() for a in assets)
        series_to_fetch = [s for s in ALL_SERIES if s["key"] in asset_set]
        missing = asset_set - {s["key"] for s in series_to_fetch}
        if missing:
            print(f"  WARNING: Unknown asset keys: {', '.join(sorted(missing))}")

    # Print fetch plan
    fred_series = [s for s in series_to_fetch if s["source_type"] == "fred"]
    yf_series = [s for s in series_to_fetch if s["source_type"] == "yfinance"]
    mode = "FULL REFRESH" if full_refresh else "INCREMENTAL (trailing 3 months)"

    print(f"\nFetch plan ({mode}):")
    print(f"  FRED series:     {len(fred_series)}")
    for s in fred_series:
        print(f"    {s['key']:20s} → {s['series_id']}")
    print(f"  yfinance series: {len(yf_series)}")
    for s in yf_series:
        print(f"    {s['key']:20s} → {s['series_id']}")
    print(f"  Total:           {len(series_to_fetch)} series")

    if dry_run:
        print("\n  --dry-run: exiting without fetching.")
        return {}

    # Initialise FRED client (only if we have FRED series to fetch)
    fred = None
    if fred_series:
        fred = _get_fred_client()
        if fred is None:
            print("\n  Cannot proceed without FRED API key for FRED series.")
            sys.exit(1)

    results: dict[str, pd.Series] = {}

    for i, spec in enumerate(series_to_fetch):
        key = spec["key"]
        source_type = spec["source_type"]
        series_id = spec["series_id"]

        # Load existing cache for incremental
        existing = None if full_refresh else _read_cache(key)
        cached_points = len(existing) if existing is not None else 0

        print(f"\n  [{i + 1}/{len(series_to_fetch)}] {key} ({source_type}:{series_id})"
              f" — cached: {cached_points} pts")

        if source_type == "fred":
            data = _fetch_fred_series(fred, series_id, full_refresh, existing)
        elif source_type == "yfinance":
            data = _fetch_yfinance_series(series_id, full_refresh, existing)
        else:
            print(f"    SKIP: unknown source_type '{source_type}'")
            continue

        if data is not None and not data.empty:
            _write_cache(key, data)
            results[key] = data
            print(f"    OK: {len(data)} data points"
                  f" ({pd.Timestamp(data.index.min()).strftime('%Y-%m')}"
                  f" → {pd.Timestamp(data.index.max()).strftime('%Y-%m')})")
        else:
            print(f"    WARNING: no data returned")
            # Use cached data if available
            if existing is not None and not existing.empty:
                results[key] = existing
                print(f"    Using cached data: {len(existing)} pts")

        # Rate-limit pause between sequential fetches
        if i < len(series_to_fetch) - 1:
            time.sleep(0.1)

    return results


# ─── Validation ──────────────────────────────────────────────────────────────

def validate(results: dict[str, pd.Series]) -> bool:
    """
    Validate fetched data. Returns True if all checks pass.

    Checks:
        - Each series is non-empty
        - Warns if series has <10 data points
    """
    print("\nValidation:")
    all_ok = True

    for spec in ALL_SERIES:
        key = spec["key"]
        data = results.get(key)
        if data is None or data.empty:
            # Try loading from cache
            data = _read_cache(key)

        if data is None or data.empty:
            print(f"  FAIL: {key} — no data")
            all_ok = False
        elif len(data) < 10:
            print(f"  WARN: {key} — only {len(data)} data points")
        else:
            print(f"  OK:   {key} — {len(data)} pts")

    return all_ok


# ─── Build Output JSON ───────────────────────────────────────────────────────

def build_output(results: dict[str, pd.Series]) -> dict[str, Any]:
    """
    Build the market_returns.json structure from fetched data.

    Returns the full JSON-serialisable dict.
    """
    output: dict[str, Any] = {
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "default_return_type": "price",
        "aux": {},
        "assets": {},
    }

    # Build aux section
    for spec in AUX_REGISTRY:
        key = spec["key"]
        data = results.get(key)
        if data is None or data.empty:
            data = _read_cache(key)
        if data is not None and not data.empty:
            output["aux"][key] = {
                "values": _series_to_monthly_dict(data),
            }

    # Build assets section
    for spec in ASSET_REGISTRY:
        key = spec["key"]
        data = results.get(key)
        if data is None or data.empty:
            data = _read_cache(key)
        if data is None or data.empty:
            continue

        monthly = _series_to_monthly_dict(data)
        if not monthly:
            continue

        sorted_months = sorted(monthly.keys())
        output["assets"][key] = {
            "meta": {
                "name": spec["name"],
                "category": spec["category"],
                "native_ccy": spec["native_ccy"],
                "source": f"{spec['source_type']}:{spec['series_id']}",
                "return_type": spec["return_type"],
                "first": sorted_months[0],
                "last": sorted_months[-1],
            },
            "close": monthly,
        }

    return output


def write_output(output: dict[str, Any], path: Path | None = None) -> None:
    """Write the market_returns.json file."""
    path = path or OUTPUT_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_dumps(output, indent=2), encoding="utf-8")
    print(f"\nWrote: {path} ({path.stat().st_size / 1024:.1f} KB)")


# ─── CLI ─────────────────────────────────────────────────────────────────────

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="predator.markets_history",
        description="Fetch monthly market close data from FRED/yfinance and build market_returns.json",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Ignore cache and fetch full history for all series.",
    )
    parser.add_argument(
        "--assets",
        type=str,
        default=None,
        help="Comma-separated list of asset keys to fetch (e.g. sp500,gold). Default: all.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the fetch plan and exit without fetching.",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate existing cached data without fetching.",
    )
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output path for market_returns.json. Default: {OUTPUT_PATH}",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for python -m predator.markets_history."""
    args = parse_args(argv)

    print("═" * 60)
    print("  Predator Protocol — Markets History Builder")
    print("═" * 60)

    assets = [a.strip() for a in args.assets.split(",")] if args.assets else None
    output_path = Path(args.output) if args.output else OUTPUT_PATH

    # --validate-only: check cached data and exit
    if args.validate_only:
        # Load all from cache
        results = {}
        for spec in ALL_SERIES:
            cached = _read_cache(spec["key"])
            if cached is not None:
                results[spec["key"]] = cached
        ok = validate(results)
        return 0 if ok else 1

    # Fetch data
    results = fetch_all(
        assets=assets,
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        return 0

    # Validate
    ok = validate(results)
    if not ok:
        print("\n  WARNING: Some series have no data. Output may be incomplete.")

    # Build and write JSON
    output = build_output(results)
    asset_count = len(output["assets"])
    aux_count = len(output["aux"])
    print(f"\nBuilt output: {asset_count} assets, {aux_count} aux series")

    write_output(output, output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
