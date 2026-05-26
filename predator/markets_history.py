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


def _write_json_atomic(path: Path, text: str) -> None:
    """Write JSON atomically: write to .tmp then os.replace to avoid truncated files."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _dumps(obj: Any, **kwargs) -> str:
    """json.dumps using _SafeEncoder (NaN → null)."""
    kwargs.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kwargs)


# ─── Asset Registry ──────────────────────────────────────────────────────────

ASSET_REGISTRY: list[dict[str, str]] = [
    # Equities
    {
        "key": "sp500", "name": "S&P 500", "category": "equity",
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "^GSPC",
        "return_type": "price", "notes": "yfinance — longer history than FRED SP500",
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
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "^DJI",
        "return_type": "price", "notes": "yfinance — longer history than FRED DJIA",
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
    # Precious Metals — use yfinance futures for longer history (FRED series are limited/discontinued)
    {
        "key": "gold", "name": "Gold (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "GC=F",
        "return_type": "price", "notes": "Gold futures, 1979+",
    },
    {
        "key": "silver", "name": "Silver (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "SI=F",
        "return_type": "price", "notes": "Silver futures, 1979+",
    },
    {
        "key": "platinum", "name": "Platinum (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "PL=F",
        "return_type": "price", "notes": "Platinum futures, 1986+",
    },
    {
        "key": "palladium", "name": "Palladium (USD/oz)", "category": "precious_metals",
        "native_ccy": "USD", "source_type": "yfinance", "series_id": "PA=F",
        "return_type": "price", "notes": "Palladium futures, 2000+",
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
    {
        "key": "coal", "name": "Coal (Aus Thermal, USD/t)", "category": "energy",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PCOALAUUSDM",
        "return_type": "price", "notes": "IMF monthly",
    },
    # Base Metals
    {
        "key": "copper", "name": "Copper (USD/mt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PCOPPUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "aluminum", "name": "Aluminum (USD/mt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PALUMUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "nickel", "name": "Nickel (USD/mt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PNICKUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "zinc", "name": "Zinc (USD/mt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PZINCUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "iron_ore", "name": "Iron Ore (USD/dmt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PIORECRUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "tin", "name": "Tin (USD/mt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PTINUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "lead", "name": "Lead (USD/mt)", "category": "base_metals",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PLEADUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    # Agriculture
    {
        "key": "wheat", "name": "Wheat (USD/mt)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PWHEAMTUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "corn", "name": "Corn / Maize (USD/mt)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PMAIZMTUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "soybeans", "name": "Soybeans (USD/mt)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PSOYBUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "cotton", "name": "Cotton (USD/kg)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PCOTTINDUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "sugar", "name": "Sugar (USD/kg)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PSUGAISAUSDM",
        "return_type": "price", "notes": "ISA price, IMF monthly",
    },
    {
        "key": "coffee", "name": "Coffee Arabica (USD/kg)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PCOFFOTMUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "cocoa", "name": "Cocoa (USD/kg)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PCOCOUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "rice", "name": "Rice (USD/mt)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PRICENPQUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    {
        "key": "palm_oil", "name": "Palm Oil (USD/mt)", "category": "agriculture",
        "native_ccy": "USD", "source_type": "fred", "series_id": "PPOILUSDM",
        "return_type": "price", "notes": "IMF/World Bank monthly",
    },
    # FX Rates (stored as USD per 1 unit of foreign currency — inverted where needed)
    {
        "key": "fx_usdinr", "name": "USD/INR", "category": "fx",
        "native_ccy": "INR", "source_type": "fred", "series_id": "DEXINUS",
        "return_type": "fx", "notes": "FRED: INR per USD → inverted to USD per INR",
        "invert": True,
    },
    {
        "key": "fx_usdjpy", "name": "USD/JPY", "category": "fx",
        "native_ccy": "JPY", "source_type": "fred", "series_id": "DEXJPUS",
        "return_type": "fx", "notes": "FRED: JPY per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_eurusd", "name": "EUR/USD", "category": "fx",
        "native_ccy": "EUR", "source_type": "fred", "series_id": "DEXUSEU",
        "return_type": "fx", "notes": "FRED: USD per EUR — no inversion",
        "invert": False,
    },
    {
        "key": "fx_gbpusd", "name": "GBP/USD", "category": "fx",
        "native_ccy": "GBP", "source_type": "fred", "series_id": "DEXUSUK",
        "return_type": "fx", "notes": "FRED: USD per GBP — no inversion",
        "invert": False,
    },
    {
        "key": "fx_usdcny", "name": "USD/CNY", "category": "fx",
        "native_ccy": "CNY", "source_type": "fred", "series_id": "DEXCHUS",
        "return_type": "fx", "notes": "FRED: CNY per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdcad", "name": "USD/CAD", "category": "fx",
        "native_ccy": "CAD", "source_type": "fred", "series_id": "DEXCAUS",
        "return_type": "fx", "notes": "FRED: CAD per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdaud", "name": "USD/AUD", "category": "fx",
        "native_ccy": "AUD", "source_type": "fred", "series_id": "DEXUSAL",
        "return_type": "fx", "notes": "FRED: AUD per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdsgd", "name": "USD/SGD", "category": "fx",
        "native_ccy": "SGD", "source_type": "fred", "series_id": "DEXSIUS",
        "return_type": "fx", "notes": "FRED: SGD per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdbrl", "name": "USD/BRL", "category": "fx",
        "native_ccy": "BRL", "source_type": "fred", "series_id": "DEXBZUS",
        "return_type": "fx", "notes": "FRED: BRL per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdmxn", "name": "USD/MXN", "category": "fx",
        "native_ccy": "MXN", "source_type": "fred", "series_id": "DEXMXUS",
        "return_type": "fx", "notes": "FRED: MXN per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdkrw", "name": "USD/KRW", "category": "fx",
        "native_ccy": "KRW", "source_type": "fred", "series_id": "DEXKOUS",
        "return_type": "fx", "notes": "FRED: KRW per USD → inverted",
        "invert": True,
    },
    {
        "key": "fx_usdchf", "name": "USD/CHF", "category": "fx",
        "native_ccy": "CHF", "source_type": "fred", "series_id": "DEXSZUS",
        "return_type": "fx", "notes": "FRED: CHF per USD → inverted",
        "invert": True,
    },
    # Real Estate
    {
        "key": "us_home_prices", "name": "US Home Prices (Case-Shiller)", "category": "real_estate",
        "native_ccy": "USD", "source_type": "fred", "series_id": "CSUSHPISA",
        "return_type": "price", "notes": "National, monthly, 1987+",
    },
]

# Interest rate series — stored as yield levels (not returns), used in View H
RATES_REGISTRY: list[dict[str, str]] = [
    {
        "key": "us_3m_tbill", "name": "US 3M T-Bill", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "TB3MS",
        "return_type": "yield", "notes": "1934+",
    },
    {
        "key": "us_2y", "name": "US 2Y Treasury", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "GS2",
        "return_type": "yield", "notes": "1976+",
    },
    {
        "key": "us_10y", "name": "US 10Y Treasury", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "GS10",
        "return_type": "yield", "notes": "1953+",
    },
    {
        "key": "us_30y", "name": "US 30Y Treasury", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "GS30",
        "return_type": "yield", "notes": "1977+",
    },
    {
        "key": "fed_funds", "name": "Fed Funds Rate", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "FEDFUNDS",
        "return_type": "yield", "notes": "1954+",
    },
    {
        "key": "moodys_baa", "name": "Moody's BAA Corp", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "DBAA",
        "return_type": "yield", "notes": "1919+",
    },
    {
        "key": "moodys_aaa", "name": "Moody's AAA Corp", "category": "rates",
        "native_ccy": "USD", "source_type": "fred", "series_id": "DAAA",
        "return_type": "yield", "notes": "1919+",
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
ALL_SERIES = ASSET_REGISTRY + AUX_REGISTRY + RATES_REGISTRY

# Cache directory for parquet files
CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "markets_history"
# Output path
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "docs" / "data" / "market_returns.json"
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
        # Log empty-fetch evidence so the L1-vs-L2 root-cause discrimination
        # in CI can confirm "yfinance/FRED returned empty" vs "live fetch
        # never ran". Required by design Fix Implementation #6.
        print(f"    EMPTY: fred returned 0 rows for {series_id}")
        return pd.Series(dtype=float)

    # Merge with existing if incremental
    if not full_refresh and existing is not None and not existing.empty:
        combined = pd.concat([existing, data])
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined.sort_index()

    return data


# ─── yfinance Fetcher ────────────────────────────────────────────────────────

# Tickers known to be intermittently empty from CI runners (Wave-0 diagnostic
# established yfinance is IP-rate-limited from GitHub Actions egress for these
# four). One bounded retry with backoff before degrading to cache.
_YFINANCE_FLAKY_TICKERS = frozenset({"^GSPC", "^NDX", "^DJI", "NASDAQCOM"})
_YFINANCE_RETRY_BACKOFF_SEC = 1.5

def _fetch_yfinance_series(
    ticker: str,
    full_refresh: bool = False,
    existing: pd.Series | None = None,
) -> pd.Series:
    """
    Fetch monthly close data from yfinance for international indices.

    Returns a Series indexed by period-end dates with monthly close values.

    For tickers in :data:`_YFINANCE_FLAKY_TICKERS` (^GSPC, ^NDX, ^DJI,
    NASDAQCOM — established by the Wave-0 live-fetch diagnostic to be
    intermittently empty from GitHub Actions egress), one bounded retry
    with a short backoff is attempted before returning empty. Retries
    only fire on the empty-result path; exceptions still flow through
    the existing try/except and degrade to cache as before.
    """
    try:
        import yfinance as yf
    except ImportError:
        print(f"    ERROR: yfinance not installed. Run: pip install yfinance")
        return pd.Series(dtype=float)

    def _download_close(_ticker: str) -> pd.Series:
        """One yfinance attempt → cleaned monthly Close Series (or empty)."""
        try:
            data = yf.download(_ticker, period="max", interval="1mo", progress=False)
        except Exception as e:
            print(f"    ERROR fetching {_ticker} from yfinance: {e}")
            return pd.Series(dtype=float)

        if data is None or data.empty:
            return pd.Series(dtype=float)

        # Extract Close column (handle MultiIndex columns from yfinance)
        if isinstance(data.columns, pd.MultiIndex):
            close = data[("Close", _ticker)] if ("Close", _ticker) in data.columns else data["Close"].iloc[:, 0]
        else:
            close = data["Close"]

        close = close.dropna()
        # Ensure tz-naive index to avoid concat issues with mixed tz-aware/naive series
        if hasattr(close.index, 'tz') and close.index.tz is not None:
            close = close.tz_localize(None)
        return close

    close = _download_close(ticker)

    # Bounded retry for known-flaky tickers (Wave-0 evidence: yfinance is
    # intermittently empty from CI runners for these). A single retry with
    # short backoff is enough to absorb transient egress-rate-limit blips
    # without slowing the build materially.
    if (close is None or close.empty) and ticker in _YFINANCE_FLAKY_TICKERS:
        print(f"    RETRY: yfinance empty for {ticker}, waiting "
              f"{_YFINANCE_RETRY_BACKOFF_SEC}s before one retry")
        time.sleep(_YFINANCE_RETRY_BACKOFF_SEC)
        close = _download_close(ticker)

    if close is None or close.empty:
        # Log empty-fetch evidence (after any retries) so the root-cause
        # discrimination loop in CI can read it from the build log.
        # Required by design Fix Implementation #6.
        print(f"    EMPTY: yfinance returned 0 rows for {ticker}")
        return pd.Series(dtype=float)

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

    # Initialise FRED client (only if we have FRED series to fetch).
    # If the API key is missing or the client fails to initialise, we DO NOT
    # abort — we simply skip the FRED-only series and proceed with yfinance
    # plus any cached parquet from previous runs. This is the "self-living"
    # contract: a single external dependency hiccup must never collapse the
    # whole live merge. The build step in CI also has continue-on-error, but
    # we'd rather complete every series we CAN reach than emit nothing.
    fred = None
    fred_skipped = False
    if fred_series:
        fred = _get_fred_client()
        if fred is None:
            fred_skipped = True
            print(f"\n  ⚠ FRED unavailable — {len(fred_series)} FRED series will fall back to "
                  f"on-disk parquet cache (charter v2 Part 2: soft-skip, never sys.exit).")

    results: dict[str, pd.Series] = {}
    fetched_live  = 0   # series we actually pulled fresh from upstream
    served_cache  = 0   # series we served from local parquet cache after upstream failure
    no_data       = 0   # series with neither live nor cached data

    for i, spec in enumerate(series_to_fetch):
        key = spec["key"]
        source_type = spec["source_type"]
        series_id = spec["series_id"]

        # Load existing cache for incremental (and as a fallback on failure)
        existing = None if full_refresh else _read_cache(key)
        cached_points = len(existing) if existing is not None else 0

        print(f"\n  [{i + 1}/{len(series_to_fetch)}] {key} ({source_type}:{series_id})"
              f" — cached: {cached_points} pts")

        # ── Per-series resilience: any single fetch exception is logged and
        # ── degrades to cache, never propagates and never kills the loop.
        data = pd.Series(dtype=float)
        try:
            if source_type == "fred":
                if fred is None:
                    print(f"    SKIP fetch (no FRED client) — will use cache if available")
                else:
                    data = _fetch_fred_series(fred, series_id, full_refresh, existing)
            elif source_type == "yfinance":
                data = _fetch_yfinance_series(series_id, full_refresh, existing)
            else:
                print(f"    SKIP: unknown source_type '{source_type}'")
                continue
        except Exception as e:
            # Per-series isolation. Charter v2 Part 3: external-data failures
            # (network blips, JSON shape changes, library bugs) must never
            # block the rest of the build.
            print(f"    ERROR: {type(e).__name__}: {e}  — degrading to cache")
            data = pd.Series(dtype=float)

        if data is not None and not data.empty:
            try:
                _write_cache(key, data)
            except Exception as e:
                print(f"    WARN: could not refresh cache: {e}")
            results[key] = data
            fetched_live += 1
            print(f"    OK (live): {len(data)} pts"
                  f" ({pd.Timestamp(data.index.min()).strftime('%Y-%m')}"
                  f" → {pd.Timestamp(data.index.max()).strftime('%Y-%m')})")
        else:
            # Live fetch failed or returned empty — fall back to cache
            if existing is not None and not existing.empty:
                results[key] = existing
                served_cache += 1
                print(f"    OK (cache): {len(existing)} pts (live unavailable)")
            else:
                no_data += 1
                print(f"    WARNING: no live data and no cache — series will be missing")

        # Rate-limit pause between sequential fetches
        if i < len(series_to_fetch) - 1:
            time.sleep(0.1)

    print(f"\n  ── Live merge summary ──")
    print(f"    {fetched_live:3d} series fetched live")
    print(f"    {served_cache:3d} series served from cache fallback")
    print(f"    {no_data:3d} series with no data (cold-start gaps)")
    if fred_skipped:
        print(f"    ⚠ FRED was skipped this run — set FRED_API_KEY in CI secrets to enable live FRED fetches.")
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

    Emits the §2.3 canonical contract shape so it is compatible with
    ingest_markets_xl.py and the dashboard JavaScript:
    {
      "asof": "YYYY-MM",
      "generated_utc": "...",
      "assets": { key: { "meta": {...}, "monthly": [[YYYY-MM, val], ...] } },
      "fx":    { "USDINR": [[YYYY-MM, val], ...], ... },
      "cpi":   { "US": [[YYYY-MM, val], ...] },
      "rates": { key: { "meta": {...}, "values": [[YYYY-MM, val], ...] } },
      "events": [...]
    }

    Key changes vs old format:
    - assets use "monthly" (sorted list of [YYYY-MM, val]) not "close" (dict)
    - FX series go into top-level "fx" section (not inside "assets")
    - CPI goes into top-level "cpi" section (not inside "aux")
    - rates use "values" as sorted list of [YYYY-MM, val]
    - "asof" field added

    This function MERGES with the existing JSON so that Excel deep-history
    (gold from 1833, S&P from 1871) is preserved when markets_history.py
    only fetches recent data.
    """
    from datetime import date as _date
    from predator.ingest_markets_xl import (
        EVENTS as _EVENTS,
        load_existing as _load_xl_existing,
        merge_monthly as _merge_monthly_fn,
    )

    # Load existing JSON to preserve Excel deep-history
    existing = _load_xl_existing(OUTPUT_PATH)
    existing_assets = existing.get("assets", {})

    def _to_monthly_list(data: pd.Series) -> list[list]:
        """Convert pandas Series with datetime index to sorted [[YYYY-MM, val]] list."""
        if data is None or data.empty:
            return []
        result = []
        for dt, val in data.items():
            if pd.isna(val):
                continue
            ym = pd.Timestamp(dt).strftime("%Y-%m")
            result.append([ym, round(float(val), 4)])
        return sorted(result, key=lambda x: x[0])

    assets_out: dict[str, Any] = {}
    fx_out: dict[str, list] = existing.get("fx", {})
    cpi_out: dict[str, list] = existing.get("cpi", {})
    rates_out: dict[str, Any] = {}

    # ── Asset series (price data) ─────────────────────────────────────────
    for spec in ASSET_REGISTRY:
        key = spec["key"]
        return_type = spec.get("return_type", "price")

        # Skip FX series — they go into the fx section
        if return_type == "fx":
            continue

        data = results.get(key)
        if data is None or data.empty:
            data = _read_cache(key)
        if data is None or data.empty:
            # No live data and no cache — fall through to the Excel-seeded
            # existing entry so the deep-history months stay on the page.
            #
            # Honest-source path (design Fix Implementation #7): when the
            # existing entry still carries the Mega_Markets_Historical Excel
            # label, mark `meta._live_holdout = True` so the verdict block
            # in predator/build.py (Task 3.5) can name this asset as a
            # genuine holdout. We DO NOT modify the `source` label — the
            # Excel label is honest when no live data is available.
            #
            # Scope: this loop only iterates ASSET_REGISTRY, every entry of
            # which has source_type ∈ {yfinance, fred}. So any asset that
            # falls through here is by construction a registry-source asset
            # whose live fetch is genuinely unavailable in this run — which
            # is the user-agreed widened holdout scope (Wave-0 finding).
            if key in existing_assets:
                existing_entry = existing_assets[key]
                existing_meta = (existing_entry or {}).get("meta") or {}
                existing_source = str(existing_meta.get("source", ""))
                if existing_source.startswith("Mega_Markets_Historical"):
                    # Shallow copy to avoid mutating the loaded `existing`
                    # JSON object (defensive: it may be re-read elsewhere).
                    new_meta = dict(existing_meta)
                    new_meta["_live_holdout"] = True
                    new_entry = dict(existing_entry)
                    new_entry["meta"] = new_meta
                    assets_out[key] = new_entry
                else:
                    assets_out[key] = existing_entry
            continue

        # Apply FX inversion if needed
        if spec.get("invert"):
            data = 1.0 / data.replace(0, float("nan"))

        new_monthly = _to_monthly_list(data)
        if not new_monthly:
            # data was non-empty but every row dropped (all NaN). Treat
            # identically to the empty-fetch case above: keep Excel
            # deep-history and mark the live-holdout sentinel.
            if key in existing_assets:
                existing_entry = existing_assets[key]
                existing_meta = (existing_entry or {}).get("meta") or {}
                existing_source = str(existing_meta.get("source", ""))
                if existing_source.startswith("Mega_Markets_Historical"):
                    new_meta = dict(existing_meta)
                    new_meta["_live_holdout"] = True
                    new_entry = dict(existing_entry)
                    new_entry["meta"] = new_meta
                    assets_out[key] = new_entry
                else:
                    assets_out[key] = existing_entry
            continue

        # Merge with existing deep-history from Excel
        if key in existing_assets:
            existing_monthly = existing_assets[key].get("monthly", [])
            # Convert old "close" dict format if present
            if not existing_monthly and "close" in existing_assets[key]:
                existing_monthly = sorted(
                    [[ym, v] for ym, v in existing_assets[key]["close"].items()],
                    key=lambda x: x[0]
                )
            merged = _merge_monthly_fn(existing_monthly, new_monthly)
        else:
            merged = new_monthly

        sorted_months = [m[0] for m in merged]
        assets_out[key] = {
            "meta": {
                "name":        spec["name"],
                "category":    spec["category"],
                "native_ccy":  spec["native_ccy"],
                "source":      f"{spec['source_type']}:{spec['series_id']}",
                "return_type": return_type,
                "first":       sorted_months[0],
                "last":        sorted_months[-1],
                "notes":       spec.get("notes", ""),
            },
            "monthly": merged,
        }

    # Preserve any Excel-only assets not in FRED/yfinance registry
    for key, val in existing_assets.items():
        if key not in assets_out:
            assets_out[key] = val

    # ── FX series → top-level "fx" section ───────────────────────────────
    for spec in ASSET_REGISTRY:
        if spec.get("return_type") != "fx":
            continue
        key = spec["key"]
        data = results.get(key)
        if data is None or data.empty:
            data = _read_cache(key)
        if data is None or data.empty:
            continue
        if spec.get("invert"):
            data = 1.0 / data.replace(0, float("nan"))
        new_monthly = _to_monthly_list(data)
        if not new_monthly:
            continue
        # Map asset key to FX pair name (e.g. fx_usdinr → USDINR)
        pair = key.replace("fx_", "").upper()
        existing_fx = fx_out.get(pair, [])
        fx_out[pair] = _merge_monthly_fn(existing_fx, new_monthly)

    # ── CPI → top-level "cpi" section ────────────────────────────────────
    for spec in AUX_REGISTRY:
        if spec["key"] != "us_cpi":
            continue
        data = results.get("us_cpi")
        if data is None or data.empty:
            data = _read_cache("us_cpi")
        if data is not None and not data.empty:
            new_monthly = _to_monthly_list(data)
            existing_cpi = cpi_out.get("US", [])
            cpi_out["US"] = _merge_monthly_fn(existing_cpi, new_monthly)

    # ── Rates → top-level "rates" section ────────────────────────────────
    for spec in RATES_REGISTRY:
        key = spec["key"]
        data = results.get(key)
        if data is None or data.empty:
            data = _read_cache(key)
        if data is None or data.empty:
            # Preserve existing
            if key in existing.get("rates", {}):
                rates_out[key] = existing["rates"][key]
            continue

        new_monthly = _to_monthly_list(data)
        if not new_monthly:
            if key in existing.get("rates", {}):
                rates_out[key] = existing["rates"][key]
            continue

        # Merge with existing
        existing_rate = existing.get("rates", {}).get(key, {})
        existing_vals = existing_rate.get("values", [])
        # Convert old dict format if present
        if not existing_vals and isinstance(existing_rate.get("values"), dict):
            existing_vals = sorted(
                [[ym, v] for ym, v in existing_rate["values"].items()],
                key=lambda x: x[0]
            )
        merged_vals = _merge_monthly_fn(existing_vals, new_monthly)
        sorted_months = [m[0] for m in merged_vals]
        rates_out[key] = {
            "meta": {
                "name":        spec["name"],
                "category":    "rates",
                "native_ccy":  spec["native_ccy"],
                "source":      f"{spec['source_type']}:{spec['series_id']}",
                "return_type": "yield",
                "first":       sorted_months[0],
                "last":        sorted_months[-1],
                "notes":       spec.get("notes", ""),
            },
            "values": merged_vals,
        }

    # Derived 10Y-2Y spread
    if "us_10y" in rates_out and "us_2y" in rates_out:
        v10 = {m: v for m, v in rates_out["us_10y"]["values"]}
        v2  = {m: v for m, v in rates_out["us_2y"]["values"]}
        spread_list = sorted(
            [[m, round(v10[m] - v2[m], 4)] for m in v10 if m in v2 and v10[m] is not None and v2[m] is not None],
            key=lambda x: x[0]
        )
        if spread_list:
            rates_out["spread_10y_2y"] = {
                "meta": {
                    "name": "10Y–2Y Spread", "category": "rates",
                    "native_ccy": "USD", "source": "derived:GS10-GS2",
                    "return_type": "yield",
                    "first": spread_list[0][0], "last": spread_list[-1][0],
                    "notes": "10Y Treasury minus 2Y Treasury yield",
                },
                "values": spread_list,
            }

    # Compute asof = max last date across ALL sources (assets, fx, cpi, rates)
    all_lasts = [v["meta"]["last"] for v in assets_out.values() if v.get("meta", {}).get("last")]
    # Include fx section lasts
    for pair_data in fx_out.values():
        if pair_data:
            all_lasts.append(pair_data[-1][0])
    # Include cpi section lasts
    for cpi_data in cpi_out.values():
        if cpi_data:
            all_lasts.append(cpi_data[-1][0])
    # Include rates section lasts
    for rate_data in rates_out.values():
        if isinstance(rate_data, dict) and rate_data.get("meta", {}).get("last"):
            all_lasts.append(rate_data["meta"]["last"])
    asof = max(all_lasts) if all_lasts else _date.today().strftime("%Y-%m")

    return {
        "asof":          asof,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "assets":        assets_out,
        "fx":            fx_out,
        "cpi":           cpi_out,
        "rates":         rates_out,
        "events":        _EVENTS,
    }


def write_output(output: dict[str, Any], path: Path | None = None) -> None:
    """Write the market_returns.json file atomically."""
    path = path or OUTPUT_PATH
    _write_json_atomic(path, _dumps(output, separators=(",", ":")))
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
    rates_count = len(output.get("rates", {}))
    fx_count = len(output.get("fx", {}))
    print(f"\nBuilt output: {asset_count} assets, {rates_count} rate series, {fx_count} FX pairs")

    write_output(output, output_path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
