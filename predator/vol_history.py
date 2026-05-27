"""
Vol History — daily close data for 7 CBOE volatility indices from FRED.

Fetches daily close data and writes docs/data/vol_history.json.

Usage:
    python -m predator.vol_history
    python -m predator.vol_history --full-refresh
    python -m predator.vol_history --dry-run
    python -m predator.vol_history --validate-only
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


# ─── Series Registry ─────────────────────────────────────────────────────────

VOL_SERIES: list[dict[str, str]] = [
    {
        "key": "vix",
        "name": "VIX",
        "description": "CBOE S&P 500 Volatility",
        "series_id": "VIXCLS",
        "first": "1990-01-02",
    },
    {
        "key": "gvz",
        "name": "GVZ",
        "description": "CBOE Gold ETF Volatility",
        "series_id": "GVZCLS",
        "first": "2008-06-03",
    },
    {
        "key": "ovx",
        "name": "OVX",
        "description": "CBOE Crude Oil ETF Volatility",
        "series_id": "OVXCLS",
        "first": "2007-05-10",
    },
    {
        "key": "vxn",
        "name": "VXN",
        "description": "CBOE Nasdaq 100 Volatility",
        "series_id": "VXNCLS",
        "first": "2001-01-02",
    },
    {
        "key": "rvx",
        "name": "RVX",
        "description": "CBOE Russell 2000 Volatility",
        "series_id": "RVXCLS",
        "first": "2004-01-02",
    },
    {
        "key": "vxd",
        "name": "VXD",
        "description": "CBOE DJIA Volatility",
        "series_id": "VXDCLS",
        "first": "2005-01-03",
    },
    {
        "key": "vxeem",
        "name": "VXEEM",
        "description": "CBOE EM ETF Volatility",
        "series_id": "VXEEMCLS",
        "first": "2011-03-16",
    },
]

# Cache directory for parquet files (one per series)
CACHE_DIR = Path(__file__).resolve().parent.parent / "data" / "vol_history"
# Output path
OUTPUT_PATH = Path(__file__).resolve().parent.parent / "docs" / "data" / "vol_history.json"
# Trailing days to refetch on incremental runs
INCREMENTAL_TRAILING_DAYS = 30


# ─── FRED Client ─────────────────────────────────────────────────────────────

def _get_fred_client():
    """Initialise fredapi.Fred with API key from env. Returns None on failure."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # python-dotenv optional if env var already set

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


# ─── FRED Fetch (daily, no frequency param) ──────────────────────────────────

def _fetch_fred_daily(
    fred,
    series_id: str,
    full_refresh: bool = False,
    existing: pd.Series | None = None,
) -> pd.Series:
    """
    Fetch a FRED daily series (no frequency/aggregation params — daily is default).

    Uses exponential backoff on 429 (rate limit) errors.
    On incremental runs, only fetches trailing 30 days and merges with cache.
    """
    kwargs: dict[str, Any] = {}

    # Incremental: restrict observation_start to trailing window
    if not full_refresh and existing is not None and not existing.empty:
        last_date = existing.index.max()
        start = last_date - pd.DateOffset(days=INCREMENTAL_TRAILING_DAYS)
        kwargs["observation_start"] = start.strftime("%Y-%m-%d")

    max_retries = 5
    for attempt in range(max_retries + 1):
        try:
            print(f"    Fetching {series_id} from FRED (attempt {attempt + 1})…", flush=True)
            data = fred.get_series(series_id, **kwargs)
            break
        except Exception as e:
            err_str = str(e)
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

    # Drop NaN (FRED publishes "." as NaN for missing observations)
    data = data.dropna()

    # Merge with existing if incremental
    if not full_refresh and existing is not None and not existing.empty:
        combined = pd.concat([existing, data])
        combined = combined[~combined.index.duplicated(keep="last")]
        return combined.sort_index()

    return data


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
            return s.dropna()
        # Fallback: if index is already datetime and value is the only column
        if df.shape[1] == 1:
            s = df.iloc[:, 0]
            s.index = pd.to_datetime(df.index)
            return s.dropna()
        return None
    except Exception as e:
        print(f"    WARNING: cache read failed for {key}: {e}")
        return None


def _write_cache(key: str, data: pd.Series) -> None:
    """Write series to parquet cache."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame({"value": data})
    df.to_parquet(CACHE_DIR / f"{key}.parquet")


# ─── Series → daily dict ─────────────────────────────────────────────────────

def _series_to_daily_dict(data: pd.Series) -> dict[str, float]:
    """
    Convert a pandas Series with datetime index to {YYYY-MM-DD: value} dict.

    Rounds values to 2 decimal places. Drops NaN entries.
    """
    if data.empty:
        return {}
    data = data.dropna()
    out = {}
    for dt, val in data.items():
        key = pd.Timestamp(dt).strftime("%Y-%m-%d")
        out[key] = round(float(val), 2)
    return out


def _series_to_monthly_avg_dict(data: pd.Series) -> dict[str, float]:
    """
    Compute monthly averages from daily data.

    Returns {YYYY-MM: avg_value} dict, rounded to 2 decimal places.
    """
    if data.empty:
        return {}
    data = data.dropna()
    # Group by YYYY-MM
    monthly = data.groupby(data.index.to_period("M")).mean()
    out = {}
    for period, val in monthly.items():
        out[str(period)] = round(float(val), 2)
    return out


# ─── Main Fetch Logic ─────────────────────────────────────────────────────────

def fetch_all(
    full_refresh: bool = False,
    dry_run: bool = False,
) -> dict[str, pd.Series]:
    """
    Fetch all 7 vol series from FRED.

    Args:
        full_refresh: If True, ignore cache and fetch full history.
        dry_run: If True, print the plan and return empty dict.

    Returns:
        Dict mapping series key → pandas Series of daily close values.
    """
    mode = "FULL REFRESH" if full_refresh else f"INCREMENTAL (trailing {INCREMENTAL_TRAILING_DAYS} days)"

    print(f"\nVol History Fetch Plan ({mode}):")
    print(f"  FRED series: {len(VOL_SERIES)}")
    for s in VOL_SERIES:
        print(f"    {s['key']:8s} → {s['series_id']} (from {s['first']})")

    if dry_run:
        print("\n  --dry-run: exiting without fetching.")
        return {}

    fred = _get_fred_client()
    if fred is None:
        print("\n  Cannot proceed without FRED API key — returning empty results (soft-skip).")
        return {}

    results: dict[str, pd.Series] = {}

    for i, spec in enumerate(VOL_SERIES):
        key = spec["key"]
        series_id = spec["series_id"]

        # Load existing cache for incremental
        existing = None if full_refresh else _read_cache(key)
        cached_points = len(existing) if existing is not None else 0

        print(f"\n  [{i + 1}/{len(VOL_SERIES)}] {key} (fred:{series_id})"
              f" — cached: {cached_points} pts")

        data = _fetch_fred_daily(fred, series_id, full_refresh, existing)

        if data is not None and not data.empty:
            _write_cache(key, data)
            results[key] = data
            first_dt = pd.Timestamp(data.index.min()).strftime("%Y-%m-%d")
            last_dt = pd.Timestamp(data.index.max()).strftime("%Y-%m-%d")
            print(f"    OK: {len(data)} data points ({first_dt} → {last_dt})")
        else:
            print(f"    WARNING: no data returned for {series_id}")
            # Fall back to cache
            if existing is not None and not existing.empty:
                results[key] = existing
                print(f"    Using cached data: {len(existing)} pts")

        # Rate-limit pause between sequential fetches
        if i < len(VOL_SERIES) - 1:
            time.sleep(0.1)

    return results


# ─── Validation ──────────────────────────────────────────────────────────────

def validate(results: dict[str, pd.Series] | None = None) -> bool:
    """
    Validate fetched data (or existing JSON). Returns True if all checks pass.

    Checks:
        - Each series is non-empty
        - VIX has data back to at least 1990
        - GVZ has data back to at least 2008
        - All 7 series populated
    """
    print("\nValidation:")
    all_ok = True

    # Load from cache if results not provided
    if results is None:
        results = {}
        for spec in VOL_SERIES:
            cached = _read_cache(spec["key"])
            if cached is not None:
                results[spec["key"]] = cached

    for spec in VOL_SERIES:
        key = spec["key"]
        data = results.get(key)

        if data is None or data.empty:
            print(f"  FAIL: {key} — no data")
            all_ok = False
            continue

        first_dt = pd.Timestamp(data.index.min()).strftime("%Y-%m-%d")
        last_dt = pd.Timestamp(data.index.max()).strftime("%Y-%m-%d")
        print(f"  OK:   {key} — {len(data)} pts ({first_dt} → {last_dt})")

        # Spot-check first dates
        if key == "vix" and first_dt > "1991-01-01":
            print(f"  WARN: VIX first date {first_dt} is later than expected 1990-01-02")
        if key == "gvz" and first_dt > "2009-01-01":
            print(f"  WARN: GVZ first date {first_dt} is later than expected 2008-06-03")

    if all_ok:
        print("\n  All checks passed.")
    else:
        print("\n  Some checks FAILED.")

    return all_ok


# ─── Build Output JSON ────────────────────────────────────────────────────────

def build_output(results: dict[str, pd.Series]) -> dict[str, Any]:
    """
    Build the vol_history.json structure from fetched data.

    Returns the full JSON-serialisable dict.
    """
    output: dict[str, Any] = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "series": {},
    }

    for spec in VOL_SERIES:
        key = spec["key"]
        data = results.get(key)
        if data is None or data.empty:
            # Try loading from cache
            data = _read_cache(key)
        if data is None or data.empty:
            print(f"  WARNING: no data for {key}, skipping")
            continue

        daily = _series_to_daily_dict(data)
        monthly_avg = _series_to_monthly_avg_dict(data)

        if not daily:
            continue

        sorted_days = sorted(daily.keys())
        output["series"][key] = {
            "meta": {
                "name": spec["name"],
                "description": spec["description"],
                "source": f"fred:{spec['series_id']}",
                "first": sorted_days[0],
            },
            "daily": daily,
            "monthly_avg": monthly_avg,
        }

    return output


# ─── Entry Point ─────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch CBOE volatility index history from FRED and write vol_history.json"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Fetch full history for all series (default: trailing 30 days only)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print fetch plan without writing any files",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Read existing cache, run spot-check assertions, exit 0/1",
    )
    args = parser.parse_args()

    if args.validate_only:
        ok = validate()
        sys.exit(0 if ok else 1)

    # Fetch data
    results = fetch_all(
        full_refresh=args.full_refresh,
        dry_run=args.dry_run,
    )

    if args.dry_run:
        return

    if not results:
        print("\nNo data fetched. Exiting.")
        sys.exit(1)

    # Build output
    print("\nBuilding vol_history.json…")
    output = build_output(results)

    n_series = len(output.get("series", {}))
    total_daily = sum(len(s.get("daily", {})) for s in output["series"].values())
    print(f"  {n_series} series, {total_daily:,} total daily data points")

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(OUTPUT_PATH, _dumps(output, indent=2, sort_keys=False))

    size_kb = OUTPUT_PATH.stat().st_size / 1024
    print(f"\n  Written: {OUTPUT_PATH} ({size_kb:.1f} KB)")

    # Run validation
    validate(results)


if __name__ == "__main__":
    main()
