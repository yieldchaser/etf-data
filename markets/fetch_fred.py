"""
markets/fetch_fred.py — Incremental FRED data fetcher.

Requires FRED_API_KEY environment variable. Exits with code 1 if not set.

Usage:
    FRED_API_KEY=xxx python -m markets.fetch_fred --out data/markets
    FRED_API_KEY=xxx python -m markets.fetch_fred --out data/markets --only VIXCLS
"""
from __future__ import annotations
import argparse
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
import pandas as pd

from .metadata import FRED_SERIES

# Re-exported alias for backwards compatibility
SERIES: dict[str, str] = FRED_SERIES

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def _fetch_json_with_retry(url: str, max_attempts: int = 3) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    last_exc: Exception | None = None
    for attempt in range(max_attempts):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if exc.code not in (429, 500, 502, 503, 504) or attempt == max_attempts - 1:
                raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
            last_exc = exc
            if attempt == max_attempts - 1:
                raise
        time.sleep(2 ** attempt)
    assert last_exc is not None
    raise last_exc


def fetch_one(series_id: str, api_key: str, out_dir: Path) -> None:
    """Incrementally fetch a FRED series, appending new rows to the existing parquet."""
    target = out_dir / f"fred_{series_id}.parquet"
    existing = pd.DataFrame()
    start_date = "1990-01-01"

    if target.exists():
        existing = pd.read_parquet(target)
        if not existing.empty and "Date" in existing.columns:
            last = pd.to_datetime(existing["Date"], errors="coerce").dropna().max()
            if not pd.isna(last):
                start_date = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    url = (
        f"{FRED_BASE}?series_id={series_id}&api_key={api_key}"
        f"&file_type=json&observation_start={start_date}"
    )
    payload = _fetch_json_with_retry(url)

    obs = payload.get("observations", [])
    if not obs:
        print(f"    no observations returned")
        return

    rows = []
    for o in obs:
        v = o.get("value", ".")
        if v == ".":
            continue  # FRED uses "." for missing values
        try:
            val = float(v)
            if not math.isfinite(val):
                continue
            rows.append({"Date": o["date"], "value": val})
        except (TypeError, ValueError, KeyError):
            continue

    if not rows:
        print(f"    all observations were missing ('.'); nothing to write")
        return

    new_df = pd.DataFrame(rows)
    if not existing.empty:
        combined = pd.concat([existing, new_df]).drop_duplicates(subset=["Date"], keep="last")
    else:
        combined = new_df
    combined = combined.sort_values("Date").reset_index(drop=True)
    # Atomic write: write to .tmp then os.replace — an interrupted write
    # leaves the previous good file intact rather than a truncated parquet.
    tmp = target.with_suffix(target.suffix + ".tmp")
    combined.to_parquet(tmp, index=False)
    os.replace(tmp, target)
    added = len(new_df)
    print(f"    → {len(combined)} total rows (+{added} new)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Fetch FRED series data")
    ap.add_argument("--out", default="data/markets")
    ap.add_argument("--only", nargs="*", default=None, help="Restrict to these series IDs")
    args = ap.parse_args()

    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        print("WARNING: FRED_API_KEY not set — skipping FRED fetch", file=sys.stderr)
        sys.exit(1)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    series = {k: v for k, v in SERIES.items() if args.only is None or k in args.only}
    print(f"Fetching {len(series)} FRED series into {out_dir}/")
    failed = 0
    for sid, desc in series.items():
        print(f"  {sid} ({desc})…")
        try:
            fetch_one(sid, api_key, out_dir)
        except Exception as e:
            failed += 1
            print(f"    ERROR: {e}")

    print(f"Done. ({len(series) - failed}/{len(series)} succeeded)")
    if series and failed == len(series):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
