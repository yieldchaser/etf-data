"""
markets/fetch_fred.py — Incremental FRED data fetcher.

Requires FRED_API_KEY environment variable. Silently skips if not set.

Usage:
    FRED_API_KEY=xxx python -m markets.fetch_fred --out data/markets
    FRED_API_KEY=xxx python -m markets.fetch_fred --out data/markets --only VIXCLS
"""
from __future__ import annotations
import argparse
import os
import sys
from pathlib import Path
import urllib.request
import json
import pandas as pd


SERIES: dict[str, str] = {
    "VIXCLS": "CBOE VIX (1-month implied vol)",
    "VXNCLS": "CBOE Nasdaq VIX",
    "VIX9D":  "9-day VIX",
    "VIX3M":  "3-month VIX",
    "VIX6M":  "6-month VIX",
    "VVIX":   "VIX of VIX (vol of vol)",
    "MOVE":   "ICE BofA MOVE (treasury vol)",
    "DGS10":  "10Y Treasury constant maturity rate",
    "DGS2":   "2Y Treasury",
    "T10Y2Y": "10Y–2Y spread",
    "SOFR":   "Secured Overnight Financing Rate",
    "DFF":    "Effective Fed Funds Rate",
    "DEXINUS": "India–US exchange rate",
    "DEXSIUS": "Singapore–US exchange rate",
}

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"


def fetch_one(series_id: str, api_key: str, out_dir: Path) -> None:
    """Incrementally fetch a FRED series, appending new rows to the existing parquet."""
    target = out_dir / f"fred_{series_id}.parquet"
    existing = pd.DataFrame()
    start_date = "1990-01-01"

    if target.exists():
        existing = pd.read_parquet(target)
        if not existing.empty:
            last = pd.to_datetime(existing["Date"]).max()
            start_date = (last + pd.Timedelta(days=1)).strftime("%Y-%m-%d")

    url = (
        f"{FRED_BASE}?series_id={series_id}&api_key={api_key}"
        f"&file_type=json&observation_start={start_date}"
    )
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read())

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
            rows.append({"Date": o["date"], "value": float(v)})
        except (ValueError, KeyError):
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
    combined.to_parquet(target, index=False)
    added = len(new_df)
    print(f"    → {len(combined)} total rows (+{added} new)")


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch FRED series data")
    ap.add_argument("--out", default="data/markets")
    ap.add_argument("--only", nargs="*", default=None, help="Restrict to these series IDs")
    args = ap.parse_args()

    api_key = os.environ.get("FRED_API_KEY", "")
    if not api_key:
        print("WARNING: FRED_API_KEY not set — skipping FRED fetch", file=sys.stderr)
        return

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    series = {k: v for k, v in SERIES.items() if args.only is None or k in args.only}
    print(f"Fetching {len(series)} FRED series into {out_dir}/")
    for sid, desc in series.items():
        print(f"  {sid} ({desc})…")
        try:
            fetch_one(sid, api_key, out_dir)
        except Exception as e:
            print(f"    ERROR: {e}")

    print("Done.")


if __name__ == "__main__":
    main()
