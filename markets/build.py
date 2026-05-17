"""
markets/build.py — Build markets.json and sim_underlyings.json from data/markets/*.parquet.

Usage:
    python -m markets.build --source data/markets --output docs/data

Outputs:
    docs/data/markets.json        — Phase 3 PRICE LOG data for markets.html
    docs/data/sim_underlyings.json — Phase 4 price history for sim.html
"""
from __future__ import annotations
import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

from markets.fetch_yf import SYMBOLS as YF_SYMBOLS
from markets.fetch_fred import SERIES as FRED_SERIES
from markets.stats import series_stats, log_rows

if sys.platform.startswith("win"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except AttributeError:
        pass


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


def _dumps(obj, **kw) -> str:
    kw.setdefault("cls", _SafeEncoder)
    return json.dumps(obj, **kw)


# ── Series metadata ───────────────────────────────────────────────────────────
YF_META: dict[str, dict] = {
    "SPX":    {"label": "S&P 500 (SPX)",            "kind": "indices"},
    "NDX":    {"label": "Nasdaq 100 (NDX)",          "kind": "indices"},
    "DJI":    {"label": "Dow Jones (DJI)",           "kind": "indices"},
    "RUT":    {"label": "Russell 2000 (RUT)",        "kind": "indices"},
    "NIKKEI": {"label": "Nikkei 225 (NIKKEI)",      "kind": "indices"},
    "DAX":    {"label": "DAX (DAX)",                 "kind": "indices"},
    "FTSE":   {"label": "FTSE 100 (FTSE)",           "kind": "indices"},
    "SENSEX": {"label": "BSE Sensex (SENSEX)",       "kind": "indices"},
    "NIFTY":  {"label": "Nifty 50 (NIFTY)",         "kind": "indices"},
    "HSI":    {"label": "Hang Seng (HSI)",           "kind": "indices"},
    "GLD":    {"label": "Gold (GC=F)",               "kind": "commodities"},
    "SLV":    {"label": "Silver (SI=F)",             "kind": "commodities"},
    "PL":     {"label": "Platinum (PL=F)",           "kind": "commodities"},
    "PA":     {"label": "Palladium (PA=F)",          "kind": "commodities"},
    "CL":     {"label": "WTI Crude (CL=F)",         "kind": "commodities"},
    "BZ":     {"label": "Brent Crude (BZ=F)",       "kind": "commodities"},
    "NG":     {"label": "Natural Gas (NG=F)",        "kind": "commodities"},
    "HG":     {"label": "Copper (HG=F)",             "kind": "commodities"},
    "USDINR": {"label": "USD/INR (INR=X)",           "kind": "fx"},
    "USDSGD": {"label": "USD/SGD (SGD=X)",           "kind": "fx"},
    "EURUSD": {"label": "EUR/USD (EURUSD=X)",        "kind": "fx"},
    "BTC":    {"label": "Bitcoin (BTC-USD)",         "kind": "crypto"},
    "ETH":    {"label": "Ethereum (ETH-USD)",        "kind": "crypto"},
}

FRED_META: dict[str, dict] = {
    "VIXCLS":  {"label": "VIX (1M implied vol)",     "kind": "vol"},
    "VXNCLS":  {"label": "Nasdaq VIX",               "kind": "vol"},
    "VIX9D":   {"label": "VIX 9-day",                "kind": "vol"},
    "VIX3M":   {"label": "VIX 3-month",              "kind": "vol"},
    "VIX6M":   {"label": "VIX 6-month",              "kind": "vol"},
    "VVIX":    {"label": "VVIX (vol of vol)",         "kind": "vol"},
    "MOVE":    {"label": "MOVE Index (Treasury vol)", "kind": "vol"},
    "DGS10":   {"label": "10Y Treasury Yield",        "kind": "rates"},
    "DGS2":    {"label": "2Y Treasury Yield",         "kind": "rates"},
    "T10Y2Y":  {"label": "10Y–2Y Spread",             "kind": "rates"},
    "SOFR":    {"label": "SOFR",                      "kind": "rates"},
    "DFF":     {"label": "Effective Fed Funds Rate",  "kind": "rates"},
    "DEXINUS": {"label": "India–US FX (DEXINUS)",     "kind": "rates"},
    "DEXSIUS": {"label": "Singapore–US FX (DEXSIUS)", "kind": "rates"},
}

LEVERAGED_UNDERLYINGS = ["NVDA", "AAPL", "TSLA", "GOOG", "GOOGL", "MSFT", "AMZN", "META", "SPX", "NDX"]


def build(source: Path, output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    series_list = []

    # ── yfinance series ───────────────────────────────────────────────────────
    print("Processing yfinance parquets…")
    for sym, meta in YF_META.items():
        p = source / f"yf_{sym}.parquet"
        if not p.exists():
            print(f"  {sym}: not found, skipping")
            continue
        try:
            df = pd.read_parquet(p)
            st = series_stats(df, value_col="Close")
            rows = log_rows(df, value_col="Close", n=200)
            series_list.append({
                "symbol": sym,
                "label":  meta["label"],
                "kind":   meta["kind"],
                "stats":  st,
                "log":    rows,
            })
            print(f"  {sym}: {st.get('sessions_observed', 0)} sessions")
        except Exception as e:
            print(f"  {sym}: ERROR — {e}")

    # ── FRED series ───────────────────────────────────────────────────────────
    print("Processing FRED parquets…")
    for sid, meta in FRED_META.items():
        p = source / f"fred_{sid}.parquet"
        if not p.exists():
            print(f"  {sid}: not found, skipping")
            continue
        try:
            df = pd.read_parquet(p)
            # FRED uses "value" column
            df = df.rename(columns={"value": "Close"})
            st = series_stats(df, value_col="Close")
            rows = log_rows(df, value_col="Close", n=200)
            series_list.append({
                "symbol": sid,
                "label":  meta["label"],
                "kind":   meta["kind"],
                "stats":  st,
                "log":    rows,
            })
            print(f"  {sid}: {st.get('sessions_observed', 0)} sessions")
        except Exception as e:
            print(f"  {sid}: ERROR — {e}")

    # Sort by kind then symbol
    series_list.sort(key=lambda x: (x["kind"], x["symbol"]))

    markets_json = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "series": series_list,
    }

    out_path = output / "markets.json"
    text = _dumps(markets_json, separators=(",", ":"))
    out_path.write_text(text, encoding="utf-8")
    size_mb = len(text.encode()) / 1_048_576
    print(f"\n✓ markets.json: {len(series_list)} series · {size_mb:.2f} MB")

    # ── sim_underlyings.json (Phase 4) ────────────────────────────────────────
    print("\nBuilding sim_underlyings.json…")
    sim_inputs: dict = {}
    for sym in LEVERAGED_UNDERLYINGS:
        p = source / f"yf_{sym}.parquet"
        if not p.exists():
            print(f"  {sym}: not found, skipping")
            continue
        try:
            df = pd.read_parquet(p)[["Date", "Close"]].copy()
            df = df.dropna(subset=["Close"])
            df = df.tail(2520)   # last 10 years (~252 trading days/year × 10)
            sim_inputs[sym] = [
                {"d": str(d)[:10], "c": round(float(c), 4)}
                for d, c in zip(df["Date"], df["Close"])
            ]
            print(f"  {sym}: {len(sim_inputs[sym])} rows")
        except Exception as e:
            print(f"  {sym}: ERROR — {e}")

    sim_out = output / "sim_underlyings.json"
    sim_out.write_text(_dumps(sim_inputs, separators=(",", ":")), encoding="utf-8")
    sim_size_kb = sim_out.stat().st_size / 1024
    print(f"✓ sim_underlyings.json: {len(sim_inputs)} underlyings · {sim_size_kb:.0f} KB")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="Build markets.json and sim_underlyings.json")
    ap.add_argument("--source", default="data/markets", help="Directory containing parquet files")
    ap.add_argument("--output", default="docs/data",   help="Output directory for JSON files")
    args = ap.parse_args(argv)
    build(Path(args.source), Path(args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
